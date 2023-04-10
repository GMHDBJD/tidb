// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package loaddata

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/intest"
	"github.com/pingcap/tidb/util/logutil"
)

func getStore(ctx context.Context, dir string) (storage.ExternalStorage, error) {
	b, err := storage.ParseBackend(dir, nil)
	if err != nil {
		return nil, err
	}

	opt := &storage.ExternalStorageOptions{}
	if intest.InTest {
		opt.NoCredentials = true
	}
	return storage.New(ctx, b, opt)
}

func makeTableRegions(ctx context.Context, task *TaskMeta, concurrency int) ([]*mydump.TableRegion, error) {
	if concurrency <= 0 {
		return nil, errors.Errorf("concurrency must be greater than 0, but got %d", concurrency)
	}

	store, err := getStore(ctx, task.Dir)
	if err != nil {
		return nil, err
	}

	meta := &mydump.MDTableMeta{
		DB:           task.Table.DBName,
		Name:         task.Table.Info.Name.String(),
		IsRowOrdered: task.Table.IsRowOrdered,
	}

	sourceType, err := transformSourceType(task.Format.Type)
	if err != nil {
		return nil, err
	}
	for _, file := range task.FileInfos {
		meta.DataFiles = append(meta.DataFiles, mydump.FileInfo{
			FileMeta: mydump.SourceFileMeta{
				Path:        file.Path,
				Type:        sourceType,
				FileSize:    file.Size,
				RealSize:    file.RealSize,
				Compression: task.Format.Compression,
			},
		})
	}
	cfg := &config.Config{
		App: config.Lightning{
			RegionConcurrency: concurrency,
			TableConcurrency:  concurrency,
		},
		Mydumper: config.MydumperRuntime{
			CSV:           task.Format.CSV.Config,
			StrictFormat:  task.Format.CSV.Strict,
			MaxRegionSize: config.MaxRegionSize,
			ReadBlockSize: config.ReadBlockSize,
			// uniform distribution
			BatchImportRatio: 0,
		},
	}

	dataDivideConfig := mydump.NewDataDivideConfig(cfg, len(task.Table.TargetColumns), nil, store, meta)
	return mydump.MakeTableRegions(ctx, dataDivideConfig)
}

func rebaseRowID(rowIDBase int64, tableRegions []*mydump.TableRegion) {
	if rowIDBase == 0 {
		return
	}
	for _, region := range tableRegions {
		region.Chunk.PrevRowIDMax += rowIDBase
		region.Chunk.RowIDMax += rowIDBase
	}
}

func transformSourceType(tp string) (mydump.SourceType, error) {
	switch tp {
	case importer.LoadDataFormatParquet:
		return mydump.SourceTypeParquet, nil
	case importer.LoadDataFormatDelimitedData:
		return mydump.SourceTypeCSV, nil
	case importer.LoadDataFormatSQLDump:
		return mydump.SourceTypeSQL, nil
	default:
		return mydump.SourceTypeIgnore, errors.Errorf("unknown source type: %s", tp)
	}
}

// TODO: merge the same logic in table_importer, load data and lightning.
func buildParser(ctx context.Context, task *MinimalTaskMeta) (mydump.Parser, error) {
	store, err := getStore(ctx, task.Dir)
	if err != nil {
		return nil, err
	}
	sourceType, err := transformSourceType(task.Format.Type)
	if err != nil {
		return nil, err
	}

	cfg := &config.Config{
		Mydumper: config.MydumperRuntime{
			ReadBlockSize:          config.ReadBlockSize,
			CSV:                    task.Format.CSV.Config,
			DataCharacterSet:       task.Format.DataCharacterSet,
			DataInvalidCharReplace: task.Format.DataInvalidCharReplace,
		},
		TiDB: config.DBStore{
			SQLMode: task.SessionVars.SQLMode,
		},
	}
	fileMeta := mydump.SourceFileMeta{
		Type: sourceType,
		Path: task.Chunk.Path,
		// only use for parquet
		FileSize:    task.Chunk.EndOffset,
		Compression: task.Format.Compression,
	}
	chunk := mydump.Chunk{
		Offset:       task.Chunk.Offset,
		PrevRowIDMax: task.Chunk.PrevRowIDMax,
	}

	parser, err := mydump.BuildParser(ctx, cfg, fileMeta, chunk, nil, store)
	if err != nil {
		return nil, err
	}
	parser.SetLogger(log.Logger{Logger: logutil.BgLogger()})
	return parser, nil
}

func buildEncoder(task *MinimalTaskMeta) (importer.KvEncoder, error) {
	idAlloc := kv.NewPanickingAllocators(task.Chunk.PrevRowIDMax)
	tbl, err := tables.TableFromMeta(idAlloc, task.Table.Info)
	if err != nil {
		return nil, err
	}
	cfg := &encode.EncodingConfig{
		SessionOptions: encode.SessionOptions{
			SQLMode:        task.SessionVars.SQLMode,
			Timestamp:      task.Mode.Physical.Timestamp,
			SysVars:        task.SessionVars.SysVars,
			AutoRandomSeed: task.Chunk.PrevRowIDMax,
		},
		Path:   task.Chunk.Path,
		Table:  tbl,
		Logger: log.Logger{Logger: logutil.BgLogger()},
	}

	// parse stmt to load data stmt
	stmt, err := parser.New().ParseOneStmt(task.Stmt, "", "")
	if err != nil {
		return nil, err
	}
	loadDataStmt, ok := stmt.(*ast.LoadDataStmt)
	if !ok {
		return nil, errors.Errorf("stmt %s is not load data stmt", task.Stmt)
	}

	fieldMappings, columns := importer.GenerateFieldMappings(loadDataStmt.ColumnsAndUserVars, loadDataStmt.ColumnAssignments, tbl)
	insertColumns, err := importer.GenerateInsertColumns(columns, loadDataStmt.ColumnAssignments, tbl)
	if err != nil {
		return nil, err
	}
	return importer.NewTableKVEncoder(cfg, loadDataStmt.ColumnAssignments, loadDataStmt.ColumnsAndUserVars, fieldMappings, insertColumns)
}

func lightningSortedKVDir(jobID int64) string {
	return fmt.Sprintf("import_%d", jobID)
}

func genBackendConfig(taskMeta *TaskMeta) (*ingest.Config, error) {
	sortedKVDir := lightningSortedKVDir(taskMeta.JobID)
	return ingest.GenConfig(ingest.WithSortedKVDir(sortedKVDir))
}

// TODO: merge the same logic in ingest and table_importer
// TODO: add MemRoot and DiskQuota
func createLocalBackend(ctx context.Context, taskMeta *TaskMeta) (*backend.Backend, error) {
	backendCfg, err := genBackendConfig(taskMeta)
	if err != nil {
		return nil, err
	}

	backend, err := ingest.CreateLocalBackend(ctx, backendCfg)
	if err != nil {
		return nil, err
	}
	return &backend, nil
}

func genEngineCfg(taskMeta *TaskMeta) *backend.EngineConfig {
	return ingest.GenerateLocalEngineConfig(taskMeta.JobID, taskMeta.Table.DBName, taskMeta.Table.Info.Name.String())
}

func openEngine(ctx context.Context, taskMeta *TaskMeta, engineID int32, backend *backend.Backend) (*backend.OpenedEngine, error) {
	cfg := genEngineCfg(taskMeta)
	return backend.OpenEngine(ctx, cfg, taskMeta.Table.Info.Name.String(), engineID)
}

func importAndCleanupEngine(ctx context.Context, engine *backend.OpenedEngine) error {
	closedEngine, err := engine.Close(ctx)
	if err != nil {
		return err
	}
	if err := closedEngine.Import(ctx, int64(config.SplitRegionSize), int64(config.SplitRegionKeys)); err != nil {
		return err
	}
	return closedEngine.Cleanup(ctx)
}
