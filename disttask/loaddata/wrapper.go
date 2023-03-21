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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/util/logutil"
)

func getStore(ctx context.Context, dir string) (storage.ExternalStorage, error) {
	b, err := storage.ParseBackend(dir, nil)
	if err != nil {
		return nil, err
	}
	opt := &storage.ExternalStorageOptions{
		NoCredentials: true,
	}
	return storage.New(ctx, b, opt)
}

func createColumnPermutation(task MinimalTask) ([]int, error) {
	var ignoreColumns map[string]struct{}
	return common.CreateColumnPermutation(task.Table.TargetColumns, ignoreColumns, task.Table.Info, log.Logger{Logger: logutil.BgLogger()})
}

func buildParser(ctx context.Context, task MinimalTask) (mydump.Parser, error) {
	store, err := getStore(ctx, task.Dir)
	if err != nil {
		return nil, err
	}
	sourceType, err := transformSourceType(task.Format.Type)
	if err != nil {
		return nil, err
	}
	permutation, err := createColumnPermutation(task)
	if err != nil {
		return nil, err
	}

	cfg := &config.Config{
		Mydumper: config.MydumperRuntime{
			ReadBlockSize:          config.ReadBlockSize,
			CSV:                    *task.Format.CSV.Config,
			DataCharacterSet:       task.Format.DataCharacterSet,
			DataInvalidCharReplace: task.Format.DataInvalidCharReplace,
		},
		TiDB: config.DBStore{
			SQLMode: task.Format.SQLDump.SQLMode,
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

	parser, err := mydump.BuildParser(ctx, cfg, fileMeta, chunk, permutation, nil, store, task.Table.Info)
	if err != nil {
		return nil, err
	}
	parser.SetLogger(log.Logger{Logger: logutil.BgLogger()})
	return parser, nil
}

func buildEncoder(ctx context.Context, task MinimalTask) (kv.Encoder, error) {
	idAlloc := kv.NewPanickingAllocators(task.Chunk.PrevRowIDMax)
	tbl, err := tables.TableFromMeta(idAlloc, task.Table.Info)
	if err != nil {
		return nil, err
	}
	return kv.NewTableKVEncoder(tbl, &kv.SessionOptions{}, nil, log.Logger{Logger: logutil.BgLogger()})
}

func makeTableRegions(ctx context.Context, task *Task) ([]*mydump.TableRegion, error) {
	b, err := storage.ParseBackend(task.Dir, nil)
	if err != nil {
		return nil, err
	}
	opt := &storage.ExternalStorageOptions{
		NoCredentials: true,
	}
	store, err := storage.New(ctx, b, opt)
	if err != nil {
		return nil, err
	}

	meta := &mydump.MDTableMeta{
		DB:   task.Table.DBName,
		Name: task.Table.Info.Name.String(),
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
			RegionConcurrency: 2,
			TableConcurrency:  3,
		},
		Mydumper: config.MydumperRuntime{
			StrictFormat:     task.Format.CSV.Strict,
			MaxRegionSize:    config.MaxRegionSize,
			BatchImportRatio: 0,
		},
	}

	return mydump.MakeTableRegions(ctx, meta, len(task.Table.Info.Columns), cfg, nil, store)
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
		return mydump.SourceTypeIgnore, errors.Errorf("unknown source type %s", tp)
	}
}
