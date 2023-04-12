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
	"math"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/keyspace"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/size"
	"github.com/stretchr/testify/require"
)

func TestTransformSourceType(t *testing.T) {
	testCases := []struct {
		tp       string
		expected mydump.SourceType
	}{
		{
			tp:       importer.LoadDataFormatParquet,
			expected: mydump.SourceTypeParquet,
		},
		{
			tp:       importer.LoadDataFormatSQLDump,
			expected: mydump.SourceTypeSQL,
		},
		{
			tp:       importer.LoadDataFormatDelimitedData,
			expected: mydump.SourceTypeCSV,
		},
	}
	for _, tc := range testCases {
		expected, err := transformSourceType(tc.tp)
		require.NoError(t, err)
		require.Equal(t, tc.expected, expected)
	}
	expected, err := transformSourceType("unknown")
	require.EqualError(t, err, "unknown source type: unknown")
	require.Equal(t, mydump.SourceTypeIgnore, expected)
}

func TestMakeTableRegions(t *testing.T) {
	regions, err := makeTableRegions(context.Background(), &TaskMeta{}, 0, 0)
	require.EqualError(t, err, "concurrency must be greater than 0, but got 0")
	require.Nil(t, regions)

	regions, err = makeTableRegions(context.Background(), &TaskMeta{}, 1, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty store is not allowed")
	require.Nil(t, regions)

	task := &TaskMeta{
		Table: Table{
			Info: &model.TableInfo{
				Name: model.NewCIStr("test"),
			},
		},
		Dir: "/tmp/test",
	}
	regions, err = makeTableRegions(context.Background(), task, 1, 0)
	require.EqualError(t, err, "unknown source type: ")
	require.Nil(t, regions)

	// parquet
	dir := "testdata"
	filename := "000000_0.parquet"
	task = &TaskMeta{
		Table: Table{
			Info: &model.TableInfo{
				Name: model.NewCIStr("test"),
			},
			TargetColumns: []string{"a", "b"},
		},
		Format: Format{
			Type: importer.LoadDataFormatParquet,
		},
		Dir: dir,
		FileInfos: []FileInfo{
			{
				Path: filename,
			},
		},
	}
	regions, err = makeTableRegions(context.Background(), task, 1, 0)
	require.NoError(t, err)
	require.Len(t, regions, 1)
	require.Equal(t, regions[0].EngineID, int32(0))
	require.Equal(t, regions[0].Chunk.Offset, int64(0))
	require.Equal(t, regions[0].Chunk.EndOffset, int64(5))
	require.Equal(t, regions[0].Chunk.PrevRowIDMax, int64(0))
	require.Equal(t, regions[0].Chunk.RowIDMax, int64(5))

	// large csv
	originRegionSize := config.MaxRegionSize
	config.MaxRegionSize = 5
	originBatchSize := config.DefaultBatchSize
	config.DefaultBatchSize = 12
	defer func() {
		config.MaxRegionSize = originRegionSize
		config.DefaultBatchSize = originBatchSize
	}()
	filename = "split_large_file.csv"
	dataFileInfo, err := os.Stat(filepath.Join(dir, filename))
	require.NoError(t, err)
	task = &TaskMeta{
		Table: Table{
			Info: &model.TableInfo{
				Name: model.NewCIStr("test"),
			},
			TargetColumns: []string{"a", "b", "c"},
		},
		Format: Format{
			Type: importer.LoadDataFormatDelimitedData,
			CSV: CSV{
				Config: config.CSVConfig{
					Separator:         ",",
					Delimiter:         "",
					Header:            true,
					HeaderSchemaMatch: true,
					TrimLastSep:       false,
					NotNull:           false,
					Null:              []string{"NULL"},
					EscapedBy:         `\`,
				},
				Strict: true,
			},
		},
		Dir: dir,
		FileInfos: []FileInfo{
			{
				Path:     filename,
				Size:     dataFileInfo.Size(),
				RealSize: dataFileInfo.Size(),
			},
		},
	}
	regions, err = makeTableRegions(context.Background(), task, 1, 0)
	require.NoError(t, err)
	require.Len(t, regions, 4)
	chunks := []Chunk{{Offset: 6, EndOffset: 12}, {Offset: 12, EndOffset: 18}, {Offset: 18, EndOffset: 24}, {Offset: 24, EndOffset: 30}}
	for i, region := range regions {
		require.Equal(t, region.EngineID, int32(i/2))
		require.Equal(t, region.Chunk.Offset, chunks[i].Offset)
		require.Equal(t, region.Chunk.EndOffset, chunks[i].EndOffset)
		require.Equal(t, region.Chunk.RealOffset, int64(0))
		require.Equal(t, region.Chunk.PrevRowIDMax, int64(i))
		require.Equal(t, region.Chunk.RowIDMax, int64(i+1))
	}

	// compression
	filename = "split_large_file.csv.zst"
	dataFileInfo, err = os.Stat(filepath.Join(dir, filename))
	require.NoError(t, err)
	task.FileInfos[0].Path = filename
	task.FileInfos[0].Size = dataFileInfo.Size()
	task.Format.Compression = mydump.CompressionZStd
	regions, err = makeTableRegions(context.Background(), task, 1, 0)
	require.NoError(t, err)
	require.Len(t, regions, 1)
	require.Equal(t, regions[0].EngineID, int32(0))
	require.Equal(t, regions[0].Chunk.Offset, int64(0))
	require.Equal(t, regions[0].Chunk.EndOffset, mydump.TableFileSizeINF)
	require.Equal(t, regions[0].Chunk.RealOffset, int64(0))
	require.Equal(t, regions[0].Chunk.PrevRowIDMax, int64(0))
	require.Equal(t, regions[0].Chunk.RowIDMax, int64(50))
}

func TestRebaseRowID(t *testing.T) {
	regions := []*mydump.TableRegion{
		{
			Chunk: mydump.Chunk{
				PrevRowIDMax: 1,
				RowIDMax:     2,
			},
		},
		{
			Chunk: mydump.Chunk{
				PrevRowIDMax: 2,
				RowIDMax:     3,
			},
		},
	}
	rebaseRowID(0, regions)
	require.Equal(t, regions[0].Chunk.PrevRowIDMax, int64(1))
	require.Equal(t, regions[0].Chunk.RowIDMax, int64(2))
	require.Equal(t, regions[1].Chunk.PrevRowIDMax, int64(2))
	require.Equal(t, regions[1].Chunk.RowIDMax, int64(3))

	rebaseRowID(1, regions)
	require.Equal(t, regions[0].Chunk.PrevRowIDMax, int64(2))
	require.Equal(t, regions[0].Chunk.RowIDMax, int64(3))
	require.Equal(t, regions[1].Chunk.PrevRowIDMax, int64(3))
	require.Equal(t, regions[1].Chunk.RowIDMax, int64(4))
}

func TestBuildParser(t *testing.T) {
	// csv
	dir := "testdata"
	filename := "split_large_file.csv"
	sqlMode, err := mysql.GetSQLMode(mysql.DefaultSQLMode)
	require.NoError(t, err)
	task := &MinimalTaskMeta{
		Format: Format{
			Type: importer.LoadDataFormatDelimitedData,
			CSV: CSV{
				Config: config.CSVConfig{
					Separator:         ",",
					Delimiter:         "",
					Header:            true,
					HeaderSchemaMatch: true,
					TrimLastSep:       false,
					NotNull:           false,
					Null:              []string{"NULL"},
					EscapedBy:         `\`,
				},
			},
			DataCharacterSet:       "",
			DataInvalidCharReplace: "",
		},
		SessionVars: SessionVars{
			SQLMode: sqlMode,
		},
		Dir: dir,
		Chunk: Chunk{
			Path:         filename,
			Offset:       0,
			PrevRowIDMax: 0,
		},
	}
	parser, err := buildParser(context.Background(), task)
	require.NoError(t, err)
	require.NoError(t, parser.ReadRow())
}

func TestBuildEncoder(t *testing.T) {
	sqlMode, err := mysql.GetSQLMode(mysql.DefaultSQLMode)
	require.NoError(t, err)
	filename := "split_large_file.csv"
	c1 := &model.ColumnInfo{ID: 1, Name: model.NewCIStr("column1"), State: model.StatePublic, Offset: 0, FieldType: *types.NewFieldType(mysql.TypeTiny)}
	cols := []*model.ColumnInfo{c1}
	rows := []types.Datum{
		types.NewIntDatum(10000000),
	}
	task := &MinimalTaskMeta{
		Table: Table{
			Info: &model.TableInfo{ID: 1, Columns: cols, PKIsHandle: false, State: model.StatePublic},
		},
		Chunk: Chunk{
			PrevRowIDMax: 1,
			Path:         filename,
		},
		SessionVars: SessionVars{
			SQLMode: sqlMode,
			SysVars: map[string]string{"tidb_row_format_version": "1"},
		},
		Mode: Mode{
			Type: importer.PhysicalImportMode,
			Physical: Physical{
				Timestamp: 1234567892,
				Keyspace:  keyspace.CodecV1.GetKeyspace(),
			},
		},
		Stmt: "LOAD DATA INFILE 'gs://dir/test.csv?endpoint=endpoint' INTO TABLE t1 (column1, @var1) SET column2 = @var1/100",
	}

	encoder, err := buildEncoder(task)
	require.NoError(t, err)
	pairs, err := encoder.Encode(rows, 1)
	require.EqualError(t, err, "[types:1690]constant 10000000 overflows tinyint")
	require.Nil(t, pairs)

	task.SessionVars.SQLMode = mysql.ModeNone
	encoder, err = buildEncoder(task)
	require.NoError(t, err)
	pairs, err = encoder.Encode(rows, 1)
	require.NoError(t, err)
	require.Equal(t, pairs, kv.MakeRowFromKvPairs([]common.KvPair{
		{
			Key:   []uint8{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
			Val:   []uint8{0x8, 0x2, 0x8, 0xfe, 0x1},
			RowID: common.EncodeIntRowID(1),
		},
	}))
}

func TestBackendCfg(t *testing.T) {
	taskMeta := &TaskMeta{
		JobID: 1,
	}
	backendCfg, err := genBackendConfig(taskMeta)
	require.NoError(t, err)
	lightningCfg := config.Lightning{
		TableConcurrency:   6,
		IndexConcurrency:   2,
		RegionConcurrency:  runtime.NumCPU(),
		IOConcurrency:      5,
		CheckRequirements:  true,
		MetaSchemaName:     "lightning_metadata",
		MaxError:           backendCfg.Lightning.App.MaxError,
		TaskInfoSchemaName: "lightning_task_info",
	}
	tidbCfg := config.DBStore{
		Host:                       "127.0.0.1",
		Port:                       0,
		User:                       "root",
		Psw:                        "",
		StatusPort:                 10080,
		PdAddr:                     "/tmp/tidb",
		StrSQLMode:                 "ONLY_FULL_GROUP_BY,NO_AUTO_CREATE_USER",
		TLS:                        "false",
		Security:                   &config.Security{},
		SQLMode:                    268435488,
		MaxAllowedPacket:           0x4000000,
		DistSQLScanConcurrency:     15,
		BuildStatsConcurrency:      20,
		IndexSerialScanConcurrency: 20,
		ChecksumTableConcurrency:   2,
		Vars:                       map[string]string(nil),
		UUID:                       "",
	}
	tikvCfg := config.TikvImporter{
		Addr:                    "",
		Backend:                 "local",
		OnDuplicate:             "replace",
		MaxKVPairs:              4096,
		SendKVPairs:             config.KVWriteBatchSize,
		SortedKVDir:             "import_1",
		DiskQuota:               config.ByteSize(math.MaxInt64),
		RangeConcurrency:        16,
		EngineMemCacheSize:      config.DefaultEngineMemCacheSize,
		LocalWriterMemCacheSize: config.DefaultLocalWriterMemCacheSize,
	}
	require.Equal(t, lightningCfg, backendCfg.Lightning.App)
	require.Equal(t, tidbCfg, backendCfg.Lightning.TiDB)
	require.Equal(t, tikvCfg, backendCfg.Lightning.TikvImporter)
}

func TestEngineCfg(t *testing.T) {
	taskMeta := &TaskMeta{
		JobID: 1,
		Table: Table{
			DBName: "db",
			Info:   &model.TableInfo{Name: model.NewCIStr("tb")},
		},
	}
	engineCfg := genEngineCfg(taskMeta)
	expected := &backend.EngineConfig{
		TableInfo: &checkpoints.TidbTableInfo{
			ID:   1,
			DB:   "db",
			Name: "tb",
		},
		Local: backend.LocalEngineConfig{
			Compact:            true,
			CompactThreshold:   int64(1 * size.GB),
			CompactConcurrency: 4,
		},
	}
	require.Equal(t, expected, engineCfg)
}
