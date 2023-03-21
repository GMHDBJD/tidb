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
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
)

// TaskStep of LoadData.
const (
	ReadSortImport = 1
)

type Task struct {
	Table     Table
	Format    Format
	Dir       string
	FileInfos []FileInfo
}

type Subtask struct {
	Table  Table
	Format Format
	Dir    string
	Chunks []Chunk
}

type MinimalTask struct {
	Table  Table
	Format Format
	Dir    string
	Chunk  Chunk
	Writer *backend.LocalEngineWriter
}

func (MinimalTask) IsMinimalTask() {}

type CSV struct {
	Config                *config.CSVConfig
	LoadDataReadBlockSize int64
	Strict                bool
}

type SQLDump struct {
	SQLMode               mysql.SQLMode
	LoadDataReadBlockSize int64
}

type Parquet struct{}

type Format struct {
	Type                   string
	Compression            mydump.Compression
	CSV                    CSV
	SQLDump                SQLDump
	Parquet                Parquet
	DataCharacterSet       string
	DataInvalidCharReplace string
}

type Table struct {
	DBName        string
	Info          *model.TableInfo
	TargetColumns []string
}

type Chunk struct {
	Path         string
	Offset       int64
	EndOffset    int64
	RealOffset   int64
	PrevRowIDMax int64
	RowIDMax     int64
}

type FileInfo struct {
	Path     string
	Size     int64
	RealSize int64
}
