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
	"io"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/keyspace"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// ReadWriteSubtaskExecutor is an example subtask executor.
type ReadWriteSubtaskExecutor struct {
	task MinimalTaskMeta
}

// Run implements the SubtaskExecutor interface.
func (e *ReadWriteSubtaskExecutor) Run(ctx context.Context) error {
	var (
		dataKVs       = kv.MakeRowsFromKvPairs(nil)
		indexKVs      = kv.MakeRowsFromKvPairs(nil)
		offset        = int64(0)
		dataChecksum  = verify.NewKVChecksumWithKeyspace(keyspace.CodecV1)
		indexChecksum = verify.NewKVChecksumWithKeyspace(keyspace.CodecV1)
	)

	logutil.BgLogger().Info("subtask executor run", zap.Any("task", e.task))

	parser, err := BuildParser(ctx, e.task)
	if err != nil {
		return err
	}
	encoder, err := buildEncoder(ctx, e.task)
	if err != nil {
		return err
	}
	permutation, err := createColumnPermutation(e.task)
	if err != nil {
		return err
	}

	for {
		err := parser.ReadRow()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				break
			}
			return err
		}

		lastRow := parser.LastRow()
		kvs, err := encoder.Encode(lastRow.Row, lastRow.RowID, permutation, offset)
		if err != nil {
			return err
		}
		offset, _ = parser.Pos()
		parser.RecycleRow(lastRow)
		kvs.ClassifyAndAppend(&dataKVs, dataChecksum, &indexKVs, indexChecksum)
		logutil.BgLogger().Info("sub task executor run", zap.Any("dataKVs", dataKVs), zap.Any("indexKVs", indexKVs), zap.Any("column", e.task.Table.TargetColumns))
		if err := e.task.Writer.WriteRows(ctx, e.task.Table.TargetColumns, dataKVs); err != nil {
			return err
		}
	}
	return nil
}

type LogicalSubtaskExecutor struct {
	Task MinimalTaskMeta
}

func (e *LogicalSubtaskExecutor) Run(ctx context.Context) error {
	logutil.BgLogger().Info("subtask executor run", zap.Any("task", e.Task))
	parser, err := BuildParser(ctx, e.Task)
	if err != nil {
		return err
	}
	for {
		err := parser.ReadRow()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				break
			}
			return err
		}

	}
	return nil
}
