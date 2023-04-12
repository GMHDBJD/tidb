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
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// ImportSubtaskExecutor is a subtask executor for load data.
type ImportSubtaskExecutor struct {
	task *MinimalTaskMeta
}

// Run implements the SubtaskExecutor.Run interface.
func (e *ImportSubtaskExecutor) Run(ctx context.Context) error {
	logutil.BgLogger().Info("subtask executor run", zap.Any("task", e.task))
	parser, err := buildParser(ctx, e.task)
	if err != nil {
		return err
	}
	defer parser.Close()
	encoder, err := buildEncoder(e.task)
	if err != nil {
		return err
	}
	defer encoder.Close()

	hasAutoIncrementAutoID := common.TableHasAutoRowID(e.task.Table.Info) &&
		e.task.Table.Info.AutoRandomBits == 0 && e.task.Table.Info.ShardRowIDBits == 0 &&
		e.task.Table.Info.Partition == nil
	dataWriter, err := e.task.DataEngine.LocalWriter(ctx, &backend.LocalWriterConfig{IsKVSorted: hasAutoIncrementAutoID})
	if err != nil {
		return err
	}
	defer dataWriter.Close(ctx)
	indexWriter, err := e.task.IndexEngine.LocalWriter(ctx, &backend.LocalWriterConfig{})
	if err != nil {
		return err
	}
	defer indexWriter.Close(ctx)

	cp := importer.NewChunkProcessor(
		parser,
		encoder,
		&checkpoints.ChunkCheckpoint{
			Key: checkpoints.ChunkCheckpointKey{
				Path:   e.task.Chunk.Path,
				Offset: e.task.Chunk.Offset,
			},
		},
		logutil.BgLogger(),
		dataWriter,
		indexWriter,
		e.task.Mode.Physical.Keyspace,
	)
	err = cp.Process(ctx)
	if err != nil {
		return err
	}
	return nil
}

func init() {
	scheduler.RegisterSubtaskExectorConstructor(
		proto.LoadData,
		// The order of the subtask executors is the same as the order of the subtasks.
		func(minimalTask proto.MinimalTask, step int64) (scheduler.SubtaskExecutor, error) {
			task, ok := minimalTask.(MinimalTaskMeta)
			if !ok {
				return nil, errors.Errorf("invalid task type %T", minimalTask)
			}
			return &ImportSubtaskExecutor{task: &task}, nil
		},
	)
}
