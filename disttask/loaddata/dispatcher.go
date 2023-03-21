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
	"encoding/json"

	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type Dispatcher struct {
}

func (d *Dispatcher) ProcessNormalFlow(dispatch dispatcher.Dispatch, gTask *proto.Task) ([][]byte, error) {
	task := &Task{}
	err := json.Unmarshal(gTask.Meta, task)
	if err != nil {
		return nil, err
	}
	logutil.BgLogger().Info("process normal flow", zap.Any("task", task), zap.Any("step", gTask.Step))

	switch gTask.Step {
	case ReadSortImport:
		gTask.State = proto.TaskStateSucceed
		return nil, nil
	default:
	}

	subtasks, err := generateSubtasks(context.Background(), task)
	if err != nil {
		return nil, err
	}
	logutil.BgLogger().Info("generate subtasks", zap.Any("subtasks", subtasks))
	subtaskMetas := make([][]byte, 0, len(task.FileInfos))
	for _, subtask := range subtasks {
		bs, err := json.Marshal(subtask)
		if err != nil {
			return nil, err
		}
		subtaskMetas = append(subtaskMetas, bs)
	}
	gTask.Step = ReadSortImport
	return subtaskMetas, nil
}

func (d *Dispatcher) ProcessErrFlow(dispatch dispatcher.Dispatch, gTask *proto.Task, errMsg string) ([]byte, error) {
	logutil.BgLogger().Info("process error flow", zap.String("error message", errMsg))
	return nil, nil
}

func generateSubtasks(ctx context.Context, task *Task) ([]*Subtask, error) {
	tableRegions, err := makeTableRegions(ctx, task)
	if err != nil {
		return nil, err
	}

	subtasks := make([]*Subtask, 0, 3)
	for _, region := range tableRegions {
		if region.EngineID >= int32(len(subtasks)) {
			subtasks = append(subtasks, &Subtask{
				Table:  task.Table,
				Format: task.Format,
				Dir:    task.Dir,
			})
		}
		subtask := subtasks[region.EngineID]
		subtask.Chunks = append(subtask.Chunks, Chunk{
			Path:         region.FileMeta.Path,
			Offset:       region.Chunk.Offset,
			EndOffset:    region.Chunk.EndOffset,
			RealOffset:   region.Chunk.RealOffset,
			PrevRowIDMax: region.Chunk.PrevRowIDMax,
			RowIDMax:     region.Chunk.RowIDMax,
		})
	}
	return subtasks, nil
}

func init() {
	dispatcher.RegisterTaskFlowHandle(proto.LoadData, &Dispatcher{})
}
