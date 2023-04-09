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

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// ImportScheduler is a scheduler for load data.
type ImportScheduler struct {
	taskMeta         *TaskMeta
	lightningBackend *backend.Backend
	indexEngine      *backend.OpenedEngine
	dataEngines      []*backend.OpenedEngine
}

// InitSubtaskExecEnv is used to initialize the environment for the subtask executor.
func (s *ImportScheduler) InitSubtaskExecEnv(ctx context.Context) error {
	logutil.BgLogger().Info("InitSubtaskExecEnv", zap.Any("taskMeta", s.taskMeta))
	backend, err := createLocalBackend(ctx, s.taskMeta)
	if err != nil {
		return err
	}
	s.lightningBackend = backend

	indexEngine, err := openEngine(ctx, s.taskMeta, common.IndexEngineID, backend)
	if err != nil {
		return err
	}
	s.indexEngine = indexEngine
	return nil
}

// SplitSubtask is used to split the subtask into multiple minimal tasks.
func (s *ImportScheduler) SplitSubtask(ctx context.Context, bs []byte) ([]proto.MinimalTask, error) {
	logutil.BgLogger().Info("SplitSubtask", zap.Any("taskMeta", s.taskMeta))
	var subtaskMeta SubtaskMeta
	err := json.Unmarshal(bs, &subtaskMeta)
	if err != nil {
		return nil, err
	}

	dataEngine, err := openEngine(ctx, s.taskMeta, subtaskMeta.ID, s.lightningBackend)
	if err != nil {
		return nil, err
	}
	s.dataEngines = append(s.dataEngines, dataEngine)

	miniTask := make([]proto.MinimalTask, 0, len(subtaskMeta.Chunks))
	for _, chunk := range subtaskMeta.Chunks {
		miniTask = append(miniTask, MinimalTaskMeta{
			JobID:       subtaskMeta.JobID,
			Table:       subtaskMeta.Table,
			Format:      subtaskMeta.Format,
			Dir:         subtaskMeta.Dir,
			Chunk:       chunk,
			Mode:        s.taskMeta.Mode,
			SessionVars: s.taskMeta.SessionVars,
			Stmt:        s.taskMeta.Stmt,
			DataEngine:  dataEngine,
			IndexEngine: s.indexEngine,
		})
	}
	return miniTask, nil
}

// CleanupSubtaskExecEnv is used to clean up the environment for the subtask executor.
func (s *ImportScheduler) CleanupSubtaskExecEnv(ctx context.Context) error {
	logutil.BgLogger().Info("CleanupSubtaskExecEnv", zap.Any("taskMeta", s.taskMeta))
	// TODO: add OnSubtaskFinish callback in scheduler framework.
	// If subtask failed, we should not import data.
	for _, engine := range s.dataEngines {
		if err := importAndCleanupEngine(ctx, engine); err != nil {
			return err
		}
	}
	if err := importAndCleanupEngine(ctx, s.indexEngine); err != nil {
		return err
	}
	s.lightningBackend.Close()
	return nil
}

// Rollback is used to rollback all subtasks.
// TODO: add rollback
func (s *ImportScheduler) Rollback(context.Context) error {
	logutil.BgLogger().Info("rollback", zap.Any("taskMeta", s.taskMeta))
	return nil
}

func init() {
	scheduler.RegisterSchedulerConstructor(
		proto.LoadData,
		func(bs []byte, step int64) (scheduler.Scheduler, error) {
			taskMeta := &TaskMeta{}
			if err := json.Unmarshal(bs, &taskMeta); err != nil {
				return nil, err
			}
			logutil.BgLogger().Info("register scheduler constructor", zap.Any("taskMeta", taskMeta))
			return &ImportScheduler{taskMeta: taskMeta}, nil
		},
	)
}
