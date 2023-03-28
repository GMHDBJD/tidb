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
	"fmt"
	"sync"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// ImportScheduler is a scheduler for load data.
type ImportScheduler struct {
	taskMeta         *TaskMeta
	lightningBackend backend.Backend
	mu               sync.RWMutex
	engines          map[int]*backend.OpenedEngine
}

// InitSubtaskExecEnv is used to initialize the environment for the subtask executor.
func (s *ImportScheduler) InitSubtaskExecEnv(ctx context.Context) error {
	logutil.BgLogger().Info("InitSubtaskExecEnv", zap.Any("taskMeta", s.taskMeta))
	// create backend
	backendCfg, err := ingest.GenConfig(ingest.WithSortedKVDir(lightningSortedKVDir(s.taskMeta.Table.Info.ID)))
	if err != nil {
		return err
	}
	backend, err := ingest.CreateLocalBackend(ctx, backendCfg)
	if err != nil {
		return err
	}
	s.lightningBackend = backend
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

	s.mu.Lock()
	engine, ok := s.engines[subtaskMeta.ID]
	s.mu.Unlock()
	if !ok {
		cfg := ingest.GenerateLocalEngineConfig(subtaskMeta.Table.Info.ID, subtaskMeta.Table.DBName, subtaskMeta.Table.Info.Name.String())
		engine, err = s.lightningBackend.OpenEngine(ctx, cfg, subtaskMeta.Table.Info.Name.String(), int32(subtaskMeta.ID))
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		s.engines[subtaskMeta.ID] = engine
		s.mu.Unlock()
	}

	miniTask := make([]proto.MinimalTask, 0, len(subtaskMeta.Chunks))
	for _, chunk := range subtaskMeta.Chunks {
		writer, err := engine.LocalWriter(ctx, &backend.LocalWriterConfig{IsKVSorted: subtaskMeta.Table.IsRowOrdered})
		if err != nil {
			panic(err.Error())
		}
		miniTask = append(miniTask, MinimalTaskMeta{
			Table:  subtaskMeta.Table,
			Format: subtaskMeta.Format,
			Dir:    subtaskMeta.Dir,
			Chunk:  chunk,
			Writer: writer,
		})
	}
	return miniTask, nil
}

// CleanupSubtaskExecEnv is used to clean up the environment for the subtask executor.
func (s *ImportScheduler) CleanupSubtaskExecEnv(ctx context.Context) error {
	logutil.BgLogger().Info("CleanupSubtaskExecEnv", zap.Any("taskMeta", s.taskMeta))

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, engine := range s.engines {
		closedEngine, err := engine.Close(ctx)
		if err != nil {
			return err
		}
		if err := closedEngine.Import(ctx, int64(config.SplitRegionSize), int64(config.SplitRegionKeys)); err != nil {
			return err
		}
		if err := closedEngine.Cleanup(ctx); err != nil {
			return err
		}
	}
	s.lightningBackend.Close()
	return nil
}

// Rollback is used to rollback all subtasks.
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

func lightningSortedKVDir(tableID int64) string {
	return fmt.Sprintf("import_%d", tableID)
}
