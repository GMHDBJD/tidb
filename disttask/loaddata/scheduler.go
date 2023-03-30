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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// ImportScheduler is a scheduler for load data.
type ImportScheduler struct {
	sctx             sessionctx.Context
	taskMeta         *TaskMeta
	lightningBackend *backend.Backend
	openedEngine     *backend.OpenedEngine
	writers          []*backend.LocalEngineWriter
}

// InitSubtaskExecEnv is used to initialize the environment for the subtask executor.
func (s *ImportScheduler) InitSubtaskExecEnv(ctx context.Context) error {
	logutil.BgLogger().Info("InitSubtaskExecEnv", zap.Any("taskMeta", s.taskMeta))
	// create backend
	backend, err := createLocalBackend(ctx, s.taskMeta)
	if err != nil {
		return err
	}
	s.lightningBackend = backend

	engine, err := openEngine(ctx, s.taskMeta, backend)
	if err != nil {
		return err
	}
	s.openedEngine = engine
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

	miniTask := make([]proto.MinimalTask, 0, len(subtaskMeta.Chunks))
	for _, chunk := range subtaskMeta.Chunks {
		writer, err := s.openedEngine.LocalWriter(ctx, &backend.LocalWriterConfig{IsKVSorted: subtaskMeta.Table.IsRowOrdered})
		if err != nil {
			return nil, err
		}
		s.writers = append(s.writers, writer)
		miniTask = append(miniTask, MinimalTaskMeta{
			Sctx:   s.sctx,
			Table:  subtaskMeta.Table,
			Format: subtaskMeta.Format,
			Mode:   subtaskMeta.Mode,
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
	closedEngine, err := s.openedEngine.Close(ctx)
	if err != nil {
		return err
	}
	for _, writer := range s.writers {
		if _, err := writer.Close(ctx); err != nil {
			return err
		}
	}
	if err := closedEngine.Import(ctx, int64(config.SplitRegionSize), int64(config.SplitRegionKeys)); err != nil {
		return err
	}
	if err := closedEngine.Cleanup(ctx); err != nil {
		return err
	}
	s.lightningBackend.Close()
	return nil
}

// Rollback is used to rollback all subtasks.
func (s *ImportScheduler) Rollback(context.Context) error {
	logutil.BgLogger().Info("rollback", zap.Any("taskMeta", s.taskMeta))
	return nil
}

// LogicalModeScheduler is a scheduler for load data.
type LogicalModeScheduler struct {
	sctx     sessionctx.Context
	taskMeta *TaskMeta
}

// InitSubtaskExecEnv is used to initialize the environment for the subtask executor.
func (s *LogicalModeScheduler) InitSubtaskExecEnv(ctx context.Context) error {
	logutil.BgLogger().Info("InitSubtaskExecEnv", zap.Any("taskMeta", s.taskMeta))
	return nil
}

// SplitSubtask is used to split the subtask into multiple minimal tasks.
func (s *LogicalModeScheduler) SplitSubtask(ctx context.Context, bs []byte) ([]proto.MinimalTask, error) {
	logutil.BgLogger().Info("SplitSubtask", zap.Any("taskMeta", s.taskMeta))
	var subtaskMeta SubtaskMeta
	err := json.Unmarshal(bs, &subtaskMeta)
	if err != nil {
		return nil, err
	}

	miniTask := make([]proto.MinimalTask, 0, len(subtaskMeta.Chunks))
	for _, chunk := range subtaskMeta.Chunks {
		if err != nil {
			return nil, err
		}
		miniTask = append(miniTask, MinimalTaskMeta{
			Sctx:   s.sctx,
			Table:  subtaskMeta.Table,
			Format: subtaskMeta.Format,
			Mode:   subtaskMeta.Mode,
			Dir:    subtaskMeta.Dir,
			Chunk:  chunk,
		})
	}
	return miniTask, nil
}

// CleanupSubtaskExecEnv is used to clean up the environment for the subtask executor.
func (s *LogicalModeScheduler) CleanupSubtaskExecEnv(ctx context.Context) error {
	logutil.BgLogger().Info("CleanupSubtaskExecEnv", zap.Any("taskMeta", s.taskMeta))
	return nil
}

// Rollback is used to rollback all subtasks.
func (s *LogicalModeScheduler) Rollback(context.Context) error {
	logutil.BgLogger().Info("rollback", zap.Any("taskMeta", s.taskMeta))
	return nil
}

func init() {
	scheduler.RegisterSchedulerConstructor(
		proto.LoadData,
		func(sctx sessionctx.Context, bs []byte, step int64) (scheduler.Scheduler, error) {
			taskMeta := &TaskMeta{}
			if err := json.Unmarshal(bs, &taskMeta); err != nil {
				return nil, err
			}
			logutil.BgLogger().Info("register scheduler constructor", zap.Any("taskMeta", taskMeta))
			switch taskMeta.Mode.Type {
			case importer.LogicalImportMode:
				return &LogicalModeScheduler{
					sctx:     sctx,
					taskMeta: taskMeta,
				}, nil
			case importer.PhysicalImportMode:
				return &ImportScheduler{
					sctx:     sctx,
					taskMeta: taskMeta,
				}, nil
			default:
				return nil, errors.Errorf("unknown mode %s", taskMeta.Mode)
			}
		},
	)
}
