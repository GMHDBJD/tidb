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
	"errors"

	"github.com/pingcap/tidb/ddl/ingest"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// ReadSortImportScheduler is a scheduler for load data.
type ReadSortImportScheduler struct {
	task *Task
}

//
//type Config struct {
//	Lightning    *lightning.Config
//	KeyspaceName string
//}
//
//func genLightningDataDir() (string, error) {
//	tidbCfg := config.GetGlobalConfig()
//	sortPathSuffix := "/tmp_import-" + strconv.Itoa(int(tidbCfg.Port))
//	sortPath := filepath.Join(tidbCfg.TempDir, sortPathSuffix)
//
//	if info, err := os.Stat(sortPath); err != nil {
//		if !os.IsNotExist(err) {
//			logutil.BgLogger().Error("LitErrStatDirFail", zap.String("sort path", sortPath), zap.Error(err))
//			return "", err
//		}
//	} else if info.IsDir() {
//		// Currently remove all dir to clean garbage data.
//		// TODO: when do checkpoint should change follow logic.
//		err := os.RemoveAll(sortPath)
//		if err != nil {
//			logutil.BgLogger().Error("LitErrDeleteDirFail", zap.String("sort path", sortPath), zap.Error(err))
//		}
//	}
//
//	err := os.MkdirAll(sortPath, 0o700)
//	if err != nil {
//		logutil.BgLogger().Error("LitErrCreateDirFail", zap.String("sort path", sortPath), zap.Error(err))
//		return "", err
//	}
//	logutil.BgLogger().Info("LitInfoSortDir", zap.String("data path:", sortPath))
//	return sortPath, nil
//}
//
//func genConfig(id int64, unique bool) (*Config, error) {
//	tidbCfg := tidb.GetGlobalConfig()
//	cfg := lightning.NewConfig()
//	cfg.TikvImporter.Backend = lightning.BackendLocal
//	// Each backend will build a single dir in lightning dir.
//	dir, err := genLightningDataDir()
//	if err != nil {
//		return nil, err
//	}
//	cfg.TikvImporter.SortedKVDir = filepath.Join(dir, fmt.Sprintf("%d", id))
//	_, err = cfg.AdjustCommon()
//	if err != nil {
//		logutil.BgLogger().Warn("LitWarnConfigError", zap.Error(err))
//		return nil, err
//	}
//	cfg.Checkpoint.Enable = true
//	if unique {
//		cfg.TikvImporter.DuplicateResolution = lightning.DupeResAlgErr
//	} else {
//		cfg.TikvImporter.DuplicateResolution = lightning.DupeResAlgNone
//	}
//	cfg.TiDB.PdAddr = tidbCfg.Path
//	cfg.TiDB.Host = "127.0.0.1"
//	cfg.TiDB.StatusPort = int(tidbCfg.Status.StatusPort)
//	// Set TLS related information
//	cfg.Security.CAPath = tidbCfg.Security.ClusterSSLCA
//	cfg.Security.CertPath = tidbCfg.Security.ClusterSSLCert
//	cfg.Security.KeyPath = tidbCfg.Security.ClusterSSLKey
//
//	c := &Config{
//		Lightning:    cfg,
//		KeyspaceName: tidb.GetGlobalKeyspaceName(),
//	}
//
//	return c, err
//}
//
//var (
//	compactMemory      = 1 * size.GB
//	compactConcurrency = 4
//)
//
//func generateLocalEngineConfig(id int64, dbName, tbName string) *backend.EngineConfig {
//	return &backend.EngineConfig{
//		Local: backend.LocalEngineConfig{
//			Compact:            true,
//			CompactThreshold:   int64(compactMemory),
//			CompactConcurrency: compactConcurrency,
//		},
//		TableInfo: &checkpoints.TidbTableInfo{
//			ID:   id,
//			DB:   dbName,
//			Name: tbName,
//		},
//	}
//}

//// InitSubtaskExecEnv is used to initialize the environment for the subtask executor.
//func (s *ReadSortImportScheduler) InitSubtaskExecEnv(ctx context.Context) error {
//	cfg, err := genConfig(s.task.Table.Info.ID, false)
//	if err != nil {
//		return err
//	}
//	tls, err := cfg.Lightning.ToTLS()
//	if err != nil {
//		logutil.BgLogger().Error("LitErrCreateBackendFail", zap.Error(err))
//		return err
//	}
//	lightningConfig := generateLocalEngineConfig(s.task.Table.Info.ID, "db", s.task.Table.Info.Name.String())
//	logutil.BgLogger().Info("[ddl-ingest] create local backend for adding index", zap.String("keyspaceName", cfg.KeyspaceName))
//	errorMgr := errormanager.New(nil, cfg.Lightning, log.Logger{Logger: logutil.BgLogger()})
//	be, err := local.NewLocalBackend(ctx, tls, cfg.Lightning, &glue.GlueLit{}, int(util.GenRLimit()), errorMgr, cfg.KeyspaceName)
//	if err != nil {
//		return err
//	}
//	s.be = &be
//	openedEn, err := be.OpenEngine(ctx, lightningConfig, s.task.Table.Info.Name.String(), 1)
//	if err != nil {
//		return err
//	}
//
//	s.engine = openedEn
//	writer, err := openedEn.LocalWriter(ctx, &backend.LocalWriterConfig{})
//	if err != nil {
//		return err
//	}
//	s.writer = writer
//	return nil
//}
//
//// CleanupSubtaskExecEnv is used to clean up the environment for the subtask executor.
//func (s *ReadSortImportScheduler) CleanupSubtaskExecEnv(ctx context.Context) error {
//	closeEngine, err := s.engine.Close(ctx)
//	if err != nil {
//		return err
//	}
//	if err := closeEngine.Import(ctx, int64(lightning.SplitRegionSize), int64(lightning.SplitRegionKeys)); err != nil {
//		return err
//	}
//	if err := closeEngine.Cleanup(ctx); err != nil {
//		return err
//	}
//	s.be.Close()
//	return nil
//}

// InitSubtaskExecEnv is used to initialize the environment for the subtask executor.
func (s *ReadSortImportScheduler) InitSubtaskExecEnv(ctx context.Context) error {
	logutil.BgLogger().Info("LitInfoInitSubtaskExecEnv", zap.Int64("tableID", s.task.Table.Info.ID))
	// TODO: use real jobID
	_, err := ingest.LitBackCtxMgr.Register(ctx, false, s.task.Table.Info.ID, 0)
	logutil.BgLogger().Info("ok", zap.Int64("tableID", s.task.Table.Info.ID))
	return err
}

// SplitSubtask is used to split the subtask into multiple minimal tasks.
func (s *ReadSortImportScheduler) SplitSubtask(subtaskMeta []byte) []proto.MinimalTask {
	logutil.BgLogger().Info("LitInfoSplitSubtask", zap.Int64("tableID", s.task.Table.Info.ID))
	var subtask Subtask
	err := json.Unmarshal(subtaskMeta, &subtask)
	if err != nil {
		logutil.BgLogger().Error("unmarshal subtask error", zap.Error(err))
		return nil
	}

	bc, ok := ingest.LitBackCtxMgr.Load(subtask.Table.Info.ID)
	if !ok {
		panic("no backend context found")
	}
	ei, err := bc.EngMgr.Register(bc, subtask.Table.Info.ID, 1, subtask.Table.DBName, subtask.Table.Info.Name.String())
	if err != nil {
		panic(err.Error())
	}

	miniTask := make([]proto.MinimalTask, 0, len(subtask.Chunks))
	for i, chunk := range subtask.Chunks {
		lwCtx, err := ei.NewWriterCtx(i, false)
		if err != nil {
			panic(err.Error())
		}
		miniTask = append(miniTask, MinimalTask{
			ID:     i,
			Table:  subtask.Table,
			Format: subtask.Format,
			Dir:    subtask.Dir,
			Chunk:  chunk,
			Writer: lwCtx,
		})
	}
	return miniTask
}

// CleanupSubtaskExecEnv is used to clean up the environment for the subtask executor.
func (s *ReadSortImportScheduler) CleanupSubtaskExecEnv(ctx context.Context) error {
	logutil.BgLogger().Info("LitInfoCleanupSubtaskExecEnv", zap.Int64("tableID", s.task.Table.Info.ID))
	bc, ok := ingest.LitBackCtxMgr.Load(s.task.Table.Info.ID)
	if !ok {
		return errors.New("no backend context found")
	}
	err := bc.FinishImport(1, false, nil)
	if err != nil {
		return err
	}
	ingest.LitBackCtxMgr.Unregister(s.task.Table.Info.ID)
	return nil
}

// Rollback is used to rollback all subtasks.
func (*ReadSortImportScheduler) Rollback(context.Context) error {
	logutil.BgLogger().Info("rollback step one")
	return nil
}

func init() {
	scheduler.RegisterSchedulerConstructor(
		proto.LoadData,
		func(taskMeta []byte, step int64) (scheduler.Scheduler, error) {
			var task Task
			if err := json.Unmarshal(taskMeta, &task); err != nil {
				return nil, err
			}
			logutil.BgLogger().Info("register scheduler constructor", zap.Any("task", task))
			return &ReadSortImportScheduler{task: &task}, nil
		},
	)
}
