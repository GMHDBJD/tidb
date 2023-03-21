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
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	lightning "github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/config"
	tidb "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/size"
	"go.uber.org/zap"
)

// ReadSortImportScheduler is a scheduler for load data.
type ReadSortImportScheduler struct {
	task   *Task
	be     *backend.Backend
	engine *backend.OpenedEngine
	writer *backend.LocalEngineWriter
}

type Config struct {
	Lightning    *lightning.Config
	KeyspaceName string
}

func genLightningDataDir() (string, error) {
	tidbCfg := config.GetGlobalConfig()
	sortPathSuffix := "/tmp_import-" + strconv.Itoa(int(tidbCfg.Port))
	sortPath := filepath.Join(tidbCfg.TempDir, sortPathSuffix)

	if info, err := os.Stat(sortPath); err != nil {
		if !os.IsNotExist(err) {
			logutil.BgLogger().Error("LitErrStatDirFail", zap.String("sort path", sortPath), zap.Error(err))
			return "", err
		}
	} else if info.IsDir() {
		// Currently remove all dir to clean garbage data.
		// TODO: when do checkpoint should change follow logic.
		err := os.RemoveAll(sortPath)
		if err != nil {
			logutil.BgLogger().Error("LitErrDeleteDirFail", zap.String("sort path", sortPath), zap.Error(err))
		}
	}

	err := os.MkdirAll(sortPath, 0o700)
	if err != nil {
		logutil.BgLogger().Error("LitErrCreateDirFail", zap.String("sort path", sortPath), zap.Error(err))
		return "", err
	}
	logutil.BgLogger().Info("LitInfoSortDir", zap.String("data path:", sortPath))
	return sortPath, nil
}

func genConfig(id int64, unique bool) (*Config, error) {
	tidbCfg := tidb.GetGlobalConfig()
	cfg := lightning.NewConfig()
	cfg.TikvImporter.Backend = lightning.BackendLocal
	// Each backend will build a single dir in lightning dir.
	dir, err := genLightningDataDir()
	if err != nil {
		return nil, err
	}
	cfg.TikvImporter.SortedKVDir = filepath.Join(dir, fmt.Sprintf("%d", id))
	_, err = cfg.AdjustCommon()
	if err != nil {
		logutil.BgLogger().Warn("LitWarnConfigError", zap.Error(err))
		return nil, err
	}
	cfg.Checkpoint.Enable = true
	if unique {
		cfg.TikvImporter.DuplicateResolution = lightning.DupeResAlgErr
	} else {
		cfg.TikvImporter.DuplicateResolution = lightning.DupeResAlgNone
	}
	cfg.TiDB.PdAddr = tidbCfg.Path
	cfg.TiDB.Host = "127.0.0.1"
	cfg.TiDB.StatusPort = int(tidbCfg.Status.StatusPort)
	// Set TLS related information
	cfg.Security.CAPath = tidbCfg.Security.ClusterSSLCA
	cfg.Security.CertPath = tidbCfg.Security.ClusterSSLCert
	cfg.Security.KeyPath = tidbCfg.Security.ClusterSSLKey

	c := &Config{
		Lightning:    cfg,
		KeyspaceName: tidb.GetGlobalKeyspaceName(),
	}

	return c, err
}

// glueLit is used as a placeholder for the local backend initialization.
type glueLit struct{}

// OwnsSQLExecutor Implement interface OwnsSQLExecutor.
func (glueLit) OwnsSQLExecutor() bool {
	return false
}

// GetSQLExecutor Implement interface GetSQLExecutor.
func (glueLit) GetSQLExecutor() glue.SQLExecutor {
	return nil
}

// GetDB Implement interface GetDB.
func (glueLit) GetDB() (*sql.DB, error) {
	return nil, nil
}

// GetParser Implement interface GetParser.
func (glueLit) GetParser() *parser.Parser {
	return nil
}

// GetTables Implement interface GetTables.
func (glueLit) GetTables(context.Context, string) ([]*model.TableInfo, error) {
	return nil, nil
}

// GetSession Implement interface GetSession.
func (glueLit) GetSession(context.Context) (checkpoints.Session, error) {
	return nil, nil
}

// OpenCheckpointsDB Implement interface OpenCheckpointsDB.
func (glueLit) OpenCheckpointsDB(context.Context, *lightning.Config) (checkpoints.DB, error) {
	return nil, nil
}

// Record is used to report some information (key, value) to host TiDB, including progress, stage currently.
func (glueLit) Record(string, uint64) {
}

var (
	compactMemory      = 1 * size.GB
	compactConcurrency = 4
)

func generateLocalEngineConfig(id int64, dbName, tbName string) *backend.EngineConfig {
	return &backend.EngineConfig{
		Local: backend.LocalEngineConfig{
			Compact:            true,
			CompactThreshold:   int64(compactMemory),
			CompactConcurrency: compactConcurrency,
		},
		TableInfo: &checkpoints.TidbTableInfo{
			ID:   id,
			DB:   dbName,
			Name: tbName,
		},
	}
}

// InitSubtaskExecEnv is used to initialize the environment for the subtask executor.
func (s *ReadSortImportScheduler) InitSubtaskExecEnv(ctx context.Context) error {
	cfg, err := genConfig(s.task.Table.Info.ID, false)
	if err != nil {
		return err
	}
	tls, err := cfg.Lightning.ToTLS()
	if err != nil {
		logutil.BgLogger().Error("LitErrCreateBackendFail", zap.Error(err))
		return err
	}
	lightningConfig := generateLocalEngineConfig(s.task.Table.Info.ID, "db", s.task.Table.Info.Name.String())
	logutil.BgLogger().Info("[ddl-ingest] create local backend for adding index", zap.String("keyspaceName", cfg.KeyspaceName))
	errorMgr := errormanager.New(nil, cfg.Lightning, log.Logger{Logger: logutil.BgLogger()})
	be, err := local.NewLocalBackend(ctx, tls, cfg.Lightning, &glueLit{}, int(util.GenRLimit()), errorMgr, cfg.KeyspaceName)
	if err != nil {
		return err
	}
	s.be = &be
	openedEn, err := be.OpenEngine(ctx, lightningConfig, s.task.Table.Info.Name.String(), 1)
	if err != nil {
		return err
	}

	s.engine = openedEn
	writer, err := openedEn.LocalWriter(ctx, &backend.LocalWriterConfig{})
	if err != nil {
		return err
	}
	s.writer = writer
	return nil
}

// CleanupSubtaskExecEnv is used to clean up the environment for the subtask executor.
func (s *ReadSortImportScheduler) CleanupSubtaskExecEnv(ctx context.Context) error {
	closeEngine, err := s.engine.Close(ctx)
	if err != nil {
		logutil.BgLogger().Error("LitErrCloseEngine", zap.Error(err))
		return err
	}
	if err := closeEngine.Import(ctx, int64(lightning.SplitRegionSize), int64(lightning.SplitRegionKeys)); err != nil {
		logutil.BgLogger().Error("LitErrImportEngine", zap.Error(err))
		return err
	}
	if err := closeEngine.Cleanup(ctx); err != nil {
		logutil.BgLogger().Error("LitErrCleanupEngine", zap.Error(err))
		return err
	}
	s.be.Close()
	return nil
}

// SplitSubtask is used to split the subtask into multiple minimal tasks.
func (s *ReadSortImportScheduler) SplitSubtask(subtaskMeta []byte) []proto.MinimalTask {
	var subtask Subtask
	err := json.Unmarshal(subtaskMeta, &subtask)
	if err != nil {
		logutil.BgLogger().Error("unmarshal subtask error", zap.Error(err))
		return nil
	}

	miniTask := make([]proto.MinimalTask, 0, len(subtask.Chunks))
	for _, chunk := range subtask.Chunks {
		miniTask = append(miniTask, MinimalTask{
			Table:  subtask.Table,
			Format: subtask.Format,
			Dir:    subtask.Dir,
			Chunk:  chunk,
			Writer: s.writer,
		})
	}
	return miniTask
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
