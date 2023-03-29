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

package handle

import (
	"context"
	"time"

	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type Handle struct {
	ctx context.Context
	se  sessionctx.Context
	gm  *storage.GlobalTaskManager
}

func (h *Handle) checkGlobalTaskDone(id int64, ch chan struct{}) {
	tk := time.Tick(50 * time.Millisecond)
	for {
		select {
		case <-h.ctx.Done():
			logutil.BgLogger().Error("check global task done failed", zap.Error(h.ctx.Err()))
			return
		case <-tk:
			finish, err := h.gm.HasTaskInStates(id, proto.TaskStateSucceed, proto.TaskStateReverted, proto.TaskStateRevertFailed)
			if err != nil {
				logutil.BgLogger().Error("check global task done failed", zap.Error(err))
				continue
			}
			if finish {
				close(ch)
				return
			}
		}
	}
}

func NewHandle(ctx context.Context, se sessionctx.Context) (Handle, error) {
	gm, err := storage.GetGlobalTaskManager()
	if err != nil {
		return Handle{}, err
	}
	return Handle{
		ctx: ctx,
		se:  se,
		gm:  gm,
	}, nil
}

func (h *Handle) SubmitGlobalTaskAndRun(key, tp string, concurrency int, taskMeta []byte) (taskID int64, done chan struct{}, err error) {
	id, err := h.gm.AddNewTask(key, tp, concurrency, taskMeta)
	if err != nil {
		return 0, nil, err
	}

	done = make(chan struct{})
	go func() {
		h.checkGlobalTaskDone(id, done)
	}()

	return id, done, nil
}
