// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package request

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type reqInspector struct {
}

func (ri *reqInspector) RequestID(req []byte) string {
	digest := sha256.Sum256(req)
	return hex.EncodeToString(digest[:])
}

func TestRequestPool(t *testing.T) {
	sugaredLogger := createLogger(t, 0)

	requestInspector := &reqInspector{}

	primaryPool := NewPool(sugaredLogger, requestInspector, PoolOptions{
		FirstStrikeThreshold:  time.Second * 5,
		SecondStrikeThreshold: time.Minute / 2,
		BatchMaxSize:          10000,
		MaxSize:               1000 * 100,
		AutoRemoveTimeout:     time.Second * 10,
		SubmitTimeout:         time.Second * 10,
	})

	secondaryPool := NewPool(sugaredLogger, requestInspector, PoolOptions{
		FirstStrikeThreshold:  time.Second * 5,
		SecondStrikeThreshold: time.Minute / 2,
		BatchMaxSize:          10000,
		MaxSize:               1000 * 100,
		AutoRemoveTimeout:     time.Second * 10,
		SubmitTimeout:         time.Second * 10,
		OnFirstStrikeTimeout: func(_ []byte) {
			panic("timed out on request")
		},
	})

	primaryPool.SetBatching(true)

	var submittedCount uint32
	var committedReqCount int

	workerNum := runtime.NumCPU()
	workerPerWorker := 100000

	for worker := 0; worker < workerNum; worker++ {
		go func(worker int) {

			for i := 0; i < workerPerWorker; i++ {
				req := make([]byte, 8)
				binary.BigEndian.PutUint32(req, uint32(worker))
				binary.BigEndian.PutUint32(req[4:], uint32(i))
				atomic.AddUint32(&submittedCount, 1)

				if err := secondaryPool.Submit(req); err != nil {
					panic(err)
				}

				if err := primaryPool.Submit(req); err != nil {
					panic(err)
				}
			}
		}(worker)
	}

	for committedReqCount < workerPerWorker*workerNum {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		batch := primaryPool.NextRequests(ctx)
		cancel()
		committedReqCount += len(batch)

		workerNum := runtime.NumCPU()

		removeRequests(workerNum, batch, requestInspector, primaryPool)
		removeRequests(workerNum, batch, requestInspector, secondaryPool)

	}
}

func removeRequests(workerNum int, batch [][]byte, requestInspector *reqInspector, pool *Pool) {
	var wg sync.WaitGroup
	wg.Add(workerNum)

	for workerID := 0; workerID < workerNum; workerID++ {
		go func(workerID int) {
			defer wg.Done()
			reqInfos := make([]string, 0, len(batch))
			for i, req := range batch {
				if i%workerNum != workerID {
					continue
				}
				reqInfos = append(reqInfos, requestInspector.RequestID(req))
			}

			pool.RemoveRequests(reqInfos...)

		}(workerID)
	}

	wg.Wait()
}

func createLogger(t *testing.T, i int) *zap.SugaredLogger {
	logConfig := zap.NewDevelopmentConfig()
	logConfig.Level.SetLevel(zapcore.WarnLevel)
	logger, _ := logConfig.Build()
	logger = logger.With(zap.String("t", t.Name())).With(zap.Int64("id", int64(i)))
	sugaredLogger := logger.Sugar()
	return sugaredLogger
}
