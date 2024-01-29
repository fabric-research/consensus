// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package request

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/semaphore"
)

const (
	defaultSubmitTimeout = 10 * time.Second // for unit tests only
	defaultBatchTimeout  = time.Second
	defaultMaxBytes      = 100 * 1024 // default max request size would be of size 100Kb
)

type Logger interface {
	Debugf(template string, args ...interface{})
	Infof(template string, args ...interface{})
	Errorf(template string, args ...interface{})
	Warnf(template string, args ...interface{})
	Panicf(template string, args ...interface{})
}

// RequestInspector extracts the id of a given request.
type RequestInspector interface {
	// RequestID returns the id of the given request.
	RequestID(req []byte) string
}

// Pool implements requests pool, maintains pool of given size provided during
// construction. In case there are more incoming request than the given size it will
// block during submit until there will be space to submit new ones.
type Pool struct {
	lock            sync.RWMutex
	pending         *PendingStore
	logger          Logger
	inspector       RequestInspector
	options         PoolOptions
	batchStore      *BatchStore
	semaphore       *semaphore.Weighted
	closed          uint32
	stopped         uint32
	batchingEnabled bool
}

// requestItem captures request related information
type requestItem struct {
	request []byte
}

// PoolOptions is the pool configuration
type PoolOptions struct {
	MaxSize               uint64
	BatchMaxSize          uint32
	BatchMaxSizeBytes     uint32
	RequestMaxBytes       uint64
	SubmitTimeout         time.Duration
	BatchTimeout          time.Duration
	OnFirstStrikeTimeout  func([]byte)
	FirstStrikeThreshold  time.Duration
	OnSecondStrikeTimeout func()
	SecondStrikeThreshold time.Duration
	AutoRemoveTimeout     time.Duration
}

// NewPool constructs a new requests pool
func NewPool(log Logger, inspector RequestInspector, options PoolOptions) *Pool {

	// TODO check pool options

	if options.SubmitTimeout == 0 {
		options.SubmitTimeout = defaultSubmitTimeout
	}
	if options.BatchTimeout == 0 {
		options.BatchTimeout = defaultBatchTimeout
	}
	if options.BatchMaxSize == 0 {
		options.BatchMaxSize = 1000
	}
	if options.BatchMaxSizeBytes == 0 {
		options.BatchMaxSizeBytes = 100000
	}
	if options.MaxSize == 0 {
		options.MaxSize = 10000
	}
	if options.RequestMaxBytes == 0 {
		options.RequestMaxBytes = defaultMaxBytes
	}

	rp := &Pool{
		logger:    log,
		inspector: inspector,
		semaphore: semaphore.NewWeighted(int64(options.MaxSize)),
		options:   options,
	}

	rp.start()

	return rp
}

func (rp *Pool) start() {
	rp.batchStore = rp.createBatchStore()
	rp.pending = rp.createPendingStore()
	rp.pending.Init()
	rp.pending.Start()
}

func (rp *Pool) createPendingStore() *PendingStore {
	return &PendingStore{
		Inspector:             rp.inspector,
		ReqIDGCInterval:       rp.options.AutoRemoveTimeout / 4,
		ReqIDLifetime:         rp.options.AutoRemoveTimeout,
		Time:                  time.NewTicker(time.Second).C,
		StartTime:             time.Now(),
		Logger:                rp.logger,
		SecondStrikeThreshold: rp.options.SecondStrikeThreshold,
		FirstStrikeThreshold:  rp.options.FirstStrikeThreshold,
		OnDelete: func(key string) {
			rp.semaphore.Release(1)
		},
		Epoch:                time.Second,
		FirstStrikeCallback:  rp.options.OnFirstStrikeTimeout,
		SecondStrikeCallback: rp.options.OnSecondStrikeTimeout,
	}
}

func (rp *Pool) createBatchStore() *BatchStore {
	return NewBatchStore(rp.options.BatchMaxSize, rp.options.BatchMaxSizeBytes, func(key string) {
		rp.semaphore.Release(1)
	}, rp.logger)
}

// Submit a request into the pool, returns an error when request is already in the pool
func (rp *Pool) Submit(request []byte) error {
	rp.lock.RLock()
	defer rp.lock.RUnlock()

	if rp.isClosed() || rp.isStopped() {
		return errors.Errorf("pool halted or closed, request rejected")
	}

	if uint64(len(request)) > rp.options.RequestMaxBytes {
		return fmt.Errorf(
			"submitted request (%d) is bigger than request max bytes (%d)",
			len(request),
			rp.options.RequestMaxBytes,
		)
	}

	reqID := rp.inspector.RequestID(request)

	ctx, cancel := context.WithTimeout(context.Background(), rp.options.SubmitTimeout)
	defer cancel()

	if err := rp.semaphore.Acquire(ctx, 1); err != nil {
		rp.logger.Warnf("timed out enqueuing request %s to pool", reqID)
		return fmt.Errorf("timed out enqueuing request %s to pool", reqID)
	}

	if !rp.isBatchingEnabled() {
		return rp.submitToPendingStore(reqID, request)
	}

	return rp.submitToBatchStore(reqID, request)
}

func (rp *Pool) submitToPendingStore(reqID string, request []byte) error {
	rp.logger.Debugf("submitting request %s to pending store", reqID)
	err := rp.pending.Submit(request)
	if err != nil {
		rp.semaphore.Release(1)
		rp.logger.Debugf("request %s has been already added to the pool", reqID)
		return err
	}
	rp.logger.Debugf("submitted request %s to pending store", reqID)
	return nil
}

func (rp *Pool) submitToBatchStore(reqID string, request []byte) error {
	rp.logger.Debugf("submitting request %s to batch store", reqID)

	reqCopy := make([]byte, len(request))
	copy(reqCopy, request)

	reqItem := &requestItem{
		request: reqCopy,
	}

	inserted := rp.batchStore.Insert(reqID, reqItem, uint32(len(request)))
	if !inserted {
		rp.semaphore.Release(1)
		rp.logger.Debugf("request %s has been already added to the pool", reqID)
		return nil
	}

	rp.logger.Debugf("submitted request %s to batch store", reqID)
	return nil
}

// NextRequests returns the next requests to be batched.
func (rp *Pool) NextRequests() [][]byte {
	rp.lock.RLock()
	defer rp.lock.RUnlock()

	if rp.isClosed() || rp.isStopped() {
		rp.logger.Warnf("pool halted or closed, returning nil")
		return nil
	}

	if !rp.isBatchingEnabled() {
		rp.logger.Warnf("NextRequests is called when batching is not enabled")
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), rp.options.BatchTimeout)
	defer cancel()
	requests := rp.batchStore.Fetch(ctx)

	size := len(requests)

	rawRequests := make([][]byte, size)
	for i := 0; i < size; i++ {
		rawRequests[i] = requests[i].(*requestItem).request
	}

	rp.semaphore.Release(int64(size))

	return rawRequests
}

func (rp *Pool) RemoveRequests(requestsIDs ...string) {
	rp.lock.RLock()
	defer rp.lock.RUnlock()

	if !rp.isBatchingEnabled() {
		rp.pending.RemoveRequests(requestsIDs...)
		return
	}

	// No need to remove requests from batch store
}

func (rp *Pool) Prune(predicate func([]byte) error) {
	rp.lock.RLock()
	defer rp.lock.RUnlock()

	requestsToRemove := make([]string, 0, rp.options.MaxSize)
	if rp.isBatchingEnabled() {
		rp.batchStore.ForEach(func(_, v interface{}) {
			req := v.(*requestItem).request
			if predicate(req) != nil {
				requestsToRemove = append(requestsToRemove, rp.inspector.RequestID(req))
			}
		})
		for _, requestID := range requestsToRemove {
			rp.batchStore.Remove(requestID)
		}
	} else {
		requests := rp.pending.GetAllRequests(rp.options.MaxSize)
		for _, req := range requests {
			if predicate(req) != nil {
				requestsToRemove = append(requestsToRemove, rp.inspector.RequestID(req))
			}
		}
		rp.pending.RemoveRequests(requestsToRemove...)
	}
}

// Reset resets the pool, with new pool options
func (rp *Pool) Reset(options PoolOptions, batching bool) {

	// TODO finish and test
	// TODO what should happen when sizes change?

	rp.lock.Lock()
	defer rp.lock.Unlock()

	defer atomic.StoreUint32(&rp.stopped, 0)

	rp.Halt()

	if options.MaxSize < rp.options.MaxSize {
		rp.logger.Panicf("The new max size is smaller than the original max size")
	}

	requests := make([][]byte, 0, rp.options.MaxSize)
	batchingWasEnabled := rp.isBatchingEnabled()
	if batchingWasEnabled {
		rp.batchStore.ForEach(func(_, v interface{}) {
			requests = append(requests, v.(*requestItem).request)
		})
	} else {
		rp.pending.Close()
		requests = rp.pending.GetAllRequests(rp.options.MaxSize)
	}

	rp.batchStore = nil
	rp.pending = nil

	if options.SubmitTimeout == 0 {
		options.SubmitTimeout = defaultSubmitTimeout
	}
	if options.BatchTimeout == 0 {
		options.BatchTimeout = defaultBatchTimeout
	}
	if options.BatchMaxSize == 0 {
		options.BatchMaxSize = 1000
	}
	if options.BatchMaxSizeBytes == 0 {
		options.BatchMaxSizeBytes = 100000
	}
	if options.MaxSize == 0 {
		options.MaxSize = 10000
	}
	if options.RequestMaxBytes == 0 {
		options.RequestMaxBytes = defaultMaxBytes
	}

	rp.options = options
	//rp.semaphore = semaphore.NewWeighted(int64(options.MaxSize)) // TODO change size

	rp.setBatching(batching)
	if batching {
		rp.batchStore = rp.createBatchStore()
		for _, req := range requests {
			reqInfo := rp.inspector.RequestID(req)
			if err := rp.submitToBatchStore(reqInfo, req); err != nil {
				rp.logger.Errorf("Could not submit request into batch store; error: %s", err)
				return
			}
		}
	} else {
		rp.pending = rp.createPendingStore()
		rp.pending.Init()
		for _, req := range requests {
			reqInfo := rp.inspector.RequestID(req)
			if err := rp.submitToPendingStore(reqInfo, req); err != nil {
				rp.logger.Errorf("Could not submit request to pending store; error: %s", err)
				return
			}
		}
		rp.pending.Start()
	}

}

// Close closes the pool
func (rp *Pool) Close() {
	rp.lock.Lock()
	defer rp.lock.Unlock()
	atomic.StoreUint32(&rp.closed, 1)
	if rp.pending != nil {
		rp.pending.Close()
	}
	rp.pending = nil
	rp.batchStore = nil
}

func (rp *Pool) isClosed() bool {
	return atomic.LoadUint32(&rp.closed) == 1
}

// Halt stops the callbacks of the first and second strikes.
func (rp *Pool) Halt() {
	atomic.StoreUint32(&rp.stopped, 1)
	if !rp.isBatchingEnabled() {
		rp.pending.Stop()
	}
}

func (rp *Pool) isStopped() bool {
	return atomic.LoadUint32(&rp.stopped) == 1
}

// Restart restarts the pool.
// When batching is set to true the pool is expected to respond to NextRequests.
func (rp *Pool) Restart(batching bool) {
	rp.lock.Lock()
	defer rp.lock.Unlock()

	defer atomic.StoreUint32(&rp.stopped, 0)

	rp.Halt()

	batchingWasEnabled := rp.isBatchingEnabled()

	if batchingWasEnabled && batching {
		// if batching was already enabled there is nothing to do
		return
	}

	if !batchingWasEnabled && !batching {
		// if batching was not enabled anyway just reset timestamps of the pending store
		rp.pending.ResetTimestamps()
		return
	}

	rp.setBatching(batching) // change the batching

	if batchingWasEnabled { // batching was enabled and now it is not
		rp.moveToPendingStore()
		return
	}

	// batching was not enabled but now it is
	rp.moveToBatchStore()
	return

}

func (rp *Pool) setBatching(enabled bool) {
	rp.batchingEnabled = enabled
}

func (rp *Pool) isBatchingEnabled() bool {
	return rp.batchingEnabled
}

func (rp *Pool) moveToPendingStore() {
	requests := make([][]byte, 0, rp.options.MaxSize)
	rp.batchStore.ForEach(func(_, v interface{}) {
		requests = append(requests, v.(*requestItem).request)
	})
	rp.pending = rp.createPendingStore()
	rp.pending.Init()
	for _, req := range requests {
		reqInfo := rp.inspector.RequestID(req)
		if err := rp.submitToPendingStore(reqInfo, req); err != nil {
			rp.logger.Errorf("Could not submit request to pending store; error: %s", err)
			return
		}
	}
	rp.pending.Start()
	rp.batchStore = nil
}

func (rp *Pool) moveToBatchStore() {
	rp.pending.Close()
	requests := rp.pending.GetAllRequests(rp.options.MaxSize)
	rp.batchStore = rp.createBatchStore()
	for _, req := range requests {
		reqInfo := rp.inspector.RequestID(req)
		if err := rp.submitToBatchStore(reqInfo, req); err != nil {
			rp.logger.Errorf("Could not submit request into batch store; error: %s", err)
			return
		}
	}
	rp.pending = nil
}
