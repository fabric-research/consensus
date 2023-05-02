// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package mem

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SmartBFT-Go/consensus/pkg/api"
	"github.com/SmartBFT-Go/consensus/pkg/types"
	"github.com/pkg/errors"
	"golang.org/x/sync/semaphore"
)

const (
	defaultRequestTimeout                 = 10 * time.Second // for unit tests only
	defaultMaxBytes                       = 100 * 1024       // default max request size would be of size 100Kb
	defaultProcessedGarbageCollectionTime = time.Minute      // TODO: make this configurable
)

var (
	ErrReqAlreadyExists    = fmt.Errorf("request already exists")
	ErrReqAlreadyProcessed = fmt.Errorf("request already processed")
	ErrRequestTooBig       = fmt.Errorf("submitted request is too big")
	ErrSubmitTimeout       = fmt.Errorf("timeout submitting to request pool")
)

//go:generate mockery --dir . --name RequestTimeoutHandler --case underscore --output ./mocks/

// RequestTimeoutHandler defines the methods called by request timeout timers created by time.AfterFunc.
// This interface is implemented by the bft.Controller.
type RequestTimeoutHandler interface {

	// OnRequestTimeout is called when a request timeout expires.
	OnRequestTimeout(request []byte, requestInfo types.RequestInfo)

	// OnLeaderFwdRequestTimeout is called when a leader forwarding timeout expires.
	OnLeaderFwdRequestTimeout(request []byte, requestInfo types.RequestInfo)

	// OnAutoRemoveTimeout is called when a auto-remove timeout expires.
	OnAutoRemoveTimeout(requestInfo types.RequestInfo)
}

// Pool implements requests pool, maintains pool of given size provided during
// construction. In case there are more incoming request than given size it will
// block during submit until there will be place to submit new ones.
type Pool struct {
	Time           <-chan time.Time
	closeC         chan struct{}
	Logger         api.Logger
	TimeoutHandler RequestTimeoutHandler
	ReqInspector   api.RequestInspector

	// time management
	timestamp          time.Time
	lastGCProcessed    *time.Time
	lastGCPending      *time.Time
	lastLeaderFwdScan  *time.Time
	lastCensorshipScan *time.Time

	initialized     bool
	lock            sync.RWMutex
	pending         sync.Map
	Options         PoolOptions
	batchStore      *BatchStore
	cancel          context.CancelFunc
	semaphore       *semaphore.Weighted
	closed          uint32
	stopped         uint32
	processed       *processedRequests
	batchingEnabled uint32
}

type processedRequests struct {
	logger api.Logger
	sync.Map
}

func (p *processedRequests) Exists(key any) bool {
	_, exists := p.Map.Load(key)
	return exists
}

func (p *processedRequests) Store(key string, timestamp time.Time) {
	p.Map.Store(key, timestamp)
}

func (p *processedRequests) GC(now time.Time) {
	t1 := time.Now()
	var cleaned int
	var total int
	p.Map.Range(func(k any, v any) bool {
		total++
		ts := v.(time.Time)
		if now.After(ts.Add(defaultProcessedGarbageCollectionTime)) {
			p.Map.Delete(k)
			cleaned++
		}
		return true
	})

	p.logger.Debugf("Garbage collection of %d out of %d and processed request references older than %d took", cleaned, total, now, time.Since(t1))
}

func (p *processedRequests) Size() int {
	var count int
	p.Map.Range(func(_ any, _ any) bool {
		count++
		return true
	})
	return count
}

// requestItem captures request related information
type requestItem struct {
	request []byte
}

// PoolOptions is the pool configuration
type PoolOptions struct {
	MaxSize           int
	BatchMaxSize      int
	ForwardTimeout    time.Duration
	ComplainTimeout   time.Duration
	AutoRemoveTimeout time.Duration
	RequestMaxBytes   uint64
	SubmitTimeout     time.Duration
}

func (rp *Pool) Init() {
	defer func() {
		rp.initialized = true
	}()

	rp.setDefaults()

	rp.semaphore = semaphore.NewWeighted(int64(rp.Options.MaxSize))
	rp.processed = new(processedRequests)
	rp.processed.logger = rp.Logger
	rp.closeC = make(chan struct{})

	rp.batchStore = NewBatchStore(rp.Options.MaxSize, rp.Options.BatchMaxSize, func(key string) {
		rp.lock.RLock()
		now := rp.timestamp
		rp.lock.RUnlock()

		rp.processed.Store(key, now)
		rp.semaphore.Release(1)
	})

	go rp.manageTimestamps()
}

func (rp *Pool) setDefaults() {
	if rp.Options.ForwardTimeout == 0 {
		rp.Options.ForwardTimeout = defaultRequestTimeout
	}
	if rp.Options.ComplainTimeout == 0 {
		rp.Options.ComplainTimeout = defaultRequestTimeout
	}
	if rp.Options.AutoRemoveTimeout == 0 {
		rp.Options.AutoRemoveTimeout = defaultRequestTimeout
	}
	if rp.Options.RequestMaxBytes == 0 {
		rp.Options.RequestMaxBytes = defaultMaxBytes
	}
	if rp.Options.SubmitTimeout == 0 {
		rp.Options.SubmitTimeout = defaultRequestTimeout
	}
	if rp.Options.BatchMaxSize == 0 {
		rp.Options.BatchMaxSize = 1000
	}
	if rp.Options.MaxSize == 0 {
		rp.Options.MaxSize = 10000
	}
}

func (rp *Pool) SetBatching(enabled bool) {
	rp.batchStore.SetBatching(enabled)
	if enabled {
		atomic.StoreUint32(&rp.batchingEnabled, 1)
		// Pour all requests into ourselves as we're the leader now
		rp.pending.Range(func(k any, v any) bool {
			req := v.(*pendingRequest)
			rp.Submit(req.request)
			rp.pending.Delete(k)
			return true
		})
	} else {
		atomic.StoreUint32(&rp.batchingEnabled, 0)
	}
}

func (rp *Pool) progressTime() {
	select {
	case now := <-rp.Time:
		rp.lock.Lock()
		defer rp.lock.Unlock()

		rp.timestamp = now

	case <-rp.closeC:
	}
}

func (rp *Pool) periodicalScan(now time.Time, lastScan *time.Time, period time.Duration, scan func(time.Time), text string) {
	var timeSinceLastScan time.Duration

	if lastScan != nil {
		timeSinceLastScan = now.Sub(*lastScan)
	} else {
		*lastScan = now
		return
	}

	if timeSinceLastScan > 0 && timeSinceLastScan <= period {
		return
	}

	rp.Logger.Infof("%s, %v since previous scan", text, now.Sub(*lastScan))
	scan(now)
	*lastScan = now
}

func (rp *Pool) garbageCollectProcessed(now time.Time) {
	rp.periodicalScan(now, rp.lastGCProcessed, defaultProcessedGarbageCollectionTime, func(time.Time) {
		rp.processed.GC(now)
	}, "Garbage collecting processed requests")
}

func (rp *Pool) manageTimestamps() {
	for {

		rp.lock.RLock()
		now := rp.timestamp
		rp.lock.RUnlock()

		// Make sure we don't get a timer restart in between the scans
		rp.lock.Lock()

		rp.garbageCollectProcessed(now)
		rp.garbageCollectPending(now)
		rp.forwardToLeaderIgnoredRequests(now)
		rp.detectCensorship(now)

		// Make sure we don't get a timer restart in between the scans
		rp.lock.Unlock()
	}
}

func (rp *Pool) detectCensorship(now time.Time) {

	rp.periodicalScan(now, rp.lastCensorshipScan, rp.Options.ComplainTimeout/4, func(time.Time) {

		var count int

		var requestInfo types.RequestInfo
		var request []byte

		rp.pending.Range(func(k any, v any) bool {
			req := v.(*pendingRequest)

			if req.forwardTime.IsZero() {
				return true
			}

			if req.forwardTime.Add(rp.Options.ComplainTimeout).Before(now) {
				return true
			}

			if !req.complainedAt.IsZero() {
				return true
			}

			count++
			requestInfo = req.ri
			request = req.request

			req.complainedAt = now

			return true
		})

		if count == 0 {
			return
		}

		rp.Logger.Infof("Complained about %d forwarded requests", count)
		rp.TimeoutHandler.OnLeaderFwdRequestTimeout(request, requestInfo)

	}, "Detecting censorship")

}

func (rp *Pool) forwardToLeaderIgnoredRequests(now time.Time) {

	rp.periodicalScan(now, rp.lastLeaderFwdScan, rp.Options.ForwardTimeout/3, func(time.Time) {
		var count int
		var forwarding int

		var prevPendingRequest *pendingRequest

		rp.pending.Range(func(k any, v any) bool {
			count++
			req := v.(*pendingRequest)

			if req.arriveTime.Add(rp.Options.ForwardTimeout).Before(now) {
				return true
			}

			if !req.forwardTime.IsZero() {
				return true
			}

			forwarding++

			req.forwardTime = now
			if prevPendingRequest == nil {
				prevPendingRequest = req
			} else {
				req.next = prevPendingRequest
				prevPendingRequest = req
			}

			return true
		})

		rp.Logger.Infof("Forwarded to leader %d out of %d", forwarding, count, "pending requests")

		go func() {
			for prevPendingRequest != nil {
				if _, exists := rp.pending.Load(prevPendingRequest.ri.ID); exists {
					rp.TimeoutHandler.OnRequestTimeout(prevPendingRequest.request, prevPendingRequest.ri)
				}
				prevPendingRequest = prevPendingRequest.next
			}
		}()
	}, "Forwarding to leader ignored requests")

}

func (rp *Pool) garbageCollectPending(now time.Time) {
	rp.periodicalScan(now, rp.lastGCPending, rp.Options.AutoRemoveTimeout/4, func(time.Time) {
		var count int
		var removed int

		t1 := time.Now()

		rp.pending.Range(func(k any, v any) bool {
			count++
			req := v.(*pendingRequest)
			if req.arriveTime.Add(rp.Options.AutoRemoveTimeout).After(now) {
				if _, loaded := rp.pending.LoadAndDelete(k); loaded {
					rp.semaphore.Release(1)
					removed++
				}
			}
			return true
		})

		fmt.Println("Garbage collected", removed, "out of", count, "pending requests in", time.Since(t1))
	}, "Garbage collecting pending requests")
}

// ChangeTimeouts changes the timeout of the pool
func (rp *Pool) ChangeTimeouts(th RequestTimeoutHandler, options PoolOptions) {

	if atomic.LoadUint32(&rp.stopped) == 0 {
		rp.Logger.Errorf("Trying to change timeouts but the pool is not stopped")
		return
	}

	if options.ForwardTimeout == 0 {
		options.ForwardTimeout = defaultRequestTimeout
	}
	if options.ComplainTimeout == 0 {
		options.ComplainTimeout = defaultRequestTimeout
	}
	if options.AutoRemoveTimeout == 0 {
		options.AutoRemoveTimeout = defaultRequestTimeout
	}

	rp.Options.ForwardTimeout = options.ForwardTimeout
	rp.Options.ComplainTimeout = options.ComplainTimeout
	rp.Options.AutoRemoveTimeout = options.AutoRemoveTimeout

	rp.TimeoutHandler = th

	rp.Logger.Debugf("Changed pool timeouts")
}

func (rp *Pool) isClosed() bool {
	return atomic.LoadUint32(&rp.closed) == 1
}

func (rp *Pool) Clear() {
	panic("should not have been called")
}

type pendingRequest struct {
	request      []byte
	ri           types.RequestInfo
	arriveTime   time.Time
	next         *pendingRequest
	forwardTime  time.Time
	complainedAt time.Time
}

func (pr *pendingRequest) reset(now time.Time) {
	pr.complainedAt = time.Time{}
	pr.forwardTime = time.Time{}
	pr.arriveTime = now
	pr.next = nil
}

// Submit a request into the pool, returns an error when request is already in the pool
func (rp *Pool) Submit(request []byte) error {
	reqInfo := rp.ReqInspector.RequestID(request)
	if rp.isClosed() {
		return errors.Errorf("pool closed, request rejected: %s", reqInfo)
	}

	if rp.processed.Exists(reqInfo.ID) {
		rp.Logger.Debugf("request %s already processed", reqInfo)
		return nil
	}

	rp.lock.RLock()
	now := rp.timestamp
	rp.lock.RUnlock()

	if atomic.LoadUint32(&rp.batchingEnabled) == 0 {
		ctx, cancel := context.WithTimeout(context.Background(), rp.Options.SubmitTimeout)
		defer cancel()

		if err := rp.semaphore.Acquire(ctx, 1); err != nil {
			rp.Logger.Warnf("Timed out enqueuing request %s to pool", reqInfo.ID)
			return nil
		}

		_, existed := rp.pending.LoadOrStore(reqInfo.ID, &pendingRequest{
			request:    request,
			arriveTime: now,
			ri:         reqInfo,
		})
		if existed {
			rp.semaphore.Release(1)
			rp.Logger.Debugf("request %s has been already added to the pool", reqInfo)
			return ErrReqAlreadyExists
		}

		rp.processed.Store(reqInfo.ID, now)

		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), rp.Options.SubmitTimeout)
	defer cancel()

	if err := rp.semaphore.Acquire(ctx, 1); err != nil {
		rp.Logger.Warnf("Timed out enqueuing request %s to pool", reqInfo.ID)
		return nil
	}

	reqCopy := make([]byte, len(request))
	copy(reqCopy, request)

	reqItem := &requestItem{
		request: reqCopy,
	}

	inserted := rp.batchStore.Insert(reqInfo.ID, reqItem)
	if !inserted {
		rp.semaphore.Release(1)
		rp.Logger.Debugf("request %s has been already added to the pool", reqInfo)
		return ErrReqAlreadyExists
	}

	rp.processed.Store(reqInfo.ID, now)

	rp.Logger.Debugf("Request %s submitted; started a timeout: %s", reqInfo, rp.Options.ForwardTimeout)

	return nil
}

// NextRequests returns the next requests to be batched.
// It returns at most maxCount requests, and at most maxSizeBytes, in a newly allocated slice.
// Return variable full indicates that the batch cannot be increased further by calling again with the same arguments.
func (rp *Pool) NextRequests(ctx context.Context) [][]byte {
	latestReturnTime, _ := ctx.Deadline()
	start := time.Now()

	timeout := latestReturnTime.Sub(start)

	var requests []interface{}

	for atomic.LoadUint32(&rp.stopped)+atomic.LoadUint32(&rp.closed) == 0 {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		requests = rp.batchStore.Fetch(ctx)
		cancel()
		if len(requests) == 0 {
			continue
		}
		break
	}

	rawRequests := make([][]byte, len(requests))
	for i := 0; i < len(requests); i++ {
		rawRequests[i] = requests[i].(*requestItem).request
	}

	return rawRequests
}

func (rp *Pool) RemoveRequests(requests ...types.RequestInfo) error {

	rp.lock.RLock()
	now := rp.timestamp
	rp.lock.RUnlock()

	t1 := time.Now()

	if atomic.LoadUint32(&rp.batchingEnabled) == 0 {

		defer func() {
			rp.Logger.Debugf("Follower removed %d in %v", len(requests), time.Since(t1))
		}()

		workerNum := runtime.NumCPU()

		var wg sync.WaitGroup
		wg.Add(workerNum)

		for workerID := 0; workerID < workerNum; workerID++ {
			go func(workerID int) {
				defer wg.Done()

				for i, requestInfo := range requests {
					if i%workerNum != workerID {
						continue
					}
					rp.processed.Store(requestInfo.ID, now)
					_, existed := rp.pending.LoadAndDelete(requestInfo.ID)
					if !existed {
						continue
					}
					rp.semaphore.Release(1)
				}
			}(workerID)
		}

		wg.Wait()

		return nil
	}

	defer func() {
		rp.Logger.Debugf("Leader removed %d in %v", len(requests), time.Since(t1))
	}()

	for _, requestInfo := range requests {
		rp.batchStore.Remove(requestInfo.ID)
	}
	return nil
}

// Close removes all the requests, stops all the timeout timers.
func (rp *Pool) Close() {
	close(rp.closeC)
	atomic.StoreUint32(&rp.closed, 1)
	rp.Clear()
}

// StopTimers stops all the timeout timers attached to the pending requests, and marks the pool as "stopped".
// This which prevents submission of new requests, and renewal of timeouts by timer go-routines that where running
// at the time of the call to StopTimers().
func (rp *Pool) StopTimers() {
	atomic.StoreUint32(&rp.stopped, 1)

	rp.Logger.Debugf("Stopped all timers")
}

// RestartTimers restarts all the timeout timers attached to the pending requests, as RequestForwardTimeout, and re-allows
// submission of new requests.
func (rp *Pool) RestartTimers() {
	rp.lock.Lock()
	defer rp.lock.Unlock()

	rp.pending.Range(func(k any, v any) bool {
		req := v.(*pendingRequest)
		req.reset(rp.timestamp)
		return true
	})
	rp.Logger.Debugf("Restarted all timers")
}
