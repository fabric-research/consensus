package request

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SmartBFT-Go/consensus/internal/bft"
	"github.com/SmartBFT-Go/consensus/pkg/types"
)

var (
	FirstStrikeTimeout  = time.Second * 30
	SecondStrikeTimeout = time.Minute
	AutoRemoveTimeout   = time.Minute * 2
	SubmitTimeout       = time.Second * 10
	MaxSize             = uint64(1000000)

	workerPerWorker = 100000
	workerNum       = runtime.NumCPU()

	BatchTimeout      = time.Second
	BatchMaxSize      = uint32(100)
	BatchMaxSizeBytes = uint32(100 * 100)
)

func TestPoolWithoutBatching_SubmitAndRemove(t *testing.T) {
	sugaredLogger := createLogger(t, 0)
	requestInspector := &reqInspector{}

	pool := NewPool(sugaredLogger, requestInspector, PoolOptions{
		FirstStrikeThreshold:  FirstStrikeTimeout,
		SecondStrikeThreshold: SecondStrikeTimeout,
		AutoRemoveTimeout:     AutoRemoveTimeout,
		SubmitTimeout:         SubmitTimeout,
		MaxSize:               MaxSize,
		OnFirstStrikeTimeout: func(_ []byte) {
			panic("timed out on request")
		},
	})

	defer pool.Close()

	pool.Restart(false)

	var wg sync.WaitGroup
	wg.Add(workerNum)

	var requestsIDs []string

	for worker := 0; worker < workerNum; worker++ {
		for i := 0; i < workerPerWorker; i++ {
			req := make([]byte, 8)
			binary.BigEndian.PutUint32(req, uint32(worker))
			binary.BigEndian.PutUint32(req[4:], uint32(i))
			requestsIDs = append(requestsIDs, requestInspector.RequestID(req))
		}
	}

	var submitted uint32

	t1 := time.Now()

	for worker := 0; worker < workerNum; worker++ {
		go func(worker int) {
			defer wg.Done()

			for i := 0; i < workerPerWorker; i++ {
				req := make([]byte, 8)
				binary.BigEndian.PutUint32(req, uint32(worker))
				binary.BigEndian.PutUint32(req[4:], uint32(i))
				if pool.Submit(req) == nil {
					atomic.AddUint32(&submitted, 1)
				}
			}
		}(worker)
	}

	pool.RemoveRequests(requestsIDs...)

	wg.Wait()

	since := time.Since(t1)
	fmt.Println(since, submitted)

}

func TestPoolWithBatching_SubmitBatchAndRemove(t *testing.T) {
	sugaredLogger := createLogger(t, 0)
	requestInspector := &reqInspector{}

	pool := NewPool(sugaredLogger, requestInspector, PoolOptions{
		FirstStrikeThreshold:  FirstStrikeTimeout,
		SecondStrikeThreshold: SecondStrikeTimeout,
		AutoRemoveTimeout:     AutoRemoveTimeout,
		SubmitTimeout:         SubmitTimeout,
		MaxSize:               MaxSize,
		BatchMaxSize:          BatchMaxSize,
		BatchMaxSizeBytes:     BatchMaxSizeBytes,
		BatchTimeout:          BatchTimeout,
	})

	defer pool.Close()

	pool.Restart(true)

	var wg sync.WaitGroup
	wg.Add(workerNum)

	var batches int
	var batchedRequests int

	t1 := time.Now()

	for worker := 0; worker < workerNum; worker++ {
		go func(worker int) {
			defer wg.Done()

			for i := 0; i < workerPerWorker; i++ {
				req := make([]byte, 8)
				binary.BigEndian.PutUint32(req, uint32(worker))
				binary.BigEndian.PutUint32(req[4:], uint32(i))
				pool.Submit(req)
			}
		}(worker)
	}

	for batchedRequests < ((workerNum * workerPerWorker) - int(BatchMaxSize)) {
		requests := pool.NextRequests()
		if len(requests) == 0 {
			break
		}
		batches++
		batchedRequests += len(requests)
		var requestsIDs []string
		for _, req := range requests {
			requestsIDs = append(requestsIDs, requestInspector.RequestID(req))
		}
		pool.RemoveRequests(requestsIDs...)
	}

	wg.Wait()

	since := time.Since(t1)
	fmt.Println(since, batches, batchedRequests)
}

func TestPoolBoth(t *testing.T) {
	sugaredLogger := createLogger(t, 0)
	requestInspector := &reqInspector{}

	primaryPool := NewPool(sugaredLogger, requestInspector, PoolOptions{
		FirstStrikeThreshold:  FirstStrikeTimeout,
		SecondStrikeThreshold: SecondStrikeTimeout,
		AutoRemoveTimeout:     AutoRemoveTimeout,
		SubmitTimeout:         SubmitTimeout,
		MaxSize:               MaxSize,
		BatchMaxSize:          BatchMaxSize,
		BatchMaxSizeBytes:     BatchMaxSizeBytes,
		BatchTimeout:          BatchTimeout,
	})

	secondaryPool := NewPool(sugaredLogger, requestInspector, PoolOptions{
		FirstStrikeThreshold:  FirstStrikeTimeout,
		SecondStrikeThreshold: SecondStrikeTimeout,
		AutoRemoveTimeout:     AutoRemoveTimeout,
		SubmitTimeout:         SubmitTimeout,
		MaxSize:               MaxSize,
		OnFirstStrikeTimeout: func(_ []byte) {
			panic("timed out on request")
		},
	})

	defer primaryPool.Close()
	defer secondaryPool.Close()

	primaryPool.Restart(true)
	secondaryPool.Restart(false)

	var wg sync.WaitGroup
	wg.Add(workerNum)

	var batches int
	var batchedRequests int

	t1 := time.Now()

	for worker := 0; worker < workerNum; worker++ {
		go func(worker int) {
			defer wg.Done()

			for i := 0; i < workerPerWorker; i++ {
				req := make([]byte, 8)
				binary.BigEndian.PutUint32(req, uint32(worker))
				binary.BigEndian.PutUint32(req[4:], uint32(i))
				primaryPool.Submit(req)
				secondaryPool.Submit(req)
			}
		}(worker)
	}

	for batchedRequests < ((workerNum * workerPerWorker) - int(BatchMaxSize)) {
		requests := primaryPool.NextRequests()
		if len(requests) == 0 {
			break
		}
		batches++
		batchedRequests += len(requests)
		var requestsIDs []string
		for _, req := range requests {
			requestsIDs = append(requestsIDs, requestInspector.RequestID(req))
		}
		primaryPool.RemoveRequests(requestsIDs...)
		secondaryPool.RemoveRequests(requestsIDs...)
	}

	wg.Wait()

	since := time.Since(t1)
	fmt.Println(since, batches, batchedRequests)
}

type requestInspectorWithClientID struct{}

func (ins *requestInspectorWithClientID) RequestID(req []byte) types.RequestInfo {
	requestInspector := &reqInspector{}
	id := requestInspector.RequestID(req)
	return types.RequestInfo{
		ID: id, ClientID: id,
	}
}

type requestTimeoutHandler struct{}

func (h *requestTimeoutHandler) OnRequestTimeout(request []byte, requestInfo types.RequestInfo) {
	panic("timed out on request")
}

func (h *requestTimeoutHandler) OnLeaderFwdRequestTimeout(request []byte, requestInfo types.RequestInfo) {
}

func (h *requestTimeoutHandler) OnAutoRemoveTimeout(requestInfo types.RequestInfo) {}

func TestOldRequestsPool_SubmitAndRemove(t *testing.T) {
	sugaredLogger := createLogger(t, 0)
	requestInspector := &requestInspectorWithClientID{}
	requestTimeoutHandler := &requestTimeoutHandler{}
	submittedChan := make(chan struct{}, 1)
	pool := bft.NewPool(sugaredLogger, requestInspector, requestTimeoutHandler, bft.PoolOptions{
		QueueSize:         int64(MaxSize),
		ForwardTimeout:    FirstStrikeTimeout,
		ComplainTimeout:   SecondStrikeTimeout,
		AutoRemoveTimeout: AutoRemoveTimeout,
		SubmitTimeout:     SubmitTimeout,
	}, submittedChan)

	defer pool.Close()

	var wg sync.WaitGroup
	wg.Add(workerNum)

	var requestsInfo []types.RequestInfo

	for worker := 0; worker < workerNum; worker++ {
		for i := 0; i < workerPerWorker; i++ {
			req := make([]byte, 8)
			binary.BigEndian.PutUint32(req, uint32(worker))
			binary.BigEndian.PutUint32(req[4:], uint32(i))
			requestsInfo = append(requestsInfo, requestInspector.RequestID(req))
		}
	}

	var submitted uint32

	t1 := time.Now()

	for worker := 0; worker < workerNum; worker++ {
		go func(worker int) {
			defer wg.Done()

			for i := 0; i < workerPerWorker; i++ {
				req := make([]byte, 8)
				binary.BigEndian.PutUint32(req, uint32(worker))
				binary.BigEndian.PutUint32(req[4:], uint32(i))
				if pool.Submit(req) == nil {
					atomic.AddUint32(&submitted, 1)
				}
			}
		}(worker)
	}

	for _, req := range requestsInfo {
		pool.RemoveRequest(req)
	}

	wg.Wait()

	since := time.Since(t1)
	fmt.Println(since, submitted)

}

func TestOldRequestsPoolAndBatcher_SubmitBatchAndRemove(t *testing.T) {
	sugaredLogger := createLogger(t, 0)
	requestInspector := &requestInspectorWithClientID{}
	requestTimeoutHandler := &requestTimeoutHandler{}
	submittedChan := make(chan struct{}, 1)
	pool := bft.NewPool(sugaredLogger, requestInspector, requestTimeoutHandler, bft.PoolOptions{
		QueueSize:         int64(MaxSize),
		ForwardTimeout:    FirstStrikeTimeout,
		ComplainTimeout:   SecondStrikeTimeout,
		AutoRemoveTimeout: AutoRemoveTimeout,
		SubmitTimeout:     SubmitTimeout,
	}, submittedChan)
	batcher := bft.NewBatchBuilder(pool, submittedChan, uint64(BatchMaxSize), uint64(BatchMaxSizeBytes), BatchTimeout)

	defer pool.Close()
	defer batcher.Close()

	var wg sync.WaitGroup
	wg.Add(workerNum)

	var batches int
	var batchedRequests int

	t1 := time.Now()

	for worker := 0; worker < workerNum; worker++ {
		go func(worker int) {
			defer wg.Done()

			for i := 0; i < workerPerWorker; i++ {
				req := make([]byte, 8)
				binary.BigEndian.PutUint32(req, uint32(worker))
				binary.BigEndian.PutUint32(req[4:], uint32(i))
				pool.Submit(req)
			}
		}(worker)
	}

	for batchedRequests < ((workerNum * workerPerWorker) - int(BatchMaxSize)) {
		requests := batcher.NextBatch()
		if len(requests) == 0 {
			break
		}
		batchedRequests += len(requests)
		batches++
		var requestsInfo []types.RequestInfo
		for _, req := range requests {
			requestsInfo = append(requestsInfo, requestInspector.RequestID(req))
		}
		for _, req := range requestsInfo {
			pool.RemoveRequest(req)
		}
	}

	wg.Wait()

	since := time.Since(t1)
	fmt.Println(since, batches, batchedRequests)

}

func TestOldTwoRequestsPoolAndBatcher(t *testing.T) {
	sugaredLogger := createLogger(t, 0)
	requestInspector := &requestInspectorWithClientID{}
	requestTimeoutHandler := &requestTimeoutHandler{}
	submittedChan := make(chan struct{}, 1)
	pool1 := bft.NewPool(sugaredLogger, requestInspector, requestTimeoutHandler, bft.PoolOptions{
		QueueSize:         int64(MaxSize),
		ForwardTimeout:    FirstStrikeTimeout,
		ComplainTimeout:   SecondStrikeTimeout,
		AutoRemoveTimeout: AutoRemoveTimeout,
		SubmitTimeout:     SubmitTimeout,
	}, submittedChan)
	batcher := bft.NewBatchBuilder(pool1, submittedChan, uint64(BatchMaxSize), uint64(BatchMaxSizeBytes), BatchTimeout)
	pool2 := bft.NewPool(sugaredLogger, requestInspector, requestTimeoutHandler, bft.PoolOptions{
		QueueSize:         int64(MaxSize),
		ForwardTimeout:    FirstStrikeTimeout,
		ComplainTimeout:   SecondStrikeTimeout,
		AutoRemoveTimeout: AutoRemoveTimeout,
		SubmitTimeout:     SubmitTimeout,
	}, submittedChan)

	defer pool1.Close()
	defer pool2.Close()
	defer batcher.Close()

	var wg sync.WaitGroup
	wg.Add(workerNum)

	var batches int
	var batchedRequests int

	t1 := time.Now()

	for worker := 0; worker < workerNum; worker++ {
		go func(worker int) {
			defer wg.Done()

			for i := 0; i < workerPerWorker; i++ {
				req := make([]byte, 8)
				binary.BigEndian.PutUint32(req, uint32(worker))
				binary.BigEndian.PutUint32(req[4:], uint32(i))
				pool1.Submit(req)
				pool2.Submit(req)
			}
		}(worker)
	}

	for batchedRequests < ((workerNum * workerPerWorker) - int(BatchMaxSize)) {
		requests := batcher.NextBatch()
		if len(requests) == 0 {
			break
		}
		batchedRequests += len(requests)
		batches++
		var requestsInfo []types.RequestInfo
		for _, req := range requests {
			requestsInfo = append(requestsInfo, requestInspector.RequestID(req))
		}
		for _, req := range requestsInfo {
			pool1.RemoveRequest(req)
			pool2.RemoveRequest(req)
		}
	}

	wg.Wait()

	since := time.Since(t1)
	fmt.Println(since, batches, batchedRequests)

}
