package request

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/SmartBFT-Go/consensus/internal/bft"
	"github.com/SmartBFT-Go/consensus/pkg/types"
)

var submittedCount uint32

func TestPoolWithoutBatching_SubmitAndRemove(t *testing.T) {
	sugaredLogger := createLogger(t, 0)
	requestInspector := &reqInspector{}

	pool := NewPool(sugaredLogger, requestInspector, PoolOptions{
		FirstStrikeThreshold:  time.Second * 20,
		SecondStrikeThreshold: time.Minute,
		MaxSize:               1000 * 100,
		AutoRemoveTimeout:     time.Second * 10,
		SubmitTimeout:         time.Second * 10,
		OnFirstStrikeTimeout: func(_ []byte) {
			panic("timed out on request")
		},
	})

	defer pool.Close()

	pool.Restart(false)

	workerNum := runtime.NumCPU()
	workerPerWorker := 10000

	var wg sync.WaitGroup
	wg.Add(workerNum)

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

	wg.Wait()

	var requestsIDs []string

	for worker := 0; worker < workerNum; worker++ {
		for i := 0; i < workerPerWorker; i++ {
			req := make([]byte, 8)
			binary.BigEndian.PutUint32(req, uint32(worker))
			binary.BigEndian.PutUint32(req[4:], uint32(i))
			requestsIDs = append(requestsIDs, requestInspector.RequestID(req))
		}
	}

	pool.RemoveRequests(requestsIDs...)

	fmt.Println(time.Since(t1))

}

func TestPoolWithBatching_SubmitAndRemove(t *testing.T) {
	sugaredLogger := createLogger(t, 0)
	requestInspector := &reqInspector{}

	pool := NewPool(sugaredLogger, requestInspector, PoolOptions{
		FirstStrikeThreshold:  time.Second * 20,
		SecondStrikeThreshold: time.Minute,
		MaxSize:               1000 * 100,
		BatchMaxSize:          100,
		BatchMaxSizeBytes:     100 * 100,
		AutoRemoveTimeout:     time.Second * 10,
		SubmitTimeout:         time.Second * 10,
		BatchTimeout:          time.Second,
	})

	defer pool.Close()

	pool.Restart(true)

	workerNum := runtime.NumCPU()
	workerPerWorker := 10000

	var wg sync.WaitGroup
	wg.Add(workerNum)

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

	wg.Wait()

	var requestsIDs []string

	for worker := 0; worker < workerNum; worker++ {
		for i := 0; i < workerPerWorker; i++ {
			req := make([]byte, 8)
			binary.BigEndian.PutUint32(req, uint32(worker))
			binary.BigEndian.PutUint32(req[4:], uint32(i))
			requestsIDs = append(requestsIDs, requestInspector.RequestID(req))
		}
	}

	pool.RemoveRequests(requestsIDs...)

	fmt.Println(time.Since(t1))
}

func TestPoolWithBatching_SubmitBatchAndRemove(t *testing.T) {
	sugaredLogger := createLogger(t, 0)
	requestInspector := &reqInspector{}

	pool := NewPool(sugaredLogger, requestInspector, PoolOptions{
		FirstStrikeThreshold:  time.Second * 20,
		SecondStrikeThreshold: time.Minute,
		MaxSize:               1000 * 100,
		BatchMaxSize:          100,
		BatchMaxSizeBytes:     100 * 100,
		AutoRemoveTimeout:     time.Second * 10,
		SubmitTimeout:         time.Second * 10,
		BatchTimeout:          time.Millisecond,
	})

	defer pool.Close()

	pool.Restart(true)

	workerNum := runtime.NumCPU()
	workerPerWorker := 100000
	fmt.Printf("workerNum: %d; workerPerWorker: %d\n", workerNum, workerPerWorker)

	var wg sync.WaitGroup
	wg.Add(workerNum)

	var batches int

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

	for {
		requests := pool.NextRequests()
		if len(requests) == 0 {
			break
		}
		batches++
		var requestsIDs []string
		for _, req := range requests {
			requestsIDs = append(requestsIDs, requestInspector.RequestID(req))
		}
		pool.RemoveRequests(requestsIDs...)
	}

	wg.Wait()

	since := time.Since(t1)
	fmt.Println(since, batches)
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
	pool := bft.NewPool(sugaredLogger, requestInspector, requestTimeoutHandler, bft.PoolOptions{QueueSize: 1000 * 100, ForwardTimeout: time.Second * 20}, submittedChan)

	defer pool.Close()

	workerNum := runtime.NumCPU()
	workerPerWorker := 10000

	var wg sync.WaitGroup
	wg.Add(workerNum)

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

	wg.Wait()

	var requestsInfo []types.RequestInfo

	for worker := 0; worker < workerNum; worker++ {
		for i := 0; i < workerPerWorker; i++ {
			req := make([]byte, 8)
			binary.BigEndian.PutUint32(req, uint32(worker))
			binary.BigEndian.PutUint32(req[4:], uint32(i))
			requestsInfo = append(requestsInfo, requestInspector.RequestID(req))
		}
	}

	for _, req := range requestsInfo {
		pool.RemoveRequest(req)
	}

	fmt.Println(time.Since(t1))

}

func TestOldRequestsPoolAndBatcher_SubmitBatchAndRemove(t *testing.T) {
	sugaredLogger := createLogger(t, 0)
	requestInspector := &requestInspectorWithClientID{}
	requestTimeoutHandler := &requestTimeoutHandler{}
	submittedChan := make(chan struct{}, 1)
	pool := bft.NewPool(sugaredLogger, requestInspector, requestTimeoutHandler, bft.PoolOptions{QueueSize: 1000 * 100, ForwardTimeout: time.Second * 20}, submittedChan)
	batcher := bft.NewBatchBuilder(pool, submittedChan, 100, 100*100, time.Millisecond)

	defer pool.Close()
	defer batcher.Close()

	workerNum := runtime.NumCPU()
	workerPerWorker := 100000
	fmt.Printf("workerNum: %d; workerPerWorker: %d\n", workerNum, workerPerWorker)

	var wg sync.WaitGroup
	wg.Add(workerNum)

	var batches int

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

	for {
		requests := batcher.NextBatch()
		if len(requests) == 0 {
			break
		}
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
	fmt.Println(since, batches)

}
