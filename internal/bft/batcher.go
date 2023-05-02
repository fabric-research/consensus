// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"context"
	"sync"
	"time"
)

// BatchBuilder implements Batcher
type BatchBuilder struct {
	pool          RequestPool
	submittedChan chan struct{}
	maxMsgCount   int
	maxSizeBytes  uint64
	batchTimeout  time.Duration
	closeChan     chan struct{}
	closeLock     sync.Mutex // Reset and Close may be called by different threads
}

// NewBatchBuilder creates a new BatchBuilder
func NewBatchBuilder(pool RequestPool, submittedChan chan struct{}, maxMsgCount uint64, maxSizeBytes uint64, batchTimeout time.Duration) *BatchBuilder {
	b := &BatchBuilder{
		pool:          pool,
		submittedChan: submittedChan,
		maxMsgCount:   int(maxMsgCount),
		maxSizeBytes:  maxSizeBytes,
		batchTimeout:  batchTimeout,
		closeChan:     make(chan struct{}),
	}
	return b
}

func (b *BatchBuilder) NextBatch() [][]byte {
	ctx, cancel := context.WithTimeout(context.Background(), b.batchTimeout)
	defer cancel()

	return b.pool.NextRequests(ctx)
}

// Close closes the close channel to stop NextBatch
func (b *BatchBuilder) Close() {
	b.closeLock.Lock()
	defer b.closeLock.Unlock()
	select {
	case <-b.closeChan:
		return
	default:
	}
	close(b.closeChan)
}

// Closed returns true if the batcher is closed
func (b *BatchBuilder) Closed() bool {
	select {
	case <-b.closeChan:
		return true
	default:
		return false
	}
}

// Reset reopens the close channel to allow calling NextBatch
func (b *BatchBuilder) Reset() {
	b.closeLock.Lock()
	defer b.closeLock.Unlock()
	b.closeChan = make(chan struct{})
}
