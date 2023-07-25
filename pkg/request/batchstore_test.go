// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package request

import (
	"encoding/binary"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatch(t *testing.T) {

	requestInspector := &reqInspector{}

	workerNum := runtime.NumCPU()
	workPerWorker := 100000

	b := &batch{m: make(map[any]any, workerNum*workPerWorker*2)}

	assert.False(t, b.isEnqueued())
	b.markEnqueued()
	assert.True(t, b.isEnqueued())

	loaded := make(chan string, workerNum*workPerWorker)

	var wg sync.WaitGroup
	wg.Add(workerNum)

	for worker := 0; worker < workerNum; worker++ {
		go func(worker int) {
			defer wg.Done()

			for i := 0; i < workPerWorker; i++ {
				key := make([]byte, 8)
				binary.BigEndian.PutUint32(key, uint32(worker))
				binary.BigEndian.PutUint32(key[4:], uint32(i))
				keyID := requestInspector.RequestID(key)
				b.Store(keyID.ID, struct{}{})
				loaded <- keyID.ID
			}
		}(worker)
	}

	wg.Wait()
	close(loaded)

	count := 0

	b.Range(func(key, value any) bool {
		count++
		return true
	})

	assert.Equal(t, workerNum*workPerWorker, count)

	wg.Add(workerNum)

	for worker := 0; worker < workerNum; worker++ {
		go func(worker int) {
			defer wg.Done()

			for i := 0; i < workPerWorker; i++ {
				key := make([]byte, 8)
				binary.BigEndian.PutUint32(key, uint32(worker))
				binary.BigEndian.PutUint32(key[4:], uint32(i))
				keyID := requestInspector.RequestID(key)
				if _, ok := b.Load(keyID.ID); !ok {
					break
				}
				b.Delete(keyID.ID)
			}
		}(worker)
	}

	wg.Wait()

	zero := 0

	b.Range(func(key, value any) bool {
		zero++
		return true
	})

	assert.Zero(t, zero)
}
