/*
 * Copyright 2018 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// unreachableWriter produces against a port nothing listens on, so every write
// fails immediately without a broker. The shutdown flag is read before the
// writer is touched, which is what these tests are about.
func unreachableWriter() *kafka.Writer {
	return &kafka.Writer{
		Addr:        kafka.TCP("127.0.0.1:1"),
		BatchSize:   1,
		MaxAttempts: 1,
		ErrorLogger: writerErrorLogger(slog.New(slog.DiscardHandler)),
	}
}

// produceUntilClosed calls both produce entry points in a loop until they report
// the closed producer, and returns false if that never happens.
func produceUntilClosed(producer ProducerInterface, withKey bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var err error
		if withKey {
			err = producer.ProduceWithKey("test", "message", "key")
		} else {
			err = producer.Produce("test", "message")
		}
		if errors.Is(err, errProducerClosed) {
			return true
		}
	}
	return false
}

// assertClosesUnderConcurrentProduce runs the shutdown goroutine of
// PrepareProducerWithConfig against concurrent produce calls. The flag is
// written there and read on every produce call, so a non atomic field makes
// `go test -race` report a data race here.
func assertClosesUnderConcurrentProduce(t *testing.T, producer ProducerInterface, closeOnDone func(ctx context.Context)) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	closeOnDone(ctx)

	const producers = 4
	closedSeen := make([]bool, producers)
	wg := &sync.WaitGroup{}
	for i := range closedSeen {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			closedSeen[i] = produceUntilClosed(producer, i%2 == 0, 10*time.Second)
		}(i)
	}

	time.Sleep(50 * time.Millisecond)
	cancel()
	wg.Wait()

	for i, seen := range closedSeen {
		if !seen {
			t.Errorf("produce call %v never reported the closed producer", i)
		}
	}
}

func TestSyncProducer_ProduceDuringShutdown(t *testing.T) {
	producer := &SyncProducer{
		logger:   slog.New(slog.DiscardHandler),
		producer: unreachableWriter(),
	}
	assertClosesUnderConcurrentProduce(t, producer, producer.closeOnDone)
}

func TestAsyncProducer_ProduceDuringShutdown(t *testing.T) {
	producer := &AsyncProducer{
		logger:   slog.New(slog.DiscardHandler),
		producer: unreachableWriter(),
	}
	assertClosesUnderConcurrentProduce(t, producer, producer.closeOnDone)
}
