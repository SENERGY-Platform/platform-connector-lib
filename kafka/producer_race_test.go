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
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
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

// createCounter counts how often a topic was created, to distinguish "created
// once" from "created by every goroutine that published first".
type createCounter struct {
	mux    sync.Mutex
	counts map[string]int
}

func (this *createCounter) create(topic string) error {
	this.mux.Lock()
	defer this.mux.Unlock()
	if this.counts == nil {
		this.counts = map[string]int{}
	}
	this.counts[topic]++
	return nil
}

// TestKnownTopics_ConcurrentFirstPublicationsToDistinctTopics is the case of the
// ticket: a service that publishes to many topics for the first time at the same
// moment, here without a broker in between so that the bookkeeping is hit hard
// enough to fail reliably. An unsynchronized map ends this test with "fatal
// error: concurrent map writes" or a race report, both of which kill the test
// binary instead of failing a single test.
func TestKnownTopics_ConcurrentFirstPublicationsToDistinctTopics(t *testing.T) {
	known := &KnownTopics{}
	counter := &createCounter{}

	const publishers = 64
	const topicsPerPublisher = 50
	start := make(chan struct{})
	wg := &sync.WaitGroup{}
	for i := 0; i < publishers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			for j := 0; j < topicsPerPublisher; j++ {
				topic := fmt.Sprintf("topic-%v-%v", i, j)
				err := known.ensure(topic, func() error { return counter.create(topic) })
				if err != nil {
					t.Errorf("ensure %v: %v", topic, err)
					return
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 0; i < publishers; i++ {
		for j := 0; j < topicsPerPublisher; j++ {
			topic := fmt.Sprintf("topic-%v-%v", i, j)
			if counter.counts[topic] != 1 {
				t.Errorf("topic %v created %v times, want 1", topic, counter.counts[topic])
			}
		}
	}
}

// TestKnownTopics_ParallelFirstPublicationsCreateTheTopicOnce covers the double
// check: the goroutines that waited for the creation must not create the topic a
// second time.
func TestKnownTopics_ParallelFirstPublicationsCreateTheTopicOnce(t *testing.T) {
	known := &KnownTopics{}
	creations := atomic.Int64{}

	const publishers = 32
	start := make(chan struct{})
	wg := &sync.WaitGroup{}
	for i := 0; i < publishers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := known.ensure("topic", func() error {
				creations.Add(1)
				return nil
			})
			if err != nil {
				t.Error(err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if creations.Load() != 1 {
		t.Errorf("topic created %v times, want 1", creations.Load())
	}
}

// TestKnownTopics_RetriesAfterFailedCreation keeps the behavior the produce paths
// rely on: EnsureTopic errors are only logged there, so a topic whose creation
// failed has to be attempted again on the next publication.
func TestKnownTopics_RetriesAfterFailedCreation(t *testing.T) {
	known := &KnownTopics{}
	creations := 0
	createErr := errors.New("broker unreachable")
	create := func() error {
		creations++
		if creations == 1 {
			return createErr
		}
		return nil
	}

	if err := known.ensure("topic", create); !errors.Is(err, createErr) {
		t.Errorf("first publication: got %v, want %v", err, createErr)
	}
	if err := known.ensure("topic", create); err != nil {
		t.Errorf("second publication: %v", err)
	}
	if err := known.ensure("topic", create); err != nil {
		t.Errorf("third publication: %v", err)
	}
	if creations != 2 {
		t.Errorf("%v creation attempts, want 2: one failed, one successful, then none", creations)
	}
}

// TestKnownTopics_SlowCreationDoesNotBlockOtherTopics pins why every topic has
// its own lock: EnsureTopic runs in the produce path, so a creation that waits
// for an unreachable broker must not hold up publications to other topics.
func TestKnownTopics_SlowCreationDoesNotBlockOtherTopics(t *testing.T) {
	known := &KnownTopics{}
	creating := make(chan struct{})
	release := make(chan struct{})
	defer close(release)

	go func() {
		_ = known.ensure("slow-topic", func() error {
			close(creating)
			<-release
			return nil
		})
	}()
	<-creating

	done := make(chan error, 1)
	go func() {
		done <- known.ensure("other-topic", func() error { return nil })
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(10 * time.Second):
		t.Error("publication to another topic waited for the pending topic creation")
	}
}
