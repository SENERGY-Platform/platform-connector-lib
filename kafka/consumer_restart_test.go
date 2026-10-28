/*
 * Copyright 2026 InfAI (CC SES)
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
	"testing"
	"time"
)

// runWithRestart calls f and errorhandler in the calling goroutine, so these
// tests drive it synchronously and need no broker

func restartTestConfig() ConsumerConfig {
	return ConsumerConfig{
		Topic:             "test",
		Logger:            slog.New(slog.DiscardHandler),
		RestartBackoffMin: 10 * time.Millisecond,
		RestartBackoffMax: 50 * time.Millisecond,
	}
}

// a reader error used to end the consumer goroutine, which left the process
// running without a consumer until someone restarted the pod by hand
func TestRunWithRestartBridgesShortDisruption(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	//a maximum far above the 10ms and 20ms of waiting, so a slow runner cannot
	//turn the short disruption into a lasting one
	config := restartTestConfig()
	config.RestartBackoffMax = time.Second
	runs := 0
	handled := []error{}
	runWithRestart(ctx, config, func(err error) {
		handled = append(handled, err)
	}, func(progress func()) error {
		runs++
		if runs <= 2 {
			return errors.New("test error")
		}
		progress()
		cancel()
		return nil
	})
	if runs != 3 {
		t.Error(runs)
	}
	if len(handled) != 0 {
		t.Error(handled)
	}
}

// a lasting disruption reaches errorhandler, and the consumer keeps restarting
// afterwards for an errorhandler that only logs
func TestRunWithRestartReportsLastingDisruption(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	testErr := errors.New("test error")
	handled := []error{}
	runWithRestart(ctx, restartTestConfig(), func(err error) {
		handled = append(handled, err)
		if len(handled) == 2 {
			cancel()
		}
	}, func(progress func()) error {
		return testErr
	})
	if len(handled) != 2 {
		t.Fatal(handled)
	}
	for _, err := range handled {
		if !errors.Is(err, testErr) {
			t.Error(err)
		}
	}
}

// progress ends a disruption: errors that are each followed by a handled
// message never add up to a lasting one, however long the consumer runs
func TestRunWithRestartProgressResetsDisruption(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	config := restartTestConfig()
	start := time.Now()
	handled := []error{}
	runWithRestart(ctx, config, func(err error) {
		handled = append(handled, err)
	}, func(progress func()) error {
		if time.Since(start) > 4*config.RestartBackoffMax {
			cancel()
			return nil
		}
		progress()
		return errors.New("test error")
	})
	if len(handled) != 0 {
		t.Error(handled)
	}
}

func TestRunWithRestartStopsWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runs := 0
	runWithRestart(ctx, restartTestConfig(), func(err error) {
		t.Error("unexpected errorhandler call", err)
	}, func(progress func()) error {
		runs++
		cancel()
		return errors.New("error while shutting down")
	})
	if runs != 1 {
		t.Error(runs)
	}
}
