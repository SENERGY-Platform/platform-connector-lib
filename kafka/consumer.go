/*
 * Copyright 2019 InfAI (CC SES)
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
	"io"
	"log"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

type ConsumerConfig struct {
	KafkaUrl          string
	GroupId           string
	Topic             string
	MinBytes          int
	MaxBytes          int
	MaxWait           time.Duration
	InitTopic         bool
	TopicConfigMap    map[string][]kafka.ConfigEntry
	AllowOldMessages  bool
	Logger            *slog.Logger
	RestartBackoffMin time.Duration //defaults to time.Second; wait before the first restart of a failed consumer
	RestartBackoffMax time.Duration //defaults to time.Minute; upper bound of the restart wait, and the point at which a disruption counts as lasting
}

func (this *ConsumerConfig) GetLogger() *slog.Logger {
	if this.Logger == nil {
		return slog.Default()
	}
	return this.Logger
}

// NewConsumer starts a consumer that runs until ctx is done. A listener error is
// logged and the message is left uncommitted; a reader error restarts the
// consumer, and errorhandler is only called once that lasts longer than
// RestartBackoffMax.
func NewConsumer(ctx context.Context, config ConsumerConfig, listener func(topic string, msg []byte, time time.Time) error, errorhandler func(err error)) (err error) {
	logger := config.GetLogger()
	logger.Info("start kafka consumer", "topic", config.Topic)
	if config.InitTopic {
		err = InitTopic(config.KafkaUrl, config.TopicConfigMap, config.Topic)
		if err != nil {
			logger.Error("unable to create topic", "topic", config.Topic, "error", err)
			return err
		}
	}
	go runWithRestart(ctx, config, errorhandler, func(progress func()) error {
		return consume(ctx, config, progress, listener)
	})
	return err
}

// consume reads until ctx is done (nil result) or until an error makes the
// reader unusable. The error is returned instead of ending the consumer, so
// runWithRestart can build a new reader, which resumes at the committed offset.
func consume(ctx context.Context, config ConsumerConfig, progress func(), listener func(topic string, msg []byte, time time.Time) error) error {
	logger := config.GetLogger()
	kafkaLogger := slog.NewLogLogger(logger.Handler(), slog.LevelError)
	kafkaLogger.SetPrefix("[KAFKA-ERR] ")
	r := kafka.NewReader(kafka.ReaderConfig{
		CommitInterval:         0, //synchronous commits
		Brokers:                []string{config.KafkaUrl},
		GroupID:                config.GroupId,
		Topic:                  config.Topic,
		MinBytes:               config.MinBytes,
		MaxBytes:               config.MaxBytes,
		MaxWait:                config.MaxWait,
		Logger:                 log.New(io.Discard, "", 0),
		ErrorLogger:            kafkaLogger,
		WatchPartitionChanges:  true,
		PartitionWatchInterval: time.Minute,
	})
	defer func() {
		logger.Info("close kafka consumer", "topic", config.Topic, "result", r.Close())
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			m, err := r.FetchMessage(ctx)
			if err != nil {
				if err == io.EOF || ctx.Err() != nil || errors.Is(err, context.Canceled) {
					logger.Info("kafka consumer closed", "topic", config.Topic)
					return nil
				}
				return fmt.Errorf("while consuming topic: %w", err)
			}
			if !config.AllowOldMessages && time.Now().Sub(m.Time) > 1*time.Hour { //floodgate to prevent old messages to DOS the consumer
				logger.Warn("kafka message older than 1h", "topic", config.Topic, "age", time.Now().Sub(m.Time))
			} else if err = listener(m.Topic, m.Value, m.Time); err != nil {
				//a later commit covers this offset as well, so the message is skipped unless the reader restarts before that
				logger.Error("unable to handle message (no commit)", "topic", config.Topic, "error", err)
				progress()
				continue
			}
			err = r.CommitMessages(ctx, m)
			if err != nil {
				if ctx.Err() != nil || errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("unable to commit message consumption: %w", err)
			}
			progress()
		}
	}
}

// runWithRestart keeps a consumer running until ctx is done. An error ends the
// current run and a new one is started after a growing wait, instead of ending
// the goroutine and leaving a process that stays healthy and ready while it
// consumes nothing. errorhandler is only called once the consumer has been
// failing for longer than RestartBackoffMax: a disruption short enough to be
// bridged stays a warning, a lasting one reaches errorhandler as before,
// including a log.Fatal in it. Same behaviour as service-commons/pkg/kafka.
func runWithRestart(ctx context.Context, config ConsumerConfig, errorhandler func(err error), f func(progress func()) error) {
	logger := config.GetLogger()
	backoffMin := config.RestartBackoffMin
	if backoffMin <= 0 {
		backoffMin = time.Second
	}
	backoffMax := config.RestartBackoffMax
	if backoffMax < backoffMin {
		backoffMax = max(time.Minute, backoffMin)
	}

	wait := backoffMin
	disruptionStart := time.Time{} //zero while the consumer makes progress
	for {
		//progress is called by f after every message that did not break the
		//reader; f runs in this goroutine, so the closure needs no synchronisation
		err := f(func() {
			disruptionStart = time.Time{}
			wait = backoffMin
		})
		if ctx.Err() != nil || err == nil {
			return
		}
		if disruptionStart.IsZero() {
			disruptionStart = time.Now()
		}
		logger.Warn("restart kafka consumer after error",
			"topic", config.Topic,
			"error", err,
			"wait", wait.String(),
			"disrupted-for", time.Since(disruptionStart).String())
		if time.Since(disruptionStart) > backoffMax {
			errorhandler(fmt.Errorf("kafka consumer failing for longer than %v: %w", backoffMax, err))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = min(2*wait, backoffMax)
	}
}
