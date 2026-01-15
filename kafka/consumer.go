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
	"io"
	"log"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

type ConsumerConfig struct {
	KafkaUrl         string
	GroupId          string
	Topic            string
	MinBytes         int
	MaxBytes         int
	MaxWait          time.Duration
	InitTopic        bool
	TopicConfigMap   map[string][]kafka.ConfigEntry
	AllowOldMessages bool
	Logger           *slog.Logger
}

func (this *ConsumerConfig) GetLogger() *slog.Logger {
	if this.Logger == nil {
		return slog.Default()
	}
	return this.Logger
}

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
	go func() {
		defer func() {
			logger.Info("close kafka consumer", "topic", config.Topic, "result", r.Close())
		}()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				m, err := r.FetchMessage(ctx)
				if err == io.EOF || errors.Is(err, context.Canceled) {
					logger.Info("kafka consumer closed", "topic", config.Topic)
					return
				}
				if err != nil {
					logger.Error("while consuming topic", "topic", config.Topic, "error", err)
					errorhandler(err)
					return
				}
				if !config.AllowOldMessages && time.Now().Sub(m.Time) > 1*time.Hour { //floodgate to prevent old messages to DOS the consumer
					logger.Warn("kafka message older than 1h", "topic", config.Topic, "age", time.Now().Sub(m.Time))
					err = r.CommitMessages(ctx, m)
					if err != nil {
						logger.Error("unable to commit message consumption", "topic", config.Topic, "error", err)
						errorhandler(err)
						return
					}
				} else {
					err = listener(m.Topic, m.Value, m.Time)
					if err != nil {
						logger.Error("unable to handle message (no commit)", "topic", config.Topic, "error", err)
					} else {
						err = r.CommitMessages(ctx, m)
						if err != nil {
							logger.Error("unable to commit message consumption", "topic", config.Topic, "error", err)
							errorhandler(err)
							return
						}
					}
				}
			}
		}
	}()
	return err
}
