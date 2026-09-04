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
	"log"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

var Fatal = false
var SlowProducerTimeout time.Duration = 2 * time.Second

var errProducerClosed = errors.New("producer closed")

type ProducerInterface interface {
	Produce(topic string, message string) (err error)
	ProduceWithKey(topic string, message string, key string) (err error)
	ProduceWithTimestamp(topic string, message string, key string, timestamp time.Time) (err error)
}

type SyncProducer struct {
	broker            []string
	logger            *slog.Logger
	producer          *kafka.Writer
	kafkaBootstrapUrl string
	syncIdempotent    bool
	usedTopics        KnownTopics
	partitionsNum     int
	replicationFactor int
	topicConfigMap    map[string][]kafka.ConfigEntry
	initTopic         bool
	isClosed          atomic.Bool
}

type AsyncProducer struct {
	broker            []string
	logger            *slog.Logger
	producer          *kafka.Writer
	kafkaBootstrapUrl string
	usedTopics        KnownTopics
	partitionsNum     int
	replicationFactor int
	topicConfigMap    map[string][]kafka.ConfigEntry
	initTopic         bool
	isClosed          atomic.Bool
}

type Config struct {
	AsyncFlushFrequency time.Duration
	AsyncCompression    kafka.Compression
	SyncCompression     kafka.Compression
	Sync                bool
	SyncIdempotent      bool
	PartitionNum        int
	ReplicationFactor   int
	AsyncFlushMessages  int
	TopicConfigMap      map[string][]kafka.ConfigEntry
	InitTopics          bool
	Logger              *slog.Logger
}

func (this *Config) GetLogger() *slog.Logger {
	if this.Logger == nil {
		return slog.Default()
	}
	return this.Logger
}

// writerErrorLogger routes the internal retry logging of kafka.Writer to WARN:
// the writer logs every failed attempt, most of which are followed by a
// successful retry, which is not something anyone needs to act on.
func writerErrorLogger(logger *slog.Logger) *log.Logger {
	result := slog.NewLogLogger(logger.Handler(), slog.LevelWarn)
	result.SetPrefix("[KAFKA-PRODUCER] ")
	return result
}

// writeMessage retries UnknownTopicOrPartition, because with
// AllowAutoTopicCreation the metadata request that returns this error is also the
// one that makes the broker create the topic, so only the first attempt fails.
// sarama hid the same round trip behind Metadata.Retry (3 attempts, 250ms apart),
// which is where these numbers come from.
func writeMessage(producer *kafka.Writer, msg kafka.Message) (err error) {
	for attempt := 0; attempt <= 3; attempt++ {
		if attempt > 0 {
			time.Sleep(250 * time.Millisecond)
		}
		err = producer.WriteMessages(context.Background(), msg)
		if !errors.Is(err, kafka.UnknownTopicOrPartition) {
			return err
		}
	}
	return err
}

func PrepareProducerWithConfig(ctx context.Context, kafkaBootstrapUrl string, config Config) (result ProducerInterface, err error) {
	broker, err := GetBroker(kafkaBootstrapUrl)
	if err != nil {
		return nil, err
	}
	if len(broker) == 0 {
		return nil, errors.New("missing kafka broker")
	}
	if config.Sync {
		temp := &SyncProducer{
			broker:            broker,
			kafkaBootstrapUrl: kafkaBootstrapUrl,
			syncIdempotent:    config.SyncIdempotent,
			partitionsNum:     config.PartitionNum,
			replicationFactor: config.ReplicationFactor,
			topicConfigMap:    config.TopicConfigMap,
			initTopic:         config.InitTopics,
			logger:            config.GetLogger(),
		}
		requiredAcks := kafka.RequireOne
		if config.SyncIdempotent {
			//kafka-go has no idempotent producer; RequireAll keeps the durability
			//guarantee, but a retried batch can produce duplicates
			requiredAcks = kafka.RequireAll
		}
		temp.producer = &kafka.Writer{
			Addr:                   kafka.TCP(temp.broker...),
			Balancer:               &kafka.Hash{}, //same partition selection as the sarama hash partitioner
			BatchSize:              1,             //flush on every call, otherwise Produce() waits for the batch timeout
			RequiredAcks:           requiredAcks,
			Compression:            config.SyncCompression,
			AllowAutoTopicCreation: true, //sarama requested this in every metadata request; produce paths without InitTopics rely on it
			ErrorLogger:            writerErrorLogger(temp.logger),
		}
		result = temp
		temp.closeOnDone(ctx)
	} else {
		temp := &AsyncProducer{
			broker:            broker,
			kafkaBootstrapUrl: kafkaBootstrapUrl,
			partitionsNum:     config.PartitionNum,
			replicationFactor: config.ReplicationFactor,
			topicConfigMap:    config.TopicConfigMap,
			initTopic:         config.InitTopics,
			logger:            config.GetLogger(),
		}
		temp.producer = &kafka.Writer{
			Addr:                   kafka.TCP(temp.broker...),
			Balancer:               &kafka.Hash{},
			Async:                  true,
			BatchTimeout:           config.AsyncFlushFrequency, //0 leaves the kafka-go default of 1s
			BatchSize:              config.AsyncFlushMessages,  //0 leaves the kafka-go default of 100
			RequiredAcks:           kafka.RequireOne,
			Compression:            config.AsyncCompression,
			AllowAutoTopicCreation: true,
			ErrorLogger:            writerErrorLogger(temp.logger),
			Completion: func(messages []kafka.Message, err error) {
				if err != nil {
					//an async produce error cannot be returned to the caller; ending
					//the process is the behavior this producer has always had
					temp.logger.Error("unable to produce async kafka message", "error", err)
					os.Exit(1)
				}
			},
		}
		result = temp
		temp.closeOnDone(ctx)
	}
	return result, nil
}

// closeOnDone marks the producer closed and closes the writer once ctx is
// cancelled. isClosed is atomic because the produce paths read it concurrently
// with this goroutine.
func (this *SyncProducer) closeOnDone(ctx context.Context) {
	go func() {
		<-ctx.Done()
		this.isClosed.Store(true)
		this.producer.Close()
	}()
}

func (this *AsyncProducer) closeOnDone(ctx context.Context) {
	go func() {
		<-ctx.Done()
		this.isClosed.Store(true)
		this.producer.Close()
	}()
}

// deprecated
func PrepareProducer(ctx context.Context, kafkaBootstrapUrl string, sync bool, syncIdempotent bool, partitionNum int, replicationFactor int, initTopics bool) (result ProducerInterface, err error) {
	return PrepareProducerWithConfig(ctx, kafkaBootstrapUrl, Config{
		AsyncFlushMessages:  0,
		AsyncFlushFrequency: 500 * time.Millisecond,
		AsyncCompression:    kafka.Snappy,
		SyncCompression:     kafka.Snappy,
		Sync:                sync,
		SyncIdempotent:      syncIdempotent,
		PartitionNum:        partitionNum,
		ReplicationFactor:   replicationFactor,
		InitTopics:          initTopics,
	})
}

func (this *SyncProducer) Produce(topic string, message string) (err error) {
	if this.isClosed.Load() {
		return errProducerClosed
	}
	this.logger.Debug("kafka produce sync", "topic", topic, "message", message)
	if this.initTopic {
		err = this.usedTopics.EnsureTopic(topic, this.kafkaBootstrapUrl, this.topicConfigMap, this.partitionsNum, this.replicationFactor)
		if err != nil {
			this.logger.Warn("unable to ensure topic", "error", err)
			err = nil
		}
	}

	start := time.Now()
	if SlowProducerTimeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), SlowProducerTimeout)
		defer cancel()
		go func() {
			<-ctx.Done()
			if ctx.Err() != nil && !errors.Is(ctx.Err(), context.Canceled) {
				this.logger.Warn("slow produce call", "topic", topic, "message", message)
			}
		}()
	}
	err = writeMessage(this.producer, kafka.Message{Topic: topic, Key: nil, Value: []byte(message), Time: time.Now()})
	if SlowProducerTimeout > 0 && time.Since(start) >= SlowProducerTimeout {
		this.logger.Warn("finished slow produce call", "duration", time.Since(start), "topic", topic, "message", message)
	}
	return err
}

func (this *AsyncProducer) Produce(topic string, message string) (err error) {
	if this.isClosed.Load() {
		return errProducerClosed
	}
	this.logger.Debug("kafka produce async", "topic", topic, "message", message)
	if this.initTopic {
		err = this.usedTopics.EnsureTopic(topic, this.kafkaBootstrapUrl, this.topicConfigMap, this.partitionsNum, this.replicationFactor)
		if err != nil {
			this.logger.Warn("unable to ensure topic", "error", err)
			err = nil
		}
	}
	return writeMessage(this.producer, kafka.Message{Topic: topic, Key: nil, Value: []byte(message), Time: time.Now()})
}

func (this *SyncProducer) ProduceWithKey(topic string, message string, key string) (err error) {
	return this.ProduceWithTimestamp(topic, message, key, time.Now())
}

func (this *SyncProducer) ProduceWithTimestamp(topic string, message string, key string, timestamp time.Time) (err error) {
	if this.isClosed.Load() {
		return errProducerClosed
	}
	this.logger.Debug("kafka produce sync", "topic", topic, "message", message)
	if this.initTopic {
		err = this.usedTopics.EnsureTopic(topic, this.kafkaBootstrapUrl, this.topicConfigMap, this.partitionsNum, this.replicationFactor)
		if err != nil {
			this.logger.Warn("unable to ensure topic", "error", err)
			err = nil
		}
	}
	start := time.Now()
	if SlowProducerTimeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), SlowProducerTimeout)
		defer cancel()
		go func() {
			<-ctx.Done()
			if ctx.Err() != nil && !errors.Is(ctx.Err(), context.Canceled) {
				this.logger.Warn("slow produce call", "topic", topic, "key", key, "message", message)
			}
		}()
	}
	err = writeMessage(this.producer, kafka.Message{Topic: topic, Key: []byte(key), Value: []byte(message), Time: timestamp})
	if SlowProducerTimeout > 0 && time.Since(start) >= SlowProducerTimeout {
		this.logger.Warn("finished slow produce call", "duration", time.Since(start), "topic", topic, "key", key, "message", message)
	}
	return err
}

func (this *AsyncProducer) ProduceWithKey(topic string, message string, key string) (err error) {
	return this.ProduceWithTimestamp(topic, message, key, time.Now())
}

func (this *AsyncProducer) ProduceWithTimestamp(topic string, message string, key string, timestamp time.Time) (err error) {
	if this.isClosed.Load() {
		return errProducerClosed
	}
	this.logger.Debug("kafka produce async", "topic", topic, "message", message)
	if this.initTopic {
		err = this.usedTopics.EnsureTopic(topic, this.kafkaBootstrapUrl, this.topicConfigMap, this.partitionsNum, this.replicationFactor)
		if err != nil {
			this.logger.Warn("unable to ensure topic", "error", err)
			err = nil
		}
	}
	return writeMessage(this.producer, kafka.Message{Topic: topic, Key: []byte(key), Value: []byte(message), Time: timestamp})
}
