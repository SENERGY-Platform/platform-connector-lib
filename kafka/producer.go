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
	"time"

	"github.com/IBM/sarama"
	"github.com/segmentio/kafka-go"
)

var Fatal = false
var SlowProducerTimeout time.Duration = 2 * time.Second

type ProducerInterface interface {
	Produce(topic string, message string) (err error)
	ProduceWithKey(topic string, message string, key string) (err error)
	ProduceWithTimestamp(topic string, message string, key string, timestamp time.Time) (err error)
}

type SyncProducer struct {
	broker            []string
	logger            *slog.Logger
	producer          sarama.SyncProducer
	kafkaBootstrapUrl string
	syncIdempotent    bool
	usedTopics        map[string]bool
	partitionsNum     int
	replicationFactor int
	topicConfigMap    map[string][]kafka.ConfigEntry
	initTopic         bool
	isClosed          bool
}

type AsyncProducer struct {
	broker            []string
	logger            *slog.Logger
	producer          sarama.AsyncProducer
	kafkaBootstrapUrl string
	usedTopics        map[string]bool
	partitionsNum     int
	replicationFactor int
	topicConfigMap    map[string][]kafka.ConfigEntry
	initTopic         bool
	isClosed          bool
}

type Config struct {
	AsyncFlushFrequency time.Duration
	AsyncCompression    sarama.CompressionCodec
	SyncCompression     sarama.CompressionCodec
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
			usedTopics:        map[string]bool{},
			partitionsNum:     config.PartitionNum,
			replicationFactor: config.ReplicationFactor,
			topicConfigMap:    config.TopicConfigMap,
			initTopic:         config.InitTopics,
			logger:            config.GetLogger(),
		}
		sarama_conf := sarama.NewConfig()
		sarama_conf.Version = sarama.V2_2_0_0
		sarama_conf.Producer.Return.Errors = true
		sarama_conf.Producer.Return.Successes = true
		sarama_conf.Producer.Compression = config.SyncCompression
		if config.SyncIdempotent {
			sarama_conf.Producer.Idempotent = true
			sarama_conf.Net.MaxOpenRequests = 1
			sarama_conf.Producer.RequiredAcks = sarama.WaitForAll
		}
		temp.producer, err = sarama.NewSyncProducer(temp.broker, sarama_conf)
		if err != nil {
			return result, err
		}
		result = temp
		go func() {
			<-ctx.Done()
			temp.isClosed = true
			temp.producer.Close()
		}()
	} else {
		temp := &AsyncProducer{
			broker:            broker,
			kafkaBootstrapUrl: kafkaBootstrapUrl,
			usedTopics:        map[string]bool{},
			partitionsNum:     config.PartitionNum,
			replicationFactor: config.ReplicationFactor,
			topicConfigMap:    config.TopicConfigMap,
			initTopic:         config.InitTopics,
			logger:            config.GetLogger(),
		}
		sarama_conf := sarama.NewConfig()
		sarama_conf.Version = sarama.V2_2_0_0
		sarama_conf.Producer.Return.Errors = true
		sarama_conf.Producer.Return.Successes = false
		sarama_conf.Producer.Flush.Frequency = config.AsyncFlushFrequency
		sarama_conf.Producer.Flush.Messages = config.AsyncFlushMessages
		sarama_conf.Producer.Compression = config.AsyncCompression
		temp.producer, err = sarama.NewAsyncProducer(temp.broker, sarama_conf)
		if err != nil {
			return result, err
		}
		go func() {
			err, ok := <-temp.producer.Errors()
			if ok {
				log.Fatal(err)
			}
		}()
		result = temp
		go func() {
			<-ctx.Done()
			temp.isClosed = true
			temp.producer.Close()
		}()
	}
	return result, nil
}

// deprecated
func PrepareProducer(ctx context.Context, kafkaBootstrapUrl string, sync bool, syncIdempotent bool, partitionNum int, replicationFactor int, initTopics bool) (result ProducerInterface, err error) {
	return PrepareProducerWithConfig(ctx, kafkaBootstrapUrl, Config{
		AsyncFlushMessages:  0,
		AsyncFlushFrequency: 500 * time.Millisecond,
		AsyncCompression:    sarama.CompressionSnappy,
		SyncCompression:     sarama.CompressionSnappy,
		Sync:                sync,
		SyncIdempotent:      syncIdempotent,
		PartitionNum:        partitionNum,
		ReplicationFactor:   replicationFactor,
		InitTopics:          initTopics,
	})
}

func (this *SyncProducer) Produce(topic string, message string) (err error) {
	if this.isClosed {
		return errors.New("producer closed")
	}
	this.logger.Debug("kafka produce sync", "topic", topic, "message", message)
	if this.initTopic {
		err = EnsureTopic(topic, this.kafkaBootstrapUrl, &this.usedTopics, this.topicConfigMap, this.partitionsNum, this.replicationFactor)
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
	_, _, err = this.producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(message), Timestamp: time.Now()})
	if SlowProducerTimeout > 0 && time.Since(start) >= SlowProducerTimeout {
		this.logger.Warn("finished slow produce call", "duration", time.Since(start), "topic", topic, "message", message)
	}
	return err
}

func (this *AsyncProducer) Produce(topic string, message string) (err error) {
	if this.isClosed {
		return errors.New("producer closed")
	}
	this.logger.Debug("kafka produce async", "topic", topic, "message", message)
	if this.initTopic {
		err = EnsureTopic(topic, this.kafkaBootstrapUrl, &this.usedTopics, this.topicConfigMap, this.partitionsNum, this.replicationFactor)
		if err != nil {
			this.logger.Warn("unable to ensure topic", "error", err)
			err = nil
		}
	}
	this.producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(message), Timestamp: time.Now()}
	return
}

func (this *SyncProducer) ProduceWithKey(topic string, message string, key string) (err error) {
	return this.ProduceWithTimestamp(topic, message, key, time.Now())
}

func (this *SyncProducer) ProduceWithTimestamp(topic string, message string, key string, timestamp time.Time) (err error) {
	if this.isClosed {
		return errors.New("producer closed")
	}
	this.logger.Debug("kafka produce sync", "topic", topic, "message", message)
	if this.initTopic {
		err = EnsureTopic(topic, this.kafkaBootstrapUrl, &this.usedTopics, this.topicConfigMap, this.partitionsNum, this.replicationFactor)
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
	_, _, err = this.producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(key), Value: sarama.StringEncoder(message), Timestamp: timestamp})
	if SlowProducerTimeout > 0 && time.Since(start) >= SlowProducerTimeout {
		this.logger.Warn("finished slow produce call", "duration", time.Since(start), "topic", topic, "key", key, "message", message)
	}
	return err
}

func (this *AsyncProducer) ProduceWithKey(topic string, message string, key string) (err error) {
	return this.ProduceWithTimestamp(topic, message, key, time.Now())
}

func (this *AsyncProducer) ProduceWithTimestamp(topic string, message string, key string, timestamp time.Time) (err error) {
	if this.isClosed {
		return errors.New("producer closed")
	}
	this.logger.Debug("kafka produce async", "topic", topic, "message", message)
	if this.initTopic {
		err = EnsureTopic(topic, this.kafkaBootstrapUrl, &this.usedTopics, this.topicConfigMap, this.partitionsNum, this.replicationFactor)
		if err != nil {
			this.logger.Warn("unable to ensure topic", "error", err)
			err = nil
		}
	}
	this.producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(key), Value: sarama.StringEncoder(message), Timestamp: timestamp}
	return
}
