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
	"github.com/Shopify/sarama"
	"log"
	"time"
)

var Fatal = false
var SlowProducerTimeout time.Duration = 2 * time.Second

type ProducerInterface interface {
	Produce(topic string, message string) (err error)
	ProduceWithKey(topic string, message string, key string) (err error)
	Log(logger *log.Logger)
}

type SyncProducer struct {
	broker            []string
	logger            *log.Logger
	producer          sarama.SyncProducer
	kafkaBootstrapUrl string
	syncIdempotent    bool
	usedTopics        map[string]bool
	partitionsNum     int
	replicationFactor int
}

type AsyncProducer struct {
	broker            []string
	logger            *log.Logger
	producer          sarama.AsyncProducer
	kafkaBootstrapUrl string
	usedTopics        map[string]bool
	partitionsNum     int
	replicationFactor int
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
			temp.producer.Close()
		}()
	} else {
		temp := &AsyncProducer{
			broker:            broker,
			kafkaBootstrapUrl: kafkaBootstrapUrl,
			usedTopics:        map[string]bool{},
			partitionsNum:     config.PartitionNum,
			replicationFactor: config.ReplicationFactor,
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
			temp.producer.Close()
		}()
	}
	return result, nil
}

//deprecated
func PrepareProducer(ctx context.Context, kafkaBootstrapUrl string, sync bool, syncIdempotent bool, partitionNum int, replicationFactor int) (result ProducerInterface, err error) {
	return PrepareProducerWithConfig(ctx, kafkaBootstrapUrl, Config{
		AsyncFlushMessages:  0,
		AsyncFlushFrequency: 500 * time.Millisecond,
		AsyncCompression:    sarama.CompressionSnappy,
		SyncCompression:     sarama.CompressionSnappy,
		Sync:                sync,
		SyncIdempotent:      syncIdempotent,
		PartitionNum:        partitionNum,
		ReplicationFactor:   replicationFactor,
	})
}

func (this *SyncProducer) Log(logger *log.Logger) {
	this.logger = logger
}

func (this *SyncProducer) Produce(topic string, message string) (err error) {
	if this.logger != nil {
		this.logger.Println("DEBUG: produce ", topic, message)
	}
	err = EnsureTopic(topic, this.kafkaBootstrapUrl, &this.usedTopics, this.partitionsNum, this.replicationFactor)
	if err != nil {
		log.Println("WARNING: unable to ensure topic", err)
		err = nil
	}

	start := time.Now()
	if SlowProducerTimeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), SlowProducerTimeout)
		defer cancel()
		go func() {
			<-ctx.Done()
			if ctx.Err() != nil && ctx.Err() != context.Canceled {
				log.Println("WARNING: slow produce call", topic, message)
			}
		}()
	}
	_, _, err = this.producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(message), Timestamp: time.Now()})
	if SlowProducerTimeout > 0 && time.Since(start) >= SlowProducerTimeout {
		log.Println("WARNING: finished slow produce call", time.Since(start), topic, message)
	}
	return err
}

func (this *AsyncProducer) Produce(topic string, message string) (err error) {
	if this.logger != nil {
		this.logger.Println("DEBUG: produce ", topic, message)
	}
	err = EnsureTopic(topic, this.kafkaBootstrapUrl, &this.usedTopics, this.partitionsNum, this.replicationFactor)
	if err != nil {
		log.Println("WARNING: unable to ensure topic", err)
		err = nil
	}
	this.producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(message), Timestamp: time.Now()}
	return
}

func (this *SyncProducer) ProduceWithKey(topic string, message string, key string) (err error) {
	if this.logger != nil {
		this.logger.Println("DEBUG: produce ", topic, message)
	}
	err = EnsureTopic(topic, this.kafkaBootstrapUrl, &this.usedTopics, this.partitionsNum, this.replicationFactor)
	if err != nil {
		log.Println("WARNING: unable to ensure topic", err)
		err = nil
	}
	start := time.Now()
	if SlowProducerTimeout > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), SlowProducerTimeout)
		defer cancel()
		go func() {
			<-ctx.Done()
			if ctx.Err() != nil && ctx.Err() != context.Canceled {
				log.Println("WARNING: slow produce call", topic, key, message)
			}
		}()
	}
	_, _, err = this.producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(key), Value: sarama.StringEncoder(message), Timestamp: time.Now()})
	if SlowProducerTimeout > 0 && time.Since(start) >= SlowProducerTimeout {
		log.Println("WARNING: finished slow produce call", time.Since(start), topic, key, message)
	}
	return err
}

func (this *AsyncProducer) ProduceWithKey(topic string, message string, key string) (err error) {
	if this.logger != nil {
		this.logger.Println("DEBUG: produce ", topic, message)
	}
	err = EnsureTopic(topic, this.kafkaBootstrapUrl, &this.usedTopics, this.partitionsNum, this.replicationFactor)
	if err != nil {
		log.Println("WARNING: unable to ensure topic", err)
		err = nil
	}
	this.producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(key), Value: sarama.StringEncoder(message), Timestamp: time.Now()}
	return
}

func (this *AsyncProducer) Log(logger *log.Logger) {
	this.logger = logger
}
