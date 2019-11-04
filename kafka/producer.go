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
	"errors"
	"github.com/Shopify/sarama"
	"log"
	"sync"
	"time"
)

var Fatal = false

type ProducerInterface interface {
	Produce(topic string, message string) (err error)
	ProduceWithKey(topic string, message string, key string) (err error)
	Log(logger *log.Logger)
	Close()
}

type SyncProducer struct {
	broker         []string
	logger         *log.Logger
	producer       sarama.SyncProducer
	zk             string
	syncIdempotent bool
	mux            sync.Mutex
	usedTopics     map[string]bool
}

func (this *SyncProducer) Close() {
	this.producer.Close()
}

type AsyncProducer struct {
	broker     []string
	logger     *log.Logger
	producer   sarama.AsyncProducer
	zk         string
	usedTopics map[string]bool
}

func (this *AsyncProducer) Close() {
	this.producer.Close()
}

func PrepareProducer(zk string, sync bool, syncIdempotent bool) (ProducerInterface, error) {
	var err error
	broker, err := GetBroker(zk)
	if err != nil {
		return nil, err
	}
	if len(broker) == 0 {
		return nil, errors.New("missing kafka broker")
	}
	if sync {
		result := &SyncProducer{broker: broker, zk: zk, syncIdempotent: syncIdempotent, usedTopics: map[string]bool{}}
		sarama_conf := sarama.NewConfig()
		sarama_conf.Version = sarama.V2_2_0_0
		sarama_conf.Producer.Return.Errors = true
		sarama_conf.Producer.Return.Successes = true
		if syncIdempotent {
			sarama_conf.Producer.Idempotent = true
			sarama_conf.Net.MaxOpenRequests = 1
			sarama_conf.Producer.RequiredAcks = sarama.WaitForAll
		}
		result.producer, err = sarama.NewSyncProducer(result.broker, sarama_conf)
		return result, err
	} else {
		result := &AsyncProducer{broker: broker, zk: zk, usedTopics: map[string]bool{}}
		sarama_conf := sarama.NewConfig()
		sarama_conf.Version = sarama.V2_2_0_0
		sarama_conf.Producer.Return.Errors = true
		sarama_conf.Producer.Return.Successes = false
		result.producer, err = sarama.NewAsyncProducer(result.broker, sarama_conf)
		go func() {
			err, ok := <-result.producer.Errors()
			if ok {
				log.Fatal(err)
			}
		}()
		return result, err
	}
}

func (this *SyncProducer) Log(logger *log.Logger) {
	this.logger = logger
}

func (this *SyncProducer) Produce(topic string, message string) (err error) {
	this.mux.Lock()
	defer this.mux.Unlock()
	if this.logger != nil {
		this.logger.Println("DEBUG: produce ", topic, message)
	}
	err = EnsureTopic(topic, this.zk, &this.usedTopics)
	if err != nil {
		return err
	}
	_, _, err = this.producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(message), Timestamp: time.Now()})
	return err
}

func (this *AsyncProducer) Produce(topic string, message string) (err error) {
	if this.logger != nil {
		this.logger.Println("DEBUG: produce ", topic, message)
	}
	err = EnsureTopic(topic, this.zk, &this.usedTopics)
	if err != nil {
		return err
	}
	this.producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(message), Timestamp: time.Now()}
	return
}

func (this *SyncProducer) ProduceWithKey(topic string, message string, key string) (err error) {
	this.mux.Lock()
	defer this.mux.Unlock()
	if this.logger != nil {
		this.logger.Println("DEBUG: produce ", topic, message)
	}
	err = EnsureTopic(topic, this.zk, &this.usedTopics)
	if err != nil {
		return err
	}
	_, _, err = this.producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(key), Value: sarama.StringEncoder(message), Timestamp: time.Now()})
	return err
}

func (this *AsyncProducer) ProduceWithKey(topic string, message string, key string) (err error) {
	if this.logger != nil {
		this.logger.Println("DEBUG: produce ", topic, message)
	}
	err = EnsureTopic(topic, this.zk, &this.usedTopics)
	if err != nil {
		return err
	}
	this.producer.Input() <- &sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder(key), Value: sarama.StringEncoder(message), Timestamp: time.Now()}
	return
}

func (this *AsyncProducer) Log(logger *log.Logger) {
	this.logger = logger
}
