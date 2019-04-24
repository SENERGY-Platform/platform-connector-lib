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
	"time"
)

type Producer struct {
	broker   []string
	logger   *log.Logger
	producer sarama.SyncProducer
}

func PrepareProducer(zk string) (*Producer, error) {
	result := &Producer{}
	var err error
	result.broker, err = GetBroker(zk)
	if err != nil {
		return result, err
	}
	if len(result.broker) == 0 {
		return result, errors.New("missing kafka broker")
	}
	sarama_conf := sarama.NewConfig()
	sarama_conf.Version = sarama.V2_2_0_0
	sarama_conf.Producer.Return.Errors = true
	sarama_conf.Producer.Return.Successes = true

	sarama_conf.Producer.Idempotent = true
	sarama_conf.Net.MaxOpenRequests = 1
	sarama_conf.Producer.RequiredAcks = sarama.WaitForAll
	result.producer, err = sarama.NewSyncProducer(result.broker, sarama_conf)
	return result, err
}

func (this *Producer) Log(logger *log.Logger) {
	this.logger = logger
}

func (this *Producer) Produce(topic string, message string) (err error) {
	if this.logger != nil {
		this.logger.Println("DEBUG: produce ", topic, message)
	}
	_, _, err = this.producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Key: nil, Value: sarama.StringEncoder(message), Timestamp: time.Now()})
	return
}
