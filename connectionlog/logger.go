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

package connectionlog

import (
	"context"
	"encoding/json"
	"github.com/SENERGY-Platform/platform-connector-lib/kafka"
	"time"
)

type connectionCheckClient interface {
	RefreshDeviceState(deviceID string, lmResult, subResult int) error
}

func NewWithKafkaConfig(ctx context.Context, kafkaBootstrapUrl string, deviceLogTopic string, hubLogTopic string, config kafka.Config) (logger Logger, err error) {
	producer, err := kafka.PrepareProducerWithConfig(ctx, kafkaBootstrapUrl, config)
	if err != nil {
		return logger, err
	}
	return &LoggerImpl{producer: producer, deviceLogTopic: deviceLogTopic, hubLogTopic: hubLogTopic}, nil
}

// deprecated
func New(ctx context.Context, kafkaBootstrapUrl string, sync bool, idempotent bool, deviceLogTopic string, hubLogTopic string, partitionNum int, replicationFactor int, initTopic bool) (logger Logger, err error) {
	producer, err := kafka.PrepareProducer(ctx, kafkaBootstrapUrl, sync, idempotent, partitionNum, replicationFactor, initTopic)
	if err != nil {
		return logger, err
	}
	return &LoggerImpl{producer: producer, deviceLogTopic: deviceLogTopic, hubLogTopic: hubLogTopic}, nil
}

func NewWithProducer(producer kafka.ProducerInterface, deviceLogTopic string, hubLogTopic string) (logger Logger, err error) {
	return &LoggerImpl{producer: producer, deviceLogTopic: deviceLogTopic, hubLogTopic: hubLogTopic}, nil
}

func NewWithProducerAndConnCheck(producer kafka.ProducerInterface, connCheckClient connectionCheckClient, deviceLogTopic string, hubLogTopic string) (logger Logger, err error) {
	return &LoggerImpl{
		producer:        producer,
		connCheckClient: connCheckClient,
		deviceLogTopic:  deviceLogTopic,
		hubLogTopic:     hubLogTopic,
	}, nil
}

type LoggerImpl struct {
	producer        kafka.ProducerInterface
	deviceLogTopic  string
	hubLogTopic     string
	connCheckClient connectionCheckClient
}

func (this *LoggerImpl) LogDeviceDisconnect(id string) error {
	b, err := json.Marshal(DeviceLog{
		Connected: false,
		Id:        id,
		Time:      time.Now(),
	})
	if err != nil {
		return err
	}
	return this.producer.ProduceWithKey(this.deviceLogTopic, string(b), id)
}

func (this *LoggerImpl) LogDeviceConnect(id string) error {
	if this.connCheckClient != nil {
		return this.connCheckClient.RefreshDeviceState(id, 0, 1)
	}
	b, err := json.Marshal(DeviceLog{
		Connected: true,
		Id:        id,
		Time:      time.Now(),
	})
	if err != nil {
		return err
	}
	return this.producer.ProduceWithKey(this.deviceLogTopic, string(b), id)
}

func (this *LoggerImpl) LogHubConnect(id string) error {
	b, err := json.Marshal(HubLog{
		Connected: true,
		Id:        id,
		Time:      time.Now(),
	})
	if err != nil {
		return err
	}
	return this.producer.ProduceWithKey(this.hubLogTopic, string(b), id)
}

func (this *LoggerImpl) LogHubDisconnect(id string) error {
	b, err := json.Marshal(HubLog{
		Connected: false,
		Id:        id,
		Time:      time.Now(),
	})
	if err != nil {
		return err
	}
	return this.producer.ProduceWithKey(this.hubLogTopic, string(b), id)
}
