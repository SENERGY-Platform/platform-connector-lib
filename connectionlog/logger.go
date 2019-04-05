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
	"encoding/json"
	"errors"
	"log"
	"time"
)

func New(amqpUrl string, connectorLogTopic string, gatewayLogTopic string, deviceLogTopic string) (logger *Logger, err error) {
	logger = &Logger{amqpUrl: amqpUrl, connectorLogTopic: connectorLogTopic, gatewayLogTopic: gatewayLogTopic, deviceLogTopic: deviceLogTopic}
	err = logger.start()
	return
}

func (this *Logger) start() (err error) {
	topics := []string{}
	if this.deviceLogTopic != "" {
		topics = append(topics, this.deviceLogTopic)
	}
	if this.gatewayLogTopic != "" {
		topics = append(topics, this.gatewayLogTopic)
	}
	if this.connectorLogTopic != "" {
		topics = append(topics, this.connectorLogTopic)
	}
	this.conn, err = NewPublisher(this.amqpUrl, topics...)
	return
}

func (this *Logger) Stop() {
	this.conn.Close()
}

type Logger struct {
	amqpUrl           string
	conn              *Publisher
	deviceLogTopic    string
	gatewayLogTopic   string
	connectorLogTopic string
	Debug             bool
}

func (this *Logger) sendEvent(topic string, event interface{}) error {
	payload, err := json.Marshal(event)
	if err != nil {
		log.Println("ERROR: event marshaling:", err)
		return err
	}
	if this.Debug {
		log.Println("DEBUG: send event", this.amqpUrl, topic, event)
	}
	return this.conn.Publish(topic, payload)
}

func (this *Logger) LogDeviceState(state DeviceLog) error {
	if this.deviceLogTopic == "" {
		return errors.New("topic not configured")
	}
	state.Time = time.Now()
	return this.sendEvent(this.deviceLogTopic, this)
}

func (this *Logger) LogGatewayState(state GatewayLog) error {
	if this.gatewayLogTopic == "" {
		return errors.New("topic not configured")
	}
	state.Time = time.Now()
	return this.sendEvent(this.gatewayLogTopic, this)
}

func (this *Logger) LogConnectorState(state ConnectorLog) error {
	if this.connectorLogTopic == "" {
		return errors.New("topic not configured")
	}
	state.Time = time.Now()
	return this.sendEvent(this.connectorLogTopic, this)
}

func (this *Logger) LogDeviceDisconnect(id string) error {
	err := this.LogDeviceState(DeviceLog{Device: id, Connected: false})
	if err != nil {
		log.Println("WARNING: unable to log device connection state ", err)
	}
	return err
}

func (this *Logger) LogDeviceConnect(id string) error {
	err := this.LogDeviceState(DeviceLog{Device: id, Connected: true})
	if err != nil {
		log.Println("WARNING: unable to log device connection state ", err)
	}
	return err
}

func (this *Logger) LogGatewayConnect(gateway string) error {
	err := this.LogGatewayState(GatewayLog{Gateway: gateway, Connected: true})
	if err != nil {
		log.Println("WARNING: unable to log gateway connection state ", err)
	}
	return err
}

func (this *Logger) LogGatewayDisconnect(gateway string) error {
	err := this.LogGatewayState(GatewayLog{Gateway: gateway, Connected: false})
	if err != nil {
		log.Println("WARNING: unable to log gateway connection state ", err)
	}
	return err
}
