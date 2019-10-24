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

package platform_connector_lib

import (
	"errors"
	"github.com/SENERGY-Platform/platform-connector-lib/iot"
	"github.com/SENERGY-Platform/platform-connector-lib/kafka"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
	"log"
	"time"
)

type ProtocolSegmentName = string
type CommandResponseMsg = map[ProtocolSegmentName]string
type CommandRequestMsg = map[ProtocolSegmentName]string
type EventMsg = map[ProtocolSegmentName]string

type EndpointCommandHandler func(endpoint string, requestMsg CommandRequestMsg) (responseMsg CommandResponseMsg, err error)
type DeviceCommandHandler func(deviceId string, deviceUri string, serviceId string, serviceUri string, requestMsg CommandRequestMsg) (responseMsg CommandResponseMsg, err error)
type AsyncCommandHandler func(commandRequest model.ProtocolMsg, requestMsg CommandRequestMsg, t time.Time) (err error)

type Connector struct {
	Config Config
	//asyncCommandHandler, endpointCommandHandler and deviceCommandHandler are mutual exclusive
	deviceCommandHandler DeviceCommandHandler //must be able to handle concurrent calls
	asyncCommandHandler  AsyncCommandHandler  //must be able to handle concurrent calls
	producer             kafka.ProducerInterface
	consumer             *kafka.Consumer
	iot                  *iot.Iot
	security             *security.Security

	IotCache *iot.PreparedCache

	kafkalogger *log.Logger
}

func New(config Config) (connector *Connector) {
	connector = &Connector{
		Config: config,
		iot:    iot.New(config.DeviceManagerUrl, config.DeviceRepoUrl),
		security: security.New(
			config.AuthEndpoint,
			config.AuthClientId,
			config.AuthClientSecret,
			config.JwtIssuer,
			config.JwtPrivateKey,
			config.JwtExpiration,
			config.AuthExpirationTimeBuffer,
			config.TokenCacheExpiration,
			config.TokenCacheUrl,
		),
	}
	connector.IotCache = iot.NewCache(connector.iot, config.DeviceExpiration, config.DeviceTypeExpiration, config.IotCacheUrl...)
	return
}

func (this *Connector) SetKafkaLogger(logger *log.Logger) {
	this.kafkalogger = logger
}

//asyncCommandHandler, endpointCommandHandler and deviceCommandHandler are mutual exclusive
func (this *Connector) SetDeviceCommandHandler(handler DeviceCommandHandler) *Connector {
	if this.asyncCommandHandler != nil {
		panic("try setting command handler while async command handler exists")
	}
	this.deviceCommandHandler = handler
	return this
}

//asyncCommandHandler, endpointCommandHandler and deviceCommandHandler are mutual exclusive
func (this *Connector) SetAsyncCommandHandler(handler AsyncCommandHandler) *Connector {
	if this.deviceCommandHandler != nil {
		panic("try setting async command handler while device command handler exists")
	}
	this.asyncCommandHandler = handler
	return this
}

func (this *Connector) Start() (err error) {
	if this.deviceCommandHandler == nil && this.asyncCommandHandler == nil {
		return errors.New("missing command handler; use SetAsyncCommandHandler() or SetDeviceCommandHandler()")
	}
	this.producer, err = kafka.PrepareProducer(this.Config.ZookeeperUrl, this.Config.SyncKafka, this.Config.SyncKafkaIdempotent)
	if err != nil {
		log.Println("ERROR: ", err)
		return err
	}
	if this.kafkalogger != nil {
		this.producer.Log(this.kafkalogger)
	}
	this.consumer, err = kafka.NewConsumer(this.Config.ZookeeperUrl, this.Config.KafkaGroupName, this.Config.Protocol, func(topic string, msg []byte, t time.Time) error {
		if string(msg) == "topic_init" {
			return nil
		}
		return this.handleCommand(msg, t)
	}, func(err error, consumer *kafka.Consumer) {
		if this.Config.FatalKafkaError {
			log.Println("FATAL ERROR: kafka", err)
			log.Fatal(err)
		} else {
			consumer.Restart()
		}
	})
	return
}

func (this *Connector) Stop() {
	this.consumer.Stop()
}

func (this *Connector) HandleDeviceEvent(username string, password string, deviceId string, serviceId string, protocolParts map[string]string) (err error) {
	token, err := this.security.GetUserToken(username, password)
	if err != nil {
		log.Println("ERROR HandleDeviceEvent::GetUserToken()", err)
		return err
	}
	return this.HandleDeviceEventWithAuthToken(token, deviceId, serviceId, protocolParts)
}

func (this *Connector) HandleDeviceEventWithAuthToken(token security.JwtToken, deviceId string, serviceId string, eventMsg EventMsg) (err error) {
	return this.handleDeviceEvent(token, deviceId, serviceId, eventMsg)
}

func (this *Connector) HandleDeviceRefEvent(username string, password string, deviceUri string, serviceUri string, eventMsg EventMsg) (err error) {
	token, err := this.security.GetUserToken(username, password)
	if err != nil {
		log.Println("ERROR HandleDeviceRefEvent::GetUserToken()", err)
		return err
	}
	return this.HandleDeviceRefEventWithAuthToken(token, deviceUri, serviceUri, eventMsg)
}

func (this *Connector) HandleDeviceRefEventWithAuthToken(token security.JwtToken, deviceUri string, serviceUri string, eventMsg EventMsg) (err error) {
	return this.handleDeviceRefEvent(token, deviceUri, serviceUri, eventMsg)
}

func (this *Connector) Security() *security.Security {
	return this.security
}

func (this *Connector) Iot() *iot.Iot {
	return this.iot
}
