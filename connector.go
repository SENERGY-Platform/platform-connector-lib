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
	"encoding/json"
	"errors"
	"github.com/SENERGY-Platform/platform-connector-lib/iot"
	"github.com/SENERGY-Platform/platform-connector-lib/kafka"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
	kgo "github.com/segmentio/kafka-go"
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
	endpointCommandHandler EndpointCommandHandler //must be able to handle concurrent calls
	deviceCommandHandler   DeviceCommandHandler   //must be able to handle concurrent calls
	asyncCommandHandler    AsyncCommandHandler    //must be able to handle concurrent calls
	producer               kafka.ProducerInterface
	consumer               *kafka.Consumer
	iot                    *iot.Iot
	security               *security.Security

	IotCache *iot.PreparedCache

	kafkalogger *log.Logger
}

func New(config Config) (connector *Connector) {
	connector = &Connector{
		Config: config,
		iot:    iot.New(config.IotRepoUrl, config.DeviceRepoUrl, config.Protocol),
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
func (this *Connector) SetEndpointCommandHandler(handler EndpointCommandHandler) *Connector {
	if this.deviceCommandHandler != nil {
		panic("try setting endpoint command handler while device command handler exists")
	}
	if this.asyncCommandHandler != nil {
		panic("try setting endpoint command handler while async command handler exists")
	}
	this.endpointCommandHandler = handler
	return this
}

//asyncCommandHandler, endpointCommandHandler and deviceCommandHandler are mutual exclusive
func (this *Connector) SetDeviceCommandHandler(handler DeviceCommandHandler) *Connector {
	if this.endpointCommandHandler != nil {
		panic("try setting endpoint command handler while endpoint command handler exists")
	}
	if this.asyncCommandHandler != nil {
		panic("try setting endpoint command handler while async command handler exists")
	}
	this.deviceCommandHandler = handler
	return this
}

//asyncCommandHandler, endpointCommandHandler and deviceCommandHandler are mutual exclusive
func (this *Connector) SetAsyncCommandHandler(handler AsyncCommandHandler) *Connector {
	if this.deviceCommandHandler != nil {
		panic("try setting endpoint command handler while device command handler exists")
	}
	if this.endpointCommandHandler != nil {
		panic("try setting endpoint command handler while endpoint command handler exists")
	}
	this.asyncCommandHandler = handler
	return this
}

func (this *Connector) Start() (err error) {
	if this.deviceCommandHandler == nil && this.endpointCommandHandler == nil && this.asyncCommandHandler == nil {
		return errors.New("missing command handler; use SetAsyncCommandHandler(), SetDeviceCommandHandler() or SetEndpointCommandHandler()")
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
		envelope := model.Envelope{}
		err = json.Unmarshal(msg, &envelope)
		if err != nil {
			log.Println("ERROR: ", err)
			return nil //ignore marshaling errors --> no repeat; errors would definitely reoccur
		}
		payload, err := json.Marshal(envelope.Value)
		if err != nil {
			log.Println("ERROR: ", err)
			return nil //ignore marshaling errors --> no repeat; errors would definitely reoccur
		}
		return this.handleCommand(payload, t)
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

func (this *Connector) HandleEndpointEvent(username string, password string, endpoint string, eventMsg EventMsg) (total int, success int, fail int, err error) {
	token, err := this.security.GetUserToken(username, password)
	if err != nil {
		log.Println("ERROR HandleEndpointEvent::GetUserToken()", err)
		return total, success, fail, err
	}
	return this.HandleEndpointEventWithAuthToken(token, endpoint, eventMsg)
}

func (this *Connector) HandleEndpointEventWithAuthToken(token security.JwtToken, endpoint string, eventMsg EventMsg) (total int, success int, fail int, err error) {
	protocol := []model.ProtocolPart{}
	for segmentName, value := range eventMsg {
		protocol = append(protocol, model.ProtocolPart{Name: segmentName, Value: value})
	}
	return this.handleEndpointEvent(token, endpoint, protocol)
}

func (this *Connector) HandleDeviceEvent(username string, password string, deviceId string, serviceId string, protocolParts map[string]string) (err error) {
	token, err := this.security.GetUserToken(username, password)
	if err != nil {
		log.Println("ERROR HandleEndpointEvent::GetUserToken()", err)
		return err
	}
	return this.HandleDeviceEventWithAuthToken(token, deviceId, serviceId, protocolParts)
}

func (this *Connector) HandleDeviceEventWithAuthToken(token security.JwtToken, deviceId string, serviceId string, eventMsg EventMsg) (err error) {
	protocol := []model.ProtocolPart{}
	for segmentName, value := range eventMsg {
		protocol = append(protocol, model.ProtocolPart{Name: segmentName, Value: value})
	}
	return this.handleDeviceEvent(token, deviceId, serviceId, protocol)
}

func (this *Connector) HandleDeviceRefEvent(username string, password string, deviceUri string, serviceUri string, eventMsg EventMsg) (err error) {
	token, err := this.security.GetUserToken(username, password)
	if err != nil {
		log.Println("ERROR HandleEndpointEvent::GetUserToken()", err)
		return err
	}
	return this.HandleDeviceRefEventWithAuthToken(token, deviceUri, serviceUri, eventMsg)
}

func (this *Connector) HandleDeviceRefEventWithAuthToken(token security.JwtToken, deviceUri string, serviceUri string, eventMsg EventMsg) (err error) {
	protocol := []model.ProtocolPart{}
	for segmentName, value := range eventMsg {
		protocol = append(protocol, model.ProtocolPart{Name: segmentName, Value: value})
	}
	return this.handleDeviceRefEvent(token, deviceUri, serviceUri, protocol)
}

func (this *Connector) Security() *security.Security {
	return this.security
}

func (this *Connector) Iot() *iot.Iot {
	return this.iot
}

func (this *Connector) EnsureTopics(token security.JwtToken, batchsize int, numPartitions int, replicationFactor int) {
	controller, err := kafka.GetKafkaController(this.Config.ZookeeperUrl)
	if err != nil {
		log.Println("ERROR: EnsureTopics:GetKafkaController()", err)
		return
	}
	if controller == "" {
		log.Println("ERROR: EnsureTopics:GetKafkaController(): unable to find controller")
		return
	}
	conn, err := kgo.Dial("tcp", controller)
	if err != nil {
		log.Println("ERROR: EnsureTopics:Dial()", err)
		return
	}
	limit := batchsize
	deviceTypes := make(chan string, limit)
	go func() {
		doneDeviceTypes := map[string]bool{}
		offset := 0
		defer close(deviceTypes)
		for {
			devices, err := this.Iot().GetDevices(token, limit, offset)
			if err != nil {
				log.Println("ERROR: EnsureTopics:GetDevices()", err)
				return
			}
			for _, device := range devices {
				if !doneDeviceTypes[device.DeviceType] {
					doneDeviceTypes[device.DeviceType] = true
					deviceTypes <- device.DeviceType
				}
			}
			if len(devices) != limit {
				return
			}
			offset = offset + limit
		}
	}()

	topics := make(chan string)
	go func() {
		defer close(topics)
		cache := this.IotCache.WithToken(token)
		for dtId := range deviceTypes {
			dt, err := cache.GetDeviceType(dtId)
			if err != nil {
				log.Println("ERROR: EnsureTopics:GetDeviceType()", dtId, err)
				return
			}
			for _, service := range dt.Services {
				topics <- formatId(service.Id)
			}
		}
	}()

	go func() {
		for topic := range topics {
			err := conn.CreateTopics(kgo.TopicConfig{
				Topic:             topic,
				NumPartitions:     numPartitions,
				ReplicationFactor: replicationFactor,
			})
			if err != nil {
				log.Println("ERROR: EnsureTopics:CreateTopics()", topic, err)
			}
		}
	}()
}
