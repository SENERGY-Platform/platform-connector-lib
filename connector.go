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
	"context"
	"errors"
	"github.com/SENERGY-Platform/platform-connector-lib/iot"
	"github.com/SENERGY-Platform/platform-connector-lib/kafka"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/SENERGY-Platform/platform-connector-lib/msgvalidation"
	"github.com/SENERGY-Platform/platform-connector-lib/psql"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
	"log"
	"strconv"
	"sync"
	"time"
)

type ProtocolSegmentName = string
type CommandResponseMsg = map[ProtocolSegmentName]string
type CommandRequestMsg = map[ProtocolSegmentName]string
type EventMsg = map[ProtocolSegmentName]string

type DeviceCommandHandler func(deviceId string, deviceUri string, serviceId string, serviceUri string, requestMsg CommandRequestMsg) (responseMsg CommandResponseMsg, qos Qos, err error)
type AsyncCommandHandler func(commandRequest model.ProtocolMsg, requestMsg CommandRequestMsg, t time.Time) (err error)

var ErrorUnknownLocalServiceId = errors.New("unknown local service id")

type Qos int

const (
	Async Qos = iota
	Sync
	SyncIdempotent
)

type Connector struct {
	Config Config
	//asyncCommandHandler, endpointCommandHandler and deviceCommandHandler are mutual exclusive
	deviceCommandHandler DeviceCommandHandler //must be able to handle concurrent calls
	asyncCommandHandler  AsyncCommandHandler  //must be able to handle concurrent calls
	producer             map[Qos]kafka.ProducerInterface
	postgresPublisher    *psql.Publisher
	iot                  *iot.Iot
	security             *security.Security

	IotCache *iot.PreparedCache

	kafkalogger *log.Logger
}

func New(config Config) (connector *Connector) {
	var publisher *psql.Publisher
	var err error
	if config.PublishToPostgres {
		publisher, err = psql.New(config.PostgresHost, config.PostgresPort, config.PostgresUser, config.PostgresPw, config.PostgresDb, config.Debug, &sync.WaitGroup{}, context.Background())
		if err != nil {
			log.Fatal(err.Error())
		}
	}

	connector = &Connector{
		Config: config,
		iot:    iot.New(config.DeviceManagerUrl, config.DeviceRepoUrl, config.SemanticRepositoryUrl),
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
		postgresPublisher: publisher,
	}
	connector.IotCache = iot.NewCache(connector.iot, config.DeviceExpiration, config.DeviceTypeExpiration, config.CharacteristicExpiration, config.IotCacheUrl...)
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

func (this *Connector) Start(ctx context.Context, qosList ...Qos) (err error) {
	list := append([]Qos{}, qosList...)
	if len(list) == 0 {
		list = []Qos{Async, Sync, SyncIdempotent}
	}
	err = this.InitProducer(ctx, list)
	if err != nil {
		return err
	}
	return this.StartConsumer(ctx)
}

func (this *Connector) StartConsumer(ctx context.Context) (err error) {
	if this.deviceCommandHandler == nil && this.asyncCommandHandler == nil {
		return errors.New("missing command handler; use SetAsyncCommandHandler() or SetDeviceCommandHandler()")
	}
	err = kafka.NewConsumer(ctx, this.Config.KafkaUrl, this.Config.KafkaGroupName, this.Config.Protocol, func(topic string, msg []byte, t time.Time) error {
		if string(msg) == "topic_init" {
			return nil
		}
		return this.handleCommand(msg, t)
	}, func(err error, consumer *kafka.Consumer) {
		log.Println("FATAL ERROR: kafka", err)
		log.Fatal(err)
	})
	return
}

func (this *Connector) InitProducer(ctx context.Context, qosList []Qos) (err error) {
	this.producer = map[Qos]kafka.ProducerInterface{}
	for _, qos := range qosList {
		err = this.initProducer(ctx, qos)
		if err != nil {
			return err
		}
	}
	return nil
}

func (this *Connector) initProducer(ctx context.Context, qos Qos) (err error) {
	partitionsNum := 1
	replFactor := 1
	sync := false
	idempotent := false
	if this.Config.PartitionsNum != 0 {
		partitionsNum = this.Config.PartitionsNum
	}
	if this.Config.ReplicationFactor != 0 {
		replFactor = this.Config.ReplicationFactor
	}
	switch qos {
	case Sync:
		sync = true
		idempotent = false
	case Async:
		sync = false
		idempotent = false
	case SyncIdempotent:
		sync = true
		idempotent = true
	default:
		return errors.New("unknown qos=" + strconv.Itoa(int(qos)))
	}
	this.producer[qos], err = kafka.PrepareProducer(ctx, this.Config.KafkaUrl, sync, idempotent, partitionsNum, replFactor)
	if err != nil {
		log.Println("ERROR: ", err)
		return err
	}
	if this.kafkalogger != nil {
		this.producer[qos].Log(this.kafkalogger)
	}
	return
}

func (this *Connector) HandleDeviceEvent(username string, password string, deviceId string, serviceId string, protocolParts map[string]string, qos Qos) (err error) {
	token, err := this.security.GetUserToken(username, password)
	if err != nil {
		log.Println("ERROR HandleDeviceEvent::GetUserToken()", err)
		return err
	}
	return this.HandleDeviceEventWithAuthToken(token, deviceId, serviceId, protocolParts, qos)
}

func (this *Connector) HandleDeviceEventWithAuthToken(token security.JwtToken, deviceId string, serviceId string, eventMsg EventMsg, qos Qos) (err error) {
	return this.handleDeviceEvent(token, deviceId, serviceId, eventMsg, qos)
}

func (this *Connector) HandleDeviceRefEvent(username string, password string, deviceUri string, serviceUri string, eventMsg EventMsg, qos Qos) (err error) {
	token, err := this.security.GetUserToken(username, password)
	if err != nil {
		log.Println("ERROR HandleDeviceRefEvent::GetUserToken()", err)
		return err
	}
	return this.HandleDeviceRefEventWithAuthToken(token, deviceUri, serviceUri, eventMsg, qos)
}

func (this *Connector) HandleDeviceRefEventWithAuthToken(token security.JwtToken, deviceUri string, serviceUri string, eventMsg EventMsg, qos Qos) (err error) {
	return this.handleDeviceRefEvent(token, deviceUri, serviceUri, eventMsg, qos)
}

func (this *Connector) HandleDeviceIdentEvent(username string, password string, deviceId string, localDeviceId string, serviceId string, localServiceId string, eventMsg EventMsg, qos Qos) (err error) {
	token, err := this.security.GetUserToken(username, password)
	if err != nil {
		log.Println("ERROR HandleDeviceRefEvent::GetUserToken()", err)
		return err
	}
	return this.HandleDeviceIdentEventWithAuthToken(token, deviceId, localDeviceId, serviceId, localServiceId, eventMsg, qos)
}

func (this *Connector) HandleDeviceIdentEventWithAuthToken(token security.JwtToken, deviceId string, localDeviceId string, serviceId string, localServiceId string, eventMsg EventMsg, qos Qos) (err error) {
	var device model.Device
	if deviceId == "" {
		if localDeviceId == "" {
			return errors.New("missing deviceId or localDeviceId")
		} else {
			device, err = this.IotCache.WithToken(token).GetDeviceByLocalId(localDeviceId)
			if err != nil {
				log.Println("ERROR: HandleDeviceIdentEventWithAuthToken::DeviceUrlToIotDevice", err)
				return err
			}
			deviceId = device.Id
		}
	}
	if serviceId == "" {
		if localServiceId == "" {
			return errors.New("missing serviceId or localServiceId")
		} else {
			if device.Id == "" {
				device, err = this.IotCache.WithToken(token).GetDevice(deviceId)
				if err != nil {
					log.Println("ERROR: HandleDeviceIdentEventWithAuthToken::GetDevice", err)
					return err
				}
			}
			dt, err := this.IotCache.WithToken(token).GetDeviceType(device.DeviceTypeId)
			if err != nil {
				log.Println("ERROR: HandleDeviceIdentEventWithAuthToken::GetDeviceType", err)
				return err
			}
			for _, service := range dt.Services {
				if service.LocalId == localServiceId && len(service.Outputs) > 0 {
					serviceId = service.Id
				}
			}
			if serviceId == "" {
				return ErrorUnknownLocalServiceId
			}
		}
	}
	err = this.handleDeviceEvent(token, deviceId, serviceId, eventMsg, qos)
	if err != nil {
		log.Println("ERROR: HandleDeviceIdentEventWithAuthToken::handleDeviceEvent", err)
		return err
	}
	return nil
}

func (this *Connector) Security() *security.Security {
	return this.security
}

func (this *Connector) Iot() *iot.Iot {
	return this.iot
}

func (this *Connector) ValidateMsg(msg map[string]interface{}, service model.Service) error {
	if !this.Config.Validate {
		return nil
	}
	return msgvalidation.Validate(msg, service, this.Config.ValidateAllowUnknownField, this.Config.ValidateAllowMissingField)
}

func (this *Connector) CleanMsg(msg map[string]interface{}, service model.Service) (map[string]interface{}, error) {
	return msgvalidation.Clean(msg, service)
}

func (this *Connector) GetProducer(qos Qos) (producer kafka.ProducerInterface, err error) {
	producer, ok := this.producer[qos]
	if !ok {
		return producer, errors.New("no matching producer for qos=" + strconv.Itoa(int(qos)) + " found")
	}
	return producer, nil
}
