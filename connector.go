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
	"encoding/json"
	"errors"
	"github.com/SENERGY-Platform/platform-connector-lib/httpcommand"
	"github.com/SENERGY-Platform/platform-connector-lib/iot"
	"github.com/SENERGY-Platform/platform-connector-lib/kafka"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/SENERGY-Platform/platform-connector-lib/msgvalidation"
	"github.com/SENERGY-Platform/platform-connector-lib/psql"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
	"github.com/SENERGY-Platform/platform-connector-lib/statistics"
	kafka2 "github.com/segmentio/kafka-go"
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

	asyncPgBackpressure chan bool //used to limit go routines for async postgres publishing

	statistics statistics.Interface
}

func New(config Config) (connector *Connector) {
	config = setConfigDefaults(config)
	var publisher *psql.Publisher
	var err error
	if config.PublishToPostgres {
		publisher, err = psql.New(config.PostgresHost, config.PostgresPort, config.PostgresUser, config.PostgresPw, config.PostgresDb, config.Debug, &sync.WaitGroup{}, context.Background())
		if err != nil {
			log.Fatal(err.Error())
		}
	}

	asyncPgThreadMax := config.AsyncPgThreadMax
	if asyncPgThreadMax == 0 {
		asyncPgThreadMax = 1000
	}

	connector = &Connector{
		Config: config,
		iot:    iot.New(config.DeviceManagerUrl, config.DeviceRepoUrl, config.SemanticRepositoryUrl, config.PermQueryUrl),
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
			5,
			500*time.Millisecond,
		),
		postgresPublisher:   publisher,
		asyncPgBackpressure: make(chan bool, asyncPgThreadMax),
		statistics:          statistics.Void{},
	}
	iotCacheTimeout := 200 * time.Millisecond
	if timeout, err := time.ParseDuration(config.IotCacheTimeout); err != nil {
		log.Println("WARNING: invalid IotCacheTimeout; use default 200ms")
	} else {
		iotCacheTimeout = timeout
	}
	connector.IotCache = iot.NewCache(connector.iot, config.DeviceExpiration, config.DeviceTypeExpiration, config.CharacteristicExpiration, config.IotCacheMaxIdleConns, iotCacheTimeout, config.IotCacheUrl...)
	return
}

func setConfigDefaults(config Config) Config {
	if config.KafkaTopicConfigs == nil {
		config.KafkaTopicConfigs = map[string][]kafka2.ConfigEntry{
			config.DeviceTypeTopic: {
				{
					ConfigName:  "retention.ms",
					ConfigValue: "-1",
				},
				{
					ConfigName:  "retention.bytes",
					ConfigValue: "-1",
				},
				{
					ConfigName:  "cleanup.policy",
					ConfigValue: "compact",
				},
				{
					ConfigName:  "delete.retention.ms",
					ConfigValue: "86400000",
				},
				{
					ConfigName:  "segment.ms",
					ConfigValue: "604800000",
				},
				{
					ConfigName:  "min.cleanable.dirty.ratio",
					ConfigValue: "0.1",
				},
			},
			config.KafkaResponseTopic: {
				{
					ConfigName:  "retention.ms",
					ConfigValue: "86400000", //1000 * 60 * 60 * 24
				},
			},
			config.Protocol: {
				{
					ConfigName:  "retention.ms",
					ConfigValue: "86400000", //1000 * 60 * 60 * 24
				},
			},
			"urn_infai_ses_service_": {
				{
					ConfigName:  "retention.ms",
					ConfigValue: "31536000000", //365 * 1000 * 60 * 60 * 24
				},
			},
		}
	}
	return config
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
	this.StatisticsLogger(ctx)
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

func (this *Connector) StatisticsLogger(ctx context.Context) {
	if this.Config.StatisticsInterval != "" && this.Config.StatisticsInterval != "-" {
		interval, err := time.ParseDuration(this.Config.StatisticsInterval)
		if err != nil {
			log.Println("WARNING: invalid statistics interval --> no statistics logging")
		} else {
			this.statistics = statistics.New(ctx, interval)
			this.iot.SetStatisticsCollector(this.statistics)
			this.IotCache.SetStatisticsCollector(this.statistics)
		}
	} else {
		log.Println("start without statistics logging")
	}
}

func (this *Connector) StartConsumer(ctx context.Context) (err error) {
	if this.deviceCommandHandler == nil && this.asyncCommandHandler == nil {
		return errors.New("missing command handler; use SetAsyncCommandHandler() or SetDeviceCommandHandler()")
	}

	used := false
	maxWait := 100 * time.Millisecond

	if this.Config.Protocol != "" && this.Config.Protocol != "-" {
		if this.Config.KafkaConsumerMaxWait != "" && this.Config.KafkaConsumerMaxWait != "-" {
			maxWait, err = time.ParseDuration(this.Config.KafkaConsumerMaxWait)
			if err != nil {
				return errors.New("unable to parse KafkaConsumerMaxWait as duration: " + err.Error())
			}
		}

		used = true
		err = kafka.NewConsumer(ctx, kafka.ConsumerConfig{
			KafkaUrl:       this.Config.KafkaUrl,
			GroupId:        this.Config.KafkaGroupName,
			Topic:          this.Config.Protocol,
			MinBytes:       this.Config.KafkaConsumerMinBytes,
			MaxBytes:       this.Config.KafkaConsumerMaxBytes,
			MaxWait:        maxWait,
			TopicConfigMap: this.Config.KafkaTopicConfigs,
		}, func(topic string, msg []byte, t time.Time) error {
			if string(msg) == "topic_init" {
				return nil
			}
			return this.handleCommand(msg, t)
		}, func(err error) {
			log.Println("FATAL ERROR: kafka", err)
			log.Fatal(err)
		})
		if err != nil {
			return err
		}
	}

	if this.Config.HttpCommandConsumerPort != "" && this.Config.HttpCommandConsumerPort != "-" {
		used = true
		err = httpcommand.StartConsumer(ctx, this.Config.HttpCommandConsumerPort, func(msg []byte) error {
			return this.handleCommand(msg, time.Now())
		})
		if err != nil {
			return err
		}
	}

	if !used {
		return errors.New("no command consumer set; at least one of the following config fields must be set: Protocol, HttpCommandConsumerPort")
	}

	//iot cache invalidation
	if this.Config.DeviceTypeTopic != "" && this.Config.DeviceTypeTopic != "-" {
		err = kafka.NewConsumer(ctx, kafka.ConsumerConfig{
			KafkaUrl:       this.Config.KafkaUrl,
			GroupId:        this.Config.KafkaGroupName,
			Topic:          this.Config.DeviceTypeTopic,
			MinBytes:       this.Config.KafkaConsumerMinBytes,
			MaxBytes:       this.Config.KafkaConsumerMaxBytes,
			MaxWait:        maxWait,
			TopicConfigMap: this.Config.KafkaTopicConfigs,
		}, func(topic string, msg []byte, t time.Time) error {
			if string(msg) == "topic_init" {
				return nil
			}
			command := DeviceTypeCommand{}
			err = json.Unmarshal(msg, &command)
			if err != nil {
				log.Println("ERROR: unable to unmarshal "+this.Config.DeviceTypeTopic+" message", err)
				return nil
			}
			log.Println("invalidate cache for", command.Id)
			this.IotCache.InvalidateDeviceTypeCache(command.Id)
			return nil
		}, func(err error) {
			log.Println("ERROR: kafka consumer", this.Config.DeviceTypeTopic, err)
		})
	}

	return nil
}

type DeviceTypeCommand struct {
	Command string `json:"command"`
	Id      string `json:"id"`
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
	this.producer[qos], err = kafka.PrepareProducerWithConfig(ctx, this.Config.KafkaUrl, kafka.Config{
		AsyncFlushMessages:  this.Config.AsyncFlushMessages,
		AsyncFlushFrequency: this.Config.AsyncFlushFrequency,
		AsyncCompression:    this.Config.AsyncCompression,
		SyncCompression:     this.Config.SyncCompression,
		Sync:                sync,
		SyncIdempotent:      idempotent,
		PartitionNum:        partitionsNum,
		ReplicationFactor:   replFactor,
		TopicConfigMap:      this.Config.KafkaTopicConfigs,
	})
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
