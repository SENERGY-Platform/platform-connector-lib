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
	"github.com/SENERGY-Platform/platform-connector-lib/marshalling"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
	"github.com/SENERGY-Platform/platform-connector-lib/statistics"
	"github.com/SENERGY-Platform/platform-connector-lib/unitreference"
	"log"
	"runtime/debug"
	"sync"
	"time"
)

func (this *Connector) unmarshalMsgFromRef(token security.JwtToken, device model.Device, service model.Service, msg map[string]string, iot *iot.Cache) (result map[string]interface{}, err error) {
	result = map[string]interface{}{}
	protocol, err := iot.GetProtocol(service.ProtocolId)
	if err != nil {
		return result, err
	}
	return this.unmarshalMsg(token, device, service, protocol, msg)
}

func (this *Connector) unmarshalMsg(token security.JwtToken, device model.Device, service model.Service, protocol model.Protocol, msg map[string]string) (result map[string]interface{}, err error) {
	result = map[string]interface{}{}
	fallback, fallbackKnown := marshalling.Get(this.Config.SerializationFallback)
	for _, output := range service.Outputs {
		if output.ContentVariable.Name != "" && (fallbackKnown || output.Serialization != "") {
			marshaller, ok := marshalling.Get(string(output.Serialization))
			if !ok {
				return result, errors.New("unknown format " + string(output.Serialization))
			}
			for _, segment := range protocol.ProtocolSegments {
				if segment.Id == output.ProtocolSegmentId {
					segmentMsg, ok := msg[segment.Name]
					if ok {
						out, err := marshaller.Unmarshal(segmentMsg, output.ContentVariable)
						if err != nil && fallbackKnown {
							out, err = fallback.Unmarshal(segmentMsg, output.ContentVariable)
						}
						if err != nil {
							this.notifyMessageFormatError(device, service, err)
							return result, err
						}
						result[output.ContentVariable.Name] = out
					}
				}
			}
		}
	}

	err = unitreference.FillUnitsForService(&service, token, this.IotCache)
	if err != nil {
		if !errors.Is(err, security.ErrorInternal) {
			this.notifyMessageFormatError(device, service, err)
		}
		return result, err
	}
	result, err = this.CleanMsg(result, service)
	if err != nil {
		this.notifyMessageFormatError(device, service, err)
		return result, err
	}
	err = this.ValidateMsg(result, service)
	if err != nil {
		this.notifyMessageFormatError(device, service, err)
		return result, err
	}
	return result, err
}

func (this *Connector) handleDeviceRefEvent(token security.JwtToken, deviceUri string, serviceUri string, msg EventMsg, qos Qos) error {
	device, err := this.IotCache.WithToken(token).GetDeviceByLocalId(deviceUri)
	if err != nil {
		log.Println("ERROR: handleDeviceRefEvent::DeviceUrlToIotDevice", err)
		return err
	}
	dt, err := this.IotCache.WithToken(token).GetDeviceType(device.DeviceTypeId)
	if err != nil {
		log.Println("ERROR: handleDeviceRefEvent::GetDeviceType", err)
		return err
	}
	found := false
	for _, service := range dt.Services {
		if service.LocalId == serviceUri && len(service.Outputs) > 0 {
			err = this.handleDeviceEvent(token, device.Id, service.Id, msg, qos)
			if err != nil {
				log.Println("ERROR: handleDeviceRefEvent::handleDeviceEvent", err)
				return err
			}
			found = true
		}
	}
	if !found {
		return ErrorUnknownLocalServiceId
	}
	return nil
}

func (this *Connector) handleDeviceEvent(token security.JwtToken, deviceId string, serviceId string, msg EventMsg, qos Qos) (err error) {
	cache := this.IotCache.WithToken(token)
	device, err := cache.GetDevice(deviceId)
	if err != nil {
		return err
	}
	dt, err := cache.GetDeviceType(device.DeviceTypeId)
	if err != nil {
		return err
	}
	service := model.Service{}
	for _, s := range dt.Services {
		if s.Id == serviceId {
			service = s
			break
		}
	}
	if len(service.Id) == 0 {
		return errors.New("unknown service id")
	}
	eventValue, err := this.unmarshalMsgFromRef(token, device, service, msg, cache)
	if err != nil {
		return err
	}
	envelope := model.Envelope{DeviceId: deviceId, ServiceId: serviceId}
	envelope.Value = eventValue
	pl, err := token.GetPayload()
	if err != nil {
		return err
	}

	return this.sendEventEnvelope(envelope, qos, service, pl.UserId)
}

func (this *Connector) trySendingResponseAsEvent(cmd model.ProtocolMsg, resp CommandResponseMsg, qos Qos) {
	token, err := this.Security().Access()
	if err != nil {
		log.Println("ERROR: trySendingResponseAsEvent()", err)
		debug.PrintStack()
		return
	}
	eventValue, err := this.unmarshalMsg(token, cmd.Metadata.Device, cmd.Metadata.Service, cmd.Metadata.Protocol, resp)
	if err != nil {
		log.Println("ERROR: trySendingResponseAsEvent()", err)
		debug.PrintStack()
		return
	}

	deviceId := TrimIdModifier(cmd.Metadata.Device.Id)
	envelope := model.Envelope{DeviceId: deviceId, ServiceId: cmd.Metadata.Service.Id}
	envelope.Value = eventValue

	pl, err := token.GetPayload()
	if err != nil {
		log.Println("ERROR: trySendingResponseAsEvent()", err)
		debug.PrintStack()
		return
	}

	err = this.sendEventEnvelope(envelope, qos, cmd.Metadata.Service, pl.UserId)
	if err != nil {
		log.Println("ERROR: trySendingResponseAsEvent()", err)
		debug.PrintStack()
		return
	}
}

func (this *Connector) sendEventEnvelope(envelope model.Envelope, qos Qos, service model.Service, userId string) error {
	jsonMsg, err := json.Marshal(envelope)
	if err != nil {
		log.Println("ERROR: handleDeviceEvent::marshaling ", err)
		return err
	}
	if this.Config.Debug {
		now := time.Now()
		defer func(start time.Time) {
			log.Println("DEBUG: kafka produce in", time.Now().Sub(start))
		}(now)
	}
	serviceTopic := model.ServiceIdToTopic(envelope.ServiceId)
	producer, err := this.GetProducer(qos)
	if err != nil {
		log.Println("ERROR in sendEventEnvelope(): ", err)
		return err
	}

	var pgErr error
	var kafkaErr error

	pgWg := sync.WaitGroup{}
	if this.Config.PublishToPostgres {
		if qos == Async {
			this.asyncPgBackpressure <- true //reserve one of the limited go routines
		} else {
			pgWg.Add(1)
		}

		go func() {
			defer func() {
				if qos == Async {
					<-this.asyncPgBackpressure //free one of the limited go routines
				} else {
					defer pgWg.Done()
				}
			}()
			timescaleStart := time.Now()
			pgErr, shouldNotify := this.postgresPublisher.Publish(envelope, service)
			if pgErr != nil {
				log.Println("ERROR: publish event to postgres ", pgErr)
				if shouldNotify {
					this.notifyDeviceOnwners(envelope.DeviceId, Notification{
						Title:   "DeviceType Timescale Configuration Error",
						Message: "Error: " + err.Error() + "\n\nDeviceId: " + envelope.DeviceId + "\nService: " + service.Name + " (" + service.LocalId + ")",
					})
				}
				return
			}
			statistics.TimescaleWrite(time.Since(timescaleStart), userId)
			return
		}()
	}
	kafkaStart := time.Now()
	kafkaErr = producer.ProduceWithKey(serviceTopic, string(jsonMsg), envelope.DeviceId)
	if err != nil {
		if this.Config.FatalKafkaError {
			debug.PrintStack()
			log.Fatal("FATAL: while producing for topic: '", serviceTopic, "' :", kafkaErr)
		}
		log.Println("ERROR: produce event on service topic ", kafkaErr)
	}
	statistics.KafkaWrite(time.Since(kafkaStart), userId)

	pgWg.Wait()

	if kafkaErr != nil {
		return kafkaErr
	}
	if pgErr != nil {
		return pgErr
	}
	return nil
}
