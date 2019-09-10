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
	"github.com/SENERGY-Platform/platform-connector-lib/marshalling"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
	"log"
	"runtime/debug"
	"time"
)

func (this *Connector) unmarshalMsgFromRef(token security.JwtToken, deviceid string, serviceid string, msg map[string]string) (result map[string]interface{}, err error) {
	result = map[string]interface{}{}
	iot := this.IotCache.WithToken(token)
	device, err := iot.GetDevice(deviceid)
	if err != nil {
		return result, err
	}
	dt, err := iot.GetDeviceType(device.DeviceTypeId)
	if err != nil {
		return result, err
	}
	for _, service := range dt.Services {
		if service.Id == serviceid {
			protocol, err := iot.GetProtocol(service.ProtocolId)
			if err != nil {
				return result, err
			}
			return this.unmarshalMsg(token, device, service, protocol, msg)
		}
	}
	return result, errors.New("unknown service id")
}

func (this *Connector) unmarshalMsg(token security.JwtToken, device model.Device, service model.Service, protocol model.Protocol, msg map[string]string) (result map[string]interface{}, err error) {
	result = map[string]interface{}{}
	for _, output := range service.Outputs {
		marshaller, ok := marshalling.Get(output.Serialization)
		if !ok {
			return result, errors.New("unknown format " + output.Serialization)
		}
		for _, segment := range protocol.ProtocolSegments {
			if segment.Id == output.ProtocolSegmentId {
				segmentMsg, ok := msg[segment.Name]
				if ok {
					out, err := marshaller.Unmarshal(segmentMsg, output.ContentVariable)
					if err != nil {
						return result, err
					}
					result[output.ContentVariable.Name] = out
				}
			}
		}
	}
	return result, nil
}

func (this *Connector) handleDeviceRefEvent(token security.JwtToken, deviceUri string, serviceUri string, msg EventMsg) error {
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
	for _, service := range dt.Services {
		if service.LocalId == serviceUri {
			err = this.handleDeviceEvent(token, device.Id, service.Id, msg)
			if err != nil {
				log.Println("ERROR: handleDeviceRefEvent::handleDeviceEvent", err)
				return err
			}
		}
	}
	return nil
}

func (this *Connector) handleDeviceEvent(token security.JwtToken, deviceId string, serviceId string, msg EventMsg) (err error) {
	eventValue, err := this.unmarshalMsgFromRef(token, deviceId, serviceId, msg)
	if err != nil {
		return err
	}
	envelope := model.Envelope{DeviceId: deviceId, ServiceId: serviceId}
	envelope.Value = eventValue
	return this.sendEventEnvelope(envelope)
}

func (this *Connector) trySendingResponseAsEvent(cmd model.ProtocolMsg, resp CommandResponseMsg) {
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

	envelope := model.Envelope{DeviceId: cmd.Metadata.Device.Id, ServiceId: cmd.Metadata.Service.Id}
	envelope.Value = eventValue

	err = this.sendEventEnvelope(envelope)
	if err != nil {
		log.Println("ERROR: trySendingResponseAsEvent()", err)
		debug.PrintStack()
		return
	}
}

func (this *Connector) sendEventEnvelope(envelope model.Envelope)error{
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
	err = this.producer.Produce(serviceTopic, string(jsonMsg))
	if err != nil {
		if this.Config.FatalKafkaError {
			debug.PrintStack()
			log.Fatal("FATAL: while producing for topic: '", serviceTopic, "' :", err)
		}
		log.Println("ERROR: produce event on service topic ", err)
		return err
	}
	return nil
}