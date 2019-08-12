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
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
	"log"
	"runtime/debug"
	"strings"
	"time"

	"github.com/SENERGY-Platform/formatter-lib"
)

func (this *Connector) formatEvent(token security.JwtToken, deviceid string, serviceid string, event formatter_lib.EventMsg) (result string, err error) {
	formatter, err := formatter_lib.NewTransformer(this.IotCache.WithToken(token), deviceid, serviceid)
	if err != nil {
		return "", err
	}
	result, err = formatter.Transform(event)
	return
}

func (this *Connector) handleEndpointEvent(token security.JwtToken, endpoint string, protocolParts []model.ProtocolPart) (total int, success int, fail int, err error) {
	endpoints, err := this.iot.GetInEndpoints(token, endpoint)
	if err != nil {
		log.Println("ERROR in handleEndpointEvent::GetInEndpoints(): ", err)
		return total, success, fail, err
	}
	total = len(endpoints)
	if total == 0 {
		log.Println("DEBUG: unknown device for endpoint ", endpoint)
		_, err = this.iot.CreateNewEndpoint(token, endpoint, protocolParts) //async on iot-repo side
		if err != nil {
			log.Println("ERROR in handleEndpointEvent::CreateNewEndpoint(): ", err)
		}
		return total, success, fail, err
	}
	for _, endpoint := range endpoints {
		err = this.handleDeviceEvent(token, endpoint.Device, endpoint.Service, protocolParts)
		if err != nil {
			fail++
		} else {
			success++
		}
	}
	return
}

func (this *Connector) handleDeviceRefEvent(token security.JwtToken, deviceUri string, serviceUri string, protocolParts []model.ProtocolPart) error {
	device, err := this.IotCache.WithToken(token).DeviceUrlToIotDevice(deviceUri)
	if err != nil {
		log.Println("ERROR: handleDeviceRefEvent::DeviceUrlToIotDevice", err)
		return err
	}
	dt, err := this.IotCache.WithToken(token).GetDeviceType(device.DeviceType)
	if err != nil {
		log.Println("ERROR: handleDeviceRefEvent::GetDeviceType", err)
		return err
	}
	for _, service := range dt.Services {
		if service.Url == serviceUri {
			err = this.handleDeviceEvent(token, device.Id, service.Id, protocolParts)
			if err != nil {
				log.Println("ERROR: handleDeviceRefEvent::handleDeviceEvent", err)
				return err
			}
		}
	}
	return nil
}

func (this *Connector) handleDeviceEvent(token security.JwtToken, deviceId string, serviceId string, protocolParts []model.ProtocolPart) (err error) {
	eventMsg := formatter_lib.EventMsg{}
	for _, part := range protocolParts {
		eventMsg = append(eventMsg, formatter_lib.ProtocolPart{Name: part.Name, Value: part.Value})
	}
	formatedEvent, err := this.formatEvent(token, deviceId, serviceId, eventMsg)
	if err != nil {
		log.Println("ERROR: handleDeviceEvent::formatEvent() ", err)
		return err
	}

	var eventValue interface{}
	err = json.Unmarshal([]byte(formatedEvent), &eventValue)
	if err != nil {
		log.Println("ERROR: handleDeviceEvent::unmarshaling ", err)
		return
	}

	serviceTopic := formatId(serviceId)
	envelope := model.Envelope{DeviceId: deviceId, ServiceId: serviceId}
	envelope.Value = eventValue

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
	err = this.producer.Produce(serviceTopic, string(jsonMsg))
	if err != nil {
		if this.Config.FatalKafkaError {
			debug.PrintStack()
			log.Fatal("FATAL: ", err)
		}
		log.Println("ERROR: produce event on service topic ", err)
		return err
	}
	if err != nil {
		log.Println("ERROR: produce event on event topic", this.Config.KafkaEventTopic, err)
		return err
	}
	return
}

func formatId(id string) string {
	return strings.Replace(id, "#", "_", -1)
}
