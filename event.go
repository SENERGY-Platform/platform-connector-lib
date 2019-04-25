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
	"strings"

	"github.com/SENERGY-Platform/formatter-lib"
)

func (this *Connector) formatEvent(token security.JwtToken, deviceid string, serviceid string, event formatter_lib.EventMsg) (result string, isSensor bool, err error) {
	isSensor = true
	formatter, err := formatter_lib.NewTransformer(this.IotCache.WithToken(token), deviceid, serviceid)
	if err != nil {
		return "", false, err
	}
	if formatter.Service.ServiceType != model.SENSOR_TYPE {
		log.Println("DEBUG Servicetype: ", formatter.Service.ServiceType, "!=", model.SENSOR_TYPE, "in ", formatter.Service.Name)
		return "", false, err
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
	entities, err := this.IotCache.WithToken(token).DeviceUrlToIotDevice(deviceUri)
	if err != nil {
		return err
	}
	for _, entity := range entities {
		for _, service := range entity.Services {
			if service.Url == serviceUri {
				err = this.handleDeviceEvent(token, entity.Device.Id, service.Id, protocolParts)
				if err != nil {
					return err
				}
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
	formatedEvent, isSensor, err := this.formatEvent(token, deviceId, serviceId, eventMsg)
	if err != nil {
		log.Println("ERROR: handleDeviceEvent::formatEvent() ", err)
		return err
	}
	if !isSensor {
		log.Println("DEBUG: is not a sensor --> ignore")
		return nil
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
	err = this.producer.Produce(serviceTopic, string(jsonMsg))
	if err != nil {
		log.Println("ERROR: produce event on service topic ", err)
		return err
	}
	if this.Config.KafkaEventTopic != "" {
		err = this.producer.Produce(this.Config.KafkaEventTopic, string(jsonMsg))
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
