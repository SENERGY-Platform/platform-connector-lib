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
	"log"
	"strings"

	"github.com/SmartEnergyPlatform/formatter-lib"
)

func (this *Connector) formatEvent(token JwtToken, deviceid string, serviceid string, event formatter_lib.EventMsg) (result string, isSensor bool, err error) {
	isSensor = true
	formatter, err := formatter_lib.NewTransformer(this.Config.IotRepoUrl, token, deviceid, serviceid)
	if err != nil {
		return "", false, err
	}
	if formatter.Service.ServiceType != SENSOR_TYPE {
		log.Println("DEBUG Servicetype: ", formatter.Service.ServiceType, "!=", SENSOR_TYPE, "in ", formatter.Service.Name)
		return "", false, err
	}
	result, err = formatter.Transform(event)
	return
}

func (this *Connector) handleEvent(token JwtToken, endpoint string, protocolParts []ProtocolPart) (total int, success int, ignore int, fail int, err error) {
	endpoints, err := this.getInEndpoints(token, endpoint)
	if err != nil {
		log.Println("ERROR in handleEvent::GetInEndpoints(): ", err)
		return total, success, ignore, fail, err
	}
	total = len(endpoints)
	if total == 0 {
		log.Println("DEBUG: unknown device for endpoint ", endpoint)
		_, err = this.createNewEndpoint(token, endpoint, protocolParts) //async on iot-repo side
		if err != nil {
			log.Println("ERROR in handleEvent::CreateNewEndpoint(): ", err)
		}
		return total, success, ignore, fail, err
	}
	eventMsg := formatter_lib.EventMsg{}
	for _, part := range protocolParts {
		eventMsg = append(eventMsg, formatter_lib.ProtocolPart{Name: part.Name, Value: part.Value})
	}
	for _, endpoint := range endpoints {
		deviceId := endpoint.Device
		serviceId := endpoint.Service
		formatedEvent, isSensor, err := this.formatEvent(token, deviceId, serviceId, eventMsg)
		if err != nil {
			log.Println("ERROR: handleEvent::formatEvent() ", err)
			fail++
			continue
		}
		if !isSensor {
			log.Println("DEBUG: is not a sensor --> ignore")
			ignore++
			continue
		}

		var eventValue interface{}
		err = json.Unmarshal([]byte(formatedEvent), &eventValue)
		if err != nil {
			log.Println("ERROR: handleEvent::unmarshaling ", err)
			fail++
			continue
		}

		serviceTopic := formatId(serviceId)
		envelope := Envelope{DeviceId: deviceId, ServiceId: serviceId}
		envelope.Value = eventValue

		jsonMsg, err := json.Marshal(envelope)
		if err != nil {
			log.Println("ERROR: handleEvent::marshaling ", err)
			fail++
		} else {
			this.produce(serviceTopic, string(jsonMsg))
			this.produce(this.Config.KafkaEventTopic, string(jsonMsg))
			success++
		}
	}
	return
}

func formatId(id string) string {
	return strings.Replace(id, "#", "_", -1)
}
