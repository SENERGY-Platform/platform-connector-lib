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
	fallback, fallbackKnown := marshalling.Get(this.Config.SerializationFallback)
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
					if err != nil && fallbackKnown {
						out, err = fallback.Unmarshal(segmentMsg, output.ContentVariable)
					}
					if err != nil {
						return result, err
					}
					result[output.ContentVariable.Name] = out
				}
			}
		}
	}
	err = this.ValidateMsg(result, service)
	return result, err
}

func (this *Connector) fillUnitsInContent(content *model.Content, token security.JwtToken) (err error) {
	return this.fillUnitsForContentVariables(&content.ContentVariable, content, token)
}

func (this *Connector) fillUnitsForContentVariables(variable *model.ContentVariable, content *model.Content, token security.JwtToken) (err error) {
	if variable.SubContentVariables != nil && len(variable.SubContentVariables) > 0 {
		for _, subVariable := range variable.SubContentVariables {
			err = this.fillUnitsForContentVariables(&subVariable, content, token)
			if err != nil {
				return err
			}
		}
	}

	if variable.UnitReference != "" {
		parent, err := findParentOfContentVariable(variable, content)
		if err != nil {
			return err
		}
		characteristicId, err := findCharacteristicIdOfChildWithName(parent, variable.UnitReference)
		if err != nil {
			return err
		}
		characteristic, err := this.semantic.GetCharacteristicById(characteristicId, this.Config, token)
		if err != nil {
			return err
		}
		variable.Value = characteristic.Name
	}
	return nil
}

func findParentOfContentVariable(search *model.ContentVariable, content *model.Content) (parent *model.ContentVariable, err error) {
	if content.ContentVariable.Id == search.Id {
		return nil, errors.New("ContentVariable with id " + search.Id + " is ContentVariable of Content with id " + content.Id)
	}
	return findParentOfContentVariableInternal(search, &content.ContentVariable, nil)
}

func findParentOfContentVariableInternal(search *model.ContentVariable, current *model.ContentVariable, currentParent *model.ContentVariable) (parent *model.ContentVariable, err error) {
	if current.Id == search.Id {
		return currentParent, nil
	}
	if current.SubContentVariables == nil || len(current.SubContentVariables) == 0 {
		return nil, errors.New("Reached bottom while looking for parent of ContentVariable with id " + search.Id)
	}
	for _, child := range current.SubContentVariables {
		parent, err = findParentOfContentVariableInternal(search, &child, current)
		if err != nil {
			return parent, err
		}
	}
	return nil, errors.New("Could not find parent of ContentVariable with id " + search.Id)
}

func findCharacteristicIdOfChildWithName(parent *model.ContentVariable, searchName string) (characteristicId string, err error) {
	for _, neighbor := range parent.SubContentVariables {
		if neighbor.Name == searchName {
			return neighbor.CharacteristicId, nil
		}
	}
	return "", errors.New("Could not find child with name " + searchName + " for parent with id " + parent.Id)
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
		if service.LocalId == serviceUri && len(service.Outputs) > 0 {
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

func (this *Connector) sendEventEnvelope(envelope model.Envelope) error {
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
	err = this.producer.ProduceWithKey(serviceTopic, string(jsonMsg), envelope.DeviceId)
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
