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
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"log"
)

func (this *Connector) handleCommand(msg []byte) (err error) {
	protocolmsg := model.ProtocolMsg{}
	err = json.Unmarshal(msg, &protocolmsg)
	if err != nil {
		log.Println("ERROR: handle command: ", err.Error())
		return
	}
	protocolParts := getProtocolPartMap(protocolmsg.ProtocolParts)
	var handlerResponse map[string]string
	if this.EndpointCommandHandler != nil {
		handlerResponse, err = this.useEndpointCommandHandler(protocolmsg, protocolParts)
	} else if this.EndpointCommandHandler != nil {
		handlerResponse, err = this.useDeviceCommandHandler(protocolmsg, protocolParts)
	} else {
		err = errors.New("missing command handler")
	}
	if err != nil {
		return err
	}
	resultProtocolParts := []model.ProtocolPart{}
	for name, value := range handlerResponse {
		resultProtocolParts = append(resultProtocolParts, model.ProtocolPart{Name: name, Value: value})
	}
	protocolmsg.ProtocolParts = resultProtocolParts
	response, err := json.Marshal(protocolmsg)
	if err != nil {
		log.Println("ERROR in handleCommand() json.Marshal(): ", err)
		return err
	}
	return this.producer.Produce(this.Config.KafkaResponseTopic, string(response))
}

func (this *Connector) useDeviceCommandHandler(msg model.ProtocolMsg, protocolParts map[string]string) (result map[string]string, err error) {
	return this.DeviceCommandHandler(msg.DeviceInstanceId, msg.DeviceUrl, msg.ServiceId, msg.ServiceUrl, protocolParts)
}

func (this *Connector) useEndpointCommandHandler(msg model.ProtocolMsg, protocolParts map[string]string) (result map[string]string, err error) {
	endpoint, err := this.getEndpoint(msg)
	if err != nil {
		log.Println("ERROR: handleCommand::getEndpoint()", err.Error())
		return
	}
	return this.EndpointCommandHandler(endpoint, protocolParts)
}

func (this *Connector) getEndpoint(msg model.ProtocolMsg) (endpoint string, err error) {
	token, err := this.security.Access()
	if err != nil {
		log.Println("ERROR getEndpoint::Access()", err)
		return endpoint, err
	}
	endpointStruct, err := this.iot.GetOutEndpoint(token, msg.DeviceInstanceId, msg.ServiceId)
	if err != nil {
		log.Println("ERROR getEndpoint::GetOutEndpoint()", err)
		this.security.ResetAccess()
		return endpoint, err
	}
	endpoint = endpointStruct.Endpoint
	return
}

func getProtocolPartMap(protocolParts []model.ProtocolPart) (result map[string]string) {
	result = map[string]string{}
	for _, pp := range protocolParts {
		result[pp.Name] = pp.Value
	}
	return
}
