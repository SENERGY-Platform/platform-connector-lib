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
)

func (this *Connector) handleCommand(msg string) (err error) {
	protocolmsg := ProtocolMsg{}
	err = json.Unmarshal([]byte(msg), &protocolmsg)
	if err != nil {
		log.Println("ERROR: handle command: ", err.Error())
		return
	}
	protocolParts := getProtocolPartMap(protocolmsg.ProtocolParts)
	endpoint, err := this.getEndpoint(protocolmsg)
	if err != nil {
		log.Println("ERROR: handleCommand::getEndpoint()", err.Error())
		return
	}
	resp, err := this.CommandHandler(endpoint, protocolParts)
	if err != nil {
		return err
	}
	protocolmsg.ProtocolParts = []ProtocolPart{}
	for name, value := range resp {
		protocolmsg.ProtocolParts = append(protocolmsg.ProtocolParts, ProtocolPart{Name: name, Value: value})
	}
	response, err := json.Marshal(protocolmsg)
	if err != nil {
		log.Println("ERROR in handleCommand() json.Marshal(): ", err)
		return err
	}
	this.produce(this.Config.KafkaResponseTopic, string(response))
	return
}

func (this *Connector) getEndpoint(msg ProtocolMsg) (endpoint string, err error) {
	token, err := this.ensureAccess()
	if err != nil {
		log.Println("ERROR getEndpoint::ensureAccess()", err)
		return endpoint, err
	}
	endpointStruct, err := this.getOutEndpoint(token, msg.DeviceInstanceId, msg.ServiceId)
	if err != nil {
		log.Println("ERROR getEndpoint::GetOutEndpoint()", err)
		this.resetAccess()
		return endpoint, err
	}
	endpoint = endpointStruct.Endpoint
	return
}

func getProtocolPartMap(protocolParts []ProtocolPart) (result map[string]string) {
	result = map[string]string{}
	for _, pp := range protocolParts {
		result[pp.Name] = pp.Value
	}
	return
}
