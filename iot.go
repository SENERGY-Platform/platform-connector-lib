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
	"log"
	"net/url"
)

func (this *Connector) getOutEndpoint(token JwtToken, deviceId, serviceId string) (endpoint Endpoint, err error) {
	err = token.PostJSON(this.Config.IotRepoUrl+"/endpoint/out", Endpoint{Device: deviceId, Service: serviceId}, &endpoint)
	if err != nil {
		log.Println("ERROR on GetOutEndpoint()", err)
	}
	return
}

func (this *Connector) CheckEndpointAuth(token JwtToken, endpoint string) (err error) {
	_, err = token.Get(this.Config.IotRepoUrl + "/endpoint/listen/auth/check/" + url.QueryEscape(this.Config.Protocol) + "/" + url.QueryEscape(endpoint))
	return
}

func (this *Connector) getInEndpoints(token JwtToken, endpointstring string) (endpoint []Endpoint, err error) {
	err = token.PostJSON(this.Config.IotRepoUrl+"/endpoint/in", Endpoint{Endpoint: endpointstring, ProtocolHandler: this.Config.Protocol}, &endpoint)
	if err != nil {
		log.Println("ERROR on GetInEndpoints()", err)
	}
	return
}

func (this *Connector) createNewEndpoint(token JwtToken, endpoint string, protocolParts []ProtocolPart) (endpoints []Endpoint, err error) {
	parts := []EndpointGenMsgPart{}
	for _, part := range protocolParts {
		parts = append(parts, EndpointGenMsgPart{MsgSegmentName:part.Name, Msg:part.Value})
	}
	err = token.PostJSON(this.Config.IotRepoUrl+"/endpoint/generate", EndpointGenMsg{
		Endpoint:        endpoint,
		ProtocolHandler: this.Config.Protocol,
		Parts: parts,
	}, &endpoints)
	return
}