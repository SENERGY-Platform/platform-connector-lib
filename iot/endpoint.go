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

package iot

import (
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
	"log"
	"net/url"
)

func (this *Iot) GetOutEndpoint(token security.JwtToken, deviceId, serviceId string) (endpoint Endpoint, err error) {
	err = token.GetJSON(this.repo_url+"/endpoints?device="+url.QueryEscape(deviceId)+"&service="+url.QueryEscape(serviceId), &endpoint)
	if err != nil {
		log.Println("ERROR on GetOutEndpoint()", err)
	}
	return
}

func (this *Iot) CheckEndpointAuth(token security.JwtToken, endpoint string) (err error) {
	_, err = token.Get(this.semantic_url + "/endpoint/listen/auth/check/" + url.QueryEscape(this.protocol) + "/" + url.QueryEscape(endpoint))
	return
}

func (this *Iot) GetInEndpoints(token security.JwtToken, endpointstring string) (endpoint []Endpoint, err error) {
	err = token.GetJSON(this.repo_url+"/endpoints?endpoint="+url.QueryEscape(endpointstring)+"&protocol="+url.QueryEscape(this.protocol), &endpoint)
	if err != nil {
		log.Println("ERROR on GetInEndpoints()", err)
	}
	return
}

func (this *Iot) CreateNewEndpoint(token security.JwtToken, endpoint string, protocolParts []model.ProtocolPart) (endpoints []Endpoint, err error) {
	parts := []EndpointGenMsgPart{}
	for _, part := range protocolParts {
		parts = append(parts, EndpointGenMsgPart{MsgSegmentName: part.Name, Msg: part.Value})
	}
	err = token.PostJSON(this.semantic_url+"/endpoint/generate", EndpointGenMsg{
		Endpoint:        endpoint,
		ProtocolHandler: this.protocol,
		Parts:           parts,
	}, &endpoints)
	return
}
