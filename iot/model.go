/*
 * Copyright 2019 InfAI (CC SES)
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

import iot_model "github.com/SENERGY-Platform/iot-device-repository/lib/model"

type ConnectorDevice struct {
	IotType string   `json:"iot_type"`
	Uri     string   `json:"uri"` //device url should be unique for the user (even if multiple connector clients of the same kind are used) for example:  <<MAC>>+<<local_device_id>>
	Name    string   `json:"name"`
	Tags    []string `json:"tags"` // tag = <<id>>:<<display>>
}

type GatewayRef struct {
	Id      string   `json:"id,omitempty"`
	Devices []string `json:"devices,omitempty"`
	Hash    string   `json:"hash,omitempty"`
}

type Gateway struct {
	Id      string                     `json:"id,omitempty"`
	Name    string                     `json:"name,omitempty"`
	Hash    string                     `json:"hash,omitempty"`
	Devices []iot_model.DeviceInstance `json:"devices,omitempty"`
}

type EndpointGenMsgPart struct {
	Msg            string `json:"msg"`
	MsgSegmentName string `json:"msg_segment_name"`
}

type EndpointGenMsg struct {
	Endpoint        string               `json:"endpoint"`
	ProtocolHandler string               `json:"protocol_handler"`
	Parts           []EndpointGenMsgPart `json:"parts"`
}

type Endpoint struct {
	Id              string `json:"id"`
	Endpoint        string `json:"endpoint"`
	Service         string `json:"service"`
	Device          string `json:"device"`
	ProtocolHandler string `json:"protocol_handler"`
}
