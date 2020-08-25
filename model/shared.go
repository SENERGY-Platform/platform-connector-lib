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

package model

type Device struct {
	Id           string `json:"id"`
	LocalId      string `json:"local_id"`
	Name         string `json:"name"`
	DeviceTypeId string `json:"device_type_id"`
}

type DeviceType struct {
	Id            string    `json:"id"`
	Name          string    `json:"name"`
	Description   string    `json:"description"`
	Services      []Service `json:"services"`
	DeviceClassId string    `json:"device_class_id"`
	RdfType       string    `json:"rdf_type"`
}

type Service struct {
	Id          string      `json:"id"`
	LocalId     string      `json:"local_id"`
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Interaction Interaction `json:"interaction"`
	AspectIds   []string    `json:"aspect_ids"`
	ProtocolId  string      `json:"protocol_id"`
	Inputs      []Content   `json:"inputs"`
	Outputs     []Content   `json:"outputs"`
	FunctionIds []string    `json:"function_ids"`
	RdfType     string      `json:"rdf_type"`
}

type Interaction string

const (
	EVENT             Interaction = "event"
	REQUEST           Interaction = "request"
	EVENT_AND_REQUEST Interaction = "event+request"
)

type Protocol struct {
	Id               string            `json:"id"`
	Name             string            `json:"name"`
	Handler          string            `json:"handler"`
	ProtocolSegments []ProtocolSegment `json:"protocol_segments"`
}

type ProtocolSegment struct {
	Id   string `json:"id"`
	Name string `json:"name"`
}
