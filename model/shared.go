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
	Id          string      `json:"id"`
	Name        string      `json:"name"`
	Description string      `json:"description"`
	Image       string      `json:"image"`
	Services    []Service   `json:"services"`
	DeviceClass DeviceClass `json:"device_class"`
	RdfType     string      `json:"rdf_type"`
}

type Service struct {
	Id          string     `json:"id"`
	LocalId     string     `json:"local_id"`
	Name        string     `json:"name"`
	Description string     `json:"description"`
	Aspects     []Aspect   `json:"aspects"`
	ProtocolId  string     `json:"protocol_id"`
	Inputs      []Content  `json:"inputs"`
	Outputs     []Content  `json:"outputs"`
	Functions   []Function `json:"functions"`
	RdfType     string     `json:"rdf_type"`
}
