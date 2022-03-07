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

type DeviceClass struct {
	Id    string `json:"id"`
	Image string `json:"image"`
	Name  string `json:"name"`
}

type Function struct {
	Id          string `json:"id"`
	Name        string `json:"name"`
	DisplayName string `json:"display_name"`
	Description string `json:"description"`
	ConceptId   string `json:"concept_id"`
	RdfType     string `json:"rdf_type"`
}

type Aspect struct {
	Id         string   `json:"id"`
	Name       string   `json:"name"`
	SubAspects []Aspect `json:"sub_aspects"`
}

type Concept struct {
	Id                   string   `json:"id"`
	Name                 string   `json:"name"`
	CharacteristicIds    []string `json:"characteristic_ids"`
	BaseCharacteristicId string   `json:"base_characteristic_id"`
}

type Characteristic struct {
	Id                 string           `json:"id"`
	Name               string           `json:"name"`
	DisplayUnit        string           `json:"display_unit"`
	Type               Type             `json:"type"`
	MinValue           interface{}      `json:"min_value,omitempty"`
	MaxValue           interface{}      `json:"max_value,omitempty"`
	Value              interface{}      `json:"value,omitempty"`
	SubCharacteristics []Characteristic `json:"sub_characteristics"`
}

type ConceptWithCharacteristics struct {
	Id                   string           `json:"id"`
	Name                 string           `json:"name"`
	BaseCharacteristicId string           `json:"base_characteristic_id"`
	Characteristics      []Characteristic `json:"characteristics"`
}

type Location struct {
	Id             string   `json:"id"`
	Name           string   `json:"name"`
	Description    string   `json:"description"`
	Image          string   `json:"image"`
	DeviceIds      []string `json:"device_ids"`
	DeviceGroupIds []string `json:"device_group_ids"`
}

type FilterCriteria struct {
	FunctionId    string `json:"function_id"`
	DeviceClassId string `json:"device_class_id"`
	AspectId      string `json:"aspect_id"`
}
