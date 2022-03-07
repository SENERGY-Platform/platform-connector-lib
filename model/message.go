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

package model

import "strings"

type TaskInfo struct {
	WorkerId            string `json:"worker_id"`
	TaskId              string `json:"task_id"`
	ProcessInstanceId   string `json:"process_instance_id"`
	ProcessDefinitionId string `json:"process_definition_id"`
	CompletionStrategy  string `json:"completion_strategy"`
	Time                string `json:"time"`
	TenantId            string `json:"tenant_id"`
}

type ProtocolRequest struct {
	Input map[string]string `json:"input"`
}

type ProtocolResponse struct {
	Output map[string]string `json:"output"`
}

type Metadata struct {
	Version              int64       `json:"version,omitempty"`
	Device               Device      `json:"device"`
	Service              Service     `json:"service"`
	Protocol             Protocol    `json:"protocol"`
	OutputPath           string      `json:"output_path,omitempty"`        //only for version >= 3
	OutputFunctionId     string      `json:"output_function_id,omitempty"` //only for version >= 3 if no OutputPath is known
	OutputAspectNode     *AspectNode `json:"output_aspect_node,omitempty"` //only for version >= 3 if no OutputPath is known
	InputCharacteristic  string      `json:"input_characteristic,omitempty"`
	OutputCharacteristic string      `json:"output_characteristic,omitempty"`
	ContentVariableHints []string    `json:"content_variable_hints,omitempty"`
	ResponseTo           string      `json:"response_to,omitempty"`
	ErrorTo              string      `json:"error_to,omitempty"`
}

type ProtocolMsg struct {
	Request  ProtocolRequest  `json:"request"`
	Response ProtocolResponse `json:"response"`
	TaskInfo TaskInfo         `json:"task_info"`
	Metadata Metadata         `json:"metadata"`
	Trace    []Trace          `json:"trace,omitempty"`
}

type Trace struct {
	TimeUnit  string `json:"time_unit"`
	Timestamp int64  `json:"timestamp"`
	Location  string `json:"location"`
}

const Optimistic = "optimistic"

type Envelope struct {
	DeviceId  string      `json:"device_id,omitempty"`
	ServiceId string      `json:"service_id,omitempty"`
	Value     interface{} `json:"value"`
}

func ServiceIdToTopic(id string) string {
	id = strings.ReplaceAll(id, "#", "_")
	id = strings.ReplaceAll(id, ":", "_")
	return id
}
