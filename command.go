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
	"runtime/debug"
	"time"
)

func (this *Connector) handleCommand(msg []byte, t time.Time) (err error) {
	protocolmsg := model.ProtocolMsg{}
	err = json.Unmarshal(msg, &protocolmsg)
	if err != nil {
		log.Println("WARNING: invalid command: ", err.Error(), string(msg))
		return nil
	}
	protocolmsg.Trace = append(protocolmsg.Trace, model.Trace{
		Timestamp: time.Now().UnixNano(),
		TimeUnit:  "unix_nano",
		Location:  "github.com/SENERGY-Platform/platform-connector-lib handleCommand() after unmarshal",
	})
	protocolParts := protocolmsg.Request.Input
	if this.deviceCommandHandler != nil {
		handlerResponse, qos, err := this.useDeviceCommandHandler(protocolmsg, protocolParts)
		if err != nil {
			return err
		}
		return this.HandleCommandResponse(protocolmsg, handlerResponse, qos)
	} else if this.asyncCommandHandler != nil {
		return this.asyncCommandHandler(protocolmsg, protocolParts, t)
	}
	return errors.New("missing command handler")
}

func (this *Connector) HandleCommandResponse(commandRequest model.ProtocolMsg, commandResponse CommandResponseMsg, qos Qos) (err error) {
	if commandRequest.TaskInfo.CompletionStrategy == model.Optimistic {
		return
	}
	commandRequest.Response.Output = commandResponse
	commandRequest.Trace = append(commandRequest.Trace, model.Trace{
		Timestamp: time.Now().UnixNano(),
		TimeUnit:  "unix_nano",
		Location:  "github.com/SENERGY-Platform/platform-connector-lib HandleCommandResponse() before marshal",
	})
	responseMsg, err := json.Marshal(commandRequest)
	if err != nil {
		log.Println("ERROR in handleCommand() json.Marshal(): ", err)
		return err
	}
	producer, err := this.GetProducer(qos)
	if err != nil {
		log.Println("ERROR in handleCommand(): ", err)
		return err
	}
	err = producer.ProduceWithKey(this.Config.KafkaResponseTopic, string(responseMsg), commandRequest.Metadata.Device.Id)
	if err != nil && this.Config.FatalKafkaError {
		debug.PrintStack()
		log.Fatal("FATAL: ", err)
	}
	this.trySendingResponseAsEvent(commandRequest, commandResponse, qos)
	return err
}

func (this *Connector) useDeviceCommandHandler(msg model.ProtocolMsg, protocolParts map[string]string) (result map[string]string, qos Qos, err error) {
	return this.deviceCommandHandler(msg.Metadata.Device.Id, msg.Metadata.Device.LocalId, msg.Metadata.Service.Id, msg.Metadata.Service.LocalId, protocolParts)
}
