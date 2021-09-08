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
	"bytes"
	"encoding/json"
	"errors"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"io"
	"log"
	"net/http"
	"runtime/debug"
	"strings"
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

	topic := commandRequest.Metadata.ResponseTo
	if topic == "" {
		topic = this.Config.KafkaResponseTopic
	}

	if strings.HasPrefix(topic, "http://") || strings.HasPrefix(topic, "https://") {
		resp, err := http.Post(topic, "application/json", bytes.NewReader(responseMsg))
		if err != nil {
			log.Println("ERROR: http producer", topic, err)
			return err
		}
		defer resp.Body.Close()
		respMsg, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			err = errors.New("http producer: " + resp.Status + " " + string(respMsg))
			log.Println("ERROR:", topic, err)
			return err
		}
	} else {
		producer, err := this.GetProducer(qos)
		if err != nil {
			log.Println("ERROR in handleCommand(): ", err)
			return err
		}
		err = producer.ProduceWithKey(topic, string(responseMsg), commandRequest.Metadata.Device.Id)
		if err != nil && this.Config.FatalKafkaError {
			debug.PrintStack()
			log.Fatal("FATAL: ", err)
		}
	}

	this.trySendingResponseAsEvent(commandRequest, commandResponse, qos)
	return err
}

func (this *Connector) useDeviceCommandHandler(msg model.ProtocolMsg, protocolParts map[string]string) (result map[string]string, qos Qos, err error) {
	return this.deviceCommandHandler(msg.Metadata.Device.Id, msg.Metadata.Device.LocalId, msg.Metadata.Service.Id, msg.Metadata.Service.LocalId, protocolParts)
}
