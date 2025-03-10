/*
 * Copyright 2020 InfAI (CC SES)
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
)

func (this *Connector) HandleClientError(userId string, clientId string, errorMessage string) {
	notificationMessage := "Error: " + errorMessage +
		"\n\nClient: " + clientId

	err := this.SendNotification(Notification{
		UserId:  userId,
		Title:   "Client-Error",
		Message: notificationMessage,
		Topic:   "mgw",
	})
	if err != nil {
		log.Println("WARNING: unable to send notification to", userId, err)
	}
}

func (this *Connector) HandleDeviceError(userId string, device model.Device, errorMessage string) {
	notificationMessage := "Error: " + errorMessage +
		"\n\nDevice: " + device.Name + " (" + device.Id + ")"

	err := this.SendNotification(Notification{
		UserId:  userId,
		Title:   "Device-Error",
		Message: notificationMessage,
		Topic:   "mgw",
	})
	if err != nil {
		log.Println("WARNING: unable to send notification to", userId, err)
	}
}

func (this *Connector) HandleCommandError(userId string, commandRequest model.ProtocolMsg, errorMessage string) {
	notificationMessage := "Error: " + errorMessage +
		"\n\nDevice: " + commandRequest.Metadata.Device.Name + " (" + commandRequest.Metadata.Device.Id + ")" +
		"\nService: " + commandRequest.Metadata.Service.Name + " (" + commandRequest.Metadata.Service.LocalId + ")"

	err := this.SendNotification(Notification{
		UserId:  userId,
		Title:   "Device-Command-Error",
		Message: notificationMessage,
		Topic:   "mgw",
	})
	if err != nil {
		log.Println("WARNING: unable to send notification to", userId, err)
		err = nil
	}

	topic := commandRequest.Metadata.ErrorTo
	if topic == "" {
		log.Println("WARNING: no Metadata.ErrorTo value set in command --> error will not be forwarded")
		return
	}

	if commandRequest.Response.Output == nil {
		commandRequest.Response.Output = map[string]string{}
	}
	commandRequest.Response.Output["error"] = errorMessage
	responseMsg, err := json.Marshal(commandRequest)
	if err != nil {
		log.Println("ERROR in handleCommand() json.Marshal(): ", err)
		return
	}

	if strings.HasPrefix(topic, "http://") || strings.HasPrefix(topic, "https://") {
		resp, err := http.Post(topic, "application/json", bytes.NewReader(responseMsg))
		if err != nil {
			log.Println("ERROR: http producer", topic, err)
			return
		}
		defer resp.Body.Close()
		respMsg, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			err = errors.New("http error producer: " + resp.Status + " " + string(respMsg))
			log.Println("ERROR:", topic, err)
			return
		}
	} else {
		producer, err := this.GetProducer(2)
		if err != nil {
			log.Println("ERROR in handleCommand(): ", err)
			return
		}
		err = producer.ProduceWithKey(topic, string(responseMsg), commandRequest.Metadata.Device.Id)
		if err != nil && this.Config.FatalKafkaError {
			if this.Config.Debug {
				debug.PrintStack()
			}
			log.Fatal("FATAL: ", err)
		}
	}
	return
}
