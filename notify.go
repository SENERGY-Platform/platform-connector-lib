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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	developerNotifications "github.com/SENERGY-Platform/developer-notifications/pkg/client"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"io"
	"log"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

const MutedDeviceErrorsAttribute = "platform/mute-format-error"

func (this *Connector) notifyMessageFormatError(device model.Device, service model.Service, errMsg error) {
	log.Printf("DEBUG: notify device %v (%v) owners of message format error\n", device.Id, device.Name)
	if this.Config.NotificationUrl == "" {
		log.Println("WARNING: no NotificationUrl configured")
		return
	}
	if !mutedDeviceErrors(device) {
		this.notifyDeviceOwners(device.Id, createMessageFormatErrorNotification(device, service, errMsg))
	} else {
		log.Printf("DEBUG: notifications for device %v (%v) are muted\n", device.Id, device.Name)
	}
}

func mutedDeviceErrors(device model.Device) bool {
	for _, attr := range device.Attributes {
		if attr.Key == MutedDeviceErrorsAttribute && strings.ToLower(strings.TrimSpace(attr.Value)) == "true" {
			return true
		}
	}
	return false
}

func (this *Connector) notifyDeviceOwners(deviceId string, message Notification) {
	if this.Config.NotificationUrl == "" {
		log.Println("WARNING: no NotificationUrl configured")
		return
	}
	if this.Config.NotificationUserOverwrite != "" && this.Config.NotificationUserOverwrite != "-" {
		err := this.SendNotification(message)
		if err != nil {
			log.Println(err)
			debug.PrintStack()
			return
		}
	}
	token, err := this.Security().Access()
	if err != nil {
		log.Println(err)
		debug.PrintStack()
		return
	}
	rights, err := this.Iot().GetDeviceUserRights(token, deviceId)
	if err != nil {
		log.Println(err)
		debug.PrintStack()
		return
	}
	for user, userRights := range rights.UserRights {
		if userRights.Administrate {
			message.UserId = user
			err = this.SendNotification(message)
			if err != nil {
				log.Println(err)
				debug.PrintStack()
				return
			}
		}
	}
}

func createMessageFormatErrorNotification(device model.Device, service model.Service, err error) Notification {
	return Notification{
		Title:   "Device-Message Format-Error",
		Message: "Error: " + err.Error() + "\n\nDevice: " + device.Name + " (" + device.Id + ")" + "\nService: " + service.Name + " (" + service.LocalId + ")\n\nadd \"platform/mute-format-error=true\" as device attribute, to mute this notification",
	}
}

func (this *Connector) SendNotification(message Notification) error {
	if this.Config.NotificationUrl == "" {
		log.Println("WARNING: no NotificationUrl configured")
		return nil
	}
	if this.Config.NotificationUserOverwrite != "" && this.Config.NotificationUserOverwrite != "-" {
		message.UserId = this.Config.NotificationUserOverwrite
	}
	if this.devNotifications != nil {
		go func() {
			log.Println("DEBUG: send developer-notification")
			err := this.devNotifications.SendMessage(developerNotifications.Message{
				Sender: "github.com/SENERGY-Platform/platform-connector-lib",
				Title:  "Connector-User-Notification",
				Tags:   []string{"connector", "user-notification", message.UserId},
				Body:   fmt.Sprintf("Notification For %v\nTitle: %v\nMessage: %v\n", message.UserId, message.Title, message.Message),
			})
			if err != nil {
				log.Println("ERROR: unable to send developer-notification", err)
			}
		}()
	}
	b := new(bytes.Buffer)
	err := json.NewEncoder(b).Encode(message)
	if err != nil {
		return err
	}
	ignoreDuplicatesWithinS := "3600"
	if this.Config.NotificationsIgnoreDuplicatesWithinS > 0 {
		ignoreDuplicatesWithinS = strconv.Itoa(this.Config.NotificationsIgnoreDuplicatesWithinS)
	}
	endpoint := this.Config.NotificationUrl + "/notifications?ignore_duplicates_within_seconds=" + ignoreDuplicatesWithinS
	log.Printf("DEBUG: send notification to %v with %v\n", message.UserId, endpoint)
	req, err := http.NewRequest("POST", endpoint, b)
	if err != nil {
		log.Printf("tried to send notification %#v\n", message)
		return err
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	req.WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("tried to send notification %#v\n", message)
		return err
	}
	if resp.StatusCode >= 300 {
		respMsg, _ := io.ReadAll(resp.Body)
		log.Printf("tried to send notification %#v\n", message)
		log.Println("ERROR: unexpected response status from notifier", resp.StatusCode, string(respMsg))
		return errors.New("unexpected response status from notifier " + resp.Status)
	}
	return nil
}

type Notification struct {
	UserId  string `json:"userId" bson:"userId"`
	Title   string `json:"title" bson:"title"`
	Message string `json:"message" bson:"message"`
}
