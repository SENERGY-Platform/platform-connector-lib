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

import (
	"encoding/json"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
	"log"
	"net/url"
	"runtime/debug"
)

func (this *Iot) GetDevice(id string, token security.JwtToken) (device model.Device, err error) {
	resp, err := token.Get(this.repo_url + "/devices/" + url.QueryEscape(id) + "?&p=x")
	if err != nil {
		log.Println("ERROR on GetDevice()", err)
		debug.PrintStack()
		return device, err
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&device)
	if err != nil {
		log.Println("ERROR on GetDevice() json decode", err)
		debug.PrintStack()
	}
	return device, err
}

func (this *Iot) GetDeviceType(id string, token security.JwtToken) (dt model.DeviceType, err error) {
	resp, err := token.Get(this.repo_url + "/device-types/" + url.QueryEscape(id))
	if err != nil {
		log.Println("ERROR on GetDeviceType()", err)
		debug.PrintStack()
		return dt, err
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&dt)
	if err != nil {
		log.Println("ERROR on GetDeviceType() json decode", err)
		debug.PrintStack()
	}
	return dt, err
}

func (this *Iot) GetDeviceByLocalId(localId string, token security.JwtToken) (device model.Device, err error) {
	resp, err := token.Get(this.manager_url + "/local-devices/" + url.QueryEscape(localId))
	if err != nil {
		log.Println("ERROR on GetDevice()", err)
		debug.PrintStack()
		return device, err
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&device)
	if err != nil {
		log.Println("ERROR on GetDevice() json decode", err)
		debug.PrintStack()
	}
	return device, err
}


func (this *Iot) CreateDevice(device model.Device, token security.JwtToken) (result model.Device, err error) {
	err = token.PostJSON(this.manager_url + "/local-devices", device, &result)
	if err != nil {
		log.Println("ERROR on CreateDevice()", err)
		debug.PrintStack()
	}
	return
}