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
	"time"
)

func (this *Iot) GetDevice(id string, token security.JwtToken) (device model.Device, err error) {
	start := time.Now()
	defer this.statistics.IotRead(time.Since(start))
	resp, err := token.Get(this.repo_url + "/devices/" + url.QueryEscape(id) + "?&p=x")
	if err != nil {
		return device, err
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&device)
	return device, err
}

func (this *Iot) GetDeviceType(id string, token security.JwtToken) (dt model.DeviceType, err error) {
	start := time.Now()
	defer this.statistics.IotRead(time.Since(start))
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

func (this *Iot) FindDeviceTypesWithAttributes(attributes []model.Attribute, token security.JwtToken) (dt []model.DeviceType, err error) {
	selection := []model.Selection{}
	for _, attr := range attributes {
		selection = append(selection, model.Selection{Condition: &model.ConditionConfig{
			Feature:   "features.attributes.key",
			Operation: model.QueryEqualOperation,
			Value:     attr.Key}})
		selection = append(selection, model.Selection{Condition: &model.ConditionConfig{
			Feature:   "features.attributes.value",
			Operation: model.QueryEqualOperation,
			Value:     attr.Value}})
	}
	query := model.QueryMessage{
		Resource: "device-types",
		Find: &model.QueryFind{
			Filter: &model.Selection{
				And: selection,
			},
		},
	}
	permSearchDeviceTypes := []model.PermSearchDeviceType{}
	err = token.PostJSON(this.permQueryUrl+"/v3/query", query, &permSearchDeviceTypes)
	if err != nil {
		log.Println("ERROR on FindDeviceTypesWithAttributes()", err)
		debug.PrintStack()
		return dt, err
	}

	for _, permDt := range permSearchDeviceTypes {
		ok := make([]bool, len(attributes))
		for i, attr := range attributes {
			for _, dtAttr := range permDt.Attributes {
				if dtAttr.Key == attr.Key && dtAttr.Value == attr.Value {
					ok[i] = true
					break
				}
			}
		}
		if allTrue(ok) {
			fullDt, err := this.GetDeviceType(permDt.Id, token)
			if err != nil {
				log.Println("ERROR on FindDeviceTypesWithAttributes()", err)
				debug.PrintStack()
				return dt, err
			}
			dt = append(dt, fullDt)
		}
	}
	return dt, err
}

func allTrue(arr []bool) bool {
	for i := range arr {
		if !arr[i] {
			return false
		}
	}
	return true
}

func (this *Iot) GetDeviceByLocalId(localId string, token security.JwtToken) (device model.Device, err error) {
	start := time.Now()
	defer this.statistics.IotRead(time.Since(start))
	resp, err := token.Get(this.manager_url + "/local-devices/" + url.QueryEscape(localId))
	if err != nil {
		if err != security.ErrorNotFound {
			log.Println("ERROR on GetDevice()", err)
			debug.PrintStack()
		}
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
	err = token.PostJSON(this.manager_url+"/local-devices", device, &result)
	if err != nil {
		log.Println("ERROR on CreateDevice()", err)
		debug.PrintStack()
	}
	return
}

func (this *Iot) UpdateDevice(device model.Device, token security.JwtToken) (result model.Device, err error) {
	err = token.PutJSON(this.manager_url+"/local-devices/"+device.LocalId, device, &result)
	if err != nil {
		log.Println("ERROR on CreateDevice()", err)
		debug.PrintStack()
	}
	return
}

func (this *Iot) CreateDeviceType(deviceType model.DeviceType, token security.JwtToken) (dt model.DeviceType, err error) {
	err = token.PostJSON(this.manager_url+"/device-types", deviceType, &dt)
	if err != nil {
		log.Println("ERROR on CreateDeviceType()", err)
		debug.PrintStack()
		return dt, err
	}
	return dt, err
}

func (this *Iot) UpdateDeviceType(deviceType model.DeviceType, token security.JwtToken) (dt model.DeviceType, err error) {
	err = token.PutJSON(this.manager_url+"/device-types/"+deviceType.Id, deviceType, &dt)
	if err != nil {
		log.Println("ERROR on UpdateDeviceType()", err)
		debug.PrintStack()
		return dt, err
	}
	return dt, err
}
