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
	"errors"
	"fmt"
	"net/url"
	"time"

	devicerepo "github.com/SENERGY-Platform/device-repository/lib/client"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
	"github.com/SENERGY-Platform/platform-connector-lib/statistics"
)

func (this *Iot) GetDevice(id string, token security.JwtToken) (device model.Device, err error) {
	start := time.Now()
	defer statistics.IotRead(time.Since(start))
	resp, err := token.Get(this.repo_url + "/devices/" + url.QueryEscape(id) + "?&p=x")
	if err != nil {
		return device, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&device)
	if err != nil {
		return device, err
	}
	if device.Id == "" || device.DeviceTypeId == "" {
		err = fmt.Errorf("receive invalid device %#v", device)
		this.GetLogger().Error("unable to get device", "error", err, "deviceId", id)
		return device, err
	}
	return device, err
}

func (this *Iot) GetDeviceType(id string, token security.JwtToken) (dt model.DeviceType, err error) {
	if id == "" {
		this.GetLogger().Error("on GetDeviceType() missing id")
		return dt, errors.New("missing id")
	}
	start := time.Now()
	defer statistics.IotRead(time.Since(start))
	resp, err := token.Get(this.repo_url + "/device-types/" + url.QueryEscape(id))
	if err != nil {
		this.GetLogger().Error("on GetDeviceType()", "error", err, "id", id)
		return dt, err
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&dt)
	if err != nil {
		this.GetLogger().Error("on GetDeviceType() json decode", "error", err, "id", id)
	}
	return dt, err
}

func (this *Iot) FindDeviceTypesWithAttributes(attributes []model.Attribute, token security.JwtToken) (dt []model.DeviceType, err error) {
	options := devicerepo.DeviceTypeListOptions{
		Limit:           9999,
		Offset:          0,
		SortBy:          "name.asc",
		AttributeKeys:   nil,
		AttributeValues: nil,
	}
	for _, attr := range attributes {
		options.AttributeKeys = append(options.AttributeKeys, attr.Key)
		options.AttributeValues = append(options.AttributeValues, attr.Value)
	}
	list, _, err, _ := this.devicerepo.ListDeviceTypesV3(string(token), options)
	if err != nil {
		return dt, err
	}
	for _, permDt := range list {
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
			dt = append(dt, permDt)
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
	defer statistics.IotRead(time.Since(start))
	resp, err := token.Get(this.manager_url + "/local-devices/" + url.QueryEscape(localId))
	if err != nil {
		if !errors.Is(err, security.ErrorNotFound) {
			this.GetLogger().Error("unable to get device", "error", err, "localId", localId)
		}
		return device, err
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&device)
	if err != nil {
		this.GetLogger().Error("unable to decode device", "error", err, "localId", localId)
	}
	return device, err
}

func (this *Iot) CreateDevice(device model.Device, token security.JwtToken) (result model.Device, err error) {
	err = token.PostJSON(this.manager_url+"/local-devices", device, &result)
	if err != nil {
		this.GetLogger().Error("unable to create device", "error", err, "device", device)
	}
	return
}

func (this *Iot) UpdateDevice(device model.Device, token security.JwtToken) (result model.Device, err error) {
	err = token.PutJSON(this.manager_url+"/local-devices/"+device.LocalId, device, &result)
	if err != nil {
		this.GetLogger().Error("unable to update device", "error", err, "device", device)
	}
	return
}

func (this *Iot) CreateDeviceType(deviceType model.DeviceType, token security.JwtToken) (dt model.DeviceType, err error) {
	err = token.PostJSON(this.manager_url+"/device-types", deviceType, &dt)
	if err != nil {
		this.GetLogger().Error("unable to create device type", "error", err, "deviceType", deviceType)
		return dt, err
	}
	return dt, err
}

func (this *Iot) UpdateDeviceType(deviceType model.DeviceType, token security.JwtToken) (dt model.DeviceType, err error) {
	err = token.PutJSON(this.manager_url+"/device-types/"+deviceType.Id, deviceType, &dt)
	if err != nil {
		this.GetLogger().Error("unable to update device type", "error", err, "deviceType", deviceType)
		return dt, err
	}
	return dt, err
}

func (this *Iot) GetDeviceUserRights(token security.JwtToken, deviceId string) (rights model.ResourceRights, err error) {
	resource, err, _ := this.perm.GetResource(string(token), "devices", deviceId)
	if err != nil {
		return rights, err
	}
	return resource.ResourcePermissions, nil
}

type ResourceRights struct {
	ResourceId  string                 `json:"resource_id"`
	Features    map[string]interface{} `json:"features"`
	UserRights  map[string]Right       `json:"user_rights"`
	GroupRights map[string]Right       `json:"group_rights"`
	Creator     string                 `json:"creator"`
}

type Right struct {
	Read         bool `json:"read"`
	Write        bool `json:"write"`
	Execute      bool `json:"execute"`
	Administrate bool `json:"administrate"`
}
