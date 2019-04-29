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
	"bytes"
	"encoding/json"
	"errors"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
	"log"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	iot_model "github.com/SENERGY-Platform/iot-device-repository/lib/model"
)

func (this *Iot) GetDevice(id string, token security.JwtToken) (device iot_model.DeviceInstance, err error) {
	resp, err := token.Get(this.url + "/deviceInstance/" + url.QueryEscape(id))
	if err != nil {
		log.Println("ERROR on GetDevice()", err)
		return device, err
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&device)
	if err != nil {
		log.Println("ERROR on GetDevice() json decode", err)
	}
	return device, err
}

func (this *Iot) GetDevices(token security.JwtToken, limit int, offset int) (devices []iot_model.DeviceInstance, err error) {
	resp, err := token.Get(this.url + "/deviceInstances/" + strconv.Itoa(limit) + "/" + strconv.Itoa(offset) + "/execute")
	if err != nil {
		log.Println("ERROR on GetDevice()", err)
		return devices, err
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&devices)
	if err != nil {
		log.Println("ERROR on GetDevice() json decode", err)
	}
	return devices, err
}

func (this *Iot) GetDeviceType(id string, token security.JwtToken) (dt iot_model.DeviceType, err error) {
	resp, err := token.Get(this.url + "/deviceType/" + url.QueryEscape(id))
	if err != nil {
		log.Println("ERROR on GetDeviceType()", err)
		return dt, err
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&dt)
	if err != nil {
		log.Println("ERROR on GetDeviceType() json decode", err)
	}
	return dt, err
}

func (this *Iot) GetService(id string, token security.JwtToken) (service iot_model.Service, err error) {
	resp, err := token.Get(this.url + "/service/" + url.QueryEscape(id))
	if err != nil {
		log.Println("ERROR on GetDeviceType()", err)
		return service, err
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&service)
	if err != nil {
		log.Println("ERROR on GetDeviceType() json decode", err)
	}
	return service, err
}

func (this *Iot) DeviceUrlToIotDevice(deviceUrl string, token security.JwtToken) (entities []iot_model.DeviceServiceEntity, err error) {
	resp, err := token.Get(this.url + "/url_to_devices/" + url.QueryEscape(deviceUrl) + "/execute")
	if err != nil {
		log.Println("error on ConnectorDeviceToIotDevice", this.url+"/url_to_devices/"+url.QueryEscape(deviceUrl)+"/execute", err)
		return entities, err
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&entities)
	if err != nil {
		log.Println("error on ConnectorDeviceToIotDevice", err)
		return entities, err
	}
	return
}

type IotErrorMessage struct {
	StatusCode int      `json:"status_code,omitempty"`
	Message    string   `json:"message"`
	ErrorCode  string   `json:"error_code,omitempty"`
	Detail     []string `json:"detail,omitempty"`
}

func (this *Iot) CreateIotDevice(device iot_model.ProvisioningDevice, token security.JwtToken) (result iot_model.DeviceInstance, err error) {
	typeid := device.IotType

	if typeid == "" {
		return result, errors.New("empty iot_type")
	}
	resp, err := token.Get(this.url + "/ui/deviceInstance/resourceSkeleton/" + url.QueryEscape(typeid))
	if err != nil {
		log.Println("error on CreateIotDevice() resourceSkeleton", err)
		return result, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		errmsg := IotErrorMessage{}
		json.NewDecoder(resp.Body).Decode(&errmsg)
		err = errors.New("error while creating device skeleton: " + errmsg.Message)
		log.Println("ERROR: CreateIotDevice()", err, errmsg)
		return
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		log.Println("error on CreateIotDevice() decode", err)
		return result, err
	}
	result.Name = device.Name
	result.Url = device.Uri
	result.Tags = device.Tags

	b := new(bytes.Buffer)
	err = json.NewEncoder(b).Encode(result)
	if err != nil {
		return result, err
	}
	resp, err = token.Post(this.url+"/deviceInstance", "application/json", b)
	if err != nil {
		log.Println("error on CreateIotDevice() create in repository", err)
		return result, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		errmsg := IotErrorMessage{}
		json.NewDecoder(resp.Body).Decode(&errmsg)
		err = errors.New("error while creating new device: " + errmsg.Message)
		log.Println("ERROR: CreateIotDevice()", err, errmsg)
		return
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	return
}

func (this *Iot) UpdateDevice(new iot_model.ProvisioningDevice, old iot_model.DeviceServiceEntity, token security.JwtToken) (result iot_model.DeviceServiceEntity, err error) {
	clientTags := IndexTags(new.Tags)
	platformTags := IndexTags(old.Device.Tags)
	mergedTags := MergeTagIndexes(platformTags, clientTags)
	tagsChanged := !reflect.DeepEqual(platformTags, mergedTags)
	nameChanged := new.Name != old.Device.Name

	result = old
	if tagsChanged || nameChanged {
		if nameChanged {
			result.Device.Name = new.Name
		}
		if tagsChanged {
			result.Device.Tags = TagIndexToTagList(mergedTags)
		}
		err = this.UpdateDeviceInstance(result.Device, token)
	}
	return
}

func (this *Iot) UpdateDeviceInstance(device iot_model.DeviceInstance, token security.JwtToken) (err error) {
	b := new(bytes.Buffer)
	err = json.NewEncoder(b).Encode(device)
	if err != nil {
		return err
	}
	resp, err := token.Post(this.url+"/deviceInstance/"+url.QueryEscape(device.Id), "application/json", b)
	if err != nil {
		log.Println("error while doing UpdateDevice() http request: ", err)
		return err
	}
	defer resp.Body.Close()
	return
}

func IndexTags(tags []string) (result map[string]string) {
	result = map[string]string{}
	for _, tag := range tags {
		parts := strings.SplitN(tag, ":", 2)
		if len(parts) != 2 {
			log.Println("ERROR: wrong tag syntax; ", tag)
			continue
		}
		result[parts[0]] = parts[1]
	}
	return result
}

func TagIndexToTagList(index map[string]string) (tags []string) {
	for key, value := range index {
		tags = append(tags, key+":"+value)
	}
	return
}

func MergeTagIndexes(platform map[string]string, client map[string]string) (result map[string]string) {
	result = map[string]string{}
	for key, value := range platform {
		result[key] = value
	}
	for key, value := range client {
		result[key] = value
	}
	return
}

func (this *Iot) DeleteDeviceInstance(uri string, token security.JwtToken) (err error) {
	entities, err := this.DeviceUrlToIotDevice(uri, token)
	if err != nil {
		return err
	}
	if len(entities) != 1 {
		return errors.New("cant find exactly one device with given uri")
	}
	_, err = token.Delete(this.url + "/deviceInstance/" + url.QueryEscape(entities[0].Device.Id))
	return
}
