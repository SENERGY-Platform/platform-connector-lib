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
	"fmt"
	"github.com/SENERGY-Platform/platform-connector-lib/iot/options"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
	"github.com/SENERGY-Platform/platform-connector-lib/statistics"
	"log"
	"net/url"
	"time"
)

func (this *Iot) GetHub(id string, cred security.JwtToken, optionals ...options.Option) (hub model.Hub, err error) {
	start := time.Now()
	defer statistics.IotRead(time.Since(start))
	resp, err := cred.Get(this.repo_url + "/hubs/" + url.QueryEscape(id) + "?&p=x")
	if err != nil {
		if !options.Silent.IsInOptions(optionals...) {
			log.Println("ERROR on GetGateway()", id, err)
		}
		return hub, err
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&hub)
	if err != nil {
		log.Println("ERROR on GetGateway() json decode", err)
	}
	return hub, err
}

func (this *Iot) CreateHub(hub model.Hub, cred security.JwtToken) (result model.Hub, err error) {
	err = cred.PostJSON(this.manager_url+"/hubs", hub, &result)
	return
}

func (this *Iot) CreateHubWithFixedId(hub model.Hub, adminToken security.JwtToken, userId string) (result model.Hub, err error) {
	err = adminToken.PutJSON(this.manager_url+"/hubs/"+hub.Id+"?user_id="+userId, hub, &result)
	return
}

func (this *Iot) ExistsHub(id string, cred security.JwtToken) (exists bool, err error) {
	var code int
	code, err = cred.Head(this.repo_url + "/hubs/" + url.QueryEscape(id) + "?p=x")
	if code < 300 {
		exists = true
	}
	if err == nil && code >= 500 {
		err = fmt.Errorf("HEAD %v/hubs/%v?p=x resulted in status code %v", this.repo_url, url.QueryEscape(id), code)
	}
	return
}

func (this *Iot) UpdateHub(id string, hub model.Hub, cred security.JwtToken) (result model.Hub, err error) {
	hub.Id = id
	err = cred.PutJSON(this.manager_url+"/hubs/"+url.QueryEscape(id), hub, &result)
	return
}

func (this *Iot) DeleteHub(id string, cred security.JwtToken) (err error) {
	_, err = cred.Delete(this.manager_url + "/hubs/" + url.QueryEscape(id))
	return
}
