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
	"github.com/SENERGY-Platform/iot-device-repository/lib/model"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
	"log"
	"net/http"
	"net/url"
	"time"
)

func (this *Iot) GetGateway(id string, cred *security.JwtToken) (gateway Gateway, err error) {
	resp, err := cred.Get(this.url + "/gateway/" + url.QueryEscape(id) + "/provide")
	if err != nil {
		log.Println("ERROR on GetGateway()", err)
		return gateway, err
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&gateway)
	if err != nil {
		log.Println("ERROR on GetGateway() json decode", err)
	}
	return gateway, err
}

func (this *Iot) GetGatewayWithoutProvisioning(id string, cred *security.JwtToken) (gateway Gateway, err error) {
	resp, err := cred.Get(this.url + "/gateway/" + url.QueryEscape(id))
	if err != nil {
		log.Println("ERROR on GetGateway()", err)
		return gateway, err
	}
	defer resp.Body.Close()

	err = json.NewDecoder(resp.Body).Decode(&gateway)
	if err != nil {
		log.Println("ERROR on GetGateway() json decode", err)
	}
	return gateway, err
}

func (this *Iot) ClearGateway(id string, cred *security.JwtToken) (err error) {
	var resp *http.Response
	for i := 0; i < 30; i++ {
		resp, err = cred.Post(this.url+"/gateway/"+url.QueryEscape(id)+"/clear", "application/json", nil)
		if resp.StatusCode == http.StatusPreconditionFailed {
			time.Sleep(1 * time.Second) //retry
			continue
		} else {
			break
		}
	}

	if err != nil {
		log.Println("error while doing ClearGateway() http request: ", err)
		return err
	}
	resp.Body.Close()
	return
}

func (this *Iot) CommitGateway(id string, gateway GatewayRef, cred *security.JwtToken) (err error) {
	var resp *http.Response
	for i := 0; i < 30; i++ {
		b := new(bytes.Buffer)
		err = json.NewEncoder(b).Encode(gateway)
		if err != nil {
			return err
		}
		resp, err = cred.Post(this.url+"/gateway/"+url.QueryEscape(id)+"/commit", "application/json", b)
		if resp.StatusCode == http.StatusPreconditionFailed {
			time.Sleep(1 * time.Second) //retry
			continue
		} else {
			break
		}
	}
	if err != nil {
		log.Println("error while doing CommitGateway() http request: ", err)
		return err
	}
	resp.Body.Close()
	return
}

func (this *Iot) CheckGateway(id string, cred *security.JwtToken) (result model.GatewayCheck, err error) {
	err = cred.GetJSON(this.url+"/gateway/"+url.QueryEscape(id)+"/check", &result)
	return
}

func (this *Iot) ProvisionGateway(id string, provisionMessage model.ProvisionMessage, cred *security.JwtToken) (resultId string, err error) {
	result := map[string]string{}
	err = cred.PostJSON(this.url+"/gateway/"+url.QueryEscape(id)+"/check", provisionMessage, &result)
	return result["gateway"], err
}
