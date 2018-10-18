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
	"io"
	"log"
	"net/http"
	"time"

	"net/url"
)

type JwtToken string

func (this JwtToken) Post(url string, contentType string, body io.Reader) (resp *http.Response, err error) {
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", string(this))
	req.Header.Set("Content-Type", contentType)

	resp, err = http.DefaultClient.Do(req)

	if err == nil && resp.StatusCode == 401 {
		buf := new(bytes.Buffer)
		buf.ReadFrom(resp.Body)
		resp.Body.Close()
		log.Println(buf.String())
		err = errors.New("access denied")
	}
	return
}

func (this JwtToken) PostJSON(url string, body interface{}, result interface{}) (err error) {
	b := new(bytes.Buffer)
	err = json.NewEncoder(b).Encode(body)
	if err != nil {
		return
	}
	resp, err := this.Post(url, "application/json", b)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if result != nil {
		err = json.NewDecoder(resp.Body).Decode(result)
	}
	return
}

func (this JwtToken) Get(url string) (resp *http.Response, err error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", string(this))
	resp, err = http.DefaultClient.Do(req)

	if err == nil && resp.StatusCode == 401 {
		buf := new(bytes.Buffer)
		buf.ReadFrom(resp.Body)
		resp.Body.Close()
		log.Println(buf.String())
		err = errors.New("access denied")
	}
	return
}

func (this JwtToken) GetJSON(url string, result interface{}) (err error) {
	resp, err := this.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(result)
}

type OpenidToken struct {
	AccessToken      string    `json:"access_token"`
	ExpiresIn        float64   `json:"expires_in"`
	RefreshExpiresIn float64   `json:"refresh_expires_in"`
	RefreshToken     string    `json:"refresh_token"`
	TokenType        string    `json:"token_type"`
	RequestTime      time.Time `json:"-"`
}

func (this *OpenidToken) JwtToken() JwtToken {
	return JwtToken("Bearer " + this.AccessToken)
}

func (this *Connector) resetAccess() {
	b, _ := json.Marshal(this.openid)
	log.Println("reset OpenidToken: ", string(b))
	this.openid = nil
}

func (this *Connector) ensureAccess() (token JwtToken, err error) {
	if this.openid == nil {
		this.openid = &OpenidToken{}
	}
	duration := time.Now().Sub(this.openid.RequestTime).Seconds()

	if this.openid.AccessToken != "" && this.openid.ExpiresIn > duration+this.Config.AuthExpirationTimeBuffer {
		token = JwtToken("Bearer " + this.openid.AccessToken)
		return
	}

	if this.openid.RefreshToken != "" && this.openid.RefreshExpiresIn < duration+this.Config.AuthExpirationTimeBuffer {
		log.Println("refresh token", this.openid.RefreshExpiresIn, duration)
		err = this.refreshOpenidToken()
		if err != nil {
			log.Println("WARNING: unable to use refreshtoken", err)
		} else {
			token = JwtToken("Bearer " + this.openid.AccessToken)
			return
		}
	}

	log.Println("get new access token")
	err = this.getOpenidToken()
	if err != nil {
		log.Println("ERROR: unable to get new access token", err)
		this.openid = &OpenidToken{}
	}
	token = JwtToken("Bearer " + this.openid.AccessToken)
	return
}

func (this *Connector) getOpenidToken() (err error) {
	requesttime := time.Now()
	resp, err := http.PostForm(this.Config.AuthEndpoint+"/auth/realms/master/protocol/openid-connect/token", url.Values{
		"client_id":     {this.Config.AuthClientId},
		"client_secret": {this.Config.AuthClientSecret},
		"grant_type":    {"client_credentials"},
	})

	if err != nil {
		return err
	}
	err = json.NewDecoder(resp.Body).Decode(this.openid)
	this.openid.RequestTime = requesttime
	return
}

func (this *Connector) refreshOpenidToken() (err error) {
	requesttime := time.Now()
	resp, err := http.PostForm(this.Config.AuthEndpoint+"/auth/realms/master/protocol/openid-connect/token", url.Values{
		"client_id":     {this.Config.AuthClientId},
		"client_secret": {this.Config.AuthClientSecret},
		"refresh_token": {this.openid.RefreshToken},
		"grant_type":    {"refresh_token"},
	})

	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusUnauthorized {
		return errors.New("access denied")
	}
	err = json.NewDecoder(resp.Body).Decode(this.openid)
	this.openid.RequestTime = requesttime
	return
}

func (this *Connector) GetOpenidPasswordToken(username, password string) (token OpenidToken, err error) {
	requesttime := time.Now()
	resp, err := http.PostForm(this.Config.AuthEndpoint+"/auth/realms/master/protocol/openid-connect/token", url.Values{
		"client_id":     {this.Config.AuthClientId},
		"client_secret": {this.Config.AuthClientSecret},
		"username":      {username},
		"password":      {password},
		"grant_type":    {"password"},
	})

	if err != nil {
		return token, err
	}
	if resp.StatusCode == http.StatusUnauthorized {
		return token, errors.New("access denied")
	}
	err = json.NewDecoder(resp.Body).Decode(&token)
	token.RequestTime = requesttime
	return
}
