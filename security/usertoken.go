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

package security

import (
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/golang-jwt/jwt"
)

func (this *Security) GetUserToken(username string, password string, remoteInfo model.RemoteInfo) (token JwtToken, err error) {
	openid, err := GetOpenidPasswordToken(this.authEndpoint, this.authClientId, this.authClientSecret, username, password, remoteInfo)
	return openid.JwtToken(), err
}

func (this *Security) ExchangeUserToken(userid string, remoteInfo model.RemoteInfo) (token JwtToken, err error) {
	client := http.Client{
		Timeout: 5 * time.Second,
	}
	req, err := http.NewRequest("POST", this.authEndpoint+"/auth/realms/master/protocol/openid-connect/token", strings.NewReader(url.Values{
		"client_id":         {this.authClientId},
		"client_secret":     {this.authClientSecret},
		"grant_type":        {"urn:ietf:params:oauth:grant-type:token-exchange"},
		"requested_subject": {userid},
	}.Encode()))
	if err != nil {
		return token, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if remoteInfo.Ip != "" {
		req.Header.Set("X-Forwarded-For", remoteInfo.Ip)
	}
	if remoteInfo.Port != "" {
		req.Header.Set("X-Forwarded-Port", remoteInfo.Port)
	}
	if remoteInfo.Port != "" {
		req.Header.Set("X-Forwarded-Proto", remoteInfo.Protocol)
	}
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		this.logger.Error("unable to get user token", "error", string(body), "status-code", resp.StatusCode)
		err = errors.New("access denied")
		resp.Body.Close()
		return
	}
	var openIdToken OpenidToken
	err = json.NewDecoder(resp.Body).Decode(&openIdToken)
	if err != nil {
		return
	}
	return JwtToken("Bearer " + openIdToken.AccessToken), nil
}

func (this *Security) GenerateUserToken(username string) (token JwtToken, err error) {
	userId, err := this.GetUserId(username)
	if err != nil {
		this.logger.Error("unable to get user id", "error", err, "username", username)
		return token, err
	}
	return this.GenerateUserTokenById(userId)
}

func (this *Security) GenerateUserTokenById(userid string) (token JwtToken, err error) {
	roles, err := this.GetUserRoles(userid)
	if err != nil {
		this.logger.Error("unable to get user roles", "error", err, "userid", userid)
		return token, err
	}

	// Create the Claims
	claims := KeycloakClaims{
		RealmAccess{Roles: roles},
		jwt.StandardClaims{
			ExpiresAt: time.Now().Add(time.Duration(this.jwtExpiration)).Unix(),
			Issuer:    this.jwtIssuer,
			Subject:   userid,
		},
	}

	jwtoken := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	if this.jwtPrivateKey == "" {
		unsignedTokenString, err := jwtoken.SigningString()
		if err != nil {
			this.logger.Error("unable to sign token", "error", err, "userid", userid)
			return token, err
		}
		tokenString := strings.Join([]string{unsignedTokenString, ""}, ".")
		token = JwtToken("Bearer " + tokenString)
	} else {
		//decode key base64 string to []byte
		b, err := base64.StdEncoding.DecodeString(this.jwtPrivateKey)
		if err != nil {
			this.logger.Error("unable to decode jwt private key", "error", err, "userid", userid)
			return token, err
		}
		//parse []byte key to go struct key (use most common encoding)
		key, err := x509.ParsePKCS1PrivateKey(b)
		tokenString, err := jwtoken.SignedString(key)
		if err != nil {
			this.logger.Error("unable to sign token", "error", err, "userid", userid)
			return token, err
		}
		token = JwtToken("Bearer " + tokenString)
	}
	return token, err
}
