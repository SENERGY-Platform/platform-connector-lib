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
	"github.com/dgrijalva/jwt-go"
	"log"
	"strings"
	"time"
)

func (this *Security) GetUserToken(username string, password string) (token JwtToken, err error) {
	openid, err := GetOpenidPasswordToken(this.authEndpoint, this.authClientId, this.authClientSecret, username, password)
	return openid.JwtToken(), err
}

func (this *Security) GenerateUserToken(username string) (token JwtToken, err error) {
	userId, err := this.GetUserId(username)
	if err != nil {
		log.Println("ERROR: GetUserId()", err, username)
		return token, err
	}
	return this.GenerateUserTokenById(userId)
}

func (this *Security) GenerateUserTokenById(userid string) (token JwtToken, err error) {
	roles, err := this.GetUserRoles(userid)
	if err != nil {
		log.Println("ERROR: GenerateUserTokenById::getUserRoles()", err, userid)
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
			log.Println("ERROR: GenerateUserTokenById::SigningString()", err, userid)
			return token, err
		}
		tokenString := strings.Join([]string{unsignedTokenString, ""}, ".")
		token = JwtToken("Bearer " + tokenString)
	} else {
		//decode key base64 string to []byte
		b, err := base64.StdEncoding.DecodeString(this.jwtPrivateKey)
		if err != nil {
			log.Println("ERROR: GenerateUserTokenById::DecodeBase64()", err, userid)
			return token, err
		}
		//parse []byte key to go struct key (use most common encoding)
		key, err := x509.ParsePKCS1PrivateKey(b)
		tokenString, err := jwtoken.SignedString(key)
		if err != nil {
			log.Println("ERROR: GenerateUserTokenById::SignedString()", err, userid)
			return token, err
		}
		token = JwtToken("Bearer " + tokenString)
	}
	return token, err
}
