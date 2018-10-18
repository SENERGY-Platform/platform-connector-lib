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
	"log"
	"net/url"
	"strings"

	"crypto/x509"
	"encoding/base64"

	"time"

	"errors"

	"github.com/dgrijalva/jwt-go"
)

type RoleMapping struct {
	Name string `json:"name"`
}

type UserRepresentation struct {
	Id   string `json:"id"`
	Name string `json:"username"`
}

func (this *Connector) getUserRoles(userid string) (roles []string, err error) {
	clientToken, err := this.ensureAccess()
	if err != nil {
		return roles, err
	}
	roleMappings := []RoleMapping{}
	err = clientToken.GetJSON(this.Config.AuthEndpoint+"/auth/admin/realms/master/users/"+userid+"/role-mappings/realm", &roleMappings)
	if err != nil {
		log.Println("ERROR: getUserRoles() ", err)
		this.resetAccess()
		return roles, err
	}
	for _, role := range roleMappings {
		roles = append(roles, role.Name)
	}
	return
}

type KeycloakClaims struct {
	RealmAccess RealmAccess `json:"realm_access"`
	jwt.StandardClaims
}

type RealmAccess struct {
	Roles []string `json:"roles"`
}

func (this *Connector) getUserTokenById(userid string) (token JwtToken, err error) {
	roles, err := this.getUserRoles(userid)
	if err != nil {
		log.Println("ERROR: GetUserTokenById::getUserRoles()", err, userid)
		return token, err
	}

	// Create the Claims
	claims := KeycloakClaims{
		RealmAccess{Roles: roles},
		jwt.StandardClaims{
			ExpiresAt: time.Now().Add(time.Duration(this.Config.JwtExpiration)).Unix(),
			Issuer:    this.Config.JwtIssuer,
			Subject:   userid,
		},
	}

	jwtoken := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	if this.Config.JwtPrivateKey == "" {
		unsignedTokenString, err := jwtoken.SigningString()
		if err != nil {
			log.Println("ERROR: GetUserTokenById::SigningString()", err, userid)
			return token, err
		}
		tokenString := strings.Join([]string{unsignedTokenString, ""}, ".")
		token = JwtToken("Bearer " + tokenString)
	} else {
		//decode key base64 string to []byte
		b, err := base64.StdEncoding.DecodeString(this.Config.JwtPrivateKey)
		if err != nil {
			log.Println("ERROR: GetUserTokenById::DecodeBase64()", err, userid)
			return token, err
		}
		//parse []byte key to go struct key (use most common encoding)
		key, err := x509.ParsePKCS1PrivateKey(b)
		tokenString, err := jwtoken.SignedString(key)
		if err != nil {
			log.Println("ERROR: GetUserTokenById::SignedString()", err, userid)
			return token, err
		}
		token = JwtToken("Bearer " + tokenString)
	}
	return token, err
}

func (this *Connector) GetUserToken(username string) (token JwtToken, err error) {
	userId, err := this.getUserId(username)
	if err != nil {
		log.Println("ERROR: GetUserId()", err, username)
		return token, err
	}
	return this.getUserTokenById(userId)
}

func (this *Connector) getUserId(username string) (userid string, err error) {
	clientToken, err := this.ensureAccess()
	if err != nil {
		log.Println("ERROR: GetUserId::EnsureAccess()", err)
		return userid, err
	}
	users := []UserRepresentation{}
	err = clientToken.GetJSON(this.Config.AuthEndpoint+"/auth/admin/realms/master/users?username="+url.QueryEscape(username), &users)
	if err != nil {
		this.resetAccess()
		return
	}
	users = filterExact(users, username)
	if len(users) == 1 {
		userid = users[0].Id
	} else {
		err = errors.New("no unambiguous user found")
	}
	return
}

func filterExact(users []UserRepresentation, name string) (result []UserRepresentation) {
	for _, user := range users {
		if user.Name == name {
			result = append(result, user)
		}
	}
	return
}
