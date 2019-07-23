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
	"errors"
	"log"
	"net/url"

	"github.com/dgrijalva/jwt-go"
)

type RoleMapping struct {
	Name string `json:"name"`
}

type UserRepresentation struct {
	Id   string `json:"id"`
	Name string `json:"username"`
}

type KeycloakClaims struct {
	RealmAccess RealmAccess `json:"realm_access"`
	jwt.StandardClaims
}

type RealmAccess struct {
	Roles []string `json:"roles"`
}

func (this *Security) GetUserId(username string) (userid string, err error) {
	clientToken, err := this.Access()
	if err != nil {
		log.Println("ERROR: GetUserId::EnsureAccess()", err)
		return userid, err
	}
	users := []UserRepresentation{}
	err = clientToken.GetJSON(this.authEndpoint+"/auth/admin/realms/master/users?username="+url.QueryEscape(username), &users)
	if err != nil {
		log.Println("ERROR: Security.GetUserId::GetJSON()", err)
		this.ResetAccess()
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

func (this *Security) GetUserRoles(userid string) (roles []string, err error) {
	clientToken, err := this.Access()
	if err != nil {
		return roles, err
	}
	roleMappings := []RoleMapping{}
	err = clientToken.GetJSON(this.authEndpoint+"/auth/admin/realms/master/users/"+userid+"/role-mappings/realm", &roleMappings)
	if err != nil {
		log.Println("ERROR: getUserRoles() ", err)
		this.ResetAccess()
		return roles, err
	}
	for _, role := range roleMappings {
		roles = append(roles, role.Name)
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
