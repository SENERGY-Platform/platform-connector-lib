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
	"encoding/json"
	"github.com/SENERGY-Platform/platform-connector-lib/cache"
	"log"
	"sync"
	"time"
)

func New(authEndpoint string, authClientId string, authClientSecret string, jwtIssuer string, jwtPrivateKey string, jwtExpiration int64, authExpirationTimeBuffer float64, tokenCacheExpiration int32, cacheUrls []string) *Security {
	result := &Security{
		authEndpoint:             authEndpoint,
		authClientSecret:         authClientSecret,
		authClientId:             authClientId,
		jwtIssuer:                jwtIssuer,
		jwtPrivateKey:            jwtPrivateKey,
		authExpirationTimeBuffer: authExpirationTimeBuffer,
		jwtExpiration:            jwtExpiration,
		tokenCacheExpiration:     tokenCacheExpiration,
	}
	if tokenCacheExpiration != 0 && len(cacheUrls) > 0 {
		result.cache = cache.New(cacheUrls...)
	}
	return result
}

type Security struct {
	authEndpoint             string
	jwtIssuer                string
	jwtExpiration            int64
	jwtPrivateKey            string
	authExpirationTimeBuffer float64
	authClientId             string
	authClientSecret         string
	openid                   *OpenidToken
	mux                      sync.Mutex

	cache                *cache.Cache
	tokenCacheExpiration int32
}

func (this *Security) ResetAccess() {
	this.mux.Lock()
	defer this.mux.Unlock()
	b, _ := json.Marshal(this.openid)
	log.Println("reset OpenidToken: ", string(b))
	this.openid = nil
}

func (this *Security) Access() (token JwtToken, err error) {
	this.mux.Lock()
	defer this.mux.Unlock()
	if this.openid == nil {
		this.openid = &OpenidToken{}
	}
	duration := time.Now().Sub(this.openid.RequestTime).Seconds()

	if this.openid.AccessToken != "" && this.openid.ExpiresIn > duration+this.authExpirationTimeBuffer {
		token = JwtToken("Bearer " + this.openid.AccessToken)
		return
	}

	if this.openid.RefreshToken != "" && this.openid.RefreshExpiresIn > duration+this.authExpirationTimeBuffer {
		log.Println("refresh token", this.openid.RefreshExpiresIn, duration)
		openid, err := RefreshOpenidToken(this.authEndpoint, this.authClientId, this.authClientSecret, *this.openid)
		if err != nil {
			log.Println("WARNING: unable to use refreshtoken", err)
		} else {
			this.openid = &openid
			token = JwtToken("Bearer " + this.openid.AccessToken)
			return token, err
		}
	}

	log.Println("get new access token")
	openid, err := GetOpenidToken(this.authEndpoint, this.authClientId, this.authClientSecret)
	this.openid = &openid
	if err != nil {
		log.Println("ERROR: unable to get new access token", err)
		this.openid = &OpenidToken{}
	}
	token = JwtToken("Bearer " + this.openid.AccessToken)
	return
}
