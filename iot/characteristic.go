/*
 * Copyright 2020 InfAI (CC SES)
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
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
	"github.com/SENERGY-Platform/platform-connector-lib/statistics"
	"github.com/SENERGY-Platform/service-commons/pkg/cache"
	"io"
	"time"
)

const characteristicCachePrefix = "characteristic."

func (this *PreparedCache) GetCharacteristicById(id string, token security.JwtToken) (characteristic model.Characteristic, err error) {
	return cache.Use(this.cache, characteristicCachePrefix+id, func() (model.Characteristic, error) {
		return this.iot.GetCharacteristicById(id, token)
	}, func(c model.Characteristic) error {
		if c.Id == "" {
			return errors.New("missing characteristic.id")
		}
		return nil
	}, time.Duration(this.characteristicExpiration)*time.Second)
}

func (this *Iot) GetCharacteristicById(id string, token security.JwtToken) (characteristic model.Characteristic, err error) {
	start := time.Now()
	defer statistics.IotRead(time.Since(start))
	if id == "" {
		return characteristic, errors.New("characteristic id can not be empty")
	}
	resp, err := token.Get(this.repo_url + "/characteristics/" + id)
	if err != nil {
		return characteristic, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return characteristic, err
	}
	err = json.Unmarshal(body, &characteristic)
	if err != nil {
		return characteristic, err
	}
	return characteristic, err
}
