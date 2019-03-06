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

package correlation

import (
	"encoding/json"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/satori/go.uuid"
)

type CorrelationService struct {
	memcached  *memcache.Client
	expiration int32
}

func New(expiration int32, memcachedServer ...string) *CorrelationService {
	return &CorrelationService{expiration: expiration, memcached: memcache.New(memcachedServer...)}
}

func (this *CorrelationService) Save(msg model.ProtocolMsg) (correlationId string, err error) {
	correlationId = uuid.NewV4().String()
	value, err := json.Marshal(msg)
	if err != nil {
		return correlationId, err
	}
	return correlationId, this.memcached.Set(&memcache.Item{Key: correlationId, Value: value, Expiration: this.expiration})
}

func (this *CorrelationService) Get(correlationId string) (msg model.ProtocolMsg, err error) {
	item, err := this.memcached.Get(correlationId)
	if err != nil {
		return msg, err
	}
	err = json.Unmarshal(item.Value, &msg)
	return
}
