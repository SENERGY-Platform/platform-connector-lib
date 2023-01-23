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

package connectionlimit

import (
	"errors"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"strconv"
	"time"
)

type ConnectionLimitHandler struct {
	limit             int
	durationInSeconds int32
	cache             *memcache.Client
}

func New(limit int, durationInSeconds int32, memcacheUrl ...string) *ConnectionLimitHandler {
	m := memcache.New(memcacheUrl...)
	m.MaxIdleConns = 5
	m.Timeout = 500 * time.Millisecond
	return &ConnectionLimitHandler{
		limit:             limit,
		durationInSeconds: durationInSeconds,
		cache:             m,
	}
}

var ReachedLimitErr = errors.New("reached connection limit")

func (this *ConnectionLimitHandler) Check(connectionId string) error {
	if this == nil {
		return nil
	}
	key := "connection_limit_counter_" + connectionId
	item, err := this.cache.Get(key)
	if err == memcache.ErrCacheMiss {
		err = nil
		item = &memcache.Item{
			Key:        key,
			Value:      []byte(strconv.Itoa(0)),
			Expiration: this.durationInSeconds,
		}
	}
	if err != nil {
		return err
	}
	count, err := strconv.Atoi(string(item.Value))
	if err != nil {
		return fmt.Errorf("WARNING: unexpected value in connectionlimit value %v: %v", key, string(item.Value))
	}
	if count > this.limit {
		return ReachedLimitErr
	}
	item.Key = key
	item.Value = []byte(strconv.Itoa(count + 1))
	item.Expiration = this.durationInSeconds
	return this.cache.Set(item)
}
