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
	"context"
	iot2 "github.com/SENERGY-Platform/platform-connector-lib/iot/mock/iot"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"testing"
	"time"
)

func TestCache_GetProtocol(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mock, iotMockUrl, err := iot2.Mock(ctx)
	if err != nil {
		t.Error(err)
		return
	}
	iot := New(iotMockUrl, iotMockUrl, "")
	cache, err := NewCache(iot, 60, 60, 60, 2, 200*time.Millisecond)
	if err != nil {
		t.Error(err)
		return
	}
	protocol, err, _ := mock.PublishProtocolCreate(model.Protocol{
		Name:             "test",
		Handler:          "test",
		ProtocolSegments: nil,
	})
	if err != nil {
		t.Error(err)
		return
	}

	_, err = cache.WithToken("token").GetProtocol(protocol.Id)

	if err != nil {
		t.Error(err)
		return
	}

	_, err = cache.WithToken("token").GetProtocol(protocol.Id)

	if err != nil {
		t.Error(err)
		return
	}

	if len(mock.GetCalls()) != 1 {
		t.Error(mock.GetCalls())
		return
	}
}
