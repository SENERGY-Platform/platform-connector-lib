/*
 * Copyright 2023 InfAI (CC SES)
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
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/slack-go/slack"
	"log"
	"strings"
	"sync"
	"time"
)

type SlackNotificationClient struct {
	mux    sync.Mutex
	hashes []SlackNotificationHash
	config Config
}

func NewSlackNotificationClient(config Config) *SlackNotificationClient {
	return &SlackNotificationClient{
		mux:    sync.Mutex{},
		hashes: []SlackNotificationHash{},
		config: config,
	}
}

func (this *SlackNotificationClient) Send(message Notification) error {
	if this == nil {
		return nil
	}
	msg := fmt.Sprintf("Notification For %v\nTitle: %v\nMessage: %v\n", message.UserId, message.Title, message.Message)
	if this.isSlackMsgDuplicate(msg) {
		return nil
	}
	timeout, _ := context.WithTimeout(context.Background(), 10*time.Second)
	err := slack.PostWebhookContext(timeout, this.config.NotificationSlackWebhookUrl, &slack.WebhookMessage{
		Text: msg,
	})
	if err != nil {
		//hide secret webhook url
		err = errors.New(strings.ReplaceAll(err.Error(), this.config.NotificationSlackWebhookUrl, "NotificationSlackWebhookUrl"))
		log.Println("ERROR: unable to send slack message:", err)
		return err
	}
	return nil
}

type SlackNotificationHash struct {
	Hash     [32]byte
	UnixTime int64
}

func (this *SlackNotificationClient) isSlackMsgDuplicate(msg string) bool {
	this.mux.Lock()
	defer this.mux.Unlock()
	hash := sha256.Sum256([]byte(msg))
	minUnisTimestamp := time.Now().Add(-time.Duration(this.config.NotificationsIgnoreDuplicatesWithinS) * time.Second).UnixMilli()
	newHashes := []SlackNotificationHash{}
	found := false
	for _, e := range this.hashes {
		if e.UnixTime > minUnisTimestamp {
			newHashes = append(newHashes, e)
			if hash == e.Hash {
				found = true
			}
		}
	}
	if !found {
		newHashes = append(newHashes, SlackNotificationHash{
			Hash:     hash,
			UnixTime: time.Now().UnixMilli(),
		})
	}
	this.hashes = newHashes
	return found
}
