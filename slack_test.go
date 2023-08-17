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
	"testing"
	"time"
)

func TestSlackNotification(t *testing.T) {
	t.Skip("missing test NotificationSlackWebhookUrl")
	client := NewSlackNotificationClient(Config{
		NotificationsIgnoreDuplicatesWithinS: 10,
		NotificationSlackWebhookUrl:          "",
	})

	err := client.Send(Notification{
		UserId:  "test-user",
		Title:   "test-title",
		Message: "test-message",
	})
	if err != nil {
		t.Error(err)
	}
}

func TestIsSlackMsgDuplicate(t *testing.T) {
	client := NewSlackNotificationClient(Config{
		NotificationsIgnoreDuplicatesWithinS: 1,
	})

	if client.isSlackMsgDuplicate("foo") {
		t.Fatal("unexpected isSlackMsgDuplicate() result")
	}
	if client.isSlackMsgDuplicate("bar") {
		t.Fatal("unexpected isSlackMsgDuplicate() result")
	}
	if !client.isSlackMsgDuplicate("foo") {
		t.Fatal("unexpected isSlackMsgDuplicate() result")
	}
	if !client.isSlackMsgDuplicate("foo") {
		t.Fatal("unexpected isSlackMsgDuplicate() result")
	}
	if !client.isSlackMsgDuplicate("bar") {
		t.Fatal("unexpected isSlackMsgDuplicate() result")
	}
	if !client.isSlackMsgDuplicate("foo") {
		t.Fatal("unexpected isSlackMsgDuplicate() result")
	}
	if len(client.hashes) != 2 {
		t.Fatal("unexpected hashes size", len(client.hashes))
	}
	time.Sleep(2 * time.Second)
	if client.isSlackMsgDuplicate("foo") {
		t.Fatal("unexpected isSlackMsgDuplicate() result")
	}
	if len(client.hashes) != 1 {
		t.Fatal("unexpected hashes size", len(client.hashes))
	}
	if client.isSlackMsgDuplicate("bar") {
		t.Fatal("unexpected isSlackMsgDuplicate() result")
	}
	if len(client.hashes) != 2 {
		t.Fatal("unexpected hashes size", len(client.hashes))
	}

}
