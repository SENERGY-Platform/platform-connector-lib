/*
 * Copyright 2024 InfAI (CC SES)
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
	"encoding/json"
	"errors"
	"github.com/IBM/sarama"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/SENERGY-Platform/permission-search/lib/tests/docker"
	"github.com/SENERGY-Platform/platform-connector-lib/iot"
	"github.com/SENERGY-Platform/platform-connector-lib/kafka"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
	"github.com/SENERGY-Platform/platform-connector-lib/security"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestNotifyDeviceOwners(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, zkIp, err := docker.Zookeeper(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	kafkaUrl, err := docker.Kafka(ctx, wg, zkIp+":2181")
	if err != nil {
		t.Error(err)
		return
	}

	_, openSearchIp, err := docker.OpenSearch(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	_, permIp, err := docker.PermissionSearch(ctx, wg, false, kafkaUrl, openSearchIp)
	if err != nil {
		t.Error(err)
		return
	}

	producer, err := kafka.PrepareProducerWithConfig(ctx, kafkaUrl, kafka.Config{
		SyncCompression:   sarama.CompressionSnappy,
		Sync:              true,
		SyncIdempotent:    true,
		PartitionNum:      1,
		ReplicationFactor: 1,
	})
	if err != nil {
		t.Error(err)
		return
	}

	device := models.Device{
		Id:           "did",
		LocalId:      "ldid",
		Name:         "dname",
		DeviceTypeId: "dtid",
	}
	deviceCmd := DeviceCommand{Command: "PUT", Id: device.Id, Device: device, Owner: testTokenUser}
	deviceMsg, err := json.Marshal(deviceCmd)
	if err != nil {
		t.Error(err)
		return
	}
	err = producer.ProduceWithKey("devices", string(deviceMsg), device.Id)
	if err != nil {
		t.Error(err)
		return
	}

	receivedNotificationRequests := []string{}
	mux := sync.Mutex{}
	notifyMocServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mux.Lock()
		defer mux.Unlock()
		body, _ := io.ReadAll(request.Body)
		receivedNotificationRequests = append(receivedNotificationRequests, request.URL.String()+" "+string(body))
	}))
	defer notifyMocServer.Close()

	config := Config{
		PermQueryUrl:    "http://" + permIp + ":8080",
		NotificationUrl: notifyMocServer.URL,
	}

	securityMock := &SecurityMock{}

	ctl := Connector{
		Config:           config,
		iot:              iot.New("", "", config.PermQueryUrl),
		security:         securityMock,
		IotCache:         nil,
		devNotifications: nil,
	}

	time.Sleep(10 * time.Second)

	ctl.notifyDeviceOwners(device.Id, Notification{
		Title:   "test",
		Message: "test",
	})

	ctl.notifyMessageFormatError(device, models.Service{}, errors.New("test"))
	time.Sleep(time.Second)

	t.Log(receivedNotificationRequests)
	if len(receivedNotificationRequests) != 2 {
		t.Error(receivedNotificationRequests)
	}

}

type SecurityMock struct{}

const testAdminTokenUser = "admin"
const testAdminToken = `Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiIwOGM0N2E4OC0yYzc5LTQyMGYtODEwNC02NWJkOWViYmU0MWUiLCJleHAiOjE1NDY1MDcyMzMsIm5iZiI6MCwiaWF0IjoxNTQ2NTA3MTczLCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjgwMDEvYXV0aC9yZWFsbXMvbWFzdGVyIiwiYXVkIjoiZnJvbnRlbmQiLCJzdWIiOiJhZG1pbiIsInR5cCI6IkJlYXJlciIsImF6cCI6ImZyb250ZW5kIiwibm9uY2UiOiI5MmM0M2M5NS03NWIwLTQ2Y2YtODBhZS00NWRkOTczYjRiN2YiLCJhdXRoX3RpbWUiOjE1NDY1MDcwMDksInNlc3Npb25fc3RhdGUiOiI1ZGY5MjhmNC0wOGYwLTRlYjktOWI2MC0zYTBhZTIyZWZjNzMiLCJhY3IiOiIwIiwiYWxsb3dlZC1vcmlnaW5zIjpbIioiXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbInVzZXIiLCJhZG1pbiJdfSwicmVzb3VyY2VfYWNjZXNzIjp7Im1hc3Rlci1yZWFsbSI6eyJyb2xlcyI6WyJ2aWV3LXJlYWxtIiwidmlldy1pZGVudGl0eS1wcm92aWRlcnMiLCJtYW5hZ2UtaWRlbnRpdHktcHJvdmlkZXJzIiwiaW1wZXJzb25hdGlvbiIsImNyZWF0ZS1jbGllbnQiLCJtYW5hZ2UtdXNlcnMiLCJxdWVyeS1yZWFsbXMiLCJ2aWV3LWF1dGhvcml6YXRpb24iLCJxdWVyeS1jbGllbnRzIiwicXVlcnktdXNlcnMiLCJtYW5hZ2UtZXZlbnRzIiwibWFuYWdlLXJlYWxtIiwidmlldy1ldmVudHMiLCJ2aWV3LXVzZXJzIiwidmlldy1jbGllbnRzIiwibWFuYWdlLWF1dGhvcml6YXRpb24iLCJtYW5hZ2UtY2xpZW50cyIsInF1ZXJ5LWdyb3VwcyJdfSwiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwicm9sZXMiOlsidXNlciIsImFkbWluIl19.ggcFFFEsjwdfSzEFzmZt_m6W4IiSQub2FRhZVfWttDI`

func (this *SecurityMock) Access() (token security.JwtToken, err error) {
	return testAdminToken, nil
}

func (this *SecurityMock) ResetAccess() {
	//TODO implement me
	panic("implement me")
}

const testTokenUser = "testOwner"
const testtoken = `Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiIwOGM0N2E4OC0yYzc5LTQyMGYtODEwNC02NWJkOWViYmU0MWUiLCJleHAiOjE1NDY1MDcyMzMsIm5iZiI6MCwiaWF0IjoxNTQ2NTA3MTczLCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjgwMDEvYXV0aC9yZWFsbXMvbWFzdGVyIiwiYXVkIjoiZnJvbnRlbmQiLCJzdWIiOiJ0ZXN0T3duZXIiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJmcm9udGVuZCIsIm5vbmNlIjoiOTJjNDNjOTUtNzViMC00NmNmLTgwYWUtNDVkZDk3M2I0YjdmIiwiYXV0aF90aW1lIjoxNTQ2NTA3MDA5LCJzZXNzaW9uX3N0YXRlIjoiNWRmOTI4ZjQtMDhmMC00ZWI5LTliNjAtM2EwYWUyMmVmYzczIiwiYWNyIjoiMCIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJ1c2VyIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsibWFzdGVyLXJlYWxtIjp7InJvbGVzIjpbInZpZXctcmVhbG0iLCJ2aWV3LWlkZW50aXR5LXByb3ZpZGVycyIsIm1hbmFnZS1pZGVudGl0eS1wcm92aWRlcnMiLCJpbXBlcnNvbmF0aW9uIiwiY3JlYXRlLWNsaWVudCIsIm1hbmFnZS11c2VycyIsInF1ZXJ5LXJlYWxtcyIsInZpZXctYXV0aG9yaXphdGlvbiIsInF1ZXJ5LWNsaWVudHMiLCJxdWVyeS11c2VycyIsIm1hbmFnZS1ldmVudHMiLCJtYW5hZ2UtcmVhbG0iLCJ2aWV3LWV2ZW50cyIsInZpZXctdXNlcnMiLCJ2aWV3LWNsaWVudHMiLCJtYW5hZ2UtYXV0aG9yaXphdGlvbiIsIm1hbmFnZS1jbGllbnRzIiwicXVlcnktZ3JvdXBzIl19LCJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJyb2xlcyI6WyJ1c2VyIl19.ykpuOmlpzj75ecSI6cHbCATIeY4qpyut2hMc1a67Ycg`
const secendOwnerTokenUser = "secondOwner"
const secondOwnerToken = `Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiIwOGM0N2E4OC0yYzc5LTQyMGYtODEwNC02NWJkOWViYmU0MWUiLCJleHAiOjE1NDY1MDcyMzMsIm5iZiI6MCwiaWF0IjoxNTQ2NTA3MTczLCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjgwMDEvYXV0aC9yZWFsbXMvbWFzdGVyIiwiYXVkIjoiZnJvbnRlbmQiLCJzdWIiOiJzZWNvbmRPd25lciIsInR5cCI6IkJlYXJlciIsImF6cCI6ImZyb250ZW5kIiwibm9uY2UiOiI5MmM0M2M5NS03NWIwLTQ2Y2YtODBhZS00NWRkOTczYjRiN2YiLCJhdXRoX3RpbWUiOjE1NDY1MDcwMDksInNlc3Npb25fc3RhdGUiOiI1ZGY5MjhmNC0wOGYwLTRlYjktOWI2MC0zYTBhZTIyZWZjNzMiLCJhY3IiOiIwIiwiYWxsb3dlZC1vcmlnaW5zIjpbIioiXSwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbInVzZXIiXX0sInJlc291cmNlX2FjY2VzcyI6eyJtYXN0ZXItcmVhbG0iOnsicm9sZXMiOlsidmlldy1yZWFsbSIsInZpZXctaWRlbnRpdHktcHJvdmlkZXJzIiwibWFuYWdlLWlkZW50aXR5LXByb3ZpZGVycyIsImltcGVyc29uYXRpb24iLCJjcmVhdGUtY2xpZW50IiwibWFuYWdlLXVzZXJzIiwicXVlcnktcmVhbG1zIiwidmlldy1hdXRob3JpemF0aW9uIiwicXVlcnktY2xpZW50cyIsInF1ZXJ5LXVzZXJzIiwibWFuYWdlLWV2ZW50cyIsIm1hbmFnZS1yZWFsbSIsInZpZXctZXZlbnRzIiwidmlldy11c2VycyIsInZpZXctY2xpZW50cyIsIm1hbmFnZS1hdXRob3JpemF0aW9uIiwibWFuYWdlLWNsaWVudHMiLCJxdWVyeS1ncm91cHMiXX0sImFjY291bnQiOnsicm9sZXMiOlsibWFuYWdlLWFjY291bnQiLCJtYW5hZ2UtYWNjb3VudC1saW5rcyIsInZpZXctcHJvZmlsZSJdfX0sInJvbGVzIjpbInVzZXIiXX0.cq8YeUuR0jSsXCEzp634fTzNbGkq_B8KbVrwBPgceJ4`

func (this *SecurityMock) GenerateUserTokenById(userid string) (token security.JwtToken, err error) {
	switch userid {
	case testAdminTokenUser:
		return testtoken, nil
	case testTokenUser:
		return testtoken, nil
	case secendOwnerTokenUser:
		return secondOwnerToken, nil
	}
	return token, errors.New("unknown user")
}

func (this *SecurityMock) GenerateUserToken(username string) (token security.JwtToken, err error) {
	return this.GenerateUserTokenById(username)
}

func (this *SecurityMock) ExchangeUserToken(userid string, remoteInfo model.RemoteInfo) (token security.JwtToken, err error) {
	return this.GenerateUserTokenById(userid)
}

func (this *SecurityMock) GetUserToken(username string, password string, remoteInfo model.RemoteInfo) (token security.JwtToken, err error) {
	return this.GenerateUserTokenById(username)
}

func (this *SecurityMock) GetCachedUserToken(username string, remoteInfo model.RemoteInfo) (token security.JwtToken, err error) {
	return this.GenerateUserTokenById(username)
}

func (this *SecurityMock) GetUserId(username string) (userid string, err error) {
	return username, nil
}

func (this *SecurityMock) GetUserRoles(userid string) (roles []string, err error) {
	if userid == testAdminTokenUser {
		return []string{"admin"}, nil
	}
	return []string{}, nil
}

type DeviceCommand struct {
	Command string        `json:"command"`
	Id      string        `json:"id"`
	Owner   string        `json:"owner"`
	Device  models.Device `json:"device"`
}
