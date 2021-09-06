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

package connectionlog

import (
	"context"
	"github.com/SENERGY-Platform/platform-connector-lib/connectionlog/test/helper"
	"github.com/SENERGY-Platform/platform-connector-lib/connectionlog/test/server"
	"github.com/segmentio/kafka-go"

	"testing"
	"time"
)

func Test(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer time.Sleep(10 * time.Second) //wait for docker cleanup
	defer cancel()

	config, err := server.New(ctx)
	if err != nil {
		t.Error(err)
		return
	}

	logger, err := New(ctx, config.KafkaUrl, true, false, config.DeviceLogTopic, config.HubLogTopic, 2, 1)
	if err != nil {
		t.Error(err)
		return
	}

	var deviceId string
	var hubId string

	t.Run("create device", func(t *testing.T) {
		deviceId = createDevice(t, config.KafkaUrl)
	})

	t.Run("create hub", func(t *testing.T) {
		hubId = createHub(t, config.KafkaUrl)
	})

	t.Run("send device log", func(t *testing.T) {
		err = logger.LogDeviceConnect(deviceId)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("send hub log", func(t *testing.T) {
		err = logger.LogHubConnect(hubId)
		if err != nil {
			t.Fatal(err)
		}
	})

	time.Sleep(10 * time.Second)

	t.Run("send device log", func(t *testing.T) {
		checkDeviceLog(t, config.ConnectionlogUrl, deviceId)
	})

	t.Run("send hub log", func(t *testing.T) {
		checkHubLog(t, config.ConnectionlogUrl, hubId)
	})

}

func checkDeviceLog(t *testing.T, connectionlogUrl string, id string) {
	t.Run("check device state", func(t *testing.T) {
		checkDeviceState(t, connectionlogUrl, id)
	})
	t.Run("check device history", func(t *testing.T) {
		checkDeviceHistory(t, connectionlogUrl, id)
	})
}

func checkDeviceHistory(t *testing.T, connectionlogUrl string, id string) {
	result := []interface{}{}
	err := helper.AdminPost(connectionlogUrl+"/intern/history/device/1h", []string{id}, &result)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatal(result)
	}
}

func checkDeviceState(t *testing.T, connectionlogUrl string, id string) {
	result := map[string]bool{}
	err := helper.AdminPost(connectionlogUrl+"/intern/state/device/check", []string{id}, &result)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 || result[id] != true {
		t.Fatal(result)
	}
}

func checkHubLog(t *testing.T, connectionlogUrl string, id string) {
	t.Run("check hub state", func(t *testing.T) {
		checkHubState(t, connectionlogUrl, id)
	})
	t.Run("check hub history", func(t *testing.T) {
		checkHubHistory(t, connectionlogUrl, id)
	})
}

func checkHubHistory(t *testing.T, connectionlogUrl string, id string) {
	result := []interface{}{}
	err := helper.AdminPost(connectionlogUrl+"/intern/history/gateway/1h", []string{id}, &result)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatal(result)
	}
}

func checkHubState(t *testing.T, connectionlogUrl string, id string) {
	result := map[string]bool{}
	err := helper.AdminPost(connectionlogUrl+"/intern/state/gateway/check", []string{id}, &result)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 || result[id] != true {
		t.Fatal(result)
	}
}

func createDevice(t *testing.T, kafkaUrl string) string {
	err := helper.InitTopic(kafkaUrl, "devices")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	broker, err := helper.GetBroker(kafkaUrl)
	if err != nil {
		t.Fatal(err)
	}
	if len(broker) == 0 {
		t.Fatal(broker)
	}
	producer, err := helper.GetProducer(broker, "devices", true)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	defer time.Sleep(2 * time.Second)
	err = producer.WriteMessages(
		context.Background(),
		kafka.Message{
			Key:   []byte("cmd.Id"),
			Value: []byte(`{"command":"PUT","id":"device-id","owner":"dd69ea0d-f553-4336-80f3-7f4567f85c7b","device":{"id":"device-id","local_id":"device-local-id","name":"device-name","device_type_id":"dt-id"}}`),
			Time:  time.Now(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return "device-id"
}

func createHub(t *testing.T, kafkaUrl string) string {
	err := helper.InitTopic(kafkaUrl, "hubs")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	broker, err := helper.GetBroker(kafkaUrl)
	if err != nil {
		t.Fatal(err)
	}
	if len(broker) == 0 {
		t.Fatal(broker)
	}
	producer, err := helper.GetProducer(broker, "hubs", true)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	defer time.Sleep(2 * time.Second)
	err = producer.WriteMessages(
		context.Background(),
		kafka.Message{
			Key:   []byte("cmd.Id"),
			Value: []byte(`{"command":"PUT","id":"hub-id","owner":"dd69ea0d-f553-4336-80f3-7f4567f85c7b","hub":{"id":"hub-id","name":"hub-name","hash":"hash-value","device_local_ids":["device-local-id"]}}`),
			Time:  time.Now(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	return "hub-id"
}
