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

package server

import (
	"context"
	"github.com/SENERGY-Platform/device-repository/lib/tests/testutils/docker"
	"github.com/SENERGY-Platform/platform-connector-lib/connectionlog/test/config"
	"log"
	"net"
	"runtime/debug"
	"strconv"
	"sync"
)

func New(ctx context.Context, wg *sync.WaitGroup) (config config.Config, err error) {
	config.HubLogTopic = "gateway_log"
	config.DeviceLogTopic = "device_log"

	_, zk, err := docker.Zookeeper(ctx, wg)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		return config, err
	}
	zkUrl := zk + ":2181"

	config.KafkaUrl, err = docker.Kafka(ctx, wg, zkUrl)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		return config, err
	}

	_, influxip, err := Influxdb(ctx, wg)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		return config, err
	}
	influxdbUrl := "http://" + influxip + ":8086"

	_, mongoIp, err := MongoDB(ctx, wg)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		return config, err
	}
	mongoUrl := "mongodb://" + mongoIp

	err = ConnectionlogWorker(ctx, wg, mongoUrl, influxdbUrl, config.KafkaUrl)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		return config, err
	}

	_, permV2Ip, err := docker.PermissionsV2(ctx, wg, mongoUrl, config.KafkaUrl)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		return config, err
	}
	permv2Url := "http://" + permV2Ip + ":8080"

	_, connectionlogip, err := Connectionlog(ctx, wg, mongoUrl, permv2Url, influxdbUrl)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		return config, err
	}

	config.ConnectionlogUrl = "http://" + connectionlogip + ":8080"

	return config, nil
}

func getFreePortStr() (string, error) {
	intPort, err := getFreePort()
	if err != nil {
		return "", err
	}
	return strconv.Itoa(intPort), nil
}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}
