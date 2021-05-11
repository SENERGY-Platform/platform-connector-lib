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
	"github.com/SENERGY-Platform/platform-connector-lib/connectionlog/test/config"
	"github.com/ory/dockertest"
	"github.com/ory/dockertest/docker"
	"log"
	"net"
	"os"
	"runtime/debug"
	"strconv"
)

func New(parentCtx context.Context) (config config.Config, err error) {
	ctx, cancel := context.WithCancel(parentCtx)

	config.HubLogTopic = "gateway_log"
	config.DeviceLogTopic = "device_log"

	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		cancel()
		return config, err
	}

	_, zk, err := Zookeeper(pool, ctx)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		cancel()
		return config, err
	}
	zkUrl := zk + ":2181"

	config.KafkaUrl, err = Kafka(pool, ctx, zkUrl)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		cancel()
		return config, err
	}

	_, elasticIp, err := Elasticsearch(pool, ctx)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		cancel()
		return config, err
	}

	_, influxip, err := Influxdb(pool, ctx)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		cancel()
		return config, err
	}
	influxdbUrl := "http://" + influxip + ":8086"

	_, mongoIp, err := MongoTestServer(pool, ctx)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		cancel()
		return config, err
	}
	mongoUrl := "mongodb://" + mongoIp

	_, _, err = ConnectionlogWorker(pool, ctx, mongoUrl, influxdbUrl, zkUrl)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		cancel()
		return config, err
	}

	_, permIp, err := PermSearch(pool, ctx, zkUrl, elasticIp)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		cancel()
		return config, err
	}
	permissionUrl := "http://" + permIp + ":8080"

	_, connectionlogip, err := Connectionlog(pool, ctx, mongoUrl, permissionUrl, influxdbUrl)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		cancel()
		return config, err
	}

	config.ConnectionlogUrl = "http://" + connectionlogip + ":8080"

	return config, nil
}

func Dockerlog(pool *dockertest.Pool, ctx context.Context, repo *dockertest.Resource, name string) {
	out := &LogWriter{logger: log.New(os.Stdout, "["+name+"]", 0)}
	err := pool.Client.Logs(docker.LogsOptions{
		Stdout:       true,
		Stderr:       true,
		Context:      ctx,
		Container:    repo.Container.ID,
		Follow:       true,
		OutputStream: out,
		ErrorStream:  out,
	})
	if err != nil && err != context.Canceled {
		log.Println("DEBUG-ERROR: unable to start docker log", name, err)
	}
}

type LogWriter struct {
	logger *log.Logger
}

func (this *LogWriter) Write(p []byte) (n int, err error) {
	this.logger.Print(string(p))
	return len(p), nil
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
