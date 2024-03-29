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

package helper

import (
	"github.com/SENERGY-Platform/platform-connector-lib/connectionlog/test/config"
	"github.com/segmentio/kafka-go"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
)

type Publisher struct {
	config          config.Config
	devicetypes     *kafka.Writer
	protocols       *kafka.Writer
	devices         *kafka.Writer
	hubs            *kafka.Writer
	concepts        *kafka.Writer
	characteristics *kafka.Writer
}

func GetProducer(broker []string, topic string, debug bool) (writer *kafka.Writer, err error) {
	var logger *log.Logger
	if debug {
		logger = log.New(os.Stdout, "[KAFKA-PRODUCER] ", 0)
	} else {
		logger = log.New(ioutil.Discard, "", 0)
	}
	writer = &kafka.Writer{
		Addr:        kafka.TCP(broker...),
		Topic:       topic,
		MaxAttempts: 10,
		Logger:      logger,
	}
	return writer, err
}

func GetBroker(bootstrapUrl string) (brokers []string, err error) {
	return getBroker(bootstrapUrl)
}

func getBroker(bootstrapUrl string) (result []string, err error) {
	conn, err := kafka.Dial("tcp", bootstrapUrl)
	if err != nil {
		return result, err
	}
	defer conn.Close()
	brokers, err := conn.Brokers()
	if err != nil {
		return result, err
	}
	for _, broker := range brokers {
		result = append(result, net.JoinHostPort(broker.Host, strconv.Itoa(broker.Port)))
	}
	return result, nil
}

func InitTopic(kafkaUrl string, topics ...string) (err error) {
	return InitTopicWithConfig(kafkaUrl, 1, 1, topics...)
}

func InitTopicWithConfig(bootstrapUrl string, numPartitions int, replicationFactor int, topics ...string) (err error) {
	conn, err := kafka.Dial("tcp", bootstrapUrl)
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{}

	for _, topic := range topics {
		topicConfigs = append(topicConfigs, kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
		})
	}

	return controllerConn.CreateTopics(topicConfigs...)
}
