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

package kafka

import (
	"github.com/segmentio/kafka-go"
	"log"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
)

func EnsureTopic(topic string, kafkaUrl string, knownTopics *map[string]bool, configMap map[string][]kafka.ConfigEntry, partitions int, replicationFactor int) (err error) {
	if (*knownTopics)[topic] {
		return nil
	}
	err = InitTopicWithConfig(kafkaUrl, configMap, partitions, replicationFactor, topic)
	if err != nil {
		log.Println("ERROR:", err)
		debug.PrintStack()
		return err
	}
	(*knownTopics)[topic] = true
	return
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

func InitTopic(kafkaUrl string, configMap map[string][]kafka.ConfigEntry, topics ...string) (err error) {
	return InitTopicWithConfig(kafkaUrl, configMap, 1, 1, topics...)
}

func InitTopicWithConfig(bootstrapUrl string, configMap map[string][]kafka.ConfigEntry, numPartitions int, replicationFactor int, topics ...string) (err error) {
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
			ConfigEntries:     GetTopicConfig(configMap, topic),
		})
	}

	return controllerConn.CreateTopics(topicConfigs...)
}

func GetTopicConfig(configMap map[string][]kafka.ConfigEntry, topic string) []kafka.ConfigEntry {
	if configMap == nil {
		return nil
	}
	result, exists := configMap[topic]
	if exists {
		return result
	}
	for key, conf := range configMap {
		if strings.HasPrefix(topic, key) {
			return conf
		}
	}
	return nil
}
