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
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/segmentio/kafka-go"
)

// KnownTopics remembers which topics have already been created, so that only
// the first publication to a topic talks to the broker. The produce paths of one
// producer run in several goroutines, so the check and the mark afterwards have
// to be synchronized: on a plain map two concurrent first publications end the
// process with "fatal error: concurrent map writes", which is not a panic and no
// recover catches it. The zero value is ready to use.
type KnownTopics struct {
	mux    sync.Mutex
	topics map[string]*topicState
}

// topicState guards the creation of a single topic. It carries its own lock so
// that the lock of KnownTopics is never held while the broker is called: a topic
// creation that waits for an unreachable broker would otherwise also block
// publications to topics that exist already.
type topicState struct {
	mux     sync.Mutex
	created bool
}

// EnsureTopic creates the topic unless it has been created before. Two parallel
// first publications to the same topic create it once; a failed creation is not
// remembered, so the next publication tries again.
func (this *KnownTopics) EnsureTopic(topic string, kafkaUrl string, configMap map[string][]kafka.ConfigEntry, partitions int, replicationFactor int) (err error) {
	return this.ensure(topic, func() error {
		return InitTopicWithConfig(kafkaUrl, configMap, partitions, replicationFactor, topic)
	})
}

func (this *KnownTopics) ensure(topic string, create func() error) (err error) {
	state := this.state(topic)
	state.mux.Lock()
	defer state.mux.Unlock()
	if state.created {
		return nil
	}
	err = create()
	if err != nil {
		return err
	}
	state.created = true
	return nil
}

func (this *KnownTopics) state(topic string) *topicState {
	this.mux.Lock()
	defer this.mux.Unlock()
	if this.topics == nil {
		this.topics = map[string]*topicState{}
	}
	result, ok := this.topics[topic]
	if !ok {
		result = &topicState{}
		this.topics[topic] = result
	}
	return result
}

// deprecated: not safe for concurrent use, use KnownTopics.EnsureTopic
func EnsureTopic(topic string, kafkaUrl string, knownTopics *map[string]bool, configMap map[string][]kafka.ConfigEntry, partitions int, replicationFactor int) (err error) {
	if (*knownTopics)[topic] {
		return nil
	}
	err = InitTopicWithConfig(kafkaUrl, configMap, partitions, replicationFactor, topic)
	if err != nil {
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
