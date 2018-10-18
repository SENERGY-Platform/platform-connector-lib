/*
 * Copyright 2018 InfAI (CC SES)
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
	"log"

	"time"

	"encoding/json"

	"os"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
	kazoo "github.com/wvanbergen/kazoo-go"
)

func (this *Connector) initKafkaConsumer() *RunnerTask {
	if this.Config.SaramaLog == "true" {
		sarama.Logger = log.New(os.Stderr, "[Sarama] ", log.LstdFlags)
	}
	return RunTask(func(shouldStop StopCheckFunc) error {
		log.Println("Start KAFKA-Consumer")
		this.produce(this.Config.Protocol, "topic_init")

		zk, chroot := kazoo.ParseConnectionString(this.Config.ZookeeperUrl)
		kafkaconf := consumergroup.NewConfig()
		kafkaconf.Consumer.Return.Errors = this.Config.FatalKafkaErrors == "true"
		kafkaconf.Zookeeper.Chroot = chroot
		consumerGroupName := this.Config.Protocol
		consumer, err := consumergroup.JoinConsumerGroup(
			consumerGroupName,
			[]string{this.Config.Protocol},
			zk,
			kafkaconf)

		if err != nil {
			log.Fatal("error in consumergroup.JoinConsumerGroup()", err)
		}

		defer consumer.Close()

		kafkaTimeout := this.Config.KafkaTimeout
		useTimeout := true
		if kafkaTimeout <= 0 {
			useTimeout = false
			kafkaTimeout = 3600
		}
		kafkaping := time.NewTicker(time.Second * time.Duration(kafkaTimeout/2))
		defer kafkaping.Stop()
		kafkatimout := time.NewTicker(time.Second * time.Duration(kafkaTimeout))
		defer kafkatimout.Stop()

		timeout := false

		for {
			if shouldStop() {
				return nil
			}
			select {
			case <-kafkaping.C:
				if useTimeout && timeout {
					this.produce(this.Config.Protocol, "topic_init")
				}
			case <-kafkatimout.C:
				if useTimeout && timeout {
					log.Fatal("ERROR: kafka missing ping timeout")
				}
				timeout = true
			case errMsg := <-consumer.Errors():
				log.Fatal("kafka consumer error: ", errMsg)
			case msg, ok := <-consumer.Messages():
				if !ok {
					log.Fatal("empty kafka consumer")
				} else {
					if string(msg.Value) != "topic_init" {
						err = this.handleMessage(string(msg.Value))
					}
					timeout = false
					if err != nil {
						log.Println("ERROR while handling msg:", string(msg.Value))
					} else {
						consumer.CommitUpto(msg)
					}
				}
			}
		}
	})
}

func (this *Connector) handleMessage(msg string) (err error) {
	log.Println("consume kafka msg: ", msg)
	envelope := Envelope{}
	err = json.Unmarshal([]byte(msg), &envelope)
	if err != nil {
		log.Println("ERROR: ", err)
		return nil //ignore marshaling errors --> no repeat; errors would definitely reoccur
	}
	payload, err := json.Marshal(envelope.Value)
	if err != nil {
		log.Println("ERROR: ", err)
		return nil //ignore marshaling errors --> no repeat; errors would definitely reoccur
	}
	return this.handleCommand(string(payload))
}
