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
	"github.com/SENERGY-Platform/platform-connector-lib/kafka"
	"log"

	"encoding/json"
)

func (this *Connector) initKafkaConsumer() (consumer *kafka.Consumer, err error) {
	return kafka.NewConsumer(this.Config.ZookeeperUrl, this.Config.KafkaGroupName, this.Config.Protocol, func(topic string, msg []byte) error {
		return this.handleMessage(msg)
	}, func(err error, consumer *kafka.Consumer) {
		log.Fatal("ERROR: error while consuming, fatal", err)
	})
}

func (this *Connector) handleMessage(msg []byte) (err error) {
	log.Println("consume kafka msg: ", msg)
	envelope := Envelope{}
	err = json.Unmarshal(msg, &envelope)
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
