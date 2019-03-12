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

package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
	"time"
)

type Producer struct {
	zk string
}

func PrepareProducer(zk string) *Producer {
	return &Producer{zk: zk}
}

func (this *Producer) getProducer(topic string) (writer *kafka.Writer) {
	broker, err := GetBroker(this.zk)
	if err != nil {
		log.Fatal("error while getting broker", err)
	}
	writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers:     broker,
		Topic:       topic,
		Balancer:    &kafka.LeastBytes{},
		MaxAttempts: 25,
		Logger:      log.New(os.Stdout, "KAFKA", 0), //log.New(ioutil.Discard, "", 0),
	})
	return writer
}

func (this *Producer) Produce(topic string, message string) (err error) {
	writer := this.getProducer(topic)
	defer writer.Close()
	return writer.WriteMessages(context.Background(),
		kafka.Message{
			Value: []byte(message),
			Time:  time.Now(),
		},
	)
}
