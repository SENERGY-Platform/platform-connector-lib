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
	"github.com/streadway/amqp"
	"log"
	"sync"
	"time"
)

type Publisher struct {
	connection      *amqp.Connection
	channel         *amqp.Channel
	url             string
	mux             sync.RWMutex
	topicsToDeclare []string
}

func NewPublisher(amqpUrl string, topicsToDeclare ...string) (publisher *Publisher, err error) {
	publisher = &Publisher{}
	publisher.url = amqpUrl
	publisher.topicsToDeclare = topicsToDeclare
	return publisher, publisher.start()
}

func (this *Publisher) Close() {
	this.channel.Close()
	this.connection.Close()
}

func (this *Publisher) start() (err error) {
	this.connection, err = amqp.Dial(this.url)
	if err != nil {
		return
	}
	this.channel, err = this.connection.Channel()
	if err != nil {
		return
	}
	err = this.declareTopics()
	if err != nil {
		return
	}
	this.channel.NotifyClose(this.reconnect())
	return
}

func (this *Publisher) reconnect() (ch chan *amqp.Error) {
	ch = make(chan *amqp.Error)
	go func() {
		log.Println("start error handler")
		for err := range ch {
			log.Println("receive amqp close", err)
			if err != nil {
				this.mux.Lock()
				defer this.mux.Unlock()
				this.Close()
				reconnectStart := time.Now()
				reconnectTimeout := time.Duration(1 * time.Second)
				for {
					log.Println("try reconnecting")
					err := this.start()
					if err != nil {
						log.Println("unable to reconnect", err)
						if time.Now().Sub(reconnectStart) > time.Duration(3*time.Minute) {
							log.Println("FATAL ERROR: unable to restart mqtt connection")
							panic("unable to restart mqtt connection")
						} else {
							log.Println("try again in ", reconnectTimeout.String())
						}
						time.Sleep(reconnectTimeout)
						reconnectTimeout = time.Duration(reconnectTimeout * 2)
						if reconnectTimeout > 1*time.Minute {
							reconnectTimeout = time.Duration(1 * time.Minute)
						}
					} else {
						return
					}
				}
			}
		}
		log.Println("stop error handler")
	}()
	return ch
}

func (this *Publisher) declareTopics() (err error) {
	for _, topic := range this.topicsToDeclare {
		err = this.channel.ExchangeDeclare(topic, "fanout", true, false, false, false, nil)
		if err != nil {
			log.Println("ERROR: unable to declare topic", topic)
			return err
		}
	}
	return nil
}

func (this *Publisher) Publish(topic string, payload []byte) error {
	this.mux.RLock()
	defer this.mux.RUnlock()
	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		Body:         payload,
	}
	return this.channel.Publish(topic, "", false, true, msg)
}
