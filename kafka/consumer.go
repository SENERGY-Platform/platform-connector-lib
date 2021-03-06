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
	"context"
	"github.com/segmentio/kafka-go"
	"io"
	"io/ioutil"
	"log"
	"sync"
	"time"
)

func NewConsumer(zk string, groupid string, topic string, listener func(topic string, msg []byte, time time.Time) error, errorhandler func(err error, consumer *Consumer)) (consumer *Consumer, err error) {
	consumer = &Consumer{groupId: groupid, zkUrl: zk, topic: topic, listener: listener, errorhandler: errorhandler}
	err = consumer.start()
	return
}

type Consumer struct {
	count        int
	zkUrl        string
	groupId      string
	topic        string
	ctx          context.Context
	cancel       context.CancelFunc
	listener     func(topic string, msg []byte, time time.Time) error
	errorhandler func(err error, consumer *Consumer)
	mux          sync.Mutex
}

func (this *Consumer) Stop() {
	this.cancel()
}

func (this *Consumer) start() error {
	log.Println("DEBUG: consume topic: \"" + this.topic + "\"")
	this.ctx, this.cancel = context.WithCancel(context.Background())
	broker, err := GetBroker(this.zkUrl)
	if err != nil {
		log.Println("ERROR: unable to get broker list", err)
		return err
	}
	err = InitTopic(this.zkUrl, this.topic)
	if err != nil {
		log.Println("ERROR: unable to create topic", err)
		return err
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		CommitInterval: 0, //synchronous commits
		Brokers:        broker,
		GroupID:        this.groupId,
		Topic:          this.topic,
		MaxWait:        1 * time.Second,
		Logger:         log.New(ioutil.Discard, "", 0),
		ErrorLogger:    log.New(ioutil.Discard, "", 0),
	})
	go func() {
		for {
			select {
			case <-this.ctx.Done():
				log.Println("close kafka reader ", this.topic)
				return
			default:
				m, err := r.FetchMessage(this.ctx)
				if err == io.EOF || err == context.Canceled {
					log.Println("close consumer for topic ", this.topic)
					return
				}
				if err != nil {
					log.Println("ERROR: while consuming topic ", this.topic, err)
					this.errorhandler(err, this)
					return
				}
				if time.Now().Sub(m.Time) > 1*time.Hour { //floodgate to prevent old messages to DOS the consumer
					log.Println("WARNING: kafka message older than 1h: ", this.topic, time.Now().Sub(m.Time))
					err = r.CommitMessages(this.ctx, m)
					if err != nil {
						log.Println("ERROR: while committing message ", this.topic, err)
						this.errorhandler(err, this)
						return
					}
				} else {
					err = this.listener(m.Topic, m.Value, m.Time)
					if err != nil {
						log.Println("ERROR: unable to handle message (no commit)", err, m.Topic, string(m.Value))
					} else {
						err = r.CommitMessages(this.ctx, m)
						if err != nil {
							log.Println("ERROR: while committing message ", this.topic, err)
							this.errorhandler(err, this)
							return
						}
					}
				}
			}
		}
	}()
	return err
}

func (this *Consumer) Restart() {
	this.Stop()
	this.start()
}
