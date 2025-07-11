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
	"log"
	"os"
	"time"
)

type ConsumerConfig struct {
	KafkaUrl       string
	GroupId        string
	Topic          string
	MinBytes       int
	MaxBytes       int
	MaxWait        time.Duration
	InitTopic      bool
	TopicConfigMap map[string][]kafka.ConfigEntry
}

func NewConsumer(ctx context.Context, config ConsumerConfig, listener func(topic string, msg []byte, time time.Time) error, errorhandler func(err error)) (err error) {
	log.Println("DEBUG: consume topic: \"" + config.Topic + "\"")
	if config.InitTopic {
		err = InitTopic(config.KafkaUrl, config.TopicConfigMap, config.Topic)
		if err != nil {
			log.Println("ERROR: unable to create topic", err)
			return err
		}
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		CommitInterval:         0, //synchronous commits
		Brokers:                []string{config.KafkaUrl},
		GroupID:                config.GroupId,
		Topic:                  config.Topic,
		MinBytes:               config.MinBytes,
		MaxBytes:               config.MaxBytes,
		MaxWait:                config.MaxWait,
		Logger:                 log.New(io.Discard, "", 0),
		ErrorLogger:            log.New(os.Stdout, "[KAFKA-ERR] ", log.LstdFlags),
		WatchPartitionChanges:  true,
		PartitionWatchInterval: time.Minute,
	})
	go func() {
		defer func() {
			log.Println("close kafka reader ", config.Topic, r.Close())
		}()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				m, err := r.FetchMessage(ctx)
				if err == io.EOF || err == context.Canceled {
					log.Println("close consumer for topic ", config.Topic)
					return
				}
				if err != nil {
					log.Println("ERROR: while consuming topic ", config.Topic, err)
					errorhandler(err)
					return
				}
				if time.Now().Sub(m.Time) > 1*time.Hour { //floodgate to prevent old messages to DOS the consumer
					log.Println("WARNING: kafka message older than 1h: ", config.Topic, time.Now().Sub(m.Time))
					err = r.CommitMessages(ctx, m)
					if err != nil {
						log.Println("ERROR: while committing message ", config.Topic, err)
						errorhandler(err)
						return
					}
				} else {
					err = listener(m.Topic, m.Value, m.Time)
					if err != nil {
						log.Println("ERROR: unable to handle message (no commit)", err, m.Topic, string(m.Value))
					} else {
						err = r.CommitMessages(ctx, m)
						if err != nil {
							log.Println("ERROR: while committing message ", config.Topic, err)
							errorhandler(err)
							return
						}
					}
				}
			}
		}
	}()
	return err
}
