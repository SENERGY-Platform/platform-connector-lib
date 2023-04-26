package kafka

import (
	"context"
	"github.com/SENERGY-Platform/permission-search/lib/tests/docker"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

func TestProducer_Produce(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, zkIp, err := docker.Zookeeper(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}
	zookeeperUrl := zkIp + ":2181"

	kafkaUrl, err := docker.Kafka(ctx, wg, zookeeperUrl)
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(2 * time.Second)

	err = InitTopic(kafkaUrl, nil, "test")
	if err != nil {
		t.Error(err)
		return
	}

	err = InitTopic(kafkaUrl, nil, "test2")
	if err != nil {
		t.Error(err)
		return
	}

	result := [][]byte{}

	time.Sleep(1 * time.Second)

	err = NewConsumer(ctx, ConsumerConfig{
		KafkaUrl: kafkaUrl,
		GroupId:  "test",
		Topic:    "test",
		MinBytes: 0,
		MaxBytes: 0,
		MaxWait:  0,
	}, func(topic string, msg []byte, t time.Time) error {
		result = append(result, msg)
		return nil
	}, func(err error) {
		t.Error(err)
	})

	time.Sleep(1 * time.Second)

	producer, err := PrepareProducer(ctx, kafkaUrl, true, true, 2, 1)
	if err != nil {
		t.Error(err)
		return
	}
	producer.Log(log.New(os.Stdout, "[KAFKA-PRODUCER] ", 0))
	log.Println("produce 1")
	err = producer.Produce("test", "msg1")
	if err != nil {
		t.Error(err)
		return
	}
	log.Println("produce 2")
	err = producer.Produce("test", "msg2")
	if err != nil {
		t.Error(err)
		return
	}
	log.Println("produce 3")
	err = producer.Produce("test2", "msg3")
	if err != nil {
		t.Error(err)
		return
	}
	log.Println("produced")

	time.Sleep(5 * time.Second)

	if len(result) != 2 {
		t.Error(len(result))
	}

	if len(result) > 0 && string(result[0]) != "msg1" {
		t.Error(string(result[0]))
	}
}
