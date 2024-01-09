/*
 * Copyright 2020 InfAI (CC SES)
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

package statistics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

var once sync.Once

var iotReads *prometheus.HistogramVec
var cacheReads *prometheus.HistogramVec
var cacheMiss *prometheus.CounterVec
var timescaleWrites *prometheus.HistogramVec
var kafkaWrites *prometheus.HistogramVec
var sourceWrites *prometheus.HistogramVec
var sourceHandled *prometheus.HistogramVec
var deviceMessages *prometheus.HistogramVec
var deviceMessagesHandled *prometheus.HistogramVec
var instanceId string

func IotRead(duration time.Duration) {
	once.Do(start)
	iotReads.WithLabelValues(instanceId).Observe(float64(duration.Milliseconds()))
}

func CacheRead(duration time.Duration) {
	once.Do(start)
	cacheReads.WithLabelValues(instanceId).Observe(float64(duration.Milliseconds()))
}

func CacheMiss() {
	once.Do(start)
	cacheMiss.WithLabelValues(instanceId).Inc()
}

func TimescaleWrite(duration time.Duration, userId string) {
	once.Do(start)
	timescaleWrites.WithLabelValues(userId, instanceId).Observe(float64(duration.Milliseconds()))
}

func KafkaWrite(duration time.Duration, userId string) {
	once.Do(start)
	kafkaWrites.WithLabelValues(userId, instanceId).Observe(float64(duration.Milliseconds()))
}

func SourceReceive(size float64, userId string) {
	once.Do(start)
	sourceWrites.WithLabelValues(userId, instanceId).Observe(size)
}

func SourceReceiveHandled(size float64, userId string) {
	once.Do(start)
	sourceHandled.WithLabelValues(userId, instanceId).Observe(size)
}

func DeviceMsgReceive(size float64, userId string, deviceId string, serviceIds string, deviceTypeId string) {
	once.Do(start)
	deviceMessages.WithLabelValues(userId, instanceId, deviceId, serviceIds, deviceTypeId).Observe(size)
}

func DeviceMsgHandled(size float64, userId string, deviceId string, serviceIds string, deviceTypeId string) {
	once.Do(start)
	deviceMessagesHandled.WithLabelValues(userId, instanceId, deviceId, serviceIds, deviceTypeId).Observe(size)
}

func start() {
	log.Println("start statistics collector")
	buckets := []float64{1, 5, 10, 50, 100, 200, 300, 500, 1000, 2000, 5000, 10000}
	sourceBuckets := []float64{32, 64, 128, 256, 512, 1024, 2048, 3072, 4096, 5120, 6144, 7168, 8192, 9216, 10240}

	iotReads = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "connector_iot_read_latency_ms",
		Help:    "Latency of IoT metadata reads",
		Buckets: buckets,
	}, []string{"instance_id"})
	cacheReads = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "connector_cache_read_latency_ms",
		Help:    "Latency of cache reads",
		Buckets: buckets,
	}, []string{"instance_id"})
	cacheMiss = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "connector_cache_miss_total_ms",
		Help: "Total number of cache misses",
	}, []string{"instance_id"})
	kafkaWrites = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "connector_kafka_write_latency_ms",
		Help:    "Latency of kafka publishes",
		Buckets: buckets,
	}, []string{"user_id", "instance_id"})
	deviceMessages = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "connector_source_received_device_msg_size",
		Help:    "Received device message size from source",
		Buckets: sourceBuckets,
	}, []string{"user_id", "instance_id", "device_id", "service_ids", "device_type_id"})
	deviceMessagesHandled = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "connector_source_handled_device_msg_size",
		Help:    "Handled device message size from source",
		Buckets: sourceBuckets,
	}, []string{"user_id", "instance_id", "device_id", "service_ids", "device_type_id"})
	sourceWrites = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "connector_source_received_msg_size",
		Help:    "Received message size from source",
		Buckets: sourceBuckets,
	}, []string{"user_id", "instance_id"})
	sourceHandled = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "connector_source_handled_msg_size",
		Help:    "Handled message size from source",
		Buckets: sourceBuckets,
	}, []string{"user_id", "instance_id"})
	timescaleWrites = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "connector_timescale_write_latency_ms",
		Help:    "Latency of timescale writes",
		Buckets: buckets,
	}, []string{"user_id", "instance_id"})
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
		log.Println("ERROR: Could not get hostname, using '" + hostname + "'")
	}
	instanceId = hostname

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("INFO: Starting prometheus metrics on :2112/metrics")
		log.Println("WARNING: Metrics server exited: " + http.ListenAndServe(":2112", nil).Error())
	}()
}
