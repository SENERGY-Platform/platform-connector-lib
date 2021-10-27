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
	"context"
	"fmt"
	"log"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Interface interface {
	IotRead(duration time.Duration)
	CacheRead(duration time.Duration)
	CacheMiss()
	TimescaleWrite(duration time.Duration)
	KafkaWrite(duration time.Duration)
}

type Void struct{}

func (this Void) IotRead(duration time.Duration)        {}
func (this Void) CacheRead(duration time.Duration)      {}
func (this Void) CacheMiss()                            {}
func (this Void) TimescaleWrite(duration time.Duration) {}
func (this Void) KafkaWrite(duration time.Duration)     {}

func New(ctx context.Context, logAndResetInterval time.Duration) Interface {
	result := &Implementation{}
	result.Start(ctx, logAndResetInterval)
	return result
}

type Implementation struct {
	iotReads        []time.Duration
	iotMux          sync.Mutex
	cacheReads      []time.Duration
	cacheMiss       uint64
	cacheMux        sync.Mutex
	timescaleWrites []time.Duration
	timescaleMux    sync.Mutex
	kafkaWrites     []time.Duration
	kafkaMux        sync.Mutex
}

func (this *Implementation) IotRead(duration time.Duration) {
	this.iotMux.Lock()
	defer this.iotMux.Unlock()
	this.iotReads = append(this.iotReads, duration)
}

func (this *Implementation) CacheRead(duration time.Duration) {
	this.cacheMux.Lock()
	defer this.cacheMux.Unlock()
	this.cacheReads = append(this.cacheReads, duration)
}

func (this *Implementation) CacheMiss() {
	atomic.AddUint64(&this.cacheMiss, 1)
}

func (this *Implementation) TimescaleWrite(duration time.Duration) {
	this.timescaleMux.Lock()
	defer this.timescaleMux.Unlock()
	this.timescaleWrites = append(this.timescaleWrites, duration)
}

func (this *Implementation) KafkaWrite(duration time.Duration) {
	this.kafkaMux.Lock()
	defer this.kafkaMux.Unlock()
	this.kafkaWrites = append(this.kafkaWrites, duration)
}

func (this *Implementation) Start(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				this.log()
			}
		}
	}()

}

func (this *Implementation) log() {
	this.iotMux.Lock()
	defer this.iotMux.Unlock()
	this.cacheMux.Lock()
	defer this.cacheMux.Unlock()
	this.timescaleMux.Lock()
	defer this.timescaleMux.Unlock()
	this.kafkaMux.Lock()
	defer this.kafkaMux.Unlock()

	numGoroutines := runtime.NumGoroutine()
	cacheMiss := atomic.LoadUint64(&this.cacheMiss)
	iotLog := statistics(this.iotReads).SetLabel("Iot-Reads")
	cacheLog := statistics(this.cacheReads).SetLabel("Cache-Reads")
	timescaleLog := statistics(this.timescaleWrites).SetLabel("Timescale-Writes")
	kafkaLog := statistics(this.kafkaWrites).SetLabel("kafka-Writes")

	go log.Println("LOG:",
		"\n\tnum-goroutines:", numGoroutines,
		"\n\tcache-misses:", cacheMiss,
		"\n\t", iotLog.String(),
		"\n\t", cacheLog.String(),
		"\n\t", timescaleLog.String(),
		"\n\t", kafkaLog.String())

	this.iotReads = []time.Duration{}
	this.cacheReads = []time.Duration{}
	this.timescaleWrites = []time.Duration{}
	this.kafkaWrites = []time.Duration{}
	atomic.StoreUint64(&this.cacheMiss, 0)
}

type statisticsElement struct {
	Label  string
	count  int
	median time.Duration
	avg    time.Duration
	min    time.Duration
	max    time.Duration
}

func (this statisticsElement) SetLabel(label string) statisticsElement {
	this.Label = label
	return this
}

func (this statisticsElement) String() string {
	return fmt.Sprint(this.Label+":", "\n\tcount:", this.count, "\n\tmedian:", this.median.String(), "\n\tavg:", this.avg.String(), "\n\tmin:", this.min.String(), "\n\tmax:", this.max.String())
}

func statistics(list []time.Duration) (result statisticsElement) {
	sort.Slice(list, func(i, j int) bool {
		return list[i] < list[j]
	})
	result.count = len(list)
	if len(list) > 0 {
		result.min = list[0]
		result.max = list[result.count-1]
	}

	if result.count > 2 {
		if result.count%2 == 0 {
			result.median = list[result.count/2]
		} else {
			result.median = (list[result.count/2] + list[(result.count/2)-1]) / 2
		}
	}

	sum := time.Duration(0)
	for _, element := range list {
		sum += element
	}
	if len(list) > 0 {
		result.avg = sum / time.Duration(len(list))
	}
	return
}
