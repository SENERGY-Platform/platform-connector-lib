/*
 * Copyright 2026 InfAI (CC SES)
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

package psql

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/SENERGY-Platform/converter/lib/converter"
	"github.com/SENERGY-Platform/converter/lib/converter/characteristics"
	"github.com/SENERGY-Platform/platform-connector-lib/model"
)

// whole seconds are exactly representable as float64, so the expected value does
// not depend on the json decoder handing out float64
var testTime = time.Date(2026, 8, 26, 8, 26, 43, 0, time.UTC)

func newTestPublisher(t *testing.T) *Publisher {
	t.Helper()
	conv, err := converter.New()
	if err != nil {
		t.Fatal(err)
	}
	return &Publisher{
		conv:                             conv,
		serviceIdTimeCharacteristicCache: map[string]characteristicIdTimestamp{},
		logger:                           slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func testService(id string, timePath string, characteristicId string) model.Service {
	return model.Service{
		Id:         id,
		Attributes: []model.Attribute{{Key: timeAttributeKey, Value: timePath}},
		Outputs: []model.Content{{
			ContentVariable: model.ContentVariable{
				Name: "root",
				SubContentVariables: []model.ContentVariable{{
					Name:             "time",
					CharacteristicId: characteristicId,
				}},
			},
		}},
	}
}

// testMessage decodes like a real event payload, so numbers arrive as float64
func testMessage(t *testing.T, rawValue string) map[string]interface{} {
	t.Helper()
	value := map[string]interface{}{}
	err := json.Unmarshal([]byte(`{"root":{"time":`+rawValue+`}}`), &value)
	if err != nil {
		t.Fatal(err)
	}
	return flatten(value)
}

func TestGetTimeString(t *testing.T) {
	t.Run("uses a unix nanosecond value without a cast", func(t *testing.T) {
		publisher := newTestPublisher(t)
		service := testService("service:ns", "root.time", characteristics.UnixNanoSeconds)
		m := testMessage(t, strconv.FormatInt(testTime.UnixNano(), 10))

		timeStr, err, _ := publisher.getTimeString(m, service)
		if err != nil {
			t.Fatal(err)
		}
		if timeStr != testTime.Format(time.RFC3339Nano) {
			t.Error(timeStr)
		}
	})

	t.Run("parses an iso timestamp value", func(t *testing.T) {
		publisher := newTestPublisher(t)
		service := testService("service:iso", "root.time", characteristics.IsoTimestamp)
		m := testMessage(t, `"`+testTime.Format(time.RFC3339)+`"`)

		timeStr, err, _ := publisher.getTimeString(m, service)
		if err != nil {
			t.Fatal(err)
		}
		if timeStr != testTime.Format(time.RFC3339Nano) {
			t.Error(timeStr)
		}
	})

	t.Run("casts a unix millisecond value", func(t *testing.T) {
		publisher := newTestPublisher(t)
		service := testService("service:ms", "root.time", characteristics.UnixMilliSeconds)
		m := testMessage(t, strconv.FormatInt(testTime.UnixMilli(), 10))

		timeStr, err, _ := publisher.getTimeString(m, service)
		if err != nil {
			t.Fatal(err)
		}
		if timeStr != testTime.Format(time.RFC3339Nano) {
			t.Error(timeStr)
		}
	})

	t.Run("returns an error for a value that is no timestamp", func(t *testing.T) {
		publisher := newTestPublisher(t)
		service := testService("service:bool", "root.time", characteristics.UnixNanoSeconds)
		m := testMessage(t, "true")

		_, err, notifyUser := publisher.getTimeString(m, service)
		if err == nil {
			t.Fatal("expected an error")
		}
		if !notifyUser {
			t.Error("expected the device owner to be notified")
		}
	})

	t.Run("returns an error when the time path is missing in the message", func(t *testing.T) {
		publisher := newTestPublisher(t)
		service := testService("service:missing", "root.other", characteristics.UnixNanoSeconds)
		m := testMessage(t, "42")

		_, err, notifyUser := publisher.getTimeString(m, service)
		if err == nil {
			t.Fatal("expected an error")
		}
		if !notifyUser {
			t.Error("expected the device owner to be notified")
		}
	})

	t.Run("falls back to the write time without a time_path attribute", func(t *testing.T) {
		publisher := newTestPublisher(t)
		service := model.Service{Id: "service:none"}

		timeStr, err, _ := publisher.getTimeString(flatten(map[string]interface{}{}), service)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := time.Parse(time.RFC3339Nano, timeStr)
		if err != nil {
			t.Fatal(err)
		}
		if time.Since(parsed) > time.Minute {
			t.Error(timeStr)
		}
	})
}

// the characteristic cache is shared by the publish goroutine of every event;
// fails under -race without a lock
func TestGetTimeStringIsSafeForConcurrentUse(t *testing.T) {
	publisher := newTestPublisher(t)
	m := testMessage(t, strconv.FormatInt(testTime.UnixNano(), 10))

	wg := sync.WaitGroup{}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			service := testService(fmt.Sprintf("service:%v", i%5), "root.time", characteristics.UnixNanoSeconds)
			_, err, _ := publisher.getTimeString(m, service)
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
}

func TestBuildInsertQuery(t *testing.T) {
	timeStr := testTime.Format(time.RFC3339Nano)

	t.Run("quotes string values", func(t *testing.T) {
		m := flatten(map[string]interface{}{"root": map[string]interface{}{"text": "hello"}})
		expected := `INSERT INTO "device:a_service:b"("time","root.text") VALUES ('` + timeStr + `','hello');`
		if query := buildInsertQuery("device:a_service:b", timeStr, m); query != expected {
			t.Error(query)
		}
	})

	t.Run("writes numbers unquoted", func(t *testing.T) {
		m := flatten(map[string]interface{}{"root": map[string]interface{}{"value": float64(1.5)}})
		expected := `INSERT INTO "device:a_service:b"("time","root.value") VALUES ('` + timeStr + `',1.5);`
		if query := buildInsertQuery("device:a_service:b", timeStr, m); query != expected {
			t.Error(query)
		}
	})

	t.Run("writes a nil value as NULL", func(t *testing.T) {
		m := flatten(map[string]interface{}{"root": map[string]interface{}{"value": nil}})
		expected := `INSERT INTO "device:a_service:b"("time","root.value") VALUES ('` + timeStr + `',NULL);`
		if query := buildInsertQuery("device:a_service:b", timeStr, m); query != expected {
			t.Error(query)
		}
	})
}
