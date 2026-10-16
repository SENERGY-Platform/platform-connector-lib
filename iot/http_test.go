/*
 * Copyright (c) 2026 InfAI (CC SES)
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

package iot

import (
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SENERGY-Platform/platform-connector-lib/security"
	"github.com/prometheus/client_golang/prometheus"
)

func iotReadLatencySumMs(t *testing.T) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	sum := 0.0
	for _, family := range families {
		if family.GetName() != "connector_iot_read_latency_ms" {
			continue
		}
		for _, m := range family.GetMetric() {
			sum += m.GetHistogram().GetSampleSum()
		}
	}
	return sum
}

// The recorded latency must cover the request, not the zero time between start and the defer statement.
func TestIotReadLatencyCoversRequest(t *testing.T) {
	const delay = 100 * time.Millisecond
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(delay)
		_, _ = w.Write([]byte(`{"id":"pid"}`))
	}))
	t.Cleanup(server.Close)
	repo := New(server.URL, server.URL, "", slog.New(slog.NewTextHandler(io.Discard, nil)))

	before := iotReadLatencySumMs(t)
	if _, err := repo.GetProtocol("pid", security.JwtToken("Bearer test")); err != nil {
		t.Fatal(err)
	}
	if got := iotReadLatencySumMs(t) - before; got < float64(delay.Milliseconds()) {
		t.Errorf("recorded latency %vms, want at least %vms", got, delay.Milliseconds())
	}
}

func TestDeleteHubClosesResponseBody(t *testing.T) {
	// An unclosed body keeps its connection busy, so a second call would have to dial a new one.
	var dials atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("deleted"))
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			dials.Add(1)
		}
	}
	server.Start()
	t.Cleanup(server.Close)

	repo := New(server.URL, server.URL, "", slog.New(slog.NewTextHandler(io.Discard, nil)))
	for i := 0; i < 2; i++ {
		if err := repo.DeleteHub("hid", security.JwtToken("Bearer test")); err != nil {
			t.Fatal(err)
		}
	}
	if n := dials.Load(); n != 1 {
		t.Errorf("server saw %d connections, want 1: the response body was not closed", n)
	}
}
