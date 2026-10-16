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

package platform_connector_lib

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

type notifyTrackedBody struct {
	io.ReadCloser
	closed *atomic.Bool
}

func (this notifyTrackedBody) Close() error {
	this.closed.Store(true)
	return this.ReadCloser.Close()
}

type notifyCloseRecorder struct {
	base   http.RoundTripper
	closed atomic.Bool
	seen   atomic.Int32
}

func (this *notifyCloseRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := this.base.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	this.seen.Add(1)
	resp.Body = notifyTrackedBody{ReadCloser: resp.Body, closed: &this.closed}
	return resp, nil
}

func TestSendNotificationClosesResponseBody(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				_, _ = w.Write([]byte("response"))
			}))
			t.Cleanup(server.Close)
			rec := &notifyCloseRecorder{base: http.DefaultTransport}
			http.DefaultTransport = rec
			t.Cleanup(func() { http.DefaultTransport = rec.base })

			connector := &Connector{Config: Config{
				NotificationUrl: server.URL,
				Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
			}}
			err := connector.SendNotification(Notification{UserId: "test-user", Title: "test", Message: "test"})
			if (err == nil) != (status == http.StatusOK) {
				t.Errorf("unexpected error result for status %d: %v", status, err)
			}
			if rec.seen.Load() != 1 {
				t.Fatalf("saw %d responses, want 1", rec.seen.Load())
			}
			if !rec.closed.Load() {
				t.Error("response body was not closed")
			}
		})
	}
}
