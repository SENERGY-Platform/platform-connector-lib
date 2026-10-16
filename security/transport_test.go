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

package security

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/SENERGY-Platform/platform-connector-lib/model"
)

const testTokenBody = `{"access_token":"test-access-token","expires_in":300,"token_type":"Bearer"}`

type authCall struct {
	name string
	call func(t *testing.T, endpoint string, remoteInfo model.RemoteInfo) error
}

var authCalls = []authCall{
	{"GetOpenidToken", func(t *testing.T, endpoint string, remoteInfo model.RemoteInfo) error {
		_, err := GetOpenidToken(endpoint, "test-client", "test-client-secret", remoteInfo)
		return err
	}},
	{"RefreshOpenidToken", func(t *testing.T, endpoint string, remoteInfo model.RemoteInfo) error {
		_, err := RefreshOpenidToken(endpoint, "test-client", "test-client-secret", OpenidToken{RefreshToken: "test-refresh-token"}, remoteInfo)
		return err
	}},
	{"GetOpenidPasswordToken", func(t *testing.T, endpoint string, remoteInfo model.RemoteInfo) error {
		_, err := GetOpenidPasswordToken(endpoint, "test-client", "test-client-secret", "test-user", "test-password", remoteInfo)
		return err
	}},
	{"ExchangeUserToken", func(t *testing.T, endpoint string, remoteInfo model.RemoteInfo) error {
		_, err := newTestSecurity(t, endpoint).ExchangeUserToken("test-user-id", remoteInfo)
		return err
	}},
}

func TestForwardedHeaders(t *testing.T) {
	var mux sync.Mutex
	var received http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.Lock()
		received = r.Header.Clone()
		mux.Unlock()
		_, _ = w.Write([]byte(testTokenBody))
	}))
	t.Cleanup(server.Close)

	cases := []struct {
		name       string
		remoteInfo model.RemoteInfo
		want       map[string]string // absent key means the header must not be sent
	}{
		{"protocol without port", model.RemoteInfo{Protocol: "https"}, map[string]string{"X-Forwarded-Proto": "https"}},
		{"port without protocol", model.RemoteInfo{Port: "8443"}, map[string]string{"X-Forwarded-Port": "8443"}},
		{"ip only", model.RemoteInfo{Ip: "192.0.2.10"}, map[string]string{"X-Forwarded-For": "192.0.2.10"}},
		{"all set", model.RemoteInfo{Ip: "192.0.2.10", Port: "8443", Protocol: "https"}, map[string]string{
			"X-Forwarded-For": "192.0.2.10", "X-Forwarded-Port": "8443", "X-Forwarded-Proto": "https",
		}},
		{"none set", model.RemoteInfo{}, map[string]string{}},
	}
	for _, fn := range authCalls {
		for _, c := range cases {
			t.Run(fn.name+"/"+c.name, func(t *testing.T) {
				if err := fn.call(t, server.URL, c.remoteInfo); err != nil {
					t.Fatal(err)
				}
				mux.Lock()
				h := received
				mux.Unlock()
				for _, key := range []string{"X-Forwarded-For", "X-Forwarded-Port", "X-Forwarded-Proto"} {
					got, present := h[key]
					want, expected := c.want[key]
					if present != expected {
						t.Errorf("%s: present=%v, want present=%v (values %q)", key, present, expected, got)
						continue
					}
					if expected && (len(got) != 1 || got[0] != want) {
						t.Errorf("%s = %q, want %q", key, got, want)
					}
				}
			})
		}
	}
}

type trackedBody struct {
	io.ReadCloser
	closed atomic.Bool
}

func (this *trackedBody) Close() error {
	this.closed.Store(true)
	return this.ReadCloser.Close()
}

// closeRecorder wraps every response body so a test can check that it was closed.
type closeRecorder struct {
	base   http.RoundTripper
	mux    sync.Mutex
	bodies []*trackedBody
}

func (this *closeRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := this.base.RoundTrip(req)
	if err != nil {
		return resp, err
	}
	body := &trackedBody{ReadCloser: resp.Body}
	resp.Body = body
	this.mux.Lock()
	this.bodies = append(this.bodies, body)
	this.mux.Unlock()
	return resp, nil
}

func (this *closeRecorder) checkAllClosed(t *testing.T) {
	t.Helper()
	this.mux.Lock()
	defer this.mux.Unlock()
	if len(this.bodies) == 0 {
		t.Fatal("no response recorded")
	}
	for i, b := range this.bodies {
		if !b.closed.Load() {
			t.Errorf("response body %d was not closed", i)
		}
	}
	this.bodies = nil
}

// recordClosedBodies replaces http.DefaultTransport, which every client in this package uses, for the duration of the test.
func recordClosedBodies(t *testing.T) *closeRecorder {
	t.Helper()
	rec := &closeRecorder{base: http.DefaultTransport}
	http.DefaultTransport = rec
	t.Cleanup(func() { http.DefaultTransport = rec.base })
	return rec
}

func TestAuthCallsCloseResponseBody(t *testing.T) {
	statuses := []struct {
		code int
		body string
	}{
		{http.StatusOK, testTokenBody},
		{http.StatusUnauthorized, testIdpBody},
		{http.StatusInternalServerError, "internal"},
	}
	for _, s := range statuses {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(s.code)
			_, _ = w.Write([]byte(s.body))
		}))
		t.Cleanup(server.Close)
		for _, fn := range authCalls {
			t.Run(fn.name+"/"+http.StatusText(s.code), func(t *testing.T) {
				rec := recordClosedBodies(t)
				err := fn.call(t, server.URL, model.RemoteInfo{})
				if (err == nil) != (s.code == http.StatusOK) {
					t.Errorf("unexpected error result for status %d: %v", s.code, err)
				}
				rec.checkAllClosed(t)
			})
		}
	}
}
