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
	"bytes"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func withClientTimeout(t *testing.T, timeout time.Duration) {
	t.Helper()
	old := httpClient.Timeout
	httpClient.Timeout = timeout
	t.Cleanup(func() { httpClient.Timeout = old })
}

func TestJwtTokenRequestTimeout(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })
	withClientTimeout(t, 100*time.Millisecond)

	calls := map[string]func() error{
		"Get":  func() error { _, err := JwtToken("Bearer test").Get(server.URL); return err },
		"Head": func() error { _, err := JwtToken("Bearer test").Head(server.URL); return err },
		"Delete": func() error {
			_, err := JwtToken("Bearer test").Delete(server.URL)
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			start := time.Now()
			err := call()
			if err == nil {
				t.Fatal("expected a timeout error")
			}
			if elapsed := time.Since(start); elapsed > 2*time.Second {
				t.Errorf("request took %v, timeout not applied", elapsed)
			}
		})
	}
}

// The timeout must not end with Get returning: callers read the body afterwards.
func TestJwtTokenBodyReadableAfterReturn(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		time.Sleep(50 * time.Millisecond)
		_, _ = w.Write([]byte(`{"value":"late"}`))
	}))
	t.Cleanup(server.Close)

	var result struct{ Value string }
	if err := JwtToken("Bearer test").GetJSON(server.URL, &result); err != nil {
		t.Fatal(err)
	}
	if result.Value != "late" {
		t.Errorf("value = %q, want %q", result.Value, "late")
	}
}

func TestJwtTokenUnexpectedStatusReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	defer server.Close()
	token := JwtToken("Bearer test")

	calls := map[string]func() error{
		"Get":    func() error { _, err := token.Get(server.URL); return err },
		"Post":   func() error { _, err := token.Post(server.URL, "application/json", nil); return err },
		"Put":    func() error { _, err := token.Put(server.URL, "application/json", nil); return err },
		"Delete": func() error { _, err := token.Delete(server.URL); return err },
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.Is(err, ErrorUnexpectedStatus) {
				t.Errorf("err = %v, want ErrorUnexpectedStatus", err)
			}
		})
	}
}

func TestResetAccessDoesNotLogToken(t *testing.T) {
	var buf bytes.Buffer
	s := &Security{logger: slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))}
	s.openid = &OpenidToken{AccessToken: "access-secret-value", RefreshToken: "refresh-secret-value", ExpiresIn: 300}
	s.ResetAccess()
	for _, secret := range []string{"access-secret-value", "refresh-secret-value"} {
		if strings.Contains(buf.String(), secret) {
			t.Errorf("log contains %s: %s", secret, buf.String())
		}
	}
	if s.openid != nil {
		t.Error("token not reset")
	}
}
