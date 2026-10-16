package security

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/SENERGY-Platform/platform-connector-lib/model"
)

const testIdpBody = `{"error":"invalid_grant","error_description":"Invalid user credentials"}`

func newBadLoginServer(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(testIdpBody))
	}))
	t.Cleanup(server.Close)
	return server
}

func newTestSecurity(t *testing.T, authEndpoint string) *Security {
	t.Helper()
	s, err := New(authEndpoint, "test-client", "test-client-secret", "", "", 0, 0, 0, nil, 0, 0, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func checkBadLogin(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrBadLogin) {
		t.Fatalf("expected ErrBadLogin, got %q", err)
	}
	if !strings.Contains(err.Error(), testIdpBody) {
		t.Errorf("expected the identity provider response in the error, got %q", err)
	}
	for _, secret := range []string{"test-client-secret", "test-password"} {
		if strings.Contains(err.Error(), secret) {
			t.Errorf("error text contains request credential %q", secret)
		}
	}
}

func TestGetUserTokenBadLogin(t *testing.T) {
	server := newBadLoginServer(t)
	_, err := newTestSecurity(t, server.URL).GetUserToken("test-user", "test-password", model.RemoteInfo{})
	checkBadLogin(t, err)
}

func TestExchangeUserTokenBadLogin(t *testing.T) {
	server := newBadLoginServer(t)
	_, err := newTestSecurity(t, server.URL).ExchangeUserToken("test-user-id", model.RemoteInfo{})
	checkBadLogin(t, err)
}
