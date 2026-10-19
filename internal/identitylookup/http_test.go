package identitylookup

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/config"
)

func TestHTTPSContractAndFailures(t *testing.T) {
	cases := []struct {
		name             string
		status           int
		body, kind, want string
	}{
		{"mapped", 200, `{"service_id":"s","service_name":"模型服务","provider_id":"p","provider_name":"供应商"}`, "application/json", ""},
		{"not_found", 404, `{"error":{"code":"not_found"}}`, "application/json", "unknown_host"},
		{"bad_404", 404, `<html>not found</html>`, "text/html", "lookup_invalid_response"},
		{"bad_error", 404, `{"error":{"code":"other"}}`, "application/json", "lookup_invalid_response"},
		{"auth", 401, `secret`, "text/plain", "lookup_unavailable"},
		{"forbidden", 403, `secret`, "text/plain", "lookup_unavailable"},
		{"invalid", 400, `{"error":{"code":"invalid_request"}}`, "application/json", "lookup_unavailable"},
		{"rate", 429, `{"error":{"code":"rate_limited"}}`, "application/json", "lookup_rate_limited"},
		{"ambiguous", 409, `{"error":{"code":"ambiguous_mapping"}}`, "application/json", "lookup_invalid_response"},
		{"501", 501, `secret`, "text/plain", "lookup_unavailable"},
		{"partial", 200, `{"service_id":"s","provider_id":"p"}`, "application/json", "lookup_invalid_response"},
		{"null", 200, `null`, "application/json", "lookup_invalid_response"},
		{"null_name", 200, `{"service_id":"s","service_name":null,"provider_id":"p","provider_name":"P"}`, "application/json", "lookup_invalid_response"},
		{"duplicate", 200, `{"service_id":"s","service_id":"x"}`, "application/json", "lookup_invalid_response"},
		{"utf8", 200, "{\"service_name\":\"\xff\"}", "application/json", "lookup_invalid_response"},
		{"oversize", 200, strings.Repeat("x", maxResponseBytes+1), "application/json", "lookup_invalid_response"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				var input ServiceLookupRequest
				if r.Method != "POST" || r.URL.RawQuery != "" || r.Header.Get("x-remote-data") != "observer-test-credential" || json.NewDecoder(r.Body).Decode(&input) != nil || input.RequestHost != "model.example.com" || input.ClusterName != "cluster-a" || input.DeploymentEnvironment != "stg" {
					t.Error("request contract")
				}
				w.Header().Set("Content-Type", tc.kind)
				w.WriteHeader(tc.status)
				w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			remote := serviceRemote{&remote{uri: srv.URL, credential: "observer-test-credential", client: srv.Client()}}
			got, err := remote.Resolve(context.Background(), ServiceLookupRequest{"model.example.com", "cluster-a", "stg"})
			if tc.want == "" {
				if err != nil || got.ServiceName != "模型服务" {
					t.Fatalf("valid response: %v", err)
				}
			} else if err == nil || Failure(err) != tc.want || got != (ServiceIdentity{}) || strings.Contains(err.Error(), "secret") {
				t.Fatalf("failure classification: %v", err)
			}
			if calls.Load() != 1 {
				t.Fatal("unexpected retries")
			}
		})
	}
}

func TestRemoteRateLimitAndConstruction(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"service_id":"s","service_name":"S","provider_id":"p","provider_name":"P"}`))
	}))
	defer srv.Close()
	r := serviceRemote{&remote{uri: srv.URL, credential: "synthetic", client: srv.Client(), tokens: 1, last: time.Now(), maxRPS: 1}}
	if _, err := r.Resolve(context.Background(), ServiceLookupRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Resolve(context.Background(), ServiceLookupRequest{}); err == nil || Failure(err) != "lookup_overloaded" || calls.Load() != 1 {
		t.Fatal("local rate limit waited, retried or sent a request")
	}
	if _, err := NewService(config.Lookup{}); err != nil {
		t.Fatal("empty URI requires no credential")
	}
	t.Setenv("TEST_REMOTE_CREDENTIAL", "synthetic")
	for _, uri := range []string{"ftp://example.com/", "http://user:password@example.com/", "http://example.com/?token=private", "https://user:password@example.com/", "https://example.com/?token=private", "https://example.com/#private"} {
		if _, err := NewService(config.Lookup{URI: uri, CredentialEnv: "TEST_REMOTE_CREDENTIAL"}); err == nil || strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), "private") {
			t.Fatal("invalid target or sensitive error")
		}
	}
	t.Setenv("TEST_REMOTE_CREDENTIAL", "")
	if _, err := NewService(config.Lookup{URI: srv.URL, CredentialEnv: "TEST_REMOTE_CREDENTIAL"}); err == nil {
		t.Fatal("missing credential accepted")
	}
}

func TestCustomerTokenAndNames(t *testing.T) {
	key := []byte(`AbC-_.~+/=synthetic`)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		if json.NewDecoder(r.Body).Decode(&body) != nil || len(body) != 1 || body["api_key"] != string(key) || r.Header.Get("x-remote-data") != "independent-credential" {
			t.Error("token changed or used as auth")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"api_key_id":"k","customer_id":"c","customer_name":"客户 A"}`))
	}))
	defer srv.Close()
	resolver := customerRemote{&remote{uri: srv.URL, credential: "independent-credential", client: srv.Client()}}
	got, err := resolver.Resolve(context.Background(), key)
	if err != nil || got.CustomerName != "客户 A" || string(key) != `AbC-_.~+/=synthetic` {
		t.Fatalf("customer result: %v", err)
	}
	if ValidName(strings.Repeat("中", 171)) || !ValidName(strings.Repeat("中", 170)+"ab") {
		t.Fatal("name byte limit")
	}
}

func TestTLSRedirectAndTimeout(t *testing.T) {
	var destination atomic.Int32
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { destination.Add(1) }))
	defer target.Close()
	source := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	client := source.Client()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resolver := serviceRemote{&remote{uri: source.URL, credential: "private", client: client}}
	if _, err := resolver.Resolve(context.Background(), ServiceLookupRequest{}); err == nil || Failure(err) != "lookup_invalid_response" || destination.Load() != 0 {
		t.Fatal("redirect followed")
	}
	// System trust rejects this self-signed endpoint.
	resolver.client = &http.Client{Timeout: time.Second}
	if _, err := resolver.Resolve(context.Background(), ServiceLookupRequest{}); err == nil || Failure(err) != "lookup_unavailable" {
		t.Fatal("untrusted TLS accepted")
	}
	slow := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(100 * time.Millisecond):
		}
	}))
	defer slow.Close()
	resolver.uri = slow.URL
	resolver.client = slow.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := resolver.Resolve(ctx, ServiceLookupRequest{}); err == nil || Failure(err) != "lookup_timeout" {
		t.Fatalf("timeout: %v", err)
	}
}
