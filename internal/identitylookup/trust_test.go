package identitylookup

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/config"
)

func TestHTTPQueriesIgnoreCAFile(t *testing.T) {
	t.Setenv("HTTP_LOOKUP_CREDENTIAL", "synthetic-directory")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.Header.Get("x-remote-data") != "synthetic-directory" {
			t.Error("HTTP authentication contract")
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/customer" {
			var body map[string]string
			if json.NewDecoder(r.Body).Decode(&body) != nil || body["api_key"] != "synthetic-business" {
				t.Error("HTTP customer input")
			}
			w.Write([]byte(`{"api_key_id":"k","customer_id":"c","customer_name":"客户"}`))
		} else {
			w.Write([]byte(`{"service_id":"s","service_name":"服务","provider_id":"p","provider_name":"供应商"}`))
		}
	}))
	defer srv.Close()
	cfg := config.Lookup{URI: srv.URL, CredentialEnv: "HTTP_LOOKUP_CREDENTIAL", CAFile: filepath.Join(t.TempDir(), "missing.pem")}
	service, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := service.Resolve(context.Background(), ServiceLookupRequest{}); err != nil || v.ServiceName != "服务" {
		t.Fatal("HTTP service", err)
	}
	cfg.URI += "/customer"
	customer, err := NewCustomer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := customer.Resolve(context.Background(), []byte("synthetic-business")); err != nil || v.CustomerName != "客户" {
		t.Fatal("HTTP customer", err)
	}
}

// This helper also runs in the built runtime image to inspect its real system
// roots as the image's default non-root user. A subprocess avoids Go's root cache.
func TestSystemTrustHelper(t *testing.T) {
	mode := os.Getenv("METERING_TRUST_HELPER")
	if mode == "" {
		return
	}
	pool, err := x509.SystemCertPool()
	if err != nil || len(pool.Subjects()) == 0 {
		t.Fatal("system roots unavailable", err)
	}
	if mode == "roots" {
		if b, err := os.ReadFile("/etc/ssl/certs/ca-certificates.crt"); err != nil || len(b) == 0 {
			t.Fatal("runtime CA bundle unreadable", err)
		}
		return
	}
	r, err := NewService(config.Lookup{URI: os.Getenv("METERING_TRUST_ENDPOINT"), CredentialEnv: "METERING_TRUST_CREDENTIAL"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Resolve(context.Background(), ServiceLookupRequest{}); err != nil {
		t.Fatal("default system trust failed", err)
	}
}

func TestHTTPSSystemTrustAndPrivateCA(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"service_id":"s","service_name":"S","provider_id":"p","provider_name":"P"}`))
	}))
	defer srv.Close()
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	// Default RootCAs, without a ca_file or injected Transport.
	child := exec.Command(os.Args[0], "-test.run=^TestSystemTrustHelper$")
	child.Env = append(os.Environ(), "METERING_TRUST_HELPER=resolve", "METERING_TRUST_ENDPOINT="+srv.URL, "METERING_TRUST_CREDENTIAL=synthetic", "SSL_CERT_FILE="+ca, "SSL_CERT_DIR="+dir)
	if output, err := child.CombinedOutput(); err != nil {
		t.Fatalf("system root discovery: %v\n%s", err, output)
	}
	t.Setenv("PRIVATE_CA_CREDENTIAL", "synthetic")
	cfg := config.Lookup{URI: srv.URL, CredentialEnv: "PRIVATE_CA_CREDENTIAL", CAFile: ca}
	r, err := NewService(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Resolve(context.Background(), ServiceLookupRequest{}); err != nil {
		t.Fatal("additional private CA", err)
	}
	transport := r.(serviceRemote).client.Transport.(*http.Transport)
	transport.CloseIdleConnections()
	transport.TLSClientConfig.ServerName = "wrong.example.invalid"
	if _, err := r.Resolve(context.Background(), ServiceLookupRequest{}); err == nil || Failure(err) != "lookup_unavailable" {
		t.Fatal("hostname mismatch accepted")
	}
	cfg.CAFile = filepath.Join(dir, "missing.pem")
	if _, err := NewService(cfg); err == nil || err.Error() != "lookup_ca" {
		t.Fatal("HTTPS accepted unreadable CA file")
	}
}

func TestHTTPRedirectAndResponseBodyTimeout(t *testing.T) {
	t.Setenv("HTTP_POLICY_CREDENTIAL", "synthetic")
	var calls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	r, err := NewService(config.Lookup{URI: redirect.URL, CredentialEnv: "HTTP_POLICY_CREDENTIAL"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Resolve(context.Background(), ServiceLookupRequest{}); err == nil || Failure(err) != "lookup_invalid_response" || calls.Load() != 0 {
		t.Fatal("HTTP redirect followed")
	}
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer slow.Close()
	r, err = NewService(config.Lookup{URI: slow.URL, CredentialEnv: "HTTP_POLICY_CREDENTIAL"})
	if err != nil {
		t.Fatal(err)
	}
	r.(serviceRemote).client.Timeout = 20 * time.Millisecond
	if _, err := r.Resolve(context.Background(), ServiceLookupRequest{}); err == nil || Failure(err) != "lookup_timeout" {
		t.Fatal("response body timeout misclassified", err)
	}
}
