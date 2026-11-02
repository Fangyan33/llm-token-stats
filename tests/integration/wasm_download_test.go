//go:build integration

package integration

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func availablePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

func TestWasmDownloadArtifact(t *testing.T) {
	local, err := os.ReadFile("../../dist/gateway-observer-v1.wasm")
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	res, err := client.Get("http://e2e-observer:9093/wasm/gateway-observer-v1.wasm")
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil || res.StatusCode != 200 || res.Header.Get("Content-Type") != "application/wasm" || !bytes.Equal(local, b) {
		t.Fatal("Observer must serve the exact build artifact")
	}
}

func TestRemoteWasmFailureAndRecovery(t *testing.T) {
	db := database(t)
	artifact, err := os.ReadFile("../../dist/gateway-observer-v1.wasm")
	if err != nil {
		t.Fatal(err)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(artifact))
	for _, scenario := range []string{"unavailable", "not_found", "timeout", "checksum_mismatch"} {
		t.Run(scenario, func(t *testing.T) {
			var requests atomic.Int32
			fault := reachableServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				switch scenario {
				case "not_found":
					w.WriteHeader(404)
				case "timeout":
					<-r.Context().Done()
				default:
					w.Write(artifact)
				}
			}))
			defer fault.Close()
			downloadURL := fault.URL + "/wasm/gateway-observer-v1.wasm"
			if scenario == "unavailable" {
				downloadURL = fmt.Sprintf("http://%s:%d/wasm/gateway-observer-v1.wasm", os.Getenv("POD_IP"), availablePort(t))
			}
			var cfg map[string]any
			b, err := os.ReadFile("../../dist/envoy.remote.json")
			if err != nil || json.Unmarshal(b, &cfg) != nil {
				t.Fatal("run make k8s-test")
			}
			adminPort, businessPort := 9901, 8080
			cfg["admin"].(map[string]any)["address"] = map[string]any{"socket_address": map[string]any{"address": "0.0.0.0", "port_value": adminPort}}
			resources := cfg["static_resources"].(map[string]any)
			listener := resources["listeners"].([]any)[0].(map[string]any)
			listener["address"] = map[string]any{"socket_address": map[string]any{"address": "0.0.0.0", "port_value": businessPort}}
			hcm := listener["filter_chains"].([]any)[0].(map[string]any)["filters"].([]any)[0].(map[string]any)["typed_config"].(map[string]any)
			plugin := hcm["http_filters"].([]any)[1].(map[string]any)["typed_config"].(map[string]any)["config"].(map[string]any)
			remote := plugin["vm_config"].(map[string]any)["code"].(map[string]any)["remote"].(map[string]any)
			setDownload := func(uri, checksum string) {
				u, _ := url.Parse(uri)
				port, _ := strconv.Atoi(u.Port())
				remote["http_uri"].(map[string]any)["uri"] = uri
				remote["sha256"] = checksum
				for _, value := range resources["clusters"].([]any) {
					cluster := value.(map[string]any)
					if cluster["name"] != "observer-wasm" {
						continue
					}
					endpoint := cluster["load_assignment"].(map[string]any)["endpoints"].([]any)[0].(map[string]any)["lb_endpoints"].([]any)[0].(map[string]any)["endpoint"].(map[string]any)
					endpoint["address"] = map[string]any{"socket_address": map[string]any{"address": u.Hostname(), "port_value": port}}
				}
			}
			setDownload(downloadURL, digest)
			if scenario == "checksum_mismatch" {
				remote["sha256"] = strings.Repeat("0", 64)
			}
			base, _, stop := startEnvoy(t, cfg, false)
			client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 5 * time.Second}
			defer client.CloseIdleConnections()
			request := func() string {
				id := uuid(t)
				body := []byte(`{"model":"fixture","messages":[{"role":"user","content":"synthetic download recovery"}]}`)
				req, _ := http.NewRequest("POST", base+"/v1/chat/completions", bytes.NewReader(body))
				req.Host = "model.example.invalid"
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("x-request-id", id)
				res, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				io.Copy(io.Discard, res.Body)
				res.Body.Close()
				if res.StatusCode != 200 || res.Header.Get("x-test-body-sha256") != fmt.Sprintf("%x", sha256.Sum256(body)) {
					t.Fatal("download failure changed business response or request body")
				}
				return id
			}
			id := request()
			var count int
			if err := db.QueryRow("SELECT COUNT(*) FROM metering_development.events_raw WHERE x_request_id=?", id).Scan(&count); err != nil || count != 0 {
				t.Fatalf("failed plugin produced events: count=%d err=%v", count, err)
			}
			if scenario != "unavailable" && requests.Load() != 1 {
				t.Fatalf("unexpected download retries: %d", requests.Load())
			}
			setDownload("http://e2e-observer:9093/wasm/gateway-observer-v1.wasm", digest)
			stop()
			base, _, _ = startEnvoy(t, cfg, false)
			id = request()
			eventually(t, func() bool {
				return db.QueryRow("SELECT COUNT(*) FROM metering_development.events_raw WHERE x_request_id=? AND event_type='usage_observed' AND usage_status='known'", id).Scan(&count) == nil && count == 1
			})
		})
	}
}
