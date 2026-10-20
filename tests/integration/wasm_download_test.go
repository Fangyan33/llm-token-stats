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
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func dockerCommand(t *testing.T, args ...string) string {
	t.Helper()
	b, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", args[0], err, b)
	}
	return strings.TrimSpace(string(b))
}

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
	res, err := client.Get("http://127.0.0.1:19093/wasm/gateway-observer-v1.wasm")
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
			fault := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
				downloadURL = fmt.Sprintf("http://127.0.0.1:%d/wasm/gateway-observer-v1.wasm", availablePort(t))
			}
			var cfg map[string]any
			b, err := os.ReadFile("../../dist/envoy.remote.json")
			if err != nil || json.Unmarshal(b, &cfg) != nil {
				t.Fatal("run make local-up first")
			}
			adminPort, businessPort := availablePort(t), availablePort(t)
			cfg["admin"].(map[string]any)["address"] = map[string]any{"socket_address": map[string]any{"address": "127.0.0.1", "port_value": adminPort}}
			resources := cfg["static_resources"].(map[string]any)
			listener := resources["listeners"].([]any)[0].(map[string]any)
			listener["address"] = map[string]any{"socket_address": map[string]any{"address": "127.0.0.1", "port_value": businessPort}}
			hcm := listener["filter_chains"].([]any)[0].(map[string]any)["filters"].([]any)[0].(map[string]any)["typed_config"].(map[string]any)
			plugin := hcm["http_filters"].([]any)[1].(map[string]any)["typed_config"].(map[string]any)["config"].(map[string]any)
			remote := plugin["vm_config"].(map[string]any)["code"].(map[string]any)["remote"].(map[string]any)
			setDownload := func(uri, checksum string) {
				u, _ := url.Parse(uri)
				port, _ := strconv.Atoi(u.Port())
				remote["http_uri"].(map[string]any)["uri"] = uri
				remote["sha256"] = checksum
				ports := map[string]int{"fixture": 18081, "auth": 19002, "observer": 19090, "observer-wasm": port}
				for _, value := range resources["clusters"].([]any) {
					cluster := value.(map[string]any)
					endpoint := cluster["load_assignment"].(map[string]any)["endpoints"].([]any)[0].(map[string]any)["lb_endpoints"].([]any)[0].(map[string]any)["endpoint"].(map[string]any)
					endpoint["address"] = map[string]any{"socket_address": map[string]any{"address": "127.0.0.1", "port_value": ports[cluster["name"].(string)]}}
				}
			}
			setDownload(downloadURL, digest)
			if scenario == "checksum_mismatch" {
				remote["sha256"] = strings.Repeat("0", 64)
			}
			path := filepath.Join(t.TempDir(), "envoy.json")
			writeConfig := func() {
				b, err := json.Marshal(cfg)
				if err != nil || os.WriteFile(path, b, 0644) != nil {
					t.Fatal("cannot write isolated Envoy config")
				}
			}
			writeConfig()
			name := "metering-wasm-test-" + uuid(t)
			t.Cleanup(func() {
				if t.Failed() {
					b, _ := exec.Command("docker", "logs", "--tail", "40", name).CombinedOutput()
					t.Logf("isolated Envoy: %s", b)
				}
				exec.Command("docker", "rm", "-f", name).Run()
			})
			dockerCommand(t, "run", "-d", "--name", name, "--network", "host", "--mount", "type=bind,source="+path+",target=/config/envoy.json,readonly", "--entrypoint", "/usr/local/bin/envoy", "istio/proxyv2:1.13.5", "-c", "/config/envoy.json", "--concurrency", "2", "--log-level", "warning")
			client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: time.Second}
			defer client.CloseIdleConnections()
			base := fmt.Sprintf("http://127.0.0.1:%d", businessPort)
			ready := func() {
				eventually(t, func() bool {
					res, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/ready", adminPort))
					if err != nil {
						return false
					}
					res.Body.Close()
					return res.StatusCode == 200
				})
			}
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
			ready()
			id := request()
			var count int
			if err := db.QueryRow("SELECT COUNT(*) FROM metering_development.events_raw WHERE x_request_id=?", id).Scan(&count); err != nil || count != 0 {
				t.Fatalf("failed plugin produced events: count=%d err=%v", count, err)
			}
			if scenario != "unavailable" && requests.Load() != 1 {
				t.Fatalf("unexpected download retries: %d", requests.Load())
			}
			setDownload("http://127.0.0.1:19093/wasm/gateway-observer-v1.wasm", digest)
			writeConfig()
			dockerCommand(t, "restart", name)
			ready()
			id = request()
			eventually(t, func() bool {
				return db.QueryRow("SELECT COUNT(*) FROM metering_development.events_raw WHERE x_request_id=? AND event_type='usage_observed' AND usage_status='known'", id).Scan(&count) == nil && count == 1
			})
		})
	}
}
