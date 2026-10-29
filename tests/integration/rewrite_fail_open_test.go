//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"
)

// Uses the pinned real Envoy with an unavailable observation upstream. Does not
// require Kafka/Doris: the assertions are about actual business forwarding.
func TestWasmRewriteFailOpen(t *testing.T) {
	var firstReads sync.Map
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprint(w, "ok")
			return
		}
		first := make([]byte, 1)
		n, err := r.Body.Read(first)
		if ch, ok := firstReads.Load(r.Header.Get("x-test-case")); ok && n > 0 {
			ch.(chan time.Time) <- time.Now()
		}
		if err != nil && err != io.EOF {
			w.WriteHeader(400)
			return
		}
		tail, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(400)
			return
		}
		w.Header().Set("x-test-trailer-seen", r.Trailer.Get("x-test-end"))
		w.Write(append(first[:n], tail...))
	}))
	defer upstream.Close()
	var cfg map[string]any
	b, err := os.ReadFile("../../deploy/local/envoy.json")
	if err != nil || json.Unmarshal(b, &cfg) != nil {
		t.Fatal("read Envoy template")
	}
	admin, business := availablePort(t), availablePort(t)
	addr := func(port int) map[string]any {
		return map[string]any{"socket_address": map[string]any{"address": "127.0.0.1", "port_value": port}}
	}
	cfg["admin"].(map[string]any)["address"] = addr(admin)
	cfg["node"] = map[string]any{"id": "rewrite-test", "cluster": "test", "metadata": map[string]any{"NAMESPACE": "test", "NAME": "rewrite-gateway"}}
	resources := cfg["static_resources"].(map[string]any)
	listener := resources["listeners"].([]any)[0].(map[string]any)
	listener["address"] = addr(business)
	hcm := listener["filter_chains"].([]any)[0].(map[string]any)["filters"].([]any)[0].(map[string]any)["typed_config"].(map[string]any)
	// HTTP/1 trailers are disabled by default in Envoy on both sides. Enable
	// them here so the test exercises the plugin's trailers downgrade path.
	hcm["http_protocol_options"] = map[string]any{"enable_trailers": true}
	filters := hcm["http_filters"].([]any)
	hcm["http_filters"] = filters[1:] // Isolate metering, independent of fixture auth.
	plugin := filters[1].(map[string]any)["typed_config"].(map[string]any)["config"].(map[string]any)
	plugin["vm_config"].(map[string]any)["code"] = map[string]any{"local": map[string]any{"filename": "/observer.wasm"}}
	var clusters []any
	for _, c := range resources["clusters"].([]any) {
		cluster := c.(map[string]any)
		name := cluster["name"].(string)
		if name != "fixture" && name != "observer" {
			continue
		}
		port := availablePort(t)
		if name == "fixture" {
			port = upstream.Listener.Addr().(*net.TCPAddr).Port
			cluster["http_protocol_options"] = map[string]any{"enable_trailers": true}
		}
		cluster["type"] = "STATIC"
		cluster["load_assignment"].(map[string]any)["endpoints"].([]any)[0].(map[string]any)["lb_endpoints"].([]any)[0].(map[string]any)["endpoint"].(map[string]any)["address"] = addr(port)
		clusters = append(clusters, cluster)
	}
	resources["clusters"] = clusters
	b, _ = json.Marshal(cfg)
	configPath := filepath.Join(t.TempDir(), "envoy.json")
	if err := os.WriteFile(configPath, b, 0644); err != nil {
		t.Fatal(err)
	}
	artifact, err := filepath.Abs("../../dist/gateway-observer-v1.wasm")
	if err != nil {
		t.Fatal(err)
	}
	name := "metering-rewrite-test-" + uuid(t)
	t.Cleanup(func() {
		if t.Failed() {
			logs, _ := exec.Command("docker", "logs", "--tail", "40", name).CombinedOutput()
			t.Logf("Envoy: %s", logs)
		}
		exec.Command("docker", "rm", "-f", name).Run()
	})
	dockerCommand(t, "run", "-d", "--name", name, "--network", "host", "--mount", "type=bind,source="+configPath+",target=/envoy.json,readonly", "--mount", "type=bind,source="+artifact+",target=/observer.wasm,readonly", "--entrypoint", "/usr/local/bin/envoy", "istio/proxyv2:1.13.5", "-c", "/envoy.json", "--concurrency", "1", "--log-level", "warning")
	adminTransport := &http.Transport{Proxy: nil}
	defer adminTransport.CloseIdleConnections()
	adminClient := &http.Client{Transport: adminTransport, Timeout: time.Second}
	deadline := time.Now().Add(10 * time.Second)
	for {
		res, err := adminClient.Get(fmt.Sprintf("http://127.0.0.1:%d/ready", admin))
		if err == nil {
			res.Body.Close()
			if res.StatusCode == 200 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("isolated Envoy did not become ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, protocol := range []string{"http1", "http2"} {
		t.Run(protocol, func(t *testing.T) {
			var transport http.RoundTripper
			if protocol == "http1" {
				tr := &http.Transport{Proxy: nil}
				defer tr.CloseIdleConnections()
				transport = tr
			} else {
				tr := &http2.Transport{AllowHTTP: true, DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, network, addr)
				}}
				defer tr.CloseIdleConnections()
				transport = tr
			}
			client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
			for _, scenario := range []string{"rewrite", "invalid_json", "oversized", "trailers", "timeout"} {
				t.Run(scenario, func(t *testing.T) {
					body := []byte(`{"stream":true,"messages":[]}`)
					want := []byte(`{"stream":true,"messages":[],"stream_options":{"include_usage":true}}`)
					if scenario == "invalid_json" {
						body = []byte(`{"stream":true,invalid`)
						want = body
					} else if scenario == "oversized" {
						body = []byte(`{"stream":true,"prompt":"` + strings.Repeat("x", 262144) + `"}`)
						want = body
					}
					id := protocol + "-" + scenario
					first := make(chan time.Time, 1)
					firstReads.Store(id, first)
					defer firstReads.Delete(id)
					var reader io.Reader = bytes.NewReader(body)
					var writer *io.PipeWriter
					if scenario == "timeout" {
						var pipe *io.PipeReader
						pipe, writer = io.Pipe()
						defer pipe.Close()
						defer writer.Close()
						reader, want = pipe, body
					}
					req, _ := http.NewRequest("POST", fmt.Sprintf("http://127.0.0.1:%d/v1/chat/completions", business), reader)
					req.Host = "model.example.invalid"
					req.Header.Set("content-type", "application/json")
					req.Header.Set("x-test-case", id)
					if scenario == "trailers" {
						req.ContentLength = -1
						req.Trailer = http.Header{"X-Test-End": []string{"complete"}}
						want = body
					}
					type result struct {
						body    []byte
						status  int
						err     error
						trailer string
					}
					done := make(chan result, 1)
					started := time.Now()
					go func() {
						res, err := client.Do(req)
						if err != nil {
							done <- result{err: err}
							return
						}
						defer res.Body.Close()
						b, err := io.ReadAll(res.Body)
						done <- result{body: b, status: res.StatusCode, err: err, trailer: res.Header.Get("x-test-trailer-seen")}
					}()
					if writer != nil {
						if _, err := writer.Write(body[:10]); err != nil {
							t.Fatal(err)
						}
						select {
						case at := <-first:
							t.Logf("upstream received original prefix before tail at %s", at.Sub(started))
						case <-time.After(1200 * time.Millisecond):
							t.Fatal("paused body was not released; tail is deliberately withheld")
						}
						if _, err := writer.Write(body[10:]); err != nil {
							t.Fatal(err)
						}
						writer.Close()
					}
					got := <-done
					if got.err != nil || got.status != 200 || !bytes.Equal(got.body, want) {
						t.Fatalf("business response mismatch: status=%d err=%v received=%d expected=%d", got.status, got.err, len(got.body), len(want))
					}
					if scenario == "trailers" && got.trailer != "complete" {
						t.Fatal("request trailer was lost")
					}
				})
			}
		})
	}
}
