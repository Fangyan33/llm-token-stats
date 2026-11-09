//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func TestWasmLogLevelsAndFailOpen(t *testing.T) {
	raw, err := os.ReadFile("../../dist/envoy.remote.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, level := range []string{"info", "debug", "warn", "error", "invalid"} {
		t.Run(level, func(t *testing.T) {
			var cfg map[string]any
			if err := json.Unmarshal(raw, &cfg); err != nil {
				t.Fatal(err)
			}
			resources := cfg["static_resources"].(map[string]any)
			hcm := resources["listeners"].([]any)[0].(map[string]any)["filter_chains"].([]any)[0].(map[string]any)["filters"].([]any)[0].(map[string]any)["typed_config"].(map[string]any)
			plugin := hcm["http_filters"].([]any)[1].(map[string]any)["typed_config"].(map[string]any)["config"].(map[string]any)
			plugin["vm_config"].(map[string]any)["code"] = map[string]any{"local": map[string]any{"filename": "/wasm/observer.wasm"}}
			configuration := plugin["configuration"].(map[string]any)
			var settings map[string]any
			if err := json.Unmarshal([]byte(configuration["value"].(string)), &settings); err != nil {
				t.Fatal(err)
			}
			settings["observer_cluster"] = "missing-observer-for-logging"
			encoded, _ := json.Marshal(settings)
			configuration["value"] = string(encoded)
			base, _, stop := startEnvoyWithLogLevel(t, cfg, true, level)
			defer stop()
			const secret = "SYNTHETIC-LOG-PRIVACY-SECRET"
			for _, padding := range []string{secret, secret + strings.Repeat("x", 262144)} {
				body, _ := json.Marshal(map[string]any{"stream": true, "stream_options": map[string]bool{"include_usage": true}, "messages": []any{}, "padding": padding})
				req, _ := http.NewRequest("POST", base+"/v1/chat/completions", bytes.NewReader(body))
				req.Host = "model.example.invalid"
				req.Header.Set("content-type", "application/json")
				req.Header.Set("authorization", "Bearer "+secret)
				response, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
				if err != nil {
					t.Fatal(err)
				}
				io.Copy(io.Discard, response.Body)
				response.Body.Close()
				if response.StatusCode != http.StatusOK {
					t.Fatal("logging or unavailable observer blocked business", response.StatusCode)
				}
			}
			pod := cfg["node"].(map[string]any)["id"].(string)
			var logs string
			eventually(t, func() bool {
				logs = string(kubeRequest(t, "GET", "pods/"+pod+"/log?container=envoy", nil))
				if !strings.Contains(logs, "event=grpc_open_failed") {
					return false
				}
				if level != "error" && !strings.Contains(logs, "event=rewrite_body_limit") {
					return false
				}
				return true
			})
			if strings.Contains(logs, secret) || strings.Contains(logs, "level=DEBUG component=wasm") {
				t.Fatal("Wasm leaked a body/token or emitted DEBUG")
			}
			info := strings.Contains(logs, "level=INFO component=wasm event=plugin_configured")
			warn := strings.Contains(logs, "level=WARN component=wasm event=rewrite_body_limit")
			if info != (level != "warn" && level != "error") || warn != (level != "error") {
				t.Fatalf("LOG_LEVEL not applied through real WASI environment: level=%s info=%v warn=%v logs=%s", level, info, warn, logs)
			}
			if level == "invalid" && !strings.Contains(logs, "event=invalid_log_level") {
				t.Fatal("invalid LOG_LEVEL did not warn and fall back")
			}
		})
	}
}
