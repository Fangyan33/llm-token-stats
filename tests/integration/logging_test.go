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

	"github.com/Fangyan33/llm-token-stats/internal/events"
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
				return true
			})
			if strings.Contains(logs, secret) || strings.Contains(logs, "level=DEBUG component=wasm") {
				t.Fatal("Wasm leaked a body/token or emitted DEBUG")
			}
			info := strings.Contains(logs, "level=INFO component=wasm event=plugin_configured")
			warn := strings.Contains(logs, "level=WARN component=wasm event=invalid_log_level")
			if info != (level != "warn" && level != "error") || warn != (level == "invalid") {
				t.Fatalf("LOG_LEVEL not applied through real WASI environment: level=%s info=%v warn=%v logs=%s", level, info, warn, logs)
			}
			if level == "invalid" && !strings.Contains(logs, "event=invalid_log_level") {
				t.Fatal("invalid LOG_LEVEL did not warn and fall back")
			}
		})
	}
}

func TestWasmConfigurationDiagnosticsAndFailOpen(t *testing.T) {
	raw, err := os.ReadFile("../../dist/envoy.remote.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, format := range []string{"json", "struct"} {
		for _, failure := range []string{"multiple_fields", "field_type"} {
			t.Run(format+"/"+failure, func(t *testing.T) {
				var cfg map[string]any
				if err := json.Unmarshal(raw, &cfg); err != nil {
					t.Fatal(err)
				}
				resources := cfg["static_resources"].(map[string]any)
				hcm := resources["listeners"].([]any)[0].(map[string]any)["filter_chains"].([]any)[0].(map[string]any)["filters"].([]any)[0].(map[string]any)["typed_config"].(map[string]any)
				plugin := hcm["http_filters"].([]any)[1].(map[string]any)["typed_config"].(map[string]any)["config"].(map[string]any)
				plugin["vm_config"].(map[string]any)["code"] = map[string]any{"local": map[string]any{"filename": "/wasm/observer.wasm"}}
				var settings map[string]any
				if err := json.Unmarshal([]byte(plugin["configuration"].(map[string]any)["value"].(string)), &settings); err != nil {
					t.Fatal(err)
				}
				const secret = "SYNTHETIC-CONFIGURATION-PRIVACY-SECRET"
				wanted := []string{"config_field=\"schema_version\"", "reason=\"type_mismatch\""}
				if failure == "multiple_fields" {
					settings["chunk_bytes"], settings["tick_ms"] = 16385, 11
					settings["routes"].([]any)[0].(map[string]any)["paths"] = []any{"/private?" + secret}
					wanted = []string{"config_field=\"chunk_bytes\"", "config_field=\"tick_ms\"", "config_field=\"routes[0].paths[0]\"", "actual=\"16385\""}
				} else {
					settings["schema_version"] = secret
				}
				if format == "struct" {
					plugin["configuration"] = map[string]any{"@type": "type.googleapis.com/google.protobuf.Struct", "value": settings}
				} else {
					encoded, _ := json.Marshal(settings)
					plugin["configuration"].(map[string]any)["value"] = string(encoded)
				}
				base, _, stop := startEnvoyWithLogLevel(t, cfg, true, "error")
				defer stop()
				body := []byte(`{"stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"` + secret + `"}]}`)
				req, _ := http.NewRequest("POST", base+"/v1/chat/completions", bytes.NewReader(body))
				req.Host = "model.example.invalid"
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Authorization", "Bearer "+secret)
				res, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
				if err != nil {
					t.Fatal(err)
				}
				response, readErr := io.ReadAll(res.Body)
				res.Body.Close()
				if readErr != nil || res.StatusCode != 200 || res.Header.Get("x-test-body-sha256") != events.Hash(body) || !bytes.Contains(response, []byte("[DONE]")) {
					t.Fatal("invalid configuration changed business forwarding", res.StatusCode, readErr)
				}
				pod := cfg["node"].(map[string]any)["id"].(string)
				var logs string
				eventually(t, func() bool {
					logs = string(kubeRequest(t, "GET", "pods/"+pod+"/log?container=envoy", nil))
					return strings.Contains(logs, "event=plugin_configuration_failed")
				})
				for _, want := range append(wanted, `action="passthrough"`) {
					if !strings.Contains(logs, want) {
						t.Fatalf("missing %s: %s", want, logs)
					}
				}
				if strings.Contains(logs, secret) {
					t.Fatal("configuration, body or token leaked")
				}
			})
		}
	}
}
