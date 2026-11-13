//go:build integration

package integration

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/events"
)

func TestRequestInputTokensThroughGatewayToDoris(t *testing.T) {
	db := database(t)
	bootstrap, err := os.ReadFile("../../dist/envoy.remote.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("capture_%t", enabled), func(t *testing.T) {
			var cfg map[string]any
			if err := json.Unmarshal(bootstrap, &cfg); err != nil {
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
			settings["request_body_capture_enabled"] = enabled
			encoded, _ := json.Marshal(settings)
			configuration["value"] = string(encoded)
			base, _, _ := startEnvoy(t, cfg, true)
			for _, tc := range []struct {
				name, content string
				input         *int64
			}{
				{"text", " hello中文🙂", events.Ptr(int64(3))},
				{"empty_text", "", events.Ptr(int64(0))},
				{"one_million", strings.Repeat(" word", 1000000), events.Ptr(int64(1000000))},
				{"invalid_messages", "", nil},
				{"missing_body", "", nil},
				{"missing_usage", "hello", events.Ptr(int64(1))},
				{"invalid_model", "hello", events.Ptr(int64(1))},
				{"missing_model", "hello", events.Ptr(int64(1))},
			} {
				t.Run(tc.name, func(t *testing.T) {
					xid := uuid(t)
					request := map[string]any{"model": "requested-model", "stream": true, "messages": []any{map[string]any{"role": "user", "content": tc.content}}}
					if tc.name == "invalid_messages" {
						request["messages"] = 42 // Valid JSON, invalid counting body; fixture still returns usage.
					}
					if tc.name == "missing_usage" {
						request["test_case"] = "missing_usage"
					}
					if tc.name == "invalid_model" {
						request["model"] = 42
					}
					if tc.name == "missing_model" {
						delete(request, "model")
					}
					body, _ := json.Marshal(request)
					wantStatus := http.StatusOK
					if tc.name == "missing_body" {
						body, wantStatus = nil, http.StatusBadRequest
					}
					req, _ := http.NewRequest("POST", base+"/v1/chat/completions", bytes.NewReader(body))
					req.Host = "model.example.invalid"
					req.Header.Set("Content-Type", "application/json")
					req.Header.Set("x-request-id", xid)
					res, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
					if err != nil {
						t.Fatal(err)
					}
					_, err = io.Copy(io.Discard, res.Body)
					res.Body.Close()
					if err != nil || res.StatusCode != wantStatus {
						t.Fatal("business forwarding", res.StatusCode, err)
					}
					if wantStatus == http.StatusOK && res.Header.Get("x-test-body-sha256") != fmt.Sprintf("%x", sha256.Sum256(body)) {
						t.Fatal("request body changed in gateway")
					}
					eventually(t, func() bool {
						var n int
						return db.QueryRow("SELECT COUNT(*) FROM metering_development.events_raw WHERE x_request_id=?", xid).Scan(&n) == nil && n == 2
					})
					rows, err := db.Query("SELECT event_type,input_tokens,input_tokens_source,output_tokens,usage_eligible,event_json,request_model FROM metering_development.events_raw WHERE x_request_id=?", xid)
					if err != nil {
						t.Fatal(err)
					}
					defer rows.Close()
					var observationID string
					seen := map[string]bool{}
					responseKnown := tc.name != "missing_usage" && tc.name != "missing_body"
					for rows.Next() {
						var kind, raw string
						var input, output sql.NullInt64
						var source, requestedModel sql.NullString
						var eligible int
						if err := rows.Scan(&kind, &input, &source, &output, &eligible, &raw, &requestedModel); err != nil {
							t.Fatal(err)
						}
						var e events.Event
						if err := json.Unmarshal([]byte(raw), &e); err != nil {
							t.Fatal(err)
						}
						if _, err := events.Decode([]byte(raw), []byte(e.ObservationID), time.Now()); err != nil {
							t.Fatal("stored event contract", err)
						}
						if seen[kind] || observationID != "" && observationID != e.ObservationID {
							t.Fatal("event duplicated or request metadata changed")
						}
						modelKnown := kind == "request_started" && enabled && tc.name != "missing_body" && tc.name != "missing_model" && tc.name != "invalid_model"
						if requestedModel.Valid != modelKnown || (e.RequestModel != nil) != modelKnown || modelKnown && (requestedModel.String != "requested-model" || *e.RequestModel != requestedModel.String) {
							t.Fatal("request model missing, copied to response, or changed in storage")
						}
						seen[kind], observationID = true, e.ObservationID
						if kind == "request_started" {
							known := enabled && tc.input != nil
							if input.Valid != known || known && (input.Int64 != *tc.input || source.String != "estimated") || output.Valid || eligible != 0 {
								t.Fatal("request estimate columns", input, source, output, eligible)
							}
							if enabled && !known && source.String != "unknown" || !enabled && (source.Valid || e.Usage != nil || e.UsageStatus != "not_applicable") {
								t.Fatal("unknown/disabled capture contract")
							}
						} else if kind == "usage_observed" {
							if input.Valid != responseKnown || output.Valid != responseKnown || responseKnown && (input.Int64 != 10 || source.String != "reported" || output.Int64 != 4 || eligible != 1) {
								t.Fatal("response usage replaced by request estimate", input, source, output, eligible)
							}
							if !responseKnown && (e.UsageStatus != "unknown" || eligible != 0 || !events.Has(e.AnomalyCodes, "missing_usage")) {
								t.Fatal("missing response usage was estimated")
							}
						} else {
							t.Fatal("unexpected event type", kind)
						}
					}
					if err := rows.Err(); err != nil || len(seen) != 2 {
						t.Fatal("missing events", err)
					}
					var responseInput sql.NullInt64
					var conflict int
					if err := db.QueryRow("SELECT input_tokens,has_conflict FROM metering_development.requests WHERE observation_id=?", observationID).Scan(&responseInput, &conflict); err != nil || conflict != 0 || responseInput.Valid != responseKnown || responseKnown && responseInput.Int64 != 10 {
						t.Fatal("request estimate contaminated result view", responseInput, conflict, err)
					}
					var effective int
					if err := db.QueryRow("SELECT COUNT(*) FROM metering_development.usage_effective WHERE observation_id=?", observationID).Scan(&effective); err != nil || effective != 0 && !responseKnown {
						t.Fatal("request estimate entered effective usage", effective, err)
					}
				})
			}
		})
	}
}
