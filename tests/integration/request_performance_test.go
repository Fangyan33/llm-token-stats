//go:build integration

package integration

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/events"
)

func TestPerformanceViewsAndReplay(t *testing.T) {
	db, cl := database(t), kafka(t)
	data, err := os.ReadFile("../../testdata/collection/usage-known.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"reported", "output_only", "missing_usage", "zero", "one", "non_streaming", "incomplete", "unknown_time", "unknown_last", "usage_conflict", "batched"} {
		t.Run(mode, func(t *testing.T) {
			var e events.Event
			if err := json.Unmarshal(data, &e); err != nil {
				t.Fatal(err)
			}
			e.ObservationID = uuid(t)
			e.EventID = e.ObservationID + ":usage"
			e.RequestStartedAtMs = time.Now().UnixMilli() - 1000
			e.RequestFinishedAtMs = events.Ptr(e.RequestStartedAtMs + 950)
			e.ObservationEndedAtMs = events.Ptr(time.Now().UnixMilli())
			e.EventCreatedAtMs = time.Now().UnixMilli()
			e.ResponseStreaming = events.Ptr(true)
			e.FirstOutputElapsedUS = events.Ptr(int64(100000))
			e.LastOutputElapsedUS = events.Ptr(int64(300000))
			e.Usage = events.Unknown()
			e.Usage.OutputTokens = events.Token{Value: events.Ptr(int64(5)), Source: "reported"}
			e.RawUsageJSON = `{"completion_tokens":5}`
			e.UsageStatus = "partial"
			e.UsageEligible = false
			e.AnomalyCodes = []string{}
			wantTTFT, wantTPOT := true, true
			wantTPOTValue := 50.0
			switch mode {
			case "reported":
				e.Usage.InputTokens = events.Token{Value: events.Ptr(int64(10)), Source: "reported"}
				e.RawUsageJSON = `{"prompt_tokens":10,"completion_tokens":5}`
				e.UsageStatus = "known"
				e.UsageEligible = true
			case "missing_usage":
				e.Usage = events.Unknown()
				e.RawUsageJSON = "{}"
				e.UsageStatus = "unknown"
				e.AnomalyCodes = []string{"missing_usage"}
				wantTPOT = false
			case "zero", "one":
				n := int64(0)
				if mode == "one" {
					n = 1
				}
				e.Usage.OutputTokens.Value = &n
				e.RawUsageJSON = `{"completion_tokens":` + strconv.FormatInt(n, 10) + `}`
				wantTPOT = false
			case "non_streaming":
				e.ResponseStreaming = events.Ptr(false)
				e.FirstOutputElapsedUS = nil
				e.LastOutputElapsedUS = nil
				wantTTFT = false
				wantTPOT = false
			case "incomplete":
				e.TerminalObserved = false
				e.RequestFinishedAtMs = nil
				e.ObservationStatus = events.Ptr("partial")
				e.BusinessOutcome = events.Ptr("stream_incomplete")
				e.AnomalyCodes = []string{"missing_end"}
				wantTPOT = false
			case "unknown_time":
				e.FirstOutputElapsedUS = nil
				e.LastOutputElapsedUS = nil
				wantTTFT = false
				wantTPOT = false
			case "unknown_last":
				e.LastOutputElapsedUS = nil
				wantTPOT = false
			case "usage_conflict":
				e.UsageStatus = "conflict"
				e.AnomalyCodes = []string{"usage_conflict"}
				e.Usage.InputTokens = events.Token{Value: events.Ptr(int64(10)), Source: "reported"}
				e.Usage.ReportedTotalTokens = events.Token{Value: events.Ptr(int64(999)), Source: "reported"}
				e.RawUsageJSON = `{"prompt_tokens":10,"completion_tokens":5,"total_tokens":999}`
				wantTPOT = false
			case "batched":
				e.LastOutputElapsedUS = events.Ptr(int64(100000))
				wantTPOTValue = 0
			}
			raw, err := events.Canonical(&e)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = events.Decode(raw, []byte(e.ObservationID), time.Now()); err != nil {
				t.Fatal(err)
			}
			send := func(raw []byte) {
				t.Helper()
				offset := publish(t, cl, "llm-metering-development", 0, e.ObservationID, raw)
				eventually(t, func() bool {
					var n int
					return db.QueryRow("SELECT COUNT(*) FROM metering_development.consumer_batch_receipts WHERE kafka_topic='llm-metering-development' AND kafka_partition=0 AND first_offset<=? AND next_offset>?", offset, offset).Scan(&n) == nil && n > 0
				})
			}
			send(raw)
			send(raw)
			var count int
			if err := db.QueryRow("SELECT COUNT(*) FROM metering_development.events_raw WHERE observation_id=?", e.ObservationID).Scan(&count); err != nil || count != 1 {
				t.Fatal("replay duplicated raw fact", count, err)
			}
			for _, view := range []string{"event_details", "requests"} {
				var ttft, tpot sql.NullFloat64
				if err := db.QueryRow("SELECT ttft_ms,tpot_ms FROM metering_development."+view+" WHERE observation_id=?", e.ObservationID).Scan(&ttft, &tpot); err != nil {
					t.Fatal(err)
				}
				if ttft.Valid != wantTTFT || tpot.Valid != wantTPOT || ttft.Valid && math.Abs(ttft.Float64-100) > 1e-6 || tpot.Valid && math.Abs(tpot.Float64-wantTPOTValue) > 1e-6 {
					t.Fatalf("%s: TTFT=%v TPOT=%v", view, ttft, tpot)
				}
			}
			if mode == "reported" {
				var a, b float64
				if err := db.QueryRow("SELECT ttft_ms,tpot_ms FROM metering_development.usage_effective WHERE observation_id=?", e.ObservationID).Scan(&a, &b); err != nil || a != 100 || b != 50 {
					t.Fatal("effective view", a, b, err)
				}
				e.ProducerID = "synthetic-timing-variant"
				variant, _ := events.Canonical(&e)
				send(variant)
				var ttft, tpot sql.NullFloat64
				if err := db.QueryRow("SELECT ttft_ms,tpot_ms FROM metering_development.requests WHERE observation_id=?", e.ObservationID).Scan(&ttft, &tpot); err != nil || ttft.Valid || tpot.Valid {
					t.Fatal("conflict metrics leaked", err)
				}
			}
		})
	}
}

func TestPerformanceThroughGateway(t *testing.T) {
	db := database(t)
	for _, mode := range []string{"performance", "performance_missing_usage"} {
		t.Run(mode, func(t *testing.T) {
			xid := uuid(t)
			body, _ := json.Marshal(map[string]any{"model": "fixture", "stream": true, "test_case": mode, "messages": []any{}})
			req, _ := http.NewRequest("POST", "http://llm-token-stats-e2e-gateway:8080/v1/chat/completions", bytes.NewReader(body))
			req.Host = "model.example.invalid"
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("x-request-id", xid)
			res, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
			if err != nil {
				t.Fatal(err)
			}
			out, err := io.ReadAll(io.LimitReader(res.Body, 262144))
			res.Body.Close()
			if err != nil || res.StatusCode != 200 || !bytes.Contains(out, []byte("[DONE]")) {
				t.Fatal("forwarding", err)
			}
			var raw string
			var first, last sql.NullInt64
			var streaming sql.NullBool
			var started, finished int64
			eventually(t, func() bool {
				return db.QueryRow("SELECT event_json,response_streaming,first_output_elapsed_us,last_output_elapsed_us,request_started_at_ms,request_finished_at_ms FROM metering_development.events_raw WHERE x_request_id=? AND event_type='usage_observed'", xid).Scan(&raw, &streaming, &first, &last, &started, &finished) == nil
			})
			var e events.Event
			if err := json.Unmarshal([]byte(raw), &e); err != nil {
				t.Fatal(err)
			}
			if !streaming.Valid || !streaming.Bool || !first.Valid || !last.Valid || first.Int64 < 40000 || last.Int64-first.Int64 < 40000 || (finished-started)*1000-last.Int64 < 80000 {
				t.Fatalf("invalid callback timings: stream=%v first=%v last=%v started=%d finished=%d", streaming, first, last, started, finished)
			}
			if e.ResponseStreaming == nil || !*e.ResponseStreaming || e.FirstOutputElapsedUS == nil || *e.FirstOutputElapsedUS != first.Int64 || e.LastOutputElapsedUS == nil || *e.LastOutputElapsedUS != last.Int64 {
				t.Fatal("typed columns differ from Kafka event")
			}
			var ttft, tpot sql.NullFloat64
			if err := db.QueryRow("SELECT ttft_ms,tpot_ms FROM metering_development.requests WHERE observation_id=?", e.ObservationID).Scan(&ttft, &tpot); err != nil {
				t.Fatal(err)
			}
			if !ttft.Valid || math.Abs(ttft.Float64-float64(first.Int64)/1000) > 1e-6 {
				t.Fatal("TTFT")
			}
			if mode == "performance_missing_usage" {
				if tpot.Valid || e.Usage.OutputTokens.Value != nil {
					t.Fatal("missing usage estimated")
				}
			} else if !tpot.Valid || math.Abs(tpot.Float64-float64(last.Int64-first.Int64)/1000/3) > 1e-6 {
				t.Fatal("TPOT", tpot)
			}
		})
	}
}
