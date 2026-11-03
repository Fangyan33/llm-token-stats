//go:build integration

package integration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/Fangyan33/llm-token-stats/internal/summaryworker"
)

func TestRequestHeadersThroughGatewayToDoris(t *testing.T) {
	db := database(t)
	raw, err := os.ReadFile("../../dist/envoy.remote.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, names := range [][]string{{"x-primary-user", "user-id"}, {"x-new-user"}, {}} {
		var cfg map[string]any
		if json.Unmarshal(raw, &cfg) != nil {
			t.Fatal("bootstrap")
		}
		res := cfg["static_resources"].(map[string]any)
		hcm := res["listeners"].([]any)[0].(map[string]any)["filter_chains"].([]any)[0].(map[string]any)["filters"].([]any)[0].(map[string]any)["typed_config"].(map[string]any)
		plugin := hcm["http_filters"].([]any)[1].(map[string]any)["typed_config"].(map[string]any)["config"].(map[string]any)
		plugin["vm_config"].(map[string]any)["code"] = map[string]any{"local": map[string]any{"filename": "/wasm/observer.wasm"}}
		var settings map[string]any
		configuration := plugin["configuration"].(map[string]any)
		if json.Unmarshal([]byte(configuration["value"].(string)), &settings) != nil {
			t.Fatal("settings")
		}
		settings["user_id_header"] = names
		settings["extra_request_headers"] = []string{"X-Team-ID", "x-repeat", "Content-Length"}
		encoded, _ := json.Marshal(settings)
		configuration["value"] = string(encoded)
		base, _, stop := startEnvoy(t, cfg, true)
		for _, tc := range []struct{ name, primary, user string }{{"priority", "first", "first"}, {"fallback", "", "fallback"}, {"long", strings.Repeat("x", 513), "fallback"}, {"duplicate", "duplicate", "fallback"}} {
			xid := uuid(t)
			req, _ := http.NewRequest("POST", base+"/v1/chat/completions", bytes.NewBufferString(`{"model":"fixture","stream":true,"messages":[]}`))
			originalLength := strconv.FormatInt(req.ContentLength, 10)
			req.Host = "model.example.invalid"
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("x-request-id", xid)
			req.Header.Set("User-Agent", "synthetic-sdk/1.0")
			req.Header.Set("x-primary-user", tc.primary)
			req.Header.Set("user-id", "fallback")
			req.Header.Set("x-new-user", "new-user")
			req.Header.Set("x-team-id", "team-a")
			req.Header.Add("x-repeat", "one")
			req.Header.Add("x-repeat", "two")
			req.Header.Set("x-not-selected", "ignored")
			if tc.name == "duplicate" {
				req.Header.Add("x-primary-user", "other")
			}
			want := tc.user
			if len(names) == 1 {
				want = "new-user"
			}
			if len(names) == 0 {
				want = ""
			}
			response, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(io.Discard, response.Body)
			response.Body.Close()
			if response.StatusCode != 200 {
				t.Fatal("business forwarding failed", response.StatusCode)
			}
			var id string
			eventually(t, func() bool {
				return db.QueryRow("SELECT observation_id FROM metering_development.events_raw WHERE x_request_id=? AND event_type='usage_observed'", xid).Scan(&id) == nil
			})
			rows, err := db.Query("SELECT user_id,user_agent,request_headers['x-team-id'],request_headers['x-repeat'],request_headers['content-length'],SIZE(request_headers),event_json FROM metering_development.events_raw WHERE observation_id=?", id)
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for rows.Next() {
				var uid sql.NullString
				var agent, team, repeat, contentLength, eventJSON string
				var size int
				if err := rows.Scan(&uid, &agent, &team, &repeat, &contentLength, &size, &eventJSON); err != nil {
					rows.Close()
					t.Fatal(err)
				}
				if uid.Valid != (want != "") || uid.String != want || agent != "synthetic-sdk/1.0" || team != "team-a" || repeat != "one,two" || contentLength != originalLength || size != 3 {
					rows.Close()
					t.Fatal("typed snapshot mismatch")
				}
				e, err := events.Decode([]byte(eventJSON), []byte(id), time.Now())
				if err != nil || e.UserAgent == nil || *e.UserAgent != agent || e.RequestHeaders["content-length"] != originalLength {
					rows.Close()
					t.Fatal("event contract mismatch", err)
				}
				count++
			}
			err = rows.Err()
			rows.Close()
			if err != nil || count != 2 {
				t.Fatal("start/usage missing", err, count)
			}
			for _, view := range []string{"requests", "request_statistics", "event_details", "usage_effective"} {
				var count int
				if err := db.QueryRow("SELECT COUNT(*) FROM metering_development."+view+" WHERE observation_id=? AND user_agent='synthetic-sdk/1.0' AND request_headers['x-team-id']='team-a'", id).Scan(&count); err != nil || count == 0 {
					t.Fatal("metadata view", view, count, err)
				}
			}
		}
		stop()
	}
}

func TestUserDailyMonthlySummaries(t *testing.T) {
	db := database(t)
	cl := kafka(t)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	today := time.Now().UTC().Truncate(24 * time.Hour)
	yesterday := today.AddDate(0, 0, -1)
	date := yesterday.Format("2006-01-02")
	if _, err := db.Exec(`ALTER TABLE metering_development.events_raw SET ("dynamic_partition.enable"="false")`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := db.Exec(`ALTER TABLE metering_development.events_raw SET ("dynamic_partition.enable"="true")`); err != nil {
			t.Error(err)
		}
	}()
	if _, err := db.Exec("ALTER TABLE metering_development.events_raw ADD PARTITION IF NOT EXISTS p" + yesterday.Format("20060102") + " VALUES [('" + date + "'),('" + today.Format("2006-01-02") + "'))"); err != nil {
		t.Fatal(err)
	}
	cluster := "user-summary-" + uuid(t)
	load := func(uid *string) events.Event {
		t.Helper()
		raw, err := os.ReadFile("../../testdata/collection/usage-known.json")
		if err != nil {
			t.Fatal(err)
		}
		var e events.Event
		if json.Unmarshal(raw, &e) != nil {
			t.Fatal("fixture")
		}
		e.ObservationID = uuid(t)
		e.EventID = e.ObservationID + ":usage"
		e.ClusterName = cluster
		e.UserID = uid
		delta := yesterday.Add(time.Hour).UnixMilli() - e.RequestStartedAtMs
		e.RequestStartedAtMs += delta
		e.EventCreatedAtMs = time.Now().UnixMilli()
		*e.RequestFinishedAtMs += delta
		*e.ObservationEndedAtMs += delta
		return e
	}
	send := func(e events.Event) {
		t.Helper()
		raw, err := events.Canonical(&e)
		if err != nil {
			t.Fatal(err)
		}
		offset := publish(t, cl, "llm-metering-development", 0, e.ObservationID, raw)
		eventually(t, func() bool {
			var count int
			return db.QueryRow("SELECT COUNT(*) FROM metering_development.events_raw WHERE observation_id=? AND payload_hash=?", e.ObservationID, events.Hash(raw)).Scan(&count) == nil && count == 1
		})
		// A visible raw row precedes the notification/receipt barrier. Wait for the
		// receipt before asking the summary worker to capture its refresh inputs.
		eventually(t, func() bool {
			var count int
			return db.QueryRow("SELECT COUNT(*) FROM metering_development.consumer_batch_receipts WHERE kafka_topic='llm-metering-development' AND kafka_partition=0 AND next_offset>?", offset).Scan(&count) == nil && count > 0
		})
	}
	a := load(events.Ptr("user-a"))
	send(a)
	send(load(events.Ptr("user-a")))
	send(load(events.Ptr("user-b")))
	send(load(nil))
	send(a)
	guard, err := summaryworker.LocalGuard(filepath.Join(t.TempDir(), "summary.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer guard.Close()
	worker := summaryworker.Worker{Store: &summaryworker.Store{DB: db, Database: "metering_development"}, Guard: guard, Config: summaryworker.Settings{CollectionStartDate: date, RefreshTimeoutSeconds: 60, PollIntervalSeconds: 1}}
	assert := func(a, b, unknown int64) {
		t.Helper()
		if err := worker.Run(ctx, "", ""); err != nil {
			t.Fatal("refresh", err)
		}
		for _, view := range []string{"usage_daily_mv", "usage_monthly_mv"} {
			rows, err := db.Query("SELECT user_id,SUM(request_count),SUM(total_tokens) FROM metering_development."+view+" WHERE cluster_name=? GROUP BY user_id", cluster)
			if err != nil {
				t.Fatal(err)
			}
			got := map[string]int64{}
			var total int64
			for rows.Next() {
				var uid sql.NullString
				var count, tokens int64
				if err := rows.Scan(&uid, &count, &tokens); err != nil {
					rows.Close()
					t.Fatal(err)
				}
				got[uid.String] = count
				total += count
				if tokens != 1500*count {
					rows.Close()
					t.Fatal("token totals", view, tokens, count)
				}
			}
			err = rows.Err()
			rows.Close()
			if err != nil || got["user-a"] != a || got["user-b"] != b || got[""] != unknown || total != a+b+unknown {
				t.Fatal("user grouping", view, got, err)
			}
		}
	}
	assert(2, 1, 1)
	send(load(events.Ptr("user-b")))
	assert(2, 2, 1)
	conflict := a
	conflict.UserID = events.Ptr("user-b")
	send(conflict)
	eventually(t, func() bool {
		var count int
		return db.QueryRow("SELECT COUNT(*) FROM metering_development.request_conflict_ids WHERE observation_id=?", a.ObservationID).Scan(&count) == nil && count == 1
	})
	assert(1, 2, 1)
}
