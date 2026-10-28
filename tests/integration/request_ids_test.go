//go:build integration

package integration

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/events"
)

func TestClientRequestIDsRemainSeparateObservations(t *testing.T) {
	db := database(t)
	shared := "Client-Retry:" + uuid(t)
	observations := map[string]bool{}
	for _, tc := range []struct {
		name    string
		headers [][2]string
		want    *string
	}{
		{"priority", [][2]string{{"x-client-request-id", shared}, {"request_id", "ignored"}}, &shared},
		{"retry_fallback", [][2]string{{"request_id", shared}}, &shared},
		{"empty_fallback", [][2]string{{"x-client-request-id", ""}, {"request_id", "Fallback-ID"}}, events.Ptr("Fallback-ID")},
		{"invalid_fallback", [][2]string{{"x-client-request-id", "bad id"}, {"request_id", "Fallback-ID"}}, events.Ptr("Fallback-ID")},
		{"long_fallback", [][2]string{{"x-client-request-id", strings.Repeat("a", 129)}, {"request_id", "Fallback-ID"}}, events.Ptr("Fallback-ID")},
		{"duplicate_fallback", [][2]string{{"x-client-request-id", "first"}, {"x-client-request-id", "second"}, {"request_id", "Fallback-ID"}}, events.Ptr("Fallback-ID")},
		{"missing", nil, nil},
		{"invalid", [][2]string{{"x-client-request-id", "bad id"}, {"request_id", "a,b"}}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			xid := uuid(t)
			req, _ := http.NewRequest("POST", "http://127.0.0.1:18080/v1/chat/completions", bytes.NewBufferString(`{"model":"fixture","stream":false,"messages":[]}`))
			req.Host = "model.example.invalid"
			req.Header.Set("content-type", "application/json")
			req.Header.Set("x-request-id", xid)
			for _, header := range tc.headers {
				req.Header.Add(header[0], header[1])
			}
			res, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != 200 || len(res.Header.Values("x-metering-request-id")) != 0 {
				t.Fatal("business response changed or internal ID exposed")
			}
			var id string
			eventually(t, func() bool {
				return db.QueryRow("SELECT observation_id FROM metering_development.events_raw WHERE x_request_id=? AND event_type='usage_observed'", xid).Scan(&id) == nil
			})
			if !eventsUUID(id) || observations[id] {
				t.Fatal("observation ID was reused")
			}
			observations[id] = true
			rows, err := db.Query("SELECT logical_request_id,x_request_id,event_json FROM metering_development.events_raw WHERE observation_id=?", id)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			count := 0
			for rows.Next() {
				var clientID sql.NullString
				var gatewayID, raw string
				if err := rows.Scan(&clientID, &gatewayID, &raw); err != nil {
					t.Fatal(err)
				}
				var event events.Event
				if err := json.Unmarshal([]byte(raw), &event); err != nil {
					t.Fatal(err)
				}
				if clientID.Valid != (tc.want != nil) || tc.want != nil && clientID.String != *tc.want || gatewayID != xid || event.ObservationID != id || (event.LogicalRequestID == nil) != (tc.want == nil) || tc.want != nil && *event.LogicalRequestID != *tc.want {
					t.Fatal("typed columns and event JSON do not preserve independent IDs")
				}
				if _, err := events.Decode([]byte(raw), []byte(id), time.Now()); err != nil {
					t.Fatal(err)
				}
				count++
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if count != 2 {
				t.Fatalf("event count=%d, want 2", count)
			}
		})
	}
	for _, view := range []string{"requests", "request_statistics", "usage_effective"} {
		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM metering_development."+view+" WHERE logical_request_id=?", shared).Scan(&count); err != nil || count != 2 {
			t.Fatalf("%s merged or rejected independent client retries: count=%d err=%v", view, count, err)
		}
	}
}
