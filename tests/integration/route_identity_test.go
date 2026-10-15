//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/consumer"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Mix persisted legacy hashes with current writes, using the real Doris views
// and Consumer evidence query. Payload conflict detection must stay strict.
func TestDiagnosticRouteIdentityCompatibility(t *testing.T) {
	s := localStore(t, "http://127.0.0.1:18040")
	cl := kafka(t)
	topic, group := batchingTopic(t, cl)
	for _, mode := range []string{"legacy_route", "legacy_wildcard", "tenant_conflict"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Now()
			id := uuid(t)
			c := config.Consumer{Bytes: 4 << 20, AcceptedConfigs: []string{"collection-v1"}, AcceptedServices: []string{"model-service-a"}}
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			p := &consumer.Processor{Store: s, Owns: func() bool { return true }}
			freeze := func(name, route string, offset int64) *consumer.Batch {
				t.Helper()
				data, err := os.ReadFile("../../testdata/collection/" + name + ".json")
				if err != nil {
					t.Fatal(err)
				}
				var e events.Event
				if err := json.Unmarshal(data, &e); err != nil {
					t.Fatal(err)
				}
				e.LogicalRequestID, e.ObservationID, e.XRequestID = id, id, &id
				e.EventID = id + ":start"
				if e.EventType == "usage_observed" {
					e.EventID = id + ":usage"
					e.RequestFinishedAtMs, e.ObservationEndedAtMs = events.Ptr(now.UnixMilli()), events.Ptr(now.UnixMilli())
					if mode == "tenant_conflict" {
						e.CustomerID = events.Ptr("different-customer")
					}
				}
				e.RouteName, e.RequestStartedAtMs, e.EventCreatedAtMs = route, now.UnixMilli(), now.UnixMilli()
				data, _ = events.Canonical(&e)
				b, err := consumer.Freeze([]*kgo.Record{{Topic: "test-route-identity-" + id, Offset: offset, Key: []byte(id), Value: data, Timestamp: now}}, c, now)
				if err != nil || len(b.Events) != 1 {
					t.Fatalf("freeze: %v", err)
				}
				return b
			}
			oldRoute := "old-route"
			if mode == "legacy_wildcard" {
				oldRoute = "*"
			}
			start := freeze("request-started", oldRoute, 0)
			e := start.Events[0]
			legacyHash := events.Tuple([]any{e.ServiceID, e.ApiKeyID, e.CustomerID, e.ApplicationID, e.IdentityStatus, e.ConfigVersion, e.GatewayInstanceID, e.RouteName, e.AdapterID, e.XRequestID})
			start.Raw[0]["identity_hash"] = legacyHash
			start.Loads["events_raw"] = consumer.EncodeRows(start.Raw)
			if err := p.Process(ctx, start); err != nil {
				t.Fatal(err)
			}
			usage := freeze("usage-known", "unknown", 1)
			if err := p.Process(ctx, usage); err != nil {
				t.Fatal(err)
			}
			if err := p.Process(ctx, usage); err != nil {
				t.Fatal("replay:", err)
			}
			assertConflict := func(want int) {
				t.Helper()
				for _, view := range []string{"requests", "request_statistics"} {
					var conflict int
					if err := s.DB.QueryRowContext(ctx, "SELECT has_conflict FROM metering."+view+" WHERE logical_request_id=?", id).Scan(&conflict); err != nil || conflict != want {
						t.Fatalf("%s conflict=%d want=%d: %v", view, conflict, want, err)
					}
				}
				var marks, eligible int
				if err := s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM metering.request_conflict_ids WHERE logical_request_id=?", id).Scan(&marks); err != nil || marks != want {
					t.Fatalf("persistent conflict=%d: %v", marks, err)
				}
				if err := s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM metering.usage_effective WHERE logical_request_id=?", id).Scan(&eligible); err != nil || eligible != 1-want {
					t.Fatalf("effective usage=%d: %v", eligible, err)
				}
			}
			wantConflict := 0
			if mode == "tenant_conflict" {
				wantConflict = 1
			}
			assertConflict(wantConflict)
			if wantConflict == 0 {
				// Start a real Worker after both hash versions are persisted. Its
				// startup audit must not permanently quarantine this healthy pair.
				worker := startBatchingConsumer(t, topic, group, 5000, 4<<20)
				eventually(t, func() bool { return worker.health.Ready.Load() })
				assertConflict(0)
				worker.stop()
				<-worker.done
			}
			var count int
			var stored string
			if err := s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM metering.events_raw WHERE logical_request_id=?", id).Scan(&count); err != nil || count != 2 {
				t.Fatalf("replay changed row count=%d: %v", count, err)
			}
			if err := s.DB.QueryRowContext(ctx, "SELECT identity_hash FROM metering.events_raw WHERE logical_request_id=? AND event_type='request_started'", id).Scan(&stored); err != nil || stored != legacyHash {
				t.Fatalf("legacy evidence rewritten: %v", err)
			}
			// Even a diagnostic-only change is a different immutable event payload.
			variant := freeze("usage-known", "different.actual-route", 2)
			if err := p.Process(ctx, variant); err != nil {
				t.Fatal(err)
			}
			assertConflict(1)
		})
	}
}
