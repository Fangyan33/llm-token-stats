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

// Exercise the single contract, real Doris projections and startup audit.
// Diagnostic route changes must not turn into identity conflicts.
func TestDiagnosticRouteIdentityAndSnapshotConflicts(t *testing.T) {
	s := localStore(t, "http://127.0.0.1:18040")
	cl := kafka(t)
	topic, group := batchingTopic(t, cl)
	for _, mode := range []string{"diagnostic_route", "tenant_conflict", "provider_conflict", "environment_conflict"} {
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
					if mode == "provider_conflict" {
						e.ProviderID = events.Ptr("different-provider")
					}
					if mode == "environment_conflict" {
						e.DeploymentEnvironment = "production"
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
			start := freeze("request-started", "old-route", 0)
			expectedHash := events.IdentityHash(start.Events[0])
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
					if err := s.DB.QueryRowContext(ctx, "SELECT has_conflict FROM metering_development."+view+" WHERE logical_request_id=?", id).Scan(&conflict); err != nil || conflict != want {
						t.Fatalf("%s conflict=%d want=%d: %v", view, conflict, want, err)
					}
				}
				var marks, eligible int
				if err := s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM metering_development.request_conflict_ids WHERE logical_request_id=?", id).Scan(&marks); err != nil || marks != want {
					t.Fatalf("persistent conflict=%d: %v", marks, err)
				}
				if err := s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM metering_development.usage_effective WHERE logical_request_id=?", id).Scan(&eligible); err != nil || eligible != 1-want {
					t.Fatalf("effective usage=%d: %v", eligible, err)
				}
			}
			wantConflict := 0
			if mode != "diagnostic_route" {
				wantConflict = 1
			}
			assertConflict(wantConflict)
			if wantConflict == 0 {
				// Start a real Worker after both events are persisted. Its
				// startup audit must not permanently quarantine this healthy pair.
				worker := startBatchingConsumer(t, topic, group, 5000, 4<<20)
				eventually(t, func() bool { return worker.health.Ready.Load() })
				assertConflict(0)
				worker.stop()
				<-worker.done
			}
			var count int
			var stored string
			if err := s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM metering_development.events_raw WHERE logical_request_id=?", id).Scan(&count); err != nil || count != 2 {
				t.Fatalf("replay changed row count=%d: %v", count, err)
			}
			if err := s.DB.QueryRowContext(ctx, "SELECT identity_hash FROM metering_development.events_raw WHERE logical_request_id=? AND event_type='request_started'", id).Scan(&stored); err != nil || stored != expectedHash {
				t.Fatalf("immutable evidence rewritten: %v", err)
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
