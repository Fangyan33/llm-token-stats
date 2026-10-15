package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/twmb/franz-go/pkg/kgo"
	"os"
	"testing"
	"time"
)

type fakeStore struct {
	fail          string
	calls         []string
	evidence      []Evidence
	notifications []map[string]any
}

func (f *fakeStore) Load(_ context.Context, table, label string, b []byte, rows []map[string]any) error {
	f.calls = append(f.calls, table)
	if table == f.fail {
		return errors.New("injected")
	}
	if table == "summary_dirty_notifications" {
		f.notifications = rows
	}
	return nil
}
func (f *fakeStore) Evidence(_ context.Context, _ []string) ([]Evidence, error) {
	f.calls = append(f.calls, "evidence")
	if f.fail == "evidence" {
		return nil, errors.New("injected")
	}
	return f.evidence, nil
}
func batch(t *testing.T) *Batch {
	b, _ := os.ReadFile("../../testdata/collection/usage-known.json")
	e, err := events.Decode(b, []byte("7f6c2a90-4b13-4b68-9d21-67283e0c5f44"), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	c := config.Consumer{Bytes: 4 << 20, AcceptedConfigs: []string{e.ConfigVersion}, Routes: []config.Route{{RouteName: e.RouteName, ServiceID: e.ServiceID}}}
	result, err := Freeze([]*kgo.Record{{Topic: "llm-metering-events", Partition: 0, Offset: 10, Key: []byte(e.LogicalRequestID), Value: b, Timestamp: time.UnixMilli(e.EventCreatedAtMs)}}, c, time.UnixMilli(e.EventCreatedAtMs))
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func TestCommitBarrier(t *testing.T) {
	b := batch(t)
	for _, fail := range []string{"events_raw", "evidence", "request_conflict_ids", "summary_dirty_notifications", "consumer_batch_receipts", ""} {
		f := &fakeStore{fail: fail, evidence: []Evidence{{ID: b.Events[0].LogicalRequestID, Date: "2026-09-20", EventID: b.Events[0].EventID, Hash: "a", Identity: "x", Started: 1}, {ID: b.Events[0].LogicalRequestID, Date: "2026-09-21", EventID: b.Events[0].EventID, Hash: "b", Identity: "x", Started: 2}}}
		p := Processor{Store: f, Owns: func() bool { return true }}
		err := p.Process(context.Background(), b)
		if (err == nil) != (fail == "") {
			t.Fatal("barrier result", fail)
		}
		if fail != "" && f.calls[len(f.calls)-1] != fail {
			t.Fatal("continued after failure")
		}
		if fail == "" {
			if len(f.notifications) != 3 {
				t.Fatal("missing cross-date notification", len(f.notifications))
			}
		}
	}
}
func TestFrozenLabelAndRevocation(t *testing.T) {
	b := batch(t)
	label := Label("events_raw", b, b.Loads["events_raw"])
	if label != Label("events_raw", b, b.Loads["events_raw"]) {
		t.Fatal("label changed")
	}
	body := append(append([]byte{}, b.Loads["events_raw"]...), ' ')
	if label == Label("events_raw", b, body) {
		t.Fatal("body not hashed")
	}
	f := &fakeStore{}
	p := Processor{Store: f, Owns: func() bool { return false }}
	if p.Process(context.Background(), b) == nil || len(f.calls) > 0 {
		t.Fatal("revoked owner writes")
	}
}

func TestAdmissionIndependentOfDiagnosticRoute(t *testing.T) {
	data, err := os.ReadFile("../../testdata/collection/usage-known.json")
	if err != nil {
		t.Fatal(err)
	}
	var original events.Event
	if err := json.Unmarshal(data, &original); err != nil {
		t.Fatal(err)
	}
	for _, legacy := range []bool{false, true} {
		for _, route := range []string{"unknown", "actual-route.named-match", "*", "llm-chat"} {
			for _, service := range []string{original.ServiceID, "unregistered"} {
				e := original
				e.RouteName, e.ServiceID = route, service
				payload, _ := events.Canonical(&e)
				c := config.Consumer{Bytes: 4 << 20, AcceptedConfigs: []string{e.ConfigVersion}, AcceptedServices: []string{original.ServiceID}}
				if legacy {
					c.AcceptedServices = nil
					c.Routes = []config.Route{{RouteName: "different-rule", ServiceID: original.ServiceID}}
				}
				b, err := Freeze([]*kgo.Record{{Topic: "llm-metering-events", Offset: 1, Key: []byte(e.LogicalRequestID), Value: payload, Timestamp: time.UnixMilli(e.EventCreatedAtMs)}}, c, time.UnixMilli(e.EventCreatedAtMs))
				if err != nil {
					t.Fatal(err)
				}
				if service != original.ServiceID {
					if len(b.Events) != 0 || len(b.Rejected) != 1 || b.Rejected[0]["reason"] != "event_registration" {
						t.Fatal("unregistered service accepted")
					}
				} else if len(b.Events) != 1 || len(b.Rejected) != 0 || b.Events[0].RouteName != route || b.Raw[0]["route_name"] != route || b.Raw[0]["payload_hash"] != events.Hash(payload) {
					t.Fatal("diagnostics blocked ingestion or historical payload changed")
				}
			}
		}
	}
}
