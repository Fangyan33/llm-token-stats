package consumer

import (
	"bytes"
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
	c := config.Consumer{Bytes: 4 << 20, AcceptedConfigs: []string{e.ConfigVersion}}
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

func TestNamesConflictWithoutChangingIdentity(t *testing.T) {
	b := batch(t)
	id := b.Events[0].LogicalRequestID
	f := &fakeStore{evidence: []Evidence{{ID: id, Date: "2026-10-19", Started: 1, EventID: id + ":start", Hash: "a", Identity: "same", Names: "old"}, {ID: id, Date: "2026-10-19", Started: 1, EventID: id + ":usage", Hash: "b", Identity: "same", Names: "new"}}}
	p := Processor{Store: f, Owns: func() bool { return true }}
	if err := p.Process(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, n := range f.notifications {
		found = found || n["reason"] == "CONFLICT"
	}
	if !found {
		t.Fatal("name-only snapshot mismatch was not isolated")
	}
	e := *b.Events[0]
	identity := events.IdentityHash(&e)
	before, _ := events.Canonical(&e)
	e.ServiceName = events.Ptr("重命名服务")
	after, _ := events.Canonical(&e)
	if events.IdentityHash(&e) != identity || events.Hash(before) == events.Hash(after) {
		t.Fatal("name must change payload only")
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
	for _, route := range []string{"unknown", "actual-route.named-match", "*", "llm-chat"} {
		for _, service := range []string{*original.ServiceID, "unregistered"} {
			e := original
			e.RouteName, e.ServiceID = route, events.Ptr(service)
			payload, _ := events.Canonical(&e)
			c := config.Consumer{Bytes: 4 << 20, AcceptedConfigs: []string{e.ConfigVersion}}

			b, err := Freeze([]*kgo.Record{{Topic: "llm-metering-events", Offset: 1, Key: []byte(e.LogicalRequestID), Value: payload, Timestamp: time.UnixMilli(e.EventCreatedAtMs)}}, c, time.UnixMilli(e.EventCreatedAtMs))
			if err != nil {
				t.Fatal(err)
			}
			if len(b.Events) != 1 || len(b.Rejected) != 0 || b.Events[0].RouteName != route || *b.Events[0].ServiceID != service || b.Raw[0]["route_name"] != route || b.Raw[0]["payload_hash"] != events.Hash(payload) {
				t.Fatal("diagnostics blocked ingestion or historical payload changed")
			}
		}
	}
}

func TestUnknownServiceAndSensitiveRejections(t *testing.T) {
	data, err := os.ReadFile("../../testdata/collection/usage-known.json")
	if err != nil {
		t.Fatal(err)
	}
	var e events.Event
	if json.Unmarshal(data, &e) != nil {
		t.Fatal("fixture")
	}
	e.ServiceID, e.ProviderID, e.MappingVersion = nil, nil, nil
	e.ServiceName, e.ProviderName = nil, nil
	e.MappingStatus = "lookup_not_implemented"
	payload, _ := events.Canonical(&e)
	cfg := config.Consumer{Bytes: 4 << 20, AcceptedConfigs: []string{e.ConfigVersion}}
	now := time.UnixMilli(e.EventCreatedAtMs)
	record := &kgo.Record{Topic: "test", Offset: 1, Key: []byte(e.LogicalRequestID), Value: payload, Timestamp: now}
	b, err := Freeze([]*kgo.Record{record}, cfg, now)
	if err != nil || len(b.Events) != 1 || len(b.Rejected) != 0 || !b.Events[0].UsageEligible {
		t.Fatal("unknown service blocked known customer usage")
	}
	for _, field := range []string{"api_key", "application_id"} {
		var obj map[string]any
		json.Unmarshal(payload, &obj)
		obj[field] = "synthetic-sensitive-sentinel"
		record.Value, _ = json.Marshal(obj)
		b, err = Freeze([]*kgo.Record{record}, cfg, now)
		if err != nil || len(b.Events) != 0 || len(b.Rejected) != 1 {
			t.Fatal("sensitive or removed field accepted")
		}
		for _, load := range b.Loads {
			if bytes.Contains(load, []byte("synthetic-sensitive-sentinel")) {
				t.Fatal("rejection retained sensitive input")
			}
		}
	}
}

func TestLegacyNullHostAndConfigRegistration(t *testing.T) {
	b := batch(t)
	e := *b.Events[0]
	e.RequestHost, e.ServiceID, e.ProviderID, e.MappingVersion = nil, nil, nil, nil
	e.ServiceName, e.ProviderName = nil, nil
	e.MappingStatus = "invalid_host"
	payload, _ := events.Canonical(&e)
	now := time.UnixMilli(e.EventCreatedAtMs)
	r := &kgo.Record{Topic: "legacy-host", Offset: 1, Key: []byte(e.LogicalRequestID), Value: payload, Timestamp: now}
	for _, version := range []string{e.ConfigVersion, "other-version"} {
		cfg := config.Consumer{Bytes: 4 << 20, AcceptedConfigs: []string{version}}
		got, err := Freeze([]*kgo.Record{r}, cfg, now)
		if err != nil {
			t.Fatal(err)
		}
		if version == e.ConfigVersion {
			if len(got.Events) != 1 || got.Events[0].RequestHost != nil {
				t.Fatal("legacy null Host rejected")
			}
		} else if len(got.Events) != 0 || len(got.Rejected) != 1 || got.Rejected[0]["reason"] != "event_registration" {
			t.Fatal("configuration registration bypassed")
		}
	}
}
