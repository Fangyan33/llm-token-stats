package events

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFixturesGolden(t *testing.T) {
	files, _ := filepath.Glob("../../testdata/collection/*.json")
	if len(files) != 6 {
		t.Fatal(len(files))
	}
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			b, _ := os.ReadFile(f)
			idStart := []byte(`"logical_request_id": "`)
			idx := bytes.Index(b, idStart)
			id := b[idx+len(idStart) : idx+len(idStart)+36]
			e, err := Decode(b, id, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			canonical, err := Canonical(e)
			if err != nil {
				t.Fatal(err)
			}
			want, _ := os.ReadFile(strings.TrimSuffix(f, ".json") + ".canonical")
			if !bytes.Equal(want, canonical) {
				t.Fatal("canonical mismatch")
			}
			h, _ := os.ReadFile(strings.TrimSuffix(f, ".json") + ".hashes")
			if string(h) != Hash(canonical)+"\n"+IdentityHash(e)+"\n" {
				t.Fatal("hash mismatch")
			}
			for _, bad := range [][]byte{bytes.Replace(b, []byte(`"schema_version": 1`), []byte(`"schema_version": 2`), 1), bytes.Replace(b, []byte(`"schema_version": 1`), []byte(`"schema_version": 1, "schema_version": 1`), 1)} {
				if _, err = Decode(bad, id, time.Time{}); err == nil {
					t.Fatal("accepted invalid event")
				}
			}
		})
	}
}

func TestRawUsageRejectsNull(t *testing.T) {
	for _, s := range []string{`{"prompt_tokens":null}`, `{"completion_tokens_details":{"reasoning_tokens":null}}`} {
		if RawValid(s) == nil {
			t.Fatal("raw usage null accepted as number")
		}
	}
}

func TestDiagnosticRouteExcludedFromIdentity(t *testing.T) {
	e := &Event{ServiceID: Ptr("s"), ApiKeyID: Ptr("key"), CustomerID: Ptr("customer"), IdentityStatus: "known", ConfigVersion: "v1", GatewayInstanceID: "gateway", AdapterID: "chat_completions_v1", XRequestID: Ptr("x"), RouteName: "old-route"}
	identity := IdentityHash(e)
	before, _ := Canonical(e)
	for _, route := range []string{"unknown", "new.actual-route", "*"} {
		e.RouteName = route
		after, _ := Canonical(e)
		if IdentityHash(e) != identity || Hash(before) == Hash(after) {
			t.Fatal("route must affect payload evidence only")
		}
	}
	for _, change := range []func(*Event){
		func(e *Event) { e.ServiceID = Ptr("other") },
		func(e *Event) { e.ApiKeyID = Ptr("other") },
		func(e *Event) { e.CustomerID = Ptr("other") },
		func(e *Event) { e.ProviderID = Ptr("other") },
		func(e *Event) { e.IdentityStatus = "invalid" },
		func(e *Event) { e.ConfigVersion = "v2" },
		func(e *Event) { e.GatewayInstanceID = "other" },
		func(e *Event) { e.AdapterID = "other" },
		func(e *Event) { e.XRequestID = nil },
	} {
		copy := *e
		change(&copy)
		if IdentityHash(&copy) == identity {
			t.Fatal("stable identity field ignored")
		}
	}
}

func TestMappingInvariantsAndSecretRejection(t *testing.T) {
	data, err := os.ReadFile("../../testdata/collection/usage-known.json")
	if err != nil {
		t.Fatal(err)
	}
	base, err := Decode(data, []byte("7f6c2a90-4b13-4b68-9d21-67283e0c5f44"), time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	unknown := *base
	unknown.ServiceID, unknown.ProviderID, unknown.MappingVersion = nil, nil, nil
	unknown.ServiceName, unknown.ProviderName = nil, nil
	unknown.MappingStatus = "lookup_not_implemented"
	b, _ := Canonical(&unknown)
	if _, err = Decode(b, []byte(unknown.LogicalRequestID), time.Time{}); err != nil {
		t.Fatal("known customer/unknown service rejected", err)
	}
	for _, change := range []func(*Event){
		func(e *Event) { e.ServiceName = nil }, func(e *Event) { e.ProviderName = Ptr("") },
		func(e *Event) { e.CustomerName = Ptr(strings.Repeat("中", 171)) },
		func(e *Event) { e.ProviderID = nil }, func(e *Event) { e.MappingStatus = "lookup_not_implemented" },
		func(e *Event) { e.RequestHost = Ptr("EXAMPLE.invalid") }, func(e *Event) { e.RequestHost = Ptr("127.0.0.1") },
		func(e *Event) { e.CustomerID = nil }, func(e *Event) { e.CustomerMappingStatus = "lookup_not_implemented" },
		func(e *Event) { e.MappingVersion = Ptr("not-a-sha256") }, func(e *Event) { e.DeploymentEnvironment = "staging" },
	} {
		e := *base
		change(&e)
		b, _ := Canonical(&e)
		if _, err = Decode(b, []byte(e.LogicalRequestID), time.Time{}); err == nil {
			t.Fatal("inconsistent mapping accepted")
		}
	}
	for _, field := range []string{"api_key", "application_id"} {
		injected := append([]byte(`{"`+field+`":"must-not-persist",`), data[1:]...)
		if _, err = Decode(injected, []byte(base.LogicalRequestID), time.Time{}); err == nil {
			t.Fatal("removed or sensitive event field accepted")
		}
	}
}
