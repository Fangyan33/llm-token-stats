//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestBearerPrivacyAndInvalidInputs(t *testing.T) {
	db := database(t)
	token := "synthetic-" + uuid(t)
	type expected struct{ id, host, serviceStatus, customerStatus string }
	var requests []expected
	for _, mode := range []string{"valid", "duplicate", "joined", "missing", "other_scheme"} {
		id := uuid(t)
		req, _ := http.NewRequest("POST", "http://127.0.0.1:18080/v1/chat/completions", strings.NewReader(`{"model":"fixture","stream":false}`))
		req.Host = "MODEL.Example.invalid:443"
		req.Header.Set("content-type", "application/json")
		req.Header.Set("x-request-id", id)
		req.Header.Set("customer_id", "forged-customer")
		req.Header.Set("x-api-key", token)
		customerStatus, serviceStatus, host := "lookup_not_implemented", "lookup_not_implemented", "model.example.invalid"
		switch mode {
		case "duplicate":
			req.Header.Add("Authorization", "Bearer "+token)
			req.Header.Add("Authorization", "Bearer "+token)
			customerStatus = "invalid_api_key"
		case "joined":
			req.Header.Set("Authorization", "Bearer "+token+",Bearer "+token)
			customerStatus = "invalid_api_key"
		case "missing":
			customerStatus = "invalid_api_key"
		case "other_scheme":
			req.Header.Set("Authorization", "Basic "+token)
			customerStatus = "invalid_api_key"
		default:
			req.Header.Set("Authorization", "Bearer "+token)
		}
		res, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
		if err != nil {
			t.Fatal("business request failed")
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if res.StatusCode != 200 {
			t.Fatal("identity collection changed business response")
		}
		requests = append(requests, expected{id, host, serviceStatus, customerStatus})
	}
	for _, want := range requests {
		var count int
		eventually(t, func() bool {
			return db.QueryRow("SELECT COUNT(*) FROM metering_development.events_raw WHERE x_request_id=?", want.id).Scan(&count) == nil && count == 2
		})
		rows, err := db.Query("SELECT event_json,COALESCE(request_host,'NULL') FROM metering_development.events_raw WHERE x_request_id=?", want.id)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var raw, host string
			if rows.Scan(&raw, &host) != nil {
				t.Fatal("read event")
			}
			var e events.Event
			if json.Unmarshal([]byte(raw), &e) != nil || host != want.host || e.MappingStatus != want.serviceStatus || e.CustomerMappingStatus != want.customerStatus || e.IdentityStatus != "invalid" || e.UsageEligible || e.ApiKeyID != nil || e.CustomerID != nil || e.ServiceID != nil || e.ProviderID != nil {
				t.Fatal("invalid or fabricated attribution")
			}
			if strings.Contains(raw, token) || strings.Contains(raw, `"api_key"`) {
				t.Fatal("credential escaped the observation channel")
			}
		}
		if rows.Err() != nil {
			t.Fatal(rows.Err())
		}
		rows.Close()
	}
	// Read independently without joining or committing the ingestion group.
	partitions := map[int32]kgo.Offset{}
	for partition := int32(0); partition < 24; partition++ {
		partitions[partition] = kgo.NewOffset().AtStart()
	}
	reader, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:29092"),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{"llm-metering-development": partitions}))
	if err != nil {
		t.Fatal("independent Kafka reader")
	}
	defer reader.Close()
	wanted, seen := map[string]bool{}, map[string]bool{}
	for _, request := range requests {
		wanted[request.id] = true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for len(seen) < 2*len(requests) && ctx.Err() == nil {
		fetch := reader.PollRecords(ctx, 1000)
		fetch.EachRecord(func(record *kgo.Record) {
			if bytes.Contains(record.Key, []byte(token)) || bytes.Contains(record.Value, []byte(token)) {
				t.Fatal("credential persisted in Kafka")
			}
			var event events.Event
			if json.Unmarshal(record.Value, &event) == nil && event.XRequestID != nil && wanted[*event.XRequestID] {
				seen[event.EventID] = true
			}
		})
	}
	if len(seen) != 2*len(requests) {
		t.Fatal("Kafka privacy check did not cover every request event")
	}
	logs, err := exec.Command("docker", "compose", "-f", "../../deploy/local/compose.yaml", "logs",
		"--no-color", "observer", "consumer", "gateway", "fixture").Output()
	if err != nil || bytes.Contains(logs, []byte(token)) {
		t.Fatal("component log privacy check failed")
	}
}

func TestWasmHostAllowlist(t *testing.T) {
	db := database(t)
	var ignored []string
	for _, tc := range []struct {
		host             string
		accepted, stream bool
	}{
		{"MODEL.Example.invalid.:443", true, false},
		{"a.b.example.invalid", true, false},
		{"example.invalid", false, true},
		{"badexample.invalid", false, false},
		{"a.example.invalid.evil", false, true},
		{"127.0.0.1", false, false},
		{"bad_host", false, false},
		{"model.example.invalid", true, false}, // Confirm collection still works after ignored hosts.
	} {
		id := uuid(t)
		before := gatewaySelectedRequests(t)
		body := []byte(`{"model":"fixture","stream":false}`)
		wantStatus := http.StatusOK
		if tc.stream {
			body = []byte(`{"model":"fixture","stream":true}`)
			// The fixture requires include_usage for SSE. An ignored request must
			// preserve its original 422 response instead of being rewritten to 200.
			wantStatus = http.StatusUnprocessableEntity
		}
		req, _ := http.NewRequest("POST", "http://127.0.0.1:18080/v1/chat/completions", bytes.NewReader(body))
		req.Host = tc.host
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("x-request-id", id)
		req.Header.Set("Authorization", "Bearer synthetic-host-filter")
		req.Header.Set("x-forwarded-host", "model.example.invalid")
		req.Header.Set("Forwarded", "host=model.example.invalid")
		res, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
		if err != nil {
			t.Fatal("business request failed", err)
		}
		_, readErr := io.Copy(io.Discard, res.Body)
		res.Body.Close()
		if readErr != nil || res.StatusCode != wantStatus || res.Header.Get("x-test-body-sha256") != events.Hash(body) {
			t.Fatalf("Host %q changed business response or body: status=%d want=%d err=%v", tc.host, res.StatusCode, wantStatus, readErr)
		}
		after := gatewaySelectedRequests(t)
		if !tc.accepted {
			if after != before {
				t.Fatalf("rejected Host %q selected observation", tc.host)
			}
			ignored = append(ignored, id)
			continue
		}
		if after != before+1 {
			t.Fatalf("matching Host %q was not selected", tc.host)
		}
		eventually(t, func() bool {
			var n int
			return db.QueryRow("SELECT COUNT(*) FROM metering_development.events_raw WHERE x_request_id=?", id).Scan(&n) == nil && n == 2
		})
		var host string
		if err := db.QueryRow("SELECT request_host FROM metering_development.events_raw WHERE x_request_id=? LIMIT 1", id).Scan(&host); err != nil || (tc.host == "a.b.example.invalid" && host != tc.host) || (tc.host != "a.b.example.invalid" && host != "model.example.invalid") {
			t.Fatalf("canonical Host %q: %q %v", tc.host, host, err)
		}
	}
	for _, id := range ignored {
		var n int
		if err := db.QueryRow("SELECT COUNT(*) FROM metering_development.events_raw WHERE x_request_id=?", id).Scan(&n); err != nil || n != 0 {
			t.Fatalf("ignored Host produced %d events: %v", n, err)
		}
	}
}

// This exercises reference SQL with synthetic Kafka identities; it does not
// enable an application resolver, a publication Worker or a customer query API.
func TestSummarySQLWithSyntheticMappings(t *testing.T) {
	db, cl := database(t), kafka(t)
	customer := "summary-" + uuid(t)
	ids := []string{}
	for _, mode := range []string{"mapped", "unknown_service", "unknown_customer", "unknown_usage", "reported_zero"} {
		name := "usage-known"
		if mode == "unknown_usage" {
			name = "usage-unknown"
		}
		if mode == "reported_zero" {
			name = "usage-zero"
		}
		data, err := os.ReadFile(filepath.Join("../../testdata/collection", name+".json"))
		if err != nil {
			t.Fatal(err)
		}
		var e events.Event
		if json.Unmarshal(data, &e) != nil {
			t.Fatal("fixture")
		}
		id := uuid(t)
		ids = append(ids, id)
		now := time.Now().UnixMilli()
		e.LogicalRequestID, e.ObservationID, e.EventID = id, id, id+":usage"
		e.CustomerID = events.Ptr(customer)
		e.RequestStartedAtMs = now
		e.EventCreatedAtMs = now
		if e.TerminalObserved {
			e.RequestFinishedAtMs = events.Ptr(now)
		}
		e.ObservationEndedAtMs = events.Ptr(now)
		e.RequestModel, e.ResponseModel = events.Ptr(mode), events.Ptr(mode)
		if mode == "unknown_service" {
			e.ServiceID, e.ProviderID, e.MappingVersion = nil, nil, nil
			e.ServiceName, e.ProviderName = nil, nil
			e.MappingStatus = "lookup_not_implemented"
		}
		if mode == "unknown_customer" {
			e.CustomerID, e.ApiKeyID, e.CustomerMappingVersion = nil, nil, nil
			e.CustomerName = nil
			e.CustomerMappingStatus = "lookup_not_implemented"
			e.IdentityStatus = "invalid"
			e.UsageEligible = false
			e.AnomalyCodes = events.Add(e.AnomalyCodes, "identity_invalid")
		}
		data, _ = events.Canonical(&e)
		if _, err = events.Decode(data, []byte(id), time.Now()); err != nil {
			t.Fatal("synthetic event", err)
		}
		publish(t, cl, "llm-metering-development", 0, id, data)
	}
	where := "'" + strings.Join(ids, "','") + "'"
	eventually(t, func() bool {
		var n int
		return db.QueryRow("SELECT COUNT(*) FROM metering_development.events_raw WHERE logical_request_id IN ("+where+")").Scan(&n) == nil && n == len(ids)
	})
	now := time.Now().UTC()
	month := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	token := strings.ReplaceAll(uuid(t), "-", "")
	values := strings.NewReplacer("metering.", "metering_development.", "{{yyyymmdd}}", now.Format("20060102"), "{{stat_date}}", now.Format("2006-01-02"), "{{next_date}}", now.AddDate(0, 0, 1).Format("2006-01-02"), "{{yyyymm}}", now.Format("200601"), "{{month_start}}", month.Format("2006-01-02"), "{{next_month}}", month.AddDate(0, 1, 0).Format("2006-01-02"), "{{refresh_token}}", token, "{{refresh_id}}", uuid(t), "{{source_watermark}}", events.Hash([]byte(token)))
	for _, name := range []string{"refresh_usage_daily.sql", "publish_usage_daily.sql", "refresh_usage_monthly.sql", "publish_usage_monthly.sql"} {
		data, err := os.ReadFile(filepath.Join("../../scripts/sql", name))
		if err != nil {
			t.Fatal(err)
		}
		// Strip comments before splitting the fixed reference statements.
		var lines []string
		for _, line := range strings.Split(values.Replace(string(data)), "\n") {
			if !strings.HasPrefix(strings.TrimSpace(line), "--") {
				lines = append(lines, line)
			}
		}
		for _, statement := range strings.Split(strings.Join(lines, "\n"), ";") {
			if strings.TrimSpace(statement) != "" {
				if _, err = db.Exec(statement); err != nil {
					t.Fatal(name, err)
				}
			}
		}
	}
	for _, table := range []string{"usage_daily", "usage_monthly"} {
		var count, unknownService, unknownCustomer int
		if db.QueryRow("SELECT COUNT(*) FROM metering_development."+table+" WHERE customer_id=?", customer).Scan(&count) != nil || count != 4 {
			t.Fatal("customer summary count", table, count)
		}
		if db.QueryRow("SELECT COUNT(*) FROM metering_development."+table+" WHERE customer_id=? AND provider_id='' AND service_id=''", customer).Scan(&unknownService) != nil || unknownService != 1 {
			t.Fatal("unknown service bucket", table)
		}
		if db.QueryRow("SELECT COUNT(*) FROM metering_development."+table+" WHERE customer_id=? AND model='unknown_customer'", customer).Scan(&unknownCustomer) != nil || unknownCustomer != 0 {
			t.Fatal("unknown customer attributed", table)
		}
		var raw []byte
		if db.QueryRow("SELECT CAST(input_tokens AS STRING) FROM metering_development."+table+" WHERE customer_id=? AND model='unknown_usage'", customer).Scan(&raw) != nil || raw != nil {
			t.Fatal("unknown usage became zero", table)
		}
		if db.QueryRow("SELECT CAST(input_tokens AS STRING) FROM metering_development."+table+" WHERE customer_id=? AND model='reported_zero'", customer).Scan(&raw) != nil || !bytes.Equal(raw, []byte("0")) {
			t.Fatal("reported zero lost", table)
		}
	}
}
