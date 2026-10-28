//go:build integration

package summaryworker

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/consumer"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/Fangyan33/llm-token-stats/pkg/statwindow"
	"github.com/twmb/franz-go/pkg/kgo"
)

func statements(s string) []string {
	var out []string
	var b strings.Builder
	quote := byte(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			b.WriteByte(c)
			if c == '\\' && i+1 < len(s) {
				i++
				b.WriteByte(s[i])
			} else if c == quote {
				if i+1 < len(s) && s[i+1] == quote {
					i++
					b.WriteByte(s[i])
				} else {
					quote = 0
				}
			}
			continue
		}
		if c == '-' && i+1 < len(s) && s[i+1] == '-' {
			for i < len(s) && s[i] != '\n' {
				i++
			}
			b.WriteByte('\n')
			continue
		}
		if c == '\'' || c == '"' || c == '`' {
			quote = c
		}
		if c == ';' {
			if v := strings.TrimSpace(b.String()); v != "" {
				out = append(out, v)
			}
			b.Reset()
		} else {
			b.WriteByte(c)
		}
	}
	if v := strings.TrimSpace(b.String()); v != "" {
		out = append(out, v)
	}
	return out
}
func TestDorisNestedSummaries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	name := "metering_mv_test_" + strings.ReplaceAll(newID(), "-", "")
	env := "METERING_MV_TEST_DSN"
	t.Setenv(env, "root@tcp(127.0.0.1:19030)/"+name+"?timeout=5s&interpolateParams=true")
	admin, e := sql.Open("mysql", "root@tcp(127.0.0.1:19030)/?timeout=5s&readTimeout=30s&interpolateParams=true")
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close()
	if _, e = admin.ExecContext(ctx, "CREATE DATABASE "+name); e != nil {
		t.Fatal(e)
	}
	defer admin.ExecContext(context.Background(), "DROP DATABASE "+name)
	for _, f := range []string{"001_init.sql", "002_consumer_coordination.sql", "003_usage_summaries.sql"} {
		b, e := os.ReadFile(filepath.Join("../../migrations/doris/metering", f))
		if e != nil {
			t.Fatal(e)
		}
		for _, q := range statements(strings.ReplaceAll(string(b), "metering", name)) {
			if _, e = admin.ExecContext(ctx, q); e != nil {
				t.Fatalf("%s: %s: %v", f, q[:min(160, len(q))], e)
			}
		}
	}
	store, e := Open(config.Doris{Database: name, DSNEnv: env})
	if e != nil {
		t.Fatal(e)
	}
	defer store.DB.Close()
	today := statwindow.Day(time.Now())
	yesterday := today.AddDate(0, 0, -1)
	if _, e = store.DB.ExecContext(ctx, "ALTER TABLE "+store.table("events_raw")+" SET (\"dynamic_partition.enable\"=\"false\")"); e != nil {
		t.Fatal(e)
	}
	for _, d := range []time.Time{yesterday, today} {
		q := "ALTER TABLE " + store.table("events_raw") + " ADD PARTITION IF NOT EXISTS p" + d.Format("20060102") + " VALUES [('" + d.Format("2006-01-02") + "'),('" + d.AddDate(0, 0, 1).Format("2006-01-02") + "'))"
		if _, e = store.DB.ExecContext(ctx, q); e != nil {
			t.Fatal(e)
		}
	}
	guard, e := LocalGuard(filepath.Join(t.TempDir(), "lock"))
	if e != nil {
		t.Fatal(e)
	}
	defer guard.Close()
	worker := Worker{Store: store, Config: Settings{CollectionStartDate: yesterday.Format("2006-01-02"), RefreshTimeoutSeconds: 60, PollIntervalSeconds: 1}, Guard: guard}
	insert := func(fixture string, date time.Time, unknown bool) string {
		t.Helper()
		b, e := os.ReadFile("../../testdata/collection/" + fixture + ".json")
		if e != nil {
			t.Fatal(e)
		}
		var event events.Event
		if e = json.Unmarshal(b, &event); e != nil {
			t.Fatal(e)
		}
		id := newID()
		event.ObservationID = id
		event.EventID = id + ":usage"
		delta := date.Add(time.Minute).UnixMilli() - event.RequestStartedAtMs
		event.RequestStartedAtMs += delta
		for _, p := range []*int64{event.RequestFinishedAtMs, event.ObservationEndedAtMs} {
			if p != nil {
				*p += delta
			}
		}
		event.EventCreatedAtMs += delta
		if unknown {
			event.ApiKeyID = nil
			event.CustomerID = nil
			event.CustomerName = nil
			event.CustomerMappingVersion = nil
			event.CustomerMappingStatus = "lookup_not_implemented"
			event.IdentityStatus = "invalid"
			event.UsageEligible = false
			event.AnomalyCodes = events.Add(event.AnomalyCodes, "identity_invalid")
		}
		raw, _ := events.Canonical(&event)
		batch, e := consumer.Freeze([]*kgo.Record{{Topic: "synthetic-mv-test", Partition: 0, Offset: 1, Key: []byte(id), Value: raw, Timestamp: time.Now()}}, config.Consumer{Bytes: 4 << 20, AcceptedConfigs: []string{event.ConfigVersion}}, time.Now())
		if e != nil {
			t.Fatal(e)
		}
		if len(batch.Raw) != 1 {
			t.Fatalf("fixture rejected: %+v", batch.Rejected)
		}
		row := batch.Raw[0]
		keys := make([]string, 0, len(row))
		for k := range row {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		args := []any{}
		marks := []string{}
		for _, k := range keys {
			args = append(args, row[k])
			marks = append(marks, "?")
		}
		// Array values must be represented as SQL array literals, while all scalars use parameters.
		for i, k := range keys {
			if k == "anomaly_codes" {
				arr := row[k].([]any)
				parts := []string{}
				for _, v := range arr {
					parts = append(parts, "'"+v.(string)+"'")
				}
				marks[i] = "[" + strings.Join(parts, ",") + "]"
			}
		}
		args = nil
		for i, k := range keys {
			if marks[i] == "?" {
				args = append(args, row[k])
			}
		}
		if _, e = store.DB.ExecContext(ctx, "INSERT INTO "+store.table("events_raw")+" (`"+strings.Join(keys, "`,`")+"`) VALUES ("+strings.Join(marks, ",")+")", args...); e != nil {
			t.Fatal(e)
		}
		return id
	}
	id := insert("usage-known", yesterday, true)
	insert("usage-zero", yesterday, false)
	insert("usage-unknown", yesterday, false)
	insert("usage-known", today, false)
	for _, view := range []string{"event_details", "requests", "usage_effective", "request_statistics"} {
		var count int
		if e = store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+store.table(view)+" WHERE observation_id=?", id).Scan(&count); e != nil || count != 1 {
			t.Fatalf("%s lost the valid request: count=%d err=%v", view, count, e)
		}
	}
	// Create an expired empty date and the earliest retained date; cleanup must keep the whole month.
	if _, e = store.DB.ExecContext(ctx, "ALTER TABLE "+store.table("events_raw")+" SET (\"dynamic_partition.enable\"=\"false\")"); e != nil {
		t.Fatal(e)
	}
	cutoff := statwindow.Start(today)
	for _, d := range []time.Time{cutoff.AddDate(0, 0, -1), cutoff} {
		q := "ALTER TABLE " + store.table("events_raw") + " ADD PARTITION IF NOT EXISTS p" + d.Format("20060102") + " VALUES [('" + d.Format("2006-01-02") + "'),('" + d.AddDate(0, 0, 1).Format("2006-01-02") + "'))"
		if _, e = store.DB.ExecContext(ctx, q); e != nil {
			t.Fatal(e)
		}
	}

	// Snapshot the immutable old qualification and hash before backfill.
	var original string
	if e = store.DB.QueryRowContext(ctx, "SELECT event_json FROM "+store.table("events_raw")+" WHERE observation_id=?", id).Scan(&original); e != nil {
		t.Fatal(e)
	}
	if e = worker.Run(ctx, "", ""); e != nil {
		t.Fatal(e)
	}
	var requests, eligible int
	var tokens sql.NullInt64
	if e = store.DB.QueryRowContext(ctx, "SELECT SUM(request_count),SUM(eligible_usage_request_count),SUM(total_tokens) FROM "+store.table("usage_daily_mv")+" WHERE stat_date=?", yesterday.Format("2006-01-02")).Scan(&requests, &eligible, &tokens); e != nil {
		t.Fatal(e)
	}
	if requests != 3 || eligible != 2 || !tokens.Valid || tokens.Int64 != 1500 {
		t.Fatalf("totals requests=%d eligible=%d tokens=%+v", requests, eligible, tokens)
	}
	var count int
	store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+store.table("usage_daily_mv")+" WHERE stat_date>=?", today.Format("2006-01-02")).Scan(&count)
	if count != 0 {
		t.Fatal("today was computed")
	}
	var after string
	store.DB.QueryRowContext(ctx, "SELECT event_json FROM "+store.table("events_raw")+" WHERE observation_id=?", id).Scan(&after)
	if after != original {
		t.Fatal("historical event mutated")
	}
	ps, e := store.partitions(ctx, "events_raw")
	if e != nil {
		t.Fatal(e)
	}
	if _, ok := ps[cutoff.Format("2006-01-02")]; !ok {
		t.Fatal("earliest retained month was truncated")
	}
	if _, ok := ps[cutoff.AddDate(0, 0, -1).Format("2006-01-02")]; ok {
		t.Fatal("expired date retained")
	}
	var unknown, zero sql.NullInt64
	if e = store.DB.QueryRowContext(ctx, "SELECT total_tokens FROM "+store.table("usage_daily_mv")+" WHERE stat_date=? AND usage_source='unknown'", yesterday.Format("2006-01-02")).Scan(&unknown); e != nil {
		t.Fatal(e)
	}
	if unknown.Valid {
		t.Fatal("unknown usage became zero")
	}
	if e = store.DB.QueryRowContext(ctx, "SELECT SUM(total_tokens) FROM "+store.table("usage_daily_mv")+" WHERE stat_date=? AND customer_id IS NOT NULL AND api_key_id IS NOT NULL", yesterday.Format("2006-01-02")).Scan(&zero); e != nil {
		t.Fatal(e)
	}
	if !zero.Valid || zero.Int64 != 0 {
		t.Fatal("customer query included unknown attribution", zero)
	}
	// Simulate a process dying after a successful task but before state publication.
	tasks, e := store.tasks(ctx, "usage_daily_mv")
	if e != nil || len(tasks) == 0 {
		t.Fatal(e)
	}
	recovered := Run{Kind: "DAY", Date: yesterday.Format("2006-01-02"), ID: newID(), Status: "UNCERTAIN", View: "usage_daily_mv", Partition: partitionName(yesterday, today), TaskID: tasks[0]["TaskId"], Submitted: true, InputDigest: events.Tuple([]string{}), CoverageStart: yesterday.Format("2006-01-02"), CoverageEnd: today.Format("2006-01-02")}
	if e = store.saveRun(ctx, recovered); e != nil {
		t.Fatal(e)
	}
	if e = worker.recover(ctx, today, cutoff); e != nil {
		t.Fatal(e)
	}
	var status string
	if e = store.DB.QueryRowContext(ctx, "SELECT status FROM "+store.table("summary_mv_refresh_runs")+" WHERE refresh_id=?", recovered.ID).Scan(&status); e != nil || status != "SUCCEEDED" {
		t.Fatal("lost acknowledgement not recovered", status, e)
	}

	// Incomplete input capture is discarded before submission; it never acknowledges notifications.
	incomplete := Run{Kind: "DAY", Date: yesterday.Format("2006-01-02"), ID: newID(), Status: "PENDING", View: "usage_daily_mv", Partition: partitionName(yesterday, today), InputCount: 1, InputDigest: events.Tuple([]string{"missing"}), CoverageStart: yesterday.Format("2006-01-02"), CoverageEnd: today.Format("2006-01-02")}
	if e = store.saveRun(ctx, incomplete); e != nil {
		t.Fatal(e)
	}
	if e = worker.recover(ctx, today, cutoff); e != nil {
		t.Fatal(e)
	}
	if e = store.DB.QueryRowContext(ctx, "SELECT status FROM "+store.table("summary_mv_refresh_runs")+" WHERE refresh_id=?", incomplete.ID).Scan(&status); e != nil || status != "FAILED" {
		t.Fatal("partial inputs were accepted", status, e)
	}
	// A missing task record leaves inputs pending, then safely recomputes on the next run.
	missingTask := recovered
	missingTask.ID = newID()
	missingTask.Status = "UNCERTAIN"
	missingTask.TaskID = "missing-task"
	missingTask.Submitted = true
	if e = store.saveRun(ctx, missingTask); e != nil {
		t.Fatal(e)
	}
	if e = worker.recover(ctx, today, cutoff); e == nil {
		t.Fatal("missing task acknowledged as success")
	}
	if e = worker.recover(ctx, today, cutoff); e != nil {
		t.Fatal(e)
	}
	if e = store.DB.QueryRowContext(ctx, "SELECT status FROM "+store.table("summary_mv_refresh_runs")+" WHERE refresh_id=?", missingTask.ID).Scan(&status); e != nil || status != "SUCCEEDED" {
		t.Fatal("missing task did not recover", status, e)
	}

	// A conflict-only notification must withdraw a previously counted historical request.
	_, e = store.DB.ExecContext(ctx, "INSERT INTO "+store.table("request_conflict_ids")+" VALUES (?,?)", id, stamp())
	if e != nil {
		t.Fatal(e)
	}
	_, e = store.DB.ExecContext(ctx, "INSERT INTO "+store.table("summary_dirty_notifications")+" (stat_date,notification_id,reason,source_id,evidence_hash,created_at) VALUES (?,?,'CONFLICT','synthetic',?,?)", yesterday.Format("2006-01-02"), events.Hash([]byte(id)), events.Hash([]byte(id)), stamp())
	if e != nil {
		t.Fatal(e)
	}
	if e = worker.Run(ctx, "", ""); e != nil {
		t.Fatal(e)
	}
	if e = store.DB.QueryRowContext(ctx, "SELECT SUM(request_count),SUM(total_tokens) FROM "+store.table("usage_daily_mv")+" WHERE stat_date=?", yesterday.Format("2006-01-02")).Scan(&requests, &tokens); e != nil {
		t.Fatal(e)
	}
	if requests != 2 || !tokens.Valid || tokens.Int64 != 0 {
		t.Fatalf("conflict not withdrawn: %d %+v", requests, tokens)
	}
	ids, e := store.notifications(ctx, yesterday.Format("2006-01-02"))
	if e != nil || len(ids) != 0 {
		t.Fatalf("notification not acknowledged %v %v", ids, e)
	}
	// Withdraw every remaining request: a zero-row refresh must delete old groups.
	if _, e = store.DB.ExecContext(ctx, "INSERT INTO "+store.table("request_conflict_ids")+" SELECT observation_id, NOW(3) FROM "+store.table("events_raw")+" WHERE request_date=?", yesterday.Format("2006-01-02")); e != nil {
		t.Fatal(e)
	}
	if e = worker.Run(ctx, "", ""); e != nil {
		t.Fatal(e)
	}
	if e = store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+store.table("usage_daily_mv")+" WHERE stat_date=?", yesterday.Format("2006-01-02")).Scan(&count); e != nil || count != 0 {
		t.Fatal("zero replacement retained old groups", count, e)
	}
	// A month-first run must write yesterday into the previous natural month.
	first := statwindow.Month(today)
	last := first.AddDate(0, 0, -1)
	if _, e = store.DB.ExecContext(ctx, "ALTER TABLE "+store.table("events_raw")+" SET (\"dynamic_partition.enable\"=\"false\")"); e != nil {
		t.Fatal(e)
	}
	q := "ALTER TABLE " + store.table("events_raw") + " ADD PARTITION IF NOT EXISTS p" + last.Format("20060102") + " VALUES [('" + last.Format("2006-01-02") + "'),('" + first.Format("2006-01-02") + "'))"
	if _, e = store.DB.ExecContext(ctx, q); e != nil {
		t.Fatal(e)
	}
	insert("usage-known", last, true)
	worker.Config.CollectionStartDate = last.Format("2006-01-02")
	worker.Now = func() time.Time { return first }
	if e = worker.Run(ctx, "", ""); e != nil {
		t.Fatal(e)
	}
	state, e := store.state(ctx, "MONTH", statwindow.Month(last).Format("2006-01-02"))
	if e != nil || state.Status != "SUCCEEDED" || state.CoverageEnd != first.Format("2006-01-02") {
		t.Fatal("month boundary", state, e)
	}
	// A second payload variant must disappear from the direct event_details projection.
	rows, e := store.DB.QueryContext(ctx, "SELECT * FROM "+store.table("events_raw")+" LIMIT 0")
	if e != nil {
		t.Fatal(e)
	}
	columns, e := rows.Columns()
	rows.Close()
	if e != nil {
		t.Fatal(e)
	}
	projection := make([]string, len(columns))
	for i, column := range columns {
		projection[i] = "`" + column + "`"
		if column == "payload_hash" {
			projection[i] = "'" + events.Hash([]byte("synthetic-variant")) + "'"
		}
	}
	if _, e = store.DB.ExecContext(ctx, "INSERT INTO "+store.table("events_raw")+" SELECT "+strings.Join(projection, ",")+" FROM "+store.table("events_raw")+" WHERE observation_id=?", id); e != nil {
		t.Fatal(e)
	}
	for _, test := range []struct {
		view  string
		count int
	}{{"event_conflicts", 1}, {"event_details", 0}, {"usage_effective", 0}} {
		var count int
		if e = store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+store.table(test.view)+" WHERE observation_id=?", id).Scan(&count); e != nil || count != test.count {
			t.Fatalf("%s conflict filtering: count=%d err=%v", test.view, count, e)
		}
	}
}
