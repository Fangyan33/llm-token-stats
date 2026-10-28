package summaryworker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMatchTaskDoesNotConfuseOldOrOtherRefresh(t *testing.T) {
	raw, _ := json.Marshal(map[string]any{"triggerMode": "MANUAL", "isComplete": false, "partitions": []string{"p_20261027_20261028"}})
	r := Run{Partition: "p_20261027_20261028", Baseline: []string{"old"}}
	old := map[string]string{"TaskId": "old", "TaskContext": string(raw)}
	fresh := map[string]string{"TaskId": "new", "TaskContext": string(raw)}
	if row, e := matchTask(r, []map[string]string{old, fresh}); e != nil || row["TaskId"] != "new" {
		t.Fatal(row, e)
	}
	r.TaskID = "old"
	if row, e := matchTask(r, []map[string]string{old, fresh}); e != nil || row["TaskId"] != "old" {
		t.Fatal(row, e)
	}
	r.TaskID = ""
	other := map[string]string{"TaskId": "other", "TaskContext": `{"triggerMode":"MANUAL","partitions":["p_today"]}`}
	if _, e := matchTask(r, []map[string]string{fresh, other}); e == nil {
		t.Fatal("accepted competing partition refresh")
	}
	copy := map[string]string{"TaskId": "duplicate", "TaskContext": string(raw)}
	if _, e := matchTask(r, []map[string]string{fresh, copy}); e == nil {
		t.Fatal("accepted ambiguous task")
	}
	if _, e := taskContextPartitions(`{"triggerMode":"SYSTEM","isComplete":true}`); e == nil {
		t.Fatal("accepted automatic refresh")
	}
}
func TestLocalGuardExcludesConcurrentEntrypoints(t *testing.T) {
	p := filepath.Join(t.TempDir(), "lock")
	g, e := LocalGuard(p)
	if e != nil {
		t.Fatal(e)
	}
	defer g.Close()
	if other, e := LocalGuard(p); e == nil {
		other.Close()
		t.Fatal("concurrent worker accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if g.Check(ctx) == nil {
		t.Fatal("ignored cancellation")
	}
}
func TestSummarySettingsRequireExplicitCollectionAndLock(t *testing.T) {
	c := Config{}
	if c.Defaults() == nil {
		t.Fatal("accepted unspecified source coverage")
	}
	if _, e := ParseDate("2026-02-30"); e == nil {
		t.Fatal("accepted invalid date")
	}
}

func TestLeaseGuardRejectsStaleOrLostOwnership(t *testing.T) {
	g := &kubeGuard{last: time.Now()}
	if e := g.Check(context.Background()); e != nil {
		t.Fatal(e)
	}
	g.last = time.Now().Add(-46 * time.Second)
	if g.Check(context.Background()) == nil {
		t.Fatal("stale renewal accepted")
	}
	g.last = time.Now()
	g.lost = true
	if g.Check(context.Background()) == nil {
		t.Fatal("lost lease accepted")
	}
}

func TestReadConfigAppliesTimeoutDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	b := []byte(`{"doris":{"database":"metering_test","dsn_env":"TEST_DSN"},"summary_worker":{"collection_start_date":"2026-10-01","lock_mode":"local","lock_file":"/tmp/test-summary.lock"}}`)
	if e := os.WriteFile(path, b, 0600); e != nil {
		t.Fatal(e)
	}
	c, e := ReadConfig(path)
	if e != nil {
		t.Fatal(e)
	}
	if c.Worker.RefreshTimeoutSeconds != 1800 || c.Worker.PollIntervalSeconds != 2 || c.Worker.MaxRunSeconds != 21600 {
		t.Fatalf("defaults lost: %+v", c.Worker)
	}
}

func TestTakeoverWaitsForOldSQL(t *testing.T) {
	rows := []map[string]string{{"CurrentConnected": "Yes", "Command": "Query", "Db": "metering_test"}, {"CurrentConnected": "No", "Command": "Sleep", "Db": "metering_test"}}
	if e := sqlIdle(rows, "metering_test"); e != nil {
		t.Fatal(e)
	}
	rows[1]["Command"] = "Query"
	if sqlIdle(rows, "metering_test") == nil {
		t.Fatal("in-flight old SQL ignored")
	}
	if sqlIdle([]map[string]string{{"Db": "metering_test"}}, "metering_test") == nil {
		t.Fatal("unknown metadata accepted")
	}
}
