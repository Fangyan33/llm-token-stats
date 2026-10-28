package summaryworker

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/events"
)

// Guard must cover every mutating statement and must stop after ownership loss.
type Guard interface {
	Check(context.Context) error
	Close() error
}
type Worker struct {
	Store  *Store
	Config Settings
	Guard  Guard
	Now    func() time.Time
}

func newID() string { // UUID v4 without a new dependency.
	return randomID()
}
func (s *Store) tasks(ctx context.Context, view string) ([]map[string]string, error) {
	return s.rows(ctx, "SELECT TaskId,Status,TaskContext,CompletedPartitions FROM tasks('type'='mv') WHERE MvDatabaseName=? AND MvName=?", s.Database, view)
}
func taskContextPartitions(raw string) ([]string, error) {
	var c struct {
		Partitions  []string `json:"partitions"`
		TriggerMode string   `json:"triggerMode"`
		IsComplete  bool     `json:"isComplete"`
	}
	if e := json.Unmarshal([]byte(raw), &c); e != nil {
		return nil, e
	}
	if c.TriggerMode != "MANUAL" || c.IsComplete {
		return nil, fmt.Errorf("unexpected_refresh_trigger")
	}
	return c.Partitions, nil
}
func matchTask(r Run, rows []map[string]string) (map[string]string, error) {
	old := map[string]bool{}
	for _, id := range r.Baseline {
		old[id] = true
	}
	var found map[string]string
	for _, row := range rows {
		if r.TaskID != "" {
			if row["TaskId"] == r.TaskID {
				return row, nil
			}
			continue
		}
		if old[row["TaskId"]] {
			continue
		}
		ps, e := taskContextPartitions(row["TaskContext"])
		if e != nil {
			return nil, e
		}
		if len(ps) != 1 || ps[0] != r.Partition {
			return nil, fmt.Errorf("unexpected_refresh_partitions")
		}
		if found != nil {
			return nil, fmt.Errorf("ambiguous_refresh_task")
		}
		found = row
	}
	return found, nil
}
func (w *Worker) refresh(ctx context.Context, r *Run) error {
	if e := w.Guard.Check(ctx); e != nil {
		return e
	}
	rows, e := w.Store.tasks(ctx, r.View)
	if e != nil {
		return e
	}
	// A PENDING run has never submitted; all others reconcile before resubmission.
	if !r.Submitted {
		for _, row := range rows {
			if row["Status"] == "RUNNING" || row["Status"] == "PENDING" {
				return fmt.Errorf("refresh_already_running")
			}
		}
		r.Baseline = nil
		for _, row := range rows {
			r.Baseline = append(r.Baseline, row["TaskId"])
		}
		sort.Strings(r.Baseline)
		r.Submitted = true
		r.Status = "UNCERTAIN"
		if e = w.Store.saveRun(ctx, *r); e != nil {
			return e
		}
		if e = w.Guard.Check(ctx); e != nil {
			return e
		}
		// The partition is generated from validated DATE bounds, never user SQL.
		if !identifier.MatchString(r.Partition) {
			return fmt.Errorf("invalid_partition")
		}
		_, submitErr := w.Store.DB.ExecContext(ctx, "REFRESH MATERIALIZED VIEW "+w.Store.table(r.View)+" PARTITIONS ("+r.Partition+")")
		if submitErr != nil {
			r.ErrorCode = "submit_uncertain"
			_ = w.Store.saveRun(ctx, *r)
		}
	}
	timer := time.NewTimer(time.Duration(w.Config.RefreshTimeoutSeconds) * time.Second)
	defer timer.Stop()
	poll := time.NewTicker(time.Duration(w.Config.PollIntervalSeconds) * time.Second)
	defer poll.Stop()
	missing := 0
	for {
		if e = w.Guard.Check(ctx); e != nil {
			return e
		}
		rows, e = w.Store.tasks(ctx, r.View)
		if e != nil {
			return e
		}
		row, e := matchTask(*r, rows)
		if e != nil {
			return e
		}
		if row != nil {
			missing = 0
			r.TaskID = row["TaskId"]
			r.Status = "RUNNING"
			if e = w.Store.saveRun(ctx, *r); e != nil {
				return e
			}
			switch row["Status"] {
			case "SUCCESS":
				var completed []string
				if json.Unmarshal([]byte(row["CompletedPartitions"]), &completed) != nil || len(completed) != 1 || completed[0] != r.Partition {
					return fmt.Errorf("incomplete_refresh")
				}
				return nil
			case "FAILED", "CANCELED", "CANCELLED":
				r.Status = "FAILED"
				r.ErrorCode = "doris_refresh_failed"
				if e = w.Store.saveRun(ctx, *r); e != nil {
					return e
				}
				return fmt.Errorf("doris_refresh_failed")
			}
		} else {
			missing++
			// Lost task history or lost submission: no competing/running task may exist.
			// Wait for job registration; then force the same deterministic partition anew.
			if missing >= 5 {
				for _, t := range rows {
					if t["Status"] == "RUNNING" || t["Status"] == "PENDING" {
						return fmt.Errorf("unresolved_refresh_task")
					}
				}
				r.Submitted = false
				r.TaskID = ""
				r.Status = "PENDING"
				if e = w.Store.saveRun(ctx, *r); e != nil {
					return e
				}
				return fmt.Errorf("refresh_task_missing_retry_next_run")
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			r.Status = "UNCERTAIN"
			r.ErrorCode = "refresh_timeout"
			_ = w.Store.saveRun(context.WithoutCancel(ctx), *r)
			return fmt.Errorf("refresh_timeout")
		case <-poll.C:
		}
	}
}
func (w *Worker) finish(ctx context.Context, r *Run) error {
	if e := w.validate(ctx, *r); e != nil {
		return e
	}
	s, e := w.Store.state(ctx, r.Kind, r.Date)
	if e != nil {
		return e
	}
	s.Status = "SUCCEEDED"
	s.ActiveID = ""
	s.SuccessID = r.ID
	s.RefreshedAt = stamp()
	s.CoverageStart = r.CoverageStart
	s.CoverageEnd = r.CoverageEnd
	s.DailyVersions = r.DailyVersions
	s.ErrorCode = ""
	// Publish state first; only a SUCCEEDED run acknowledges captured notifications.
	if e = w.Guard.Check(ctx); e != nil {
		return e
	}
	if e = w.Store.saveState(ctx, s); e != nil {
		return e
	}
	r.Status = "SUCCEEDED"
	r.ErrorCode = ""
	return w.Store.saveRun(ctx, *r)
}
func (w *Worker) execute(ctx context.Context, kind string, date time.Time, ids []string, versions map[string]string, start, end time.Time) error {
	view := "usage_daily_mv"
	next := date.AddDate(0, 0, 1)
	if kind == "MONTH" {
		view = "usage_monthly_mv"
		next = date.AddDate(0, 1, 0)
	}
	r := Run{Kind: kind, Date: date.Format("2006-01-02"), ID: newID(), Status: "PENDING", View: view, Partition: partitionName(date, next), InputCount: len(ids), InputDigest: events.Tuple(ids), DailyVersions: versions, CoverageStart: start.Format("2006-01-02"), CoverageEnd: end.Format("2006-01-02"), StartedAt: stamp()}
	if kind == "MONTH" {
		r.InputCount = len(versions)
		r.InputDigest = events.Tuple(versions)
	}
	if e := w.Guard.Check(ctx); e != nil {
		return e
	}
	if e := w.Store.saveRun(ctx, r); e != nil {
		return e
	}
	if e := w.Store.inputs(ctx, r, ids); e != nil {
		return e
	}
	s, e := w.Store.state(ctx, kind, r.Date)
	if e != nil {
		return e
	}
	s.Status = "RUNNING"
	s.ActiveID = r.ID
	if e = w.Store.saveState(ctx, s); e != nil {
		return e
	}
	if e = w.refresh(ctx, &r); e != nil {
		return w.failure(ctx, &r, e)
	}
	if e = w.finish(ctx, &r); e != nil {
		return w.failure(ctx, &r, e)
	}
	return nil
}
func partitionName(a, b time.Time) string {
	return "p_" + a.Format("20060102") + "_" + b.Format("20060102")
}
func (w *Worker) validate(ctx context.Context, r Run) error {
	col := "stat_date"
	if r.Kind == "MONTH" {
		col = "stat_month"
	}
	bad := `request_count <> usage_observed_request_count + usage_missing_request_count
 OR request_count <> eligible_usage_request_count + ineligible_usage_request_count
 OR started_request_count > request_count
 OR cached_read_known_requests > eligible_usage_request_count
 OR cached_write_known_requests > eligible_usage_request_count
 OR reasoning_known_requests > eligible_usage_request_count
 OR reported_total_known_requests > eligible_usage_request_count
 OR (eligible_usage_request_count=0 AND (input_tokens IS NOT NULL OR output_tokens IS NOT NULL OR total_tokens IS NOT NULL))
 OR (eligible_usage_request_count>0 AND (input_tokens IS NULL OR output_tokens IS NULL OR total_tokens IS NULL OR total_tokens<>input_tokens+output_tokens))
 OR (cached_read_known_requests=0 AND cached_read_tokens IS NOT NULL)
 OR (cached_write_known_requests=0 AND cached_write_tokens IS NOT NULL)
 OR (reasoning_known_requests=0 AND reasoning_tokens IS NOT NULL)
 OR (reported_total_known_requests=0 AND reported_total_tokens IS NOT NULL)`
	var count int
	if e := w.Store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+w.Store.table(r.View)+" WHERE "+col+"=? AND ("+bad+")", r.Date).Scan(&count); e != nil {
		return e
	}
	if count != 0 {
		return fmt.Errorf("summary_invariant")
	}
	if r.Kind == "MONTH" {
		if r.InputCount != len(r.DailyVersions) || r.InputDigest != events.Tuple(r.DailyVersions) {
			return fmt.Errorf("monthly_input_digest")
		}
		for date, version := range r.DailyVersions {
			state, e := w.Store.state(ctx, "DAY", date)
			if e != nil {
				return e
			}
			if state.Status != "SUCCEEDED" || state.SuccessID != version {
				return fmt.Errorf("monthly_input_changed")
			}
		}
		// Compare every dimension and metric, including NULL semantics. EXCEPT is set
		// equality here because both sides have one row per complete grouping key.
		dims := "cluster_name,deployment_environment,customer_id,provider_id,api_key_id,service_id,model,usage_source"
		sums := make([]string, 0, len(metrics))
		for _, m := range metrics {
			sums = append(sums, "SUM("+m+") AS "+m)
		}
		day := "SELECT " + dims + "," + strings.Join(sums, ",") + " FROM " + w.Store.table("usage_daily_mv") + " WHERE stat_date>=? AND stat_date<? GROUP BY " + dims
		month := "SELECT " + dims + "," + strings.Join(metrics, ",") + " FROM " + w.Store.table("usage_monthly_mv") + " WHERE stat_month=?"
		q := "SELECT COUNT(*) FROM ((" + day + " EXCEPT " + month + ") UNION ALL (" + month + " EXCEPT " + day + ")) differences"
		if e := w.Store.DB.QueryRowContext(ctx, q, r.CoverageStart, r.CoverageEnd, r.Date, r.Date, r.CoverageStart, r.CoverageEnd).Scan(&count); e != nil {
			return e
		}
		if count != 0 {
			return fmt.Errorf("monthly_daily_mismatch")
		}
	}
	return nil
}

var metrics = []string{"request_count", "started_request_count", "usage_observed_request_count", "usage_missing_request_count", "eligible_usage_request_count", "ineligible_usage_request_count", "http_2xx_request_count", "http_error_request_count", "terminal_missing_request_count", "input_tokens", "output_tokens", "total_tokens", "cached_read_tokens", "cached_write_tokens", "reasoning_tokens", "reported_total_tokens", "cached_read_known_requests", "cached_write_known_requests", "reasoning_known_requests", "reported_total_known_requests"}

// Best-effort diagnostic state never acknowledges inputs and never writes after
// ownership loss. The original error remains the caller's failure reason.
func (w *Worker) failure(ctx context.Context, r *Run, cause error) error {
	if w.Guard.Check(ctx) != nil {
		return cause
	}
	if r.Status != "FAILED" && r.Status != "PENDING" {
		r.Status = "UNCERTAIN"
	}
	if r.ErrorCode == "" {
		r.ErrorCode = "summary_refresh_failed"
	}
	_ = w.Store.saveRun(ctx, *r)
	s, e := w.Store.state(ctx, r.Kind, r.Date)
	if e == nil {
		s.Status = r.Status
		s.ErrorCode = r.ErrorCode
		_ = w.Store.saveState(ctx, s)
	}
	return cause
}
