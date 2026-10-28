package summaryworker

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/Fangyan33/llm-token-stats/pkg/statwindow"
)

func randomID() string {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(e)
	}
	b[6] = b[6]&15 | 64
	b[8] = b[8]&63 | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// Run captures the UTC cutoff once. from/to are optional, left-closed/right-open.
func (w *Worker) Run(ctx context.Context, from, to string) error {
	now := time.Now()
	if w.Now != nil {
		now = w.Now()
	}
	today := statwindow.Day(now)
	cutoff := statwindow.Start(now)
	start, e := ParseDate(w.Config.CollectionStartDate)
	if e != nil {
		return e
	}
	if start.Before(cutoff) {
		start = cutoff
	}
	end := today
	if start.After(today) {
		return fmt.Errorf("collection_start_in_future")
	}
	if from != "" || to != "" {
		a, e := ParseDate(from)
		if e != nil {
			return e
		}
		b, e := ParseDate(to)
		if e != nil || !b.After(a) || a.Before(start) || b.After(today) {
			return fmt.Errorf("invalid_backfill_range")
		}
		start = a
		end = b
	}
	if e = w.Guard.Check(ctx); e != nil {
		return e
	}
	if e = w.checkDefinitions(ctx); e != nil {
		return e
	}
	if e = w.recover(ctx, today, cutoff); e != nil {
		return e
	}
	if e = w.purge(ctx, cutoff); e != nil {
		return e
	}
	parts, e := w.Store.partitions(ctx, "events_raw")
	if e != nil {
		return e
	}
	// Group work by month, yesterday's month first. Earlier months remain ordered.
	months := map[string][]time.Time{}
	for d := start; d.Before(end); d = d.AddDate(0, 0, 1) {
		key := statwindow.Month(d).Format("2006-01-02")
		months[key] = append(months[key], d)
	}
	keys := make([]string, 0, len(months))
	for m := range months {
		keys = append(keys, m)
	}
	sort.Strings(keys)
	recent := statwindow.Month(today.AddDate(0, 0, -1)).Format("2006-01-02")
	if from == "" {
		for i, k := range keys {
			if k == recent {
				keys = append([]string{k}, append(keys[:i], keys[i+1:]...)...)
				break
			}
		}
	}
	gaps := false
	for _, m := range keys {
		for _, d := range months[m] {
			if e = ctx.Err(); e != nil {
				return e
			}
			date := d.Format("2006-01-02")
			state, e := w.Store.state(ctx, "DAY", date)
			if e != nil {
				return e
			}
			if p, ok := parts[date]; !ok || !p.End.Equal(d.AddDate(0, 0, 1)) {
				state.Status = "GAP"
				state.ErrorCode = "source_partition_missing"
				if e = w.Store.saveState(ctx, state); e != nil {
					return e
				}
				gaps = true
				continue
			}
			ids, e := w.Store.notifications(ctx, date)
			if e != nil {
				return e
			}
			if state.Status != "SUCCEEDED" || state.SuccessID == "" || len(ids) > 0 || date == today.AddDate(0, 0, -1).Format("2006-01-02") || from != "" {
				if e = w.execute(ctx, "DAY", d, ids, nil, d, d.AddDate(0, 0, 1)); e != nil {
					return e
				}
			}
		}
		month, _ := ParseDate(m)
		coverageStart := month
		collection, _ := ParseDate(w.Config.CollectionStartDate)
		if coverageStart.Before(collection) {
			coverageStart = collection
		}
		coverageEnd := month.AddDate(0, 1, 0)
		if coverageEnd.After(today) {
			coverageEnd = today
		}
		if !coverageEnd.After(coverageStart) {
			continue
		}
		versions := map[string]string{}
		complete := true
		for d := coverageStart; d.Before(coverageEnd); d = d.AddDate(0, 0, 1) {
			s, e := w.Store.state(ctx, "DAY", d.Format("2006-01-02"))
			if e != nil {
				return e
			}
			if s.Status != "SUCCEEDED" || s.SuccessID == "" {
				complete = false
				break
			}
			versions[s.Date] = s.SuccessID
		}
		state, e := w.Store.state(ctx, "MONTH", m)
		if e != nil {
			return e
		}
		if !complete {
			state.Status = "GAP"
			state.ErrorCode = "daily_coverage_missing"
			if e = w.Store.saveState(ctx, state); e != nil {
				return e
			}
			gaps = true
			continue
		}
		if state.Status != "SUCCEEDED" || state.SuccessID == "" || !reflect.DeepEqual(state.DailyVersions, versions) || state.CoverageEnd != coverageEnd.Format("2006-01-02") {
			if e = w.execute(ctx, "MONTH", month, nil, versions, coverageStart, coverageEnd); e != nil {
				return e
			}
		}
	}
	if gaps {
		return fmt.Errorf("summary_coverage_gap")
	}
	return nil
}
func (w *Worker) recover(ctx context.Context, today, cutoff time.Time) error {
	runs, e := w.Store.pending(ctx)
	if e != nil {
		return e
	}
	for _, r := range runs {
		d, e := ParseDate(r.Date)
		if e != nil {
			return e
		}
		if r.Kind != "DAY" && r.Kind != "MONTH" {
			return fmt.Errorf("invalid_run_kind")
		}
		if !d.Before(today) {
			return fmt.Errorf("pending_run_outside_window")
		}
		if d.Before(cutoff) {
			tasks, e := w.Store.tasks(ctx, r.View)
			if e != nil {
				return e
			}
			for _, task := range tasks {
				if task["Status"] == "RUNNING" || task["Status"] == "PENDING" {
					return fmt.Errorf("expired_refresh_in_progress")
				}
			}
			r.Status = "FAILED"
			r.ErrorCode = "source_expired"
			if e = w.Store.saveRun(ctx, r); e != nil {
				return e
			}
			state, e := w.Store.state(ctx, r.Kind, r.Date)
			if e != nil {
				return e
			}
			state.Status = "EXPIRED"
			state.ErrorCode = "source_expired"
			state.ActiveID = ""
			if e = w.Store.saveState(ctx, state); e != nil {
				return e
			}
			continue
		}
		expectedView := "usage_daily_mv"
		next := d.AddDate(0, 0, 1)
		if r.Kind == "MONTH" {
			expectedView = "usage_monthly_mv"
			next = d.AddDate(0, 1, 0)
		}
		if r.View != expectedView || r.Partition != partitionName(d, next) {
			return fmt.Errorf("invalid_run_target")
		}
		if r.Kind == "DAY" {
			rows, e := w.Store.rows(ctx, "SELECT notification_id FROM "+w.Store.table("summary_mv_refresh_inputs")+" WHERE stat_date=? AND refresh_id=? ORDER BY notification_id", r.Date, r.ID)
			if e != nil {
				return e
			}
			ids := []string{}
			for _, v := range rows {
				ids = append(ids, v["notification_id"])
			}
			if len(ids) != r.InputCount || events.Tuple(ids) != r.InputDigest {
				if r.Submitted {
					return fmt.Errorf("submitted_inputs_incomplete")
				}
				r.Status = "FAILED"
				r.ErrorCode = "input_capture_incomplete"
				if e = w.Store.saveRun(ctx, r); e != nil {
					return e
				}
				continue
			}
		}
		if e = w.refresh(ctx, &r); e != nil {
			return w.failure(ctx, &r, e)
		}
		if e = w.finish(ctx, &r); e != nil {
			return w.failure(ctx, &r, e)
		}
	}
	return nil
}
func (w *Worker) checkDefinitions(ctx context.Context) error {
	for _, name := range []string{"usage_daily_mv", "usage_monthly_mv"} {
		rows, e := w.Store.rows(ctx, "SHOW CREATE MATERIALIZED VIEW "+w.Store.table(name))
		if e != nil {
			return e
		}
		if len(rows) != 1 {
			return fmt.Errorf("summary_definition_missing")
		}
		var ddl string
		for k, v := range rows[0] {
			if strings.Contains(strings.ToLower(k), "create") {
				ddl = v
			}
		}
		normalized := strings.Join(strings.Fields(strings.ToLower(ddl)), " ")
		if !strings.Contains(normalized, "on manual") || !strings.Contains(normalized, `"use_for_rewrite" = "false"`) {
			return fmt.Errorf("summary_definition_must_be_manual")
		}
	}
	return nil
}
func (w *Worker) purge(ctx context.Context, cutoff time.Time) error {
	// Recovery precedes purge, and no MV job may still be active.
	for _, v := range []string{"usage_daily_mv", "usage_monthly_mv"} {
		ts, e := w.Store.tasks(ctx, v)
		if e != nil {
			return e
		}
		for _, t := range ts {
			if t["Status"] == "RUNNING" || t["Status"] == "PENDING" {
				return fmt.Errorf("purge_refresh_in_progress")
			}
		}
	}
	for _, table := range []string{"events_raw", "ingest_rejections"} {
		if e := w.purgeTable(ctx, table, cutoff); e != nil {
			return e
		}
	}
	// Expire month states even when an outage left no corresponding raw partition.
	rows, e := w.Store.rows(ctx, "SELECT record_json FROM "+w.Store.table("summary_mv_refresh_state")+" WHERE period_start<? AND status<>'EXPIRED'", cutoff.Format("2006-01-02"))
	if e != nil {
		return e
	}
	for _, row := range rows {
		var s State
		if e = json.Unmarshal([]byte(row["record_json"]), &s); e != nil {
			return e
		}
		s.Status = "EXPIRED"
		if e = w.Store.saveState(ctx, s); e != nil {
			return e
		}
	}
	return nil
}

func (w *Worker) purgeTable(ctx context.Context, table string, cutoff time.Time) (result error) {
	ps, e := w.Store.partitions(ctx, table)
	if e != nil {
		return e
	}
	hasExpired := false
	for _, p := range ps {
		if !p.End.After(cutoff) {
			hasExpired = true
		}
	}
	if e = w.Guard.Check(ctx); e != nil {
		return e
	}
	if hasExpired {
		if e = w.Store.dynamic(ctx, table, false); e != nil {
			return e
		}
	}
	defer func() {
		restore, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		if e := w.Guard.Check(restore); e != nil {
			if result == nil {
				result = e
			}
			return
		}
		if e := w.Store.dynamic(restore, table, true); e != nil && result == nil {
			result = e
		}
	}()

	for date, p := range ps {
		if p.End.After(cutoff) {
			continue
		}
		if !identifier.MatchString(p.Name) {
			return fmt.Errorf("invalid_source_partition")
		}
		if e = w.Guard.Check(ctx); e != nil {
			return e
		}
		// Expire reporting metadata before deleting source data. Keep audit/input facts.
		state, e := w.Store.state(ctx, "DAY", date)
		if e != nil {
			return e
		}
		state.Status = "EXPIRED"
		state.ErrorCode = ""
		if table == "events_raw" {
			ids, e := w.Store.notifications(ctx, date)
			if e != nil {
				return e
			}
			if len(ids) > 0 {
				state.ErrorCode = "unprocessed_expired_notifications"
			}
			if e = w.Store.saveState(ctx, state); e != nil {
				return e
			}
		}
		if _, e = w.Store.DB.ExecContext(ctx, "ALTER TABLE "+w.Store.table(table)+" DROP PARTITION "+p.Name); e != nil {
			return e
		}
	}
	return nil
}
