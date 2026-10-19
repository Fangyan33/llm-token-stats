package consumer

import (
	"context"
	"errors"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"sort"
)

type Evidence struct {
	Date, ID, Hash, Identity, Names string
	Started                         int64
	EventID                         string
}
type Store interface {
	Load(context.Context, string, string, []byte, []map[string]any) error
	Evidence(context.Context, []string) ([]Evidence, error)
}
type Processor struct {
	Store Store
	Owns  func() bool
}

func (p *Processor) load(ctx context.Context, t string, b *Batch, rows []map[string]any, body []byte) error {
	if len(rows) == 0 {
		return nil
	}
	if !p.Owns() {
		return errors.New("owner_revoked")
	}
	if body == nil {
		body = EncodeRows(rows)
	}
	return p.Store.Load(ctx, t, Label(t, b, body), body, rows)
}
func (p *Processor) Process(ctx context.Context, b *Batch) error {
	if err := p.load(ctx, "events_raw", b, b.Raw, b.Loads["events_raw"]); err != nil {
		return err
	}
	if err := p.load(ctx, "ingest_rejections", b, b.Rejected, b.Loads["ingest_rejections"]); err != nil {
		return err
	}
	ids := []string{}
	for _, e := range b.Events {
		ids = append(ids, e.LogicalRequestID)
	}
	ev, err := p.Store.Evidence(ctx, sorted(ids))
	if err != nil {
		return err
	}
	groups := map[string][]Evidence{}
	for _, e := range ev {
		groups[e.ID] = append(groups[e.ID], e)
	}
	marks := []map[string]any{}
	conflictDates := map[string][]string{}
	for id, es := range groups {
		starts, dates, identities := map[int64]bool{}, map[string]bool{}, map[string]bool{}
		names := map[string]bool{}
		variants := map[string]map[string]bool{}
		for _, e := range es {
			starts[e.Started] = true
			dates[e.Date] = true
			identities[e.Identity] = true
			names[e.Names] = true
			if variants[e.EventID] == nil {
				variants[e.EventID] = map[string]bool{}
			}
			variants[e.EventID][e.Hash] = true
		}
		conflict := len(starts) > 1 || len(dates) > 1 || len(identities) > 1 || len(names) > 1
		for _, vs := range variants {
			conflict = conflict || len(vs) > 1
		}
		if conflict {
			marks = append(marks, map[string]any{"logical_request_id": id, "detected_at": b.At})
			hs := []string{}
			for _, e := range es {
				hs = append(hs, e.Hash)
			}
			h := events.Tuple([]any{id, sorted(hs)})
			for d := range dates {
				conflictDates[d] = append(conflictDates[d], h)
			}
		}
	}
	sort.Slice(marks, func(i, j int) bool {
		return marks[i]["logical_request_id"].(string) < marks[j]["logical_request_id"].(string)
	})
	if err = p.load(ctx, "request_conflict_ids", b, marks, nil); err != nil {
		return err
	}
	notifications := []map[string]any{}
	for _, pair := range []struct {
		reason string
		dates  map[string][]string
	}{{"INGEST", b.Dates}, {"CONFLICT", conflictDates}} {
		for d, hs := range pair.dates {
			evidence := events.Tuple(sorted(hs))
			id := events.Tuple([]any{1, d, pair.reason, b.ID, evidence})
			notifications = append(notifications, map[string]any{"stat_date": d, "notification_id": id, "reason": pair.reason, "source_id": b.ID, "evidence_hash": evidence, "kafka_topic": b.Topic, "kafka_partition": b.Partition, "first_offset": b.First, "last_offset": b.Last, "created_at": b.At})
		}
	}
	sort.Slice(notifications, func(i, j int) bool {
		return notifications[i]["stat_date"].(string)+notifications[i]["notification_id"].(string) < notifications[j]["stat_date"].(string)+notifications[j]["notification_id"].(string)
	})
	if err = p.load(ctx, "summary_dirty_notifications", b, notifications, nil); err != nil {
		return err
	}
	pairs := []any{}
	for _, n := range notifications {
		pairs = append(pairs, []any{n["stat_date"], n["notification_id"]})
	}
	receipts := []map[string]any{{"kafka_topic": b.Topic, "kafka_partition": b.Partition, "first_offset": b.First, "last_offset": b.Last, "batch_id": b.ID, "next_offset": b.Last + 1, "notification_count": len(notifications), "notification_digest": events.Tuple(pairs), "confirmed_at": b.At}}
	return p.load(ctx, "consumer_batch_receipts", b, receipts, nil)
}
