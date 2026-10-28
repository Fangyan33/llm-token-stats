package consumer

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/Fangyan33/llm-token-stats/pkg/statwindow"
	"github.com/twmb/franz-go/pkg/kgo"
	"sort"
	"time"
)

var ErrPause = errors.New("partition_paused")

type Batch struct {
	Topic         string
	Partition     int32
	First, Last   int64
	ID            string
	At            string
	Raw, Rejected []map[string]any
	Events        []*events.Event
	Records       []*kgo.Record
	Dates         map[string][]string
	Loads         map[string][]byte
}

func date(ms int64) string     { return time.UnixMilli(ms).UTC().Format("2006-01-02") }
func Stamp(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05.000") }
func Freeze(rs []*kgo.Record, c config.Consumer, now time.Time) (*Batch, error) {
	if len(rs) == 0 {
		return nil, ErrPause
	}
	b := &Batch{Topic: rs[0].Topic, Partition: rs[0].Partition, First: rs[0].Offset, Last: rs[len(rs)-1].Offset, At: Stamp(now), Records: rs, Dates: map[string][]string{}, Loads: map[string][]byte{}}
	evidence := []any{}
	bytes := 0
	prev := int64(-1)
	for _, r := range rs {
		bytes += len(r.Value) + len(r.Key)
		if r.Topic != b.Topic || r.Partition != b.Partition || r.Offset <= prev || r.Offset < 0 || bytes > c.Bytes {
			return nil, ErrPause
		}
		prev = r.Offset
		e, err := events.Decode(r.Value, r.Key, now)
		reason := "accepted"
		if err != nil {
			reason = err.Error()
			if reason == "unknown_schema" {
				return nil, ErrPause
			}
		} else if !events.Has(c.AcceptedConfigs, e.ConfigVersion) || e.ResponseFlags != nil && !events.Has(c.Flags, *e.ResponseFlags) || e.ResponseCodeDetails != nil && !events.Has(c.Details, *e.ResponseCodeDetails) {
			reason = "event_registration"
			err = errors.New(reason)
		}
		evidence = append(evidence, []any{r.Offset, events.Hash(r.Value), reason})
		if err != nil {
			if r.Timestamp.IsZero() || r.Timestamp.UnixMilli() < 1 || r.Topic == "" || r.Partition < 0 {
				return nil, ErrPause
			}
			d := r.Timestamp.UTC().Format("2006-01-02")
			if d < statwindow.Start(now).Format("2006-01-02") || r.Timestamp.After(now.Add(5*time.Minute)) {
				return nil, ErrPause
			}
			b.Rejected = append(b.Rejected, map[string]any{"kafka_record_date": d, "kafka_topic": r.Topic, "kafka_partition": r.Partition, "kafka_offset": r.Offset, "payload_hash": events.Hash(r.Value), "reason": reason, "kafka_record_at": Stamp(r.Timestamp), "ingested_at": b.At})
			continue
		}
		canonical, _ := events.Canonical(e)
		var row map[string]any
		json.Unmarshal(canonical, &row)
		delete(row, "usage")
		if e.Usage == nil {
			for _, name := range tokenNames {
				row[name] = nil
				row[name+"_source"] = nil
			}
		} else {
			for i, t := range events.Tokens(e.Usage) {
				row[tokenNames[i]] = t.Value
				row[tokenNames[i]+"_source"] = t.Source
			}
		}
		for _, n := range []string{"terminal_observed", "usage_eligible", "raw_usage_truncated"} {
			if row[n] == true {
				row[n] = 1
			} else {
				row[n] = 0
			}
		}
		d := date(e.RequestStartedAtMs)
		h := events.Hash(canonical)
		row["request_date"] = d
		row["payload_hash"] = h
		row["identity_hash"] = events.IdentityHash(e)
		row["event_json"] = string(canonical)
		row["kafka_topic"] = r.Topic
		row["kafka_partition"] = r.Partition
		row["kafka_offset"] = r.Offset
		row["ingested_at"] = b.At
		b.Raw = append(b.Raw, row)
		b.Events = append(b.Events, e)
		b.Dates[d] = append(b.Dates[d], h)
	}
	b.ID = events.Tuple([]any{1, b.Topic, b.Partition, b.First, b.Last, evidence})
	b.Loads["events_raw"] = EncodeRows(b.Raw)
	b.Loads["ingest_rejections"] = EncodeRows(b.Rejected)
	return b, nil
}

var tokenNames = []string{"input_tokens", "output_tokens", "cached_read_tokens", "cached_write_tokens", "reasoning_tokens", "reported_total_tokens"}

func EncodeRows(rs []map[string]any) []byte {
	var b []byte
	for _, r := range rs {
		v, _ := json.Marshal(r)
		b = append(b, v...)
		b = append(b, '\n')
	}
	return b
}
func Label(table string, b *Batch, body []byte) string {
	return table + "_" + events.Tuple([]any{1, b.Topic, b.Partition, b.First, b.Last, events.Hash(body)})
}
func sorted(xs []string) []string {
	m := map[string]bool{}
	for _, x := range xs {
		m[x] = true
	}
	out := []string{}
	for x := range m {
		out = append(out, x)
	}
	sort.Strings(out)
	return out
}
func (b *Batch) String() string {
	return fmt.Sprintf("%s/%d/%d-%d", b.Topic, b.Partition, b.First, b.Last)
}
