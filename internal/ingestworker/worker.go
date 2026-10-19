package ingestworker

import (
	"context"
	"errors"
	"fmt"
	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/consumer"
	"github.com/Fangyan33/llm-token-stats/internal/doris"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/Fangyan33/llm-token-stats/internal/health"
	"github.com/Fangyan33/llm-token-stats/internal/producer"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
	"sort"
	"sync"
	"time"
)

type Worker struct {
	Cfg      config.Config
	Store    *doris.Store
	Health   *health.State
	Client   *kgo.Client
	mu       sync.Mutex
	owned    map[int32]bool
	ready    map[int32]bool
	pending  map[int32]*consumer.Batch
	buffered map[int32]*recordBuffer
	blocked  map[int32]bool
	retryAt  map[int32]time.Time
	auditOK  bool
}

func (w *Worker) owns(p int32) bool { w.mu.Lock(); defer w.mu.Unlock(); return w.owned[p] }
func New(c config.Config, s *doris.Store, h *health.State) (*Worker, error) {
	w := &Worker{Cfg: c, Store: s, Health: h, owned: map[int32]bool{}, ready: map[int32]bool{}, pending: map[int32]*consumer.Batch{}, buffered: map[int32]*recordBuffer{}, blocked: map[int32]bool{}, retryAt: map[int32]time.Time{}}
	o, err := producer.Options(c.Kafka)
	if err != nil {
		return nil, err
	}
	v := kversion.Stable()
	v.SetMaxKeyVersion(9, 7)
	o = append(o, kgo.MaxVersions(v), kgo.ConsumerGroup(c.Consumer.Group), kgo.ConsumeTopics(c.Kafka.Topic), kgo.DisableAutoCommit(), kgo.ConsumeResetOffset(kgo.NoResetOffset()), kgo.BlockRebalanceOnPoll(), kgo.FetchMaxBytes(4<<20), kgo.FetchMaxPartitionBytes(1<<20), kgo.BrokerMaxReadBytes(8<<20), kgo.MaxConcurrentFetches(1), kgo.FetchMaxWait(time.Duration(c.Consumer.FlushMS)*time.Millisecond), kgo.OnPartitionsAssigned(func(ctx context.Context, cl *kgo.Client, ps map[string][]int32) {
		w.mu.Lock()
		w.auditOK = false
		for _, p := range ps[c.Kafka.Topic] {
			w.owned[p] = true
			w.ready[p] = false
		}
		w.mu.Unlock()
	}), kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, ps map[string][]int32) { w.revoke(ps) }), kgo.OnPartitionsLost(func(ctx context.Context, cl *kgo.Client, ps map[string][]int32) { w.revoke(ps) }))
	w.Client, err = kgo.NewClient(o...)
	return w, err
}
func (w *Worker) revoke(ps map[string][]int32) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, p := range ps[w.Cfg.Kafka.Topic] {
		delete(w.owned, p)
		delete(w.ready, p)
		delete(w.pending, p)
		delete(w.buffered, p)
		delete(w.blocked, p)
		delete(w.retryAt, p)
	}
}
func (w *Worker) positions(ctx context.Context, p int32) (int64, int64, error) {
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = w.Cfg.Consumer.Group
	req.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: w.Cfg.Kafka.Topic, Partitions: []int32{p}}}
	r, err := w.Client.Request(ctx, req)
	if err != nil {
		return 0, 0, errors.New("kafka_offset")
	}
	resp := r.(*kmsg.OffsetFetchResponse)
	if resp.ErrorCode != 0 {
		return 0, 0, errors.New("kafka_offset")
	}
	committed := int64(-1)
	for _, t := range resp.Topics {
		for _, v := range t.Partitions {
			if v.ErrorCode != 0 {
				return 0, 0, errors.New("kafka_offset")
			}
			committed = v.Offset
		}
	}
	l := kmsg.NewPtrListOffsetsRequest()
	l.ReplicaID = -1
	part := kmsg.NewListOffsetsRequestTopicPartition()
	part.Partition = p
	part.Timestamp = -2
	l.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: w.Cfg.Kafka.Topic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{part}}}
	r, err = w.Client.Request(ctx, l)
	if err != nil {
		return 0, 0, errors.New("kafka_offset")
	}
	for _, t := range r.(*kmsg.ListOffsetsResponse).Topics {
		for _, v := range t.Partitions {
			if v.ErrorCode != 0 {
				return 0, 0, errors.New("kafka_offset")
			}
			return committed, v.Offset, nil
		}
	}
	return 0, 0, errors.New("kafka_offset")
}
func (w *Worker) setup(ctx context.Context, p int32) (int64, error) {
	commit, earliest, err := w.positions(ctx, p)
	if err != nil {
		return 0, err
	}
	rows, err := w.Store.DB.QueryContext(ctx, "SELECT DISTINCT initial_offset FROM "+w.Store.Database()+".consumer_partition_origins WHERE group_id=? AND kafka_topic=? AND kafka_partition=?", w.Cfg.Consumer.Group, w.Cfg.Kafka.Topic, p)
	if err != nil {
		return 0, errors.New("origin_query")
	}
	origins := []int64{}
	for rows.Next() {
		var n int64
		if rows.Scan(&n) != nil {
			rows.Close()
			return 0, errors.New("origin_query")
		}
		origins = append(origins, n)
	}
	err = rows.Err()
	rows.Close()
	if err != nil || len(origins) > 1 {
		return 0, consumer.ErrPause
	}
	next := commit
	if next < 0 {
		next = earliest
	}
	if len(origins) == 0 {
		origin := earliest
		if commit >= 0 {
			ok := false
			for _, pos := range w.Cfg.Consumer.Initial {
				if pos.Partition == p && pos.Offset >= earliest && pos.Offset <= commit {
					origin = pos.Offset
					ok = true
					break
				}
			}
			if !ok {
				return 0, consumer.ErrPause
			}
		}
		id := events.Tuple([]any{1, w.Cfg.Consumer.Group, w.Cfg.Kafka.Topic, p, origin})
		r := map[string]any{"group_id": w.Cfg.Consumer.Group, "kafka_topic": w.Cfg.Kafka.Topic, "kafka_partition": p, "origin_id": id, "initial_offset": origin, "created_at": consumer.Stamp(time.Now())}
		body := consumer.EncodeRows([]map[string]any{r})
		if !w.owns(p) {
			return 0, consumer.ErrPause
		}
		if err = w.Store.Load(ctx, "consumer_partition_origins", "origin_"+events.Hash(body), body, []map[string]any{r}); err != nil {
			return 0, err
		}
		origins = []int64{origin}
	}
	if commit < 0 {
		next = origins[0]
	}
	if commit >= 0 && commit < origins[0] {
		return 0, consumer.ErrPause
	}
	if next < earliest {
		r := map[string]any{"group_id": w.Cfg.Consumer.Group, "kafka_topic": w.Cfg.Kafka.Topic, "kafka_partition": p, "gap_id": events.Tuple([]any{1, w.Cfg.Consumer.Group, w.Cfg.Kafka.Topic, p, next, earliest}), "first_offset": next, "next_offset": earliest, "reason": "retention_loss", "detected_at": consumer.Stamp(time.Now())}
		body := consumer.EncodeRows([]map[string]any{r})
		if !w.owns(p) {
			return 0, consumer.ErrPause
		}
		if err = w.Store.Load(ctx, "consumer_partition_gaps", "gap_"+events.Hash(body), body, []map[string]any{r}); err != nil {
			return 0, err
		}
		resume := int64(-1)
		for _, v := range w.Cfg.Consumer.Resume {
			if v.Partition == p && v.Offset == earliest {
				resume = v.Offset
			}
		}
		if resume < 0 {
			return 0, consumer.ErrPause
		}
		next = resume
	}
	return next, nil
}
func (w *Worker) Run(ctx context.Context) error {
	defer w.Client.CloseAllowingRebalance()
	auditTick := time.NewTicker(time.Minute)
	defer auditTick.Stop()
	var releaseAt time.Time
	for ctx.Err() == nil {
		w.mu.Lock()
		parts := []int32{}
		resume := []int32{}
		for p, at := range w.retryAt {
			if !time.Now().Before(at) && w.owned[p] {
				resume = append(resume, p)
				delete(w.retryAt, p)
			}
		}
		for p := range w.owned {
			if !w.ready[p] {
				parts = append(parts, p)
			}
		}
		w.mu.Unlock()
		if len(resume) > 0 {
			w.Client.ResumeFetchPartitions(map[string][]int32{w.Cfg.Kafka.Topic: resume})
		}
		for _, p := range parts {
			c, cancel := context.WithTimeout(ctx, 15*time.Second)
			next, err := w.setup(c, p)
			cancel()
			if err != nil {
				w.mu.Lock()
				w.blocked[p] = true
				w.mu.Unlock()
				w.Client.PauseFetchPartitions(map[string][]int32{w.Cfg.Kafka.Topic: {p}})
				w.Health.Count("partition_paused")
				continue
			}
			w.Client.SetOffsets(map[string]map[int32]kgo.EpochOffset{w.Cfg.Kafka.Topic: {p: {Epoch: -1, Offset: next}}})
			w.mu.Lock()
			w.ready[p] = true
			delete(w.blocked, p)
			w.mu.Unlock()
			w.Client.ResumeFetchPartitions(map[string][]int32{w.Cfg.Kafka.Topic: {p}})
		}
		pollCtx, pollCancel := context.WithTimeout(ctx, w.pollWait(time.Now(), releaseAt))
		fetch := w.Client.PollRecords(pollCtx, w.Cfg.Consumer.Rows)
		pollCancel()
		if ctx.Err() != nil {
			w.Client.AllowRebalance()
			break
		}
		for _, e := range fetch.Errors() {
			if errors.Is(e.Err, context.DeadlineExceeded) || errors.Is(e.Err, context.Canceled) {
				continue
			}
			w.Health.Count("fetch_failure")
			w.mu.Lock()
			w.ready[e.Partition] = false
			delete(w.buffered, e.Partition)
			w.mu.Unlock()
			w.Client.PauseFetchPartitions(map[string][]int32{e.Topic: {e.Partition}})
		}
		receivedAt := time.Now()
		partitions := map[int32]kgo.FetchTopicPartition{}
		fetch.EachPartition(func(fp kgo.FetchTopicPartition) {
			if len(fp.Records) == 0 {
				return
			}
			if prev, ok := partitions[fp.Partition]; ok {
				fp.Records = append(prev.Records, fp.Records...)
			}
			partitions[fp.Partition] = fp
		})
		w.mu.Lock()
		force := !releaseAt.IsZero() && !receivedAt.Before(releaseAt)
		for p, b := range w.buffered {
			if _, ok := partitions[p]; !ok {
				partitions[p] = kgo.FetchTopicPartition{Topic: w.Cfg.Kafka.Topic, FetchPartition: kgo.FetchPartition{Partition: p}}
			}
			force = force || !receivedAt.Before(b.started.Add(config.Duration(w.Cfg.Consumer.FlushMS)))
		}
		w.mu.Unlock()
		var wg sync.WaitGroup
		sem := make(chan struct{}, 4)
		for _, fp := range partitions {
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				w.processPartition(ctx, fp, receivedAt, force, w.Store, w.Client)
			}()
		}
		wg.Wait()
		// Keep the poll's rebalance protection while accumulating records. At the
		// accumulation window's deadline, flush all partial buffers before
		// releasing it, so a busy partition cannot postpone rebalance indefinitely.
		w.mu.Lock()
		empty := len(w.buffered) == 0
		w.mu.Unlock()
		if empty {
			w.Client.AllowRebalance()
			releaseAt = time.Time{}
		} else if releaseAt.IsZero() {
			releaseAt = receivedAt.Add(config.Duration(w.Cfg.Consumer.FlushMS))
		}
		// Do not hold a partial batch's rebalance protection through a database
		// audit; finish the bounded accumulation window first.
		if empty {
			select {
			case <-auditTick.C:
				w.audit(ctx)
			default:
				w.mu.Lock()
				needAudit := !w.auditOK && len(w.owned) > 0 && len(w.blocked) == 0
				for p := range w.owned {
					needAudit = needAudit && w.ready[p]
				}
				w.mu.Unlock()
				if needAudit {
					w.audit(ctx)
				}
			}
		}
		w.mu.Lock()
		ready := w.auditOK && len(w.owned) > 0 && len(w.blocked) == 0
		for p := range w.owned {
			ready = ready && w.ready[p]
		}
		w.mu.Unlock()
		w.Health.Ready.Store(ready)
	}
	return ctx.Err()
}
func (w *Worker) audit(ctx context.Context) {
	w.mu.Lock()
	w.auditOK = false
	w.mu.Unlock()
	w.Health.Ready.Store(false)
	if !w.watermarks(ctx) {
		return
	}

	c, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	rows, err := w.Store.DB.QueryContext(c, "SELECT DISTINCT logical_request_id FROM "+w.Store.Database()+".events_raw GROUP BY logical_request_id HAVING COUNT(DISTINCT request_started_at_ms)>1 OR COUNT(DISTINCT request_date)>1 OR COUNT(DISTINCT "+doris.IdentityKeySQL+")>1 OR COUNT(DISTINCT "+doris.NameKeySQL+")>1 OR SUM(CASE WHEN event_type='request_started' THEN 1 ELSE 0 END)>1 OR SUM(CASE WHEN event_type='usage_observed' THEN 1 ELSE 0 END)>1")
	if err != nil {
		w.Health.Count("audit_failure")
		return
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if rows.Scan(&id) != nil {
			break
		}
		ids = append(ids, id)
		if len(ids) > 100000 {
			rows.Close()
			w.Health.Count("audit_failure")
			return
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		w.Health.Count("audit_failure")
		return
	}
	for i := 0; i < len(ids); i += 128 {
		last := min(i+128, len(ids))
		es, err := w.Store.Evidence(c, ids[i:last])
		if err != nil {
			w.Health.Count("audit_failure")
			return
		}
		marks := []map[string]any{}
		notifications := []map[string]any{}
		ds := map[string][]string{}
		for _, id := range ids[i:last] {
			marks = append(marks, map[string]any{"logical_request_id": id, "detected_at": consumer.Stamp(time.Now())})
		}
		for _, e := range es {
			ds[e.Date] = append(ds[e.Date], events.Tuple([]any{e.ID, e.Hash}))
		}
		for d, hs := range ds {
			sort.Strings(hs)
			hash := events.Tuple(hs)
			notifications = append(notifications, map[string]any{"stat_date": d, "notification_id": events.Tuple([]any{1, d, "REPAIR", hash}), "reason": "REPAIR", "source_id": "conflict_audit_v1", "evidence_hash": hash, "created_at": consumer.Stamp(time.Now())})
		}
		for _, x := range []struct {
			t string
			r []map[string]any
		}{{"request_conflict_ids", marks}, {"summary_dirty_notifications", notifications}} {
			b := consumer.EncodeRows(x.r)
			if err = w.Store.Load(c, x.t, "audit_"+events.Hash(b), b, x.r); err != nil {
				w.Health.Count("audit_failure")
				return
			}
		}
	}
	w.Health.Count("audit_success")
	w.mu.Lock()
	w.auditOK = true
	w.mu.Unlock()
}
func (w *Worker) String() string { return fmt.Sprintf("consumer %s", w.Cfg.Consumer.Group) }

// Watermark audit uses persistent origins/receipts and broker committed offsets.
// An existing gap always keeps coverage untrusted, even after traffic resumes.
func (w *Worker) watermarks(ctx context.Context) bool {
	w.mu.Lock()
	parts := []int32{}
	for p := range w.owned {
		parts = append(parts, p)
	}
	w.mu.Unlock()
	for _, p := range parts {
		if !w.owns(p) {
			continue
		}
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		commit, earliest, err := w.positions(c, p)
		if err != nil {
			cancel()
			w.Health.Count("watermark_untrusted")
			return false
		}
		var origin int64
		var count int
		err = w.Store.DB.QueryRowContext(c, "SELECT COUNT(DISTINCT initial_offset),MIN(initial_offset) FROM "+w.Store.Database()+".consumer_partition_origins WHERE group_id=? AND kafka_topic=? AND kafka_partition=?", w.Cfg.Consumer.Group, w.Cfg.Kafka.Topic, p).Scan(&count, &origin)
		if err != nil || count != 1 {
			cancel()
			w.Health.Count("watermark_untrusted")
			return false
		}
		if commit >= 0 && commit < earliest {
			_, _ = w.setup(c, p)
			cancel()
			w.Health.Count("watermark_gap")
			return false
		}
		rs, err := w.Store.DB.QueryContext(c, "SELECT first_offset,next_offset FROM "+w.Store.Database()+".consumer_batch_receipts WHERE kafka_topic=? AND kafka_partition=? ORDER BY first_offset", w.Cfg.Kafka.Topic, p)
		if err != nil {
			cancel()
			return false
		}
		ranges := []consumer.Range{}
		for rs.Next() {
			var r consumer.Range
			if rs.Scan(&r.First, &r.Next) != nil {
				err = errors.New("watermark_query")
				break
			}
			ranges = append(ranges, r)
			if len(ranges) > 100000 {
				err = errors.New("watermark_limit")
				break
			}
		}
		if rs.Err() != nil {
			err = rs.Err()
		}
		rs.Close()
		if err != nil {
			cancel()
			w.Health.Count("watermark_untrusted")
			return false
		}
		next := consumer.Continuous(origin, ranges)
		if commit >= 0 && next < commit {
			cancel()
			w.Health.Count("watermark_untrusted")
			return false
		}
		var gaps int
		err = w.Store.DB.QueryRowContext(c, "SELECT COUNT(*) FROM "+w.Store.Database()+".consumer_partition_gaps WHERE group_id=? AND kafka_topic=? AND kafka_partition=?", w.Cfg.Consumer.Group, w.Cfg.Kafka.Topic, p).Scan(&gaps)
		cancel()
		if err != nil || gaps > 0 {
			w.Health.Count("watermark_untrusted")
			return false
		}
	}
	return true
}
