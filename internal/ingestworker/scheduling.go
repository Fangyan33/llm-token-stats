package ingestworker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/consumer"
	"github.com/Fangyan33/llm-token-stats/internal/logging"
	"github.com/twmb/franz-go/pkg/kgo"
)

const roundBudget = 45 * time.Second

// Only one poll result is retained between rounds. A deferred partition can
// also own its previous partial buffer, but no further poll occurs until drained.
type queuedPartition struct {
	fetch kgo.FetchTopicPartition
	at    time.Time
	epoch uint64
}

func (w *Worker) clockNow() time.Time {
	if w.now != nil {
		return w.now()
	}
	return time.Now()
}

func (w *Worker) ownsEpoch(p int32, epoch uint64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.owned[p] && w.epochs[p] == epoch
}

func (w *Worker) assign(ps map[string][]int32) {
	w.gate.Lock()
	defer w.gate.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.epochs == nil {
		w.epochs = map[int32]uint64{}
	}
	w.revision++
	w.auditOK = false
	w.Health.Ready.Store(false)
	for _, p := range ps[w.Cfg.Kafka.Topic] {
		// Cooperative assignment includes retained partitions: resetting them
		// would seek behind the buffer and duplicate records inside a batch.
		if !w.owned[p] {
			w.owned[p], w.ready[p] = true, false
			w.epochs[p] = w.revision
		}
	}
	logging.Recovered("kafka_group", "kafka_group_recovered", "topic", w.Cfg.Kafka.Topic, "group", w.Cfg.Consumer.Group)
	logging.Info("partitions_assigned", "topic", w.Cfg.Kafka.Topic, "partition_count", len(ps[w.Cfg.Kafka.Topic]))
}

func (w *Worker) deferPartition(fp kgo.FetchTopicPartition, at time.Time, epoch uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.owned[fp.Partition] && w.epochs[fp.Partition] == epoch {
		if w.queued == nil {
			w.queued = map[int32]queuedPartition{}
		}
		w.queued[fp.Partition] = queuedPartition{fp, at, epoch}
	}
}

// The gate also protects idle deadline flushes: an empty PollRecords response
// does not necessarily retain BlockRebalanceOnPoll's protection.
func (w *Worker) runRound(ctx context.Context, store consumer.Store, client partitionClient) {
	w.gate.RLock()
	defer w.gate.RUnlock()
	c, cancel := context.WithTimeout(ctx, roundBudget)
	defer cancel()
	w.mu.Lock()
	work := make([]queuedPartition, 0, len(w.queued))
	for _, q := range w.queued {
		work = append(work, q)
	}
	w.mu.Unlock()
	sort.Slice(work, func(i, j int) bool {
		if work[i].at.Equal(work[j].at) {
			return work[i].fetch.Partition < work[j].fetch.Partition
		}
		return work[i].at.Before(work[j].at)
	})
	var wg sync.WaitGroup
	defer wg.Wait()
	sem := make(chan struct{}, 4)
	for _, q := range work {
		select {
		case sem <- struct{}{}:
		case <-c.Done():
			return
		}
		if c.Err() != nil {
			<-sem
			return
		}
		w.mu.Lock()
		delete(w.queued, q.fetch.Partition)
		valid := w.owned[q.fetch.Partition] && w.epochs[q.fetch.Partition] == q.epoch
		w.mu.Unlock()
		if !valid {
			<-sem
			continue
		}
		wg.Add(1)
		go func(q queuedPartition) {
			defer wg.Done()
			defer func() { <-sem }()
			w.processPartition(c, q.fetch, q.at, false, store, client)
		}(q)
	}
}

func (w *Worker) initialize(ctx context.Context) {
	w.mu.Lock()
	parts := map[int32]uint64{}
	for p := range w.owned {
		if !w.ready[p] {
			parts[p] = w.epochs[p]
		}
	}
	w.mu.Unlock()
	for p, epoch := range parts {
		if ctx.Err() != nil {
			return
		}
		w.gate.RLock()
		if !w.ownsEpoch(p, epoch) {
			w.gate.RUnlock()
			continue
		}
		c, cancel := context.WithTimeout(ctx, 15*time.Second)
		next, err := w.setup(c, p)
		cancel()
		if err != nil {
			logging.Failure("partition_setup_failed", "partition_setup/"+fmt.Sprint(p), err, "topic", w.Cfg.Kafka.Topic, "partition", p)
			w.mu.Lock()
			w.blocked[p] = true
			w.mu.Unlock()
			w.Client.PauseFetchPartitions(map[string][]int32{w.Cfg.Kafka.Topic: {p}})
			w.Health.Count("partition_paused")
		} else {
			w.Client.SetOffsets(map[string]map[int32]kgo.EpochOffset{w.Cfg.Kafka.Topic: {p: {Epoch: -1, Offset: next}}})
			w.mu.Lock()
			w.ready[p] = true
			delete(w.blocked, p)
			w.mu.Unlock()
			w.Client.ResumeFetchPartitions(map[string][]int32{w.Cfg.Kafka.Topic: {p}})
			logging.Recovered("partition_setup/"+fmt.Sprint(p), "partition_setup_recovered", "partition", p, "next_offset", next)
			logging.Info("partition_ready", "partition", p, "next_offset", next)
		}
		w.gate.RUnlock()
	}
}

func (w *Worker) run(ctx context.Context) error {
	defer w.Client.CloseAllowingRebalance()
	auditTick := time.NewTicker(time.Minute)
	defer auditTick.Stop()
	for ctx.Err() == nil {
		w.initialize(ctx)
		w.gate.RLock()
		w.mu.Lock()
		resume := []int32{}
		for p, at := range w.retryAt {
			if !w.clockNow().Before(at) && w.owned[p] {
				resume = append(resume, p)
				delete(w.retryAt, p)
			}
		}
		hasQueue := len(w.queued) > 0
		w.mu.Unlock()
		if len(resume) > 0 {
			w.Client.ResumeFetchPartitions(map[string][]int32{w.Cfg.Kafka.Topic: resume})
		}
		w.gate.RUnlock()
		if !hasQueue {
			pollCtx, cancel := context.WithTimeout(ctx, w.pollWait(w.clockNow()))
			fetch := w.Client.PollRecords(pollCtx, w.Cfg.Consumer.Rows)
			cancel()
			w.gate.RLock()
			for _, e := range fetch.Errors() {
				if errors.Is(e.Err, context.DeadlineExceeded) || errors.Is(e.Err, context.Canceled) {
					continue
				}
				w.Health.Count("fetch_failure")
				logging.Failure("kafka_fetch_failed", "kafka_fetch/"+fmt.Sprint(e.Partition), e.Err, "topic", e.Topic, "partition", e.Partition)
				w.mu.Lock()
				w.ready[e.Partition] = false
				delete(w.buffered, e.Partition)
				w.mu.Unlock()
				w.Client.PauseFetchPartitions(map[string][]int32{e.Topic: {e.Partition}})
			}
			at := w.clockNow()
			w.mu.Lock()
			fetch.EachPartition(func(fp kgo.FetchTopicPartition) {
				if len(fp.Records) == 0 {
					return
				}
				if prev, ok := w.queued[fp.Partition]; ok {
					fp.Records = append(prev.fetch.Records, fp.Records...)
				}
				w.queued[fp.Partition] = queuedPartition{fp, at, w.epochs[fp.Partition]}
			})
			w.mu.Unlock()
			w.gate.RUnlock()
		}
		w.mu.Lock()
		for p, b := range w.buffered {
			if _, ok := w.queued[p]; !ok {
				w.queued[p] = queuedPartition{kgo.FetchTopicPartition{Topic: w.Cfg.Kafka.Topic, FetchPartition: kgo.FetchPartition{Partition: p}}, b.started, w.epochs[p]}
			}
		}
		w.mu.Unlock()
		w.runRound(ctx, w.Store, w.Client)
		// Partial buffers no longer hold the group hostage for the flush interval.
		w.Client.AllowRebalance()
		if ctx.Err() != nil {
			break
		}
		w.mu.Lock()
		needAudit := !w.auditOK && len(w.owned) > 0 && len(w.blocked) == 0
		for p := range w.owned {
			needAudit = needAudit && w.ready[p]
		}
		w.mu.Unlock()
		select {
		case <-auditTick.C:
			needAudit = true
		default:
		}
		if needAudit {
			w.audit(ctx)
		}
		w.mu.Lock()
		ready := w.auditOK && len(w.owned) > 0 && len(w.blocked) == 0
		for p := range w.owned {
			ready = ready && w.ready[p]
		}
		previous := w.Health.Ready.Swap(ready)
		w.mu.Unlock()
		if previous != ready {
			logging.Info("readiness_changed", "ready", ready)
		}
	}
	w.Health.Ready.Store(false)
	logging.Info("consumer_stopping")
	return ctx.Err()
}
