package ingestworker

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/consumer"
	"github.com/Fangyan33/llm-token-stats/internal/logging"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type recordBuffer struct {
	records []*kgo.Record
	started time.Time
}

// Only the partition write path needs these operations; the real Kafka client
// also handles polling, assignment and recovery in Run.
type partitionClient interface {
	SetOffsets(map[string]map[int32]kgo.EpochOffset)
	PauseFetchPartitions(map[string][]int32) map[string][]int32
	CommitOffsetsSync(context.Context, map[string]map[int32]kgo.EpochOffset, func(*kgo.Client, *kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error))
}

func (w *Worker) pollWait(now time.Time) time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	wait := 500 * time.Millisecond
	for _, b := range w.buffered {
		wait = min(wait, b.started.Add(config.Duration(w.Cfg.Consumer.FlushMS)).Sub(now))
	}
	return max(wait, 0)
}

func (w *Worker) processPartition(ctx context.Context, fp kgo.FetchTopicPartition, receivedAt time.Time, force bool, store consumer.Store, client partitionClient) {
	w.mu.Lock()
	ready := w.ready[fp.Partition] && w.owned[fp.Partition]
	epoch := w.epochs[fp.Partition]
	previous := w.buffered[fp.Partition]
	delete(w.buffered, fp.Partition)
	w.mu.Unlock()
	if !ready {
		return
	}
	rs := fp.Records
	started := receivedAt
	if previous != nil {
		rs = append(previous.records, rs...)
		started = previous.started
	}
	for len(rs) > 0 {
		if ctx.Err() != nil {
			fp.Records = rs
			w.deferPartition(fp, started, epoch)
			return
		}
		batchStarted := time.Now()
		c, cancel := context.WithTimeout(ctx, 30*time.Second)
		w.mu.Lock()
		b := w.pending[fp.Partition]
		w.mu.Unlock()
		n, total := 0, 0
		var err error
		if b == nil {
			for n < len(rs) && n < w.Cfg.Consumer.Rows && total+len(rs[n].Value)+len(rs[n].Key) <= w.Cfg.Consumer.Bytes {
				total += len(rs[n].Value) + len(rs[n].Key)
				n++
			}
			if n == 0 {
				err = consumer.ErrPause
			} else if !force && n == len(rs) && n < w.Cfg.Consumer.Rows && total < w.Cfg.Consumer.Bytes && w.clockNow().Before(started.Add(config.Duration(w.Cfg.Consumer.FlushMS))) {
				w.mu.Lock()
				if w.owned[fp.Partition] {
					// Copy only the uncommitted tail; do not retain a whole fetched
					// slice or committed record pointers through its backing array.
					w.buffered[fp.Partition] = &recordBuffer{records: append([]*kgo.Record(nil), rs...), started: started}
				}
				w.mu.Unlock()
				cancel()
				return
			} else {
				b, err = consumer.FreezeContext(c, rs[:n], w.Cfg.Consumer, w.clockNow())
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					cancel()
					fp.Records = rs
					w.deferPartition(fp, started, epoch)
					return
				}
				if err == nil {
					w.mu.Lock()
					w.pending[fp.Partition] = b
					w.mu.Unlock()
				}
			}
		} else {
			// Reuse the frozen batch even if this retry poll returns only a
			// prefix. On success, skip to its confirmed next offset.
			for n < len(rs) && rs[n].Offset <= b.Last {
				n++
			}
			if n == 0 || rs[0].Offset != b.First {
				err = consumer.ErrPause
			}
		}
		if err == nil {
			logging.Debug("batch_processing", "topic", fp.Topic, "partition", fp.Partition, "first_offset", b.First, "last_offset", b.Last, "record_count", len(b.Records))
			p := consumer.Processor{Store: store, Owns: func() bool { return w.ownsEpoch(fp.Partition, epoch) }}
			err = p.Process(c, b)
			if err == nil {
				err = c.Err()
			}
			if err == nil && w.ownsEpoch(fp.Partition, epoch) {
				offsets := map[string]map[int32]kgo.EpochOffset{fp.Topic: {fp.Partition: {Epoch: b.Records[len(b.Records)-1].LeaderEpoch, Offset: b.Last + 1}}}
				client.CommitOffsetsSync(c, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, r *kmsg.OffsetCommitResponse, e error) {
					err = e
					if r != nil {
						for _, t := range r.Topics {
							for _, p := range t.Partitions {
								if p.ErrorCode != 0 {
									logging.Failure("offset_commit_failed", "offset_commit/"+strconv.Itoa(int(fp.Partition)), nil, "kafka_error_code", int(p.ErrorCode), "partition", p.Partition)
									err = errors.New("commit_failure")
								}
							}
						}
					}
				})
				if err != nil {
					logging.Failure("offset_commit_failed", "offset_commit/"+strconv.Itoa(int(fp.Partition)), err, "partition", fp.Partition)
				} else {
					logging.Recovered("offset_commit/"+strconv.Itoa(int(fp.Partition)), "offset_commit_recovered", "partition", fp.Partition)
				}
			}
		}
		cancel()
		if err != nil {
			w.Health.Count("batch_failure")
			logging.Failure("batch_failed_partition_paused", "batch/"+strconv.Itoa(int(fp.Partition)), err, "topic", fp.Topic, "partition", fp.Partition, "first_offset", rs[0].Offset, "duration_ms", time.Since(batchStarted).Milliseconds(), "retryable", err != consumer.ErrPause)
			if !w.ownsEpoch(fp.Partition, epoch) {
				return
			}
			client.SetOffsets(map[string]map[int32]kgo.EpochOffset{fp.Topic: {fp.Partition: {Epoch: -1, Offset: rs[0].Offset}}})
			w.mu.Lock()
			w.blocked[fp.Partition] = true
			if err != consumer.ErrPause {
				w.retryAt[fp.Partition] = time.Now().Add(time.Second)
			}
			w.mu.Unlock()
			client.PauseFetchPartitions(map[string][]int32{fp.Topic: {fp.Partition}})
			return
		}
		if !w.ownsEpoch(fp.Partition, epoch) {
			return
		}
		w.mu.Lock()
		delete(w.pending, fp.Partition)
		delete(w.blocked, fp.Partition)
		delete(w.retryAt, fp.Partition)
		w.mu.Unlock()
		w.Health.Count("batch_committed")
		logging.Recovered("batch/"+strconv.Itoa(int(fp.Partition)), "batch_processing_recovered", "partition", fp.Partition)
		logging.Recovered("kafka_fetch/"+strconv.Itoa(int(fp.Partition)), "kafka_fetch_recovered", "partition", fp.Partition)
		logging.Debug("batch_committed", "partition", fp.Partition, "first_offset", b.First, "last_offset", b.Last, "duration_ms", time.Since(batchStarted).Milliseconds())
		if rs[n-1].Offset < b.Last {
			client.SetOffsets(map[string]map[int32]kgo.EpochOffset{fp.Topic: {fp.Partition: {Epoch: -1, Offset: b.Last + 1}}})
			return
		}
		rs = rs[n:]
		// A buffered partial batch fits both limits, so a flushed batch always
		// consumes it completely. Any tail arrived in the current poll.
		started = receivedAt
	}
}
