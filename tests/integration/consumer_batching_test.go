//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/Fangyan33/llm-token-stats/internal/health"
	"github.com/Fangyan33/llm-token-stats/internal/ingestworker"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func batchingTopic(t *testing.T, cl *kgo.Client) (string, string) {
	t.Helper()
	id := uuid(t)
	topic, group := "metering-batch-test-"+id, "batch-test-"+id
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := kmsg.NewPtrCreateTopicsRequest()
	req.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 2, ReplicationFactor: 1}}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("create batching test topic: %v", err)
	}
	// CreateTopics can return before the new leaders serve requests. The
	// fixture producer allows only one attempt, so wait for both partitions
	// to answer leader-side offset queries before publishing any records.
	eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		req := kmsg.NewPtrListOffsetsRequest()
		req.ReplicaID = -1
		req.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: topic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{{Partition: 0, Timestamp: -1}, {Partition: 1, Timestamp: -1}}}}
		resp, err := req.RequestWith(ctx, cl)
		if err != nil || len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 2 {
			return false
		}
		for _, p := range resp.Topics[0].Partitions {
			if p.ErrorCode != 0 || p.Offset < 0 {
				return false
			}
		}
		return true
	})
	return topic, group
}

type runningBatchConsumer struct {
	health *health.State
	worker *ingestworker.Worker
	stop   context.CancelFunc
	done   <-chan struct{}
}

func startBatchingConsumer(t *testing.T, topic, group string, rows, bytes int, intervalMS ...int) *runningBatchConsumer {
	t.Helper()
	s := localStore(t, "http://llm-token-stats-doris:8040")
	c := config.Config{
		Kafka: config.Kafka{Brokers: []string{"llm-token-stats-kafka:9092"}, Topic: topic},
		Consumer: config.Consumer{Group: group, Rows: rows, Bytes: bytes, FlushMS: 500,
			AcceptedConfigs: []string{"collection-v1"}},
		Doris: s.Cfg,
	}
	if len(intervalMS) > 0 {
		c.Consumer.FlushMS = intervalMS[0]
	}
	if err := c.ConsumerDefaults(); err != nil {
		t.Fatal(err)
	}
	h := &health.State{}
	w, err := ingestworker.New(c, s, h)
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	t.Cleanup(func() { stop(); <-done })
	return &runningBatchConsumer{health: h, worker: w, stop: stop, done: done}
}

func waitBatchCommit(t *testing.T, cl *kgo.Client, topic, group string, partition int32, next int64) {
	t.Helper()
	eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = group
		req.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: topic, Partitions: []int32{partition}}}
		resp, err := req.RequestWith(ctx, cl)
		return err == nil && len(resp.Topics) == 1 && len(resp.Topics[0].Partitions) == 1 && resp.Topics[0].Partitions[0].Offset == next
	})
}

func TestConsumerBatchingAndRebalance(t *testing.T) {
	db, cl := database(t), kafka(t)
	topic, group := batchingTopic(t, cl)
	// Preload a full configured batch so broker timing cannot turn the test
	// into a series of smaller timeout batches. Permanent-invalid records
	// exercise the same batching and commit barriers through ingest_rejections.
	records := make([]*kgo.Record, 5000)
	for i := range records {
		records[i] = &kgo.Record{Topic: topic, Partition: 0, Value: []byte(`{}`)}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	err := cl.ProduceSync(ctx, records...).FirstErr()
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	h1 := startBatchingConsumer(t, topic, group, 5000, 4<<20)
	waitBatchCommit(t, cl, topic, group, 0, 5000)
	var count int
	if err = db.QueryRow("SELECT COUNT(*) FROM metering_development.consumer_batch_receipts WHERE kafka_topic=? AND kafka_partition=0 AND first_offset=0 AND next_offset=5000", topic).Scan(&count); err != nil || count != 1 {
		t.Fatalf("configured 5000-row batch was split: count=%d err=%v", count, err)
	}
	if err = db.QueryRow("SELECT COUNT(*) FROM metering_development.ingest_rejections WHERE kafka_topic=?", topic).Scan(&count); err != nil || count != 5000 {
		t.Fatalf("batch rows missing: count=%d err=%v", count, err)
	}
	eventually(t, func() bool { return h1.health.Ready.Load() })
	// Idle partitions must flush without another record arriving; joining a
	// second consumer must also complete while partial batches are present.
	publish(t, cl, topic, 0, "synthetic", []byte(`{}`))
	publish(t, cl, topic, 1, "synthetic", []byte(`{}`))
	h2 := startBatchingConsumer(t, topic, group, 5000, 4<<20)
	waitBatchCommit(t, cl, topic, group, 0, 5001)
	waitBatchCommit(t, cl, topic, group, 1, 1)
	eventually(t, func() bool { return h1.health.Ready.Load() && h2.health.Ready.Load() })
	if err = db.QueryRow("SELECT COUNT(*) FROM metering_development.ingest_rejections WHERE kafka_topic=?", topic).Scan(&count); err != nil || count != 5002 {
		t.Fatalf("rebalance lost or duplicated rows: count=%d err=%v", count, err)
	}
}

func TestConsumerBatchByteLimit(t *testing.T) {
	db, cl := database(t), kafka(t)
	topic, group := batchingTopic(t, cl)
	body := []byte(strings.Repeat("x", 20000))
	publish(t, cl, topic, 0, "synthetic", body)
	publish(t, cl, topic, 0, "synthetic", body)
	startBatchingConsumer(t, topic, group, 5000, 32768)
	waitBatchCommit(t, cl, topic, group, 0, 2)
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM metering_development.consumer_batch_receipts WHERE kafka_topic=? AND kafka_partition=0 AND next_offset-first_offset=1", topic).Scan(&count); err != nil || count != 2 {
		t.Fatalf("byte limit did not split batches: count=%d err=%v", count, err)
	}
}

func TestConsumerShutdownReplaysPartialBatch(t *testing.T) {
	db, cl := database(t), kafka(t)
	topic, group := batchingTopic(t, cl)
	first := startBatchingConsumer(t, topic, group, 5000, 4<<20)
	eventually(t, func() bool { return first.health.Ready.Load() })
	publish(t, cl, topic, 0, "synthetic", []byte(`{}`))
	eventually(t, func() bool {
		offsets := first.worker.Client.UncommittedOffsets()
		return offsets[topic][0].Offset == 1
	})
	first.stop()
	select {
	case <-first.done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown blocked while a partial batch held rebalance protection")
	}
	startBatchingConsumer(t, topic, group, 5000, 4<<20)
	waitBatchCommit(t, cl, topic, group, 0, 1)
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM metering_development.ingest_rejections WHERE kafka_topic=?", topic).Scan(&count); err != nil || count != 1 {
		t.Fatalf("shutdown replay lost or duplicated a record: count=%d err=%v", count, err)
	}
}

func syntheticBatchUsage(t *testing.T) ([]byte, string) {
	t.Helper()
	body, err := os.ReadFile("../../testdata/collection/usage-known.json")
	if err != nil {
		t.Fatal(err)
	}
	var e events.Event
	if err = json.Unmarshal(body, &e); err != nil {
		t.Fatal(err)
	}
	e.ObservationID = uuid(t)
	e.EventID = e.ObservationID + ":usage"
	now := time.Now().UTC()
	delta := now.Add(-2*time.Second).UnixMilli() - e.RequestStartedAtMs
	e.RequestStartedAtMs += delta
	*e.ObservationEndedAtMs += delta
	*e.RequestFinishedAtMs += delta
	e.EventCreatedAtMs = now.UnixMilli()
	body, err = events.Canonical(&e)
	if err != nil {
		t.Fatal(err)
	}
	return body, e.ObservationID
}

func TestConsumerMaximumBatchLimits(t *testing.T) {
	for _, tc := range []struct {
		name   string
		count  int
		padded bool
	}{
		{"20000_rows", 20000, false},
		{"16_MiB_valid_events", 512, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, cl := database(t), kafka(t)
			topic, group := batchingTopic(t, cl)
			records := make([]*kgo.Record, tc.count)
			for i := range records {
				body, key := []byte(`{}`), ""
				if tc.padded {
					body, key = syntheticBatchUsage(t)
					body = append(body, bytes.Repeat([]byte(" "), 32768-len(body)-len(key))...)
				}
				records[i] = &kgo.Record{Topic: topic, Partition: 0, Key: []byte(key), Value: body}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			err := cl.ProduceSync(ctx, records...).FirstErr()
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			startBatchingConsumer(t, topic, group, 20000, 16<<20, 600000)
			waitBatchCommit(t, cl, topic, group, 0, int64(tc.count))
			var n int
			if err = db.QueryRow("SELECT COUNT(*) FROM metering_development.consumer_batch_receipts WHERE kafka_topic=? AND first_offset=0 AND next_offset=?", topic, tc.count).Scan(&n); err != nil || n != 1 {
				t.Fatalf("batch split: count=%d err=%v", n, err)
			}
			table := "ingest_rejections"
			if tc.padded {
				table = "events_raw"
			}
			if err = db.QueryRow("SELECT COUNT(*) FROM metering_development."+table+" WHERE kafka_topic=?", topic).Scan(&n); err != nil || n != tc.count {
				t.Fatalf("visible rows=%d err=%v", n, err)
			}
		})
	}
}

func TestConsumerLongFlushRebalanceAndReplay(t *testing.T) {
	db, cl := database(t), kafka(t)
	topic, group := batchingTopic(t, cl)
	first := startBatchingConsumer(t, topic, group, 20000, 16<<20, 600000)
	eventually(t, func() bool { return first.health.Ready.Load() })
	for p := int32(0); p < 2; p++ {
		publish(t, cl, topic, p, "synthetic", []byte(`{}`))
	}
	eventually(t, func() bool {
		offsets := first.worker.Client.UncommittedOffsets()
		return offsets[topic][0].Offset == 1 && offsets[topic][1].Offset == 1
	})
	second := startBatchingConsumer(t, topic, group, 20000, 16<<20, 600000)
	eventually(t, func() bool {
		return first.health.Ready.Load() && second.health.Ready.Load() && len(first.worker.Client.UncommittedOffsets()[topic]) == 1 && len(second.worker.Client.UncommittedOffsets()[topic]) == 1
	})
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM metering_development.consumer_batch_receipts WHERE kafka_topic=?", topic).Scan(&n); err != nil || n != 0 {
		t.Fatalf("rebalance flushed partial buffers: %d %v", n, err)
	}
	first.stop()
	second.stop()
	select {
	case <-first.done:
	case <-time.After(5 * time.Second):
		t.Fatal("first shutdown blocked")
	}
	select {
	case <-second.done:
	case <-time.After(5 * time.Second):
		t.Fatal("second shutdown blocked")
	}
	startBatchingConsumer(t, topic, group, 20000, 16<<20)
	waitBatchCommit(t, cl, topic, group, 0, 1)
	waitBatchCommit(t, cl, topic, group, 1, 1)
	if err := db.QueryRow("SELECT COUNT(*) FROM metering_development.ingest_rejections WHERE kafka_topic=?", topic).Scan(&n); err != nil || n != 2 {
		t.Fatalf("replay rows=%d err=%v", n, err)
	}
}

func TestConsumerRealTenMinuteIdleFlush(t *testing.T) {
	db, cl := database(t), kafka(t)
	topic, group := batchingTopic(t, cl)
	h := startBatchingConsumer(t, topic, group, 20000, 16<<20, 600000)
	eventually(t, func() bool { return h.health.Ready.Load() })
	body, key := syntheticBatchUsage(t)
	start := time.Now()
	publish(t, cl, topic, 0, key, body)
	eventually(t, func() bool { return h.worker.Client.UncommittedOffsets()[topic][0].Offset == 1 })
	polled := time.Now()
	// Bound the real monotonic wait; also keep observing Doris throughout the
	// window, rather than proving only that the row exists ten minutes later.
	deadline := polled.Add(11 * time.Minute)
	for time.Now().Before(deadline) {
		var n int
		if err := db.QueryRow("SELECT COUNT(*) FROM metering_development.events_raw WHERE kafka_topic=?", topic).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n > 0 {
			if time.Since(start) < 10*time.Minute {
				t.Fatalf("premature flush after %v", time.Since(start))
			}
			waitBatchCommit(t, cl, topic, group, 0, 1)
			t.Logf("real idle flush visible and committed after %v", time.Since(start))
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatal("idle batch did not become visible within 11 minutes")
}
