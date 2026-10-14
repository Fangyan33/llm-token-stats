//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/config"
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

func startBatchingConsumer(t *testing.T, topic, group string, rows, bytes int) *runningBatchConsumer {
	t.Helper()
	s := localStore(t, "http://127.0.0.1:18040")
	c := config.Config{
		Kafka: config.Kafka{Brokers: []string{"127.0.0.1:29092"}, Topic: topic},
		Consumer: config.Consumer{Group: group, Rows: rows, Bytes: bytes, FlushMS: 500,
			AcceptedConfigs: []string{"collection-v1"}, Routes: []config.Route{{RouteName: "llm-chat", ServiceID: "model-service-a"}}},
		Doris: s.Cfg,
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
	if err = db.QueryRow("SELECT COUNT(*) FROM metering.consumer_batch_receipts WHERE kafka_topic=? AND kafka_partition=0 AND first_offset=0 AND next_offset=5000", topic).Scan(&count); err != nil || count != 1 {
		t.Fatalf("configured 5000-row batch was split: count=%d err=%v", count, err)
	}
	if err = db.QueryRow("SELECT COUNT(*) FROM metering.ingest_rejections WHERE kafka_topic=?", topic).Scan(&count); err != nil || count != 5000 {
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
	if err = db.QueryRow("SELECT COUNT(*) FROM metering.ingest_rejections WHERE kafka_topic=?", topic).Scan(&count); err != nil || count != 5002 {
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
	if err := db.QueryRow("SELECT COUNT(*) FROM metering.consumer_batch_receipts WHERE kafka_topic=? AND kafka_partition=0 AND next_offset-first_offset=1", topic).Scan(&count); err != nil || count != 2 {
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
	if err := db.QueryRow("SELECT COUNT(*) FROM metering.ingest_rejections WHERE kafka_topic=?", topic).Scan(&count); err != nil || count != 1 {
		t.Fatalf("shutdown replay lost or duplicated a record: count=%d err=%v", count, err)
	}
}
