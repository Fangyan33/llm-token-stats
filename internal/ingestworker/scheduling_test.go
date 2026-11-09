package ingestworker

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/consumer"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestTenMinuteDeadlineAndIndependentPartitions(t *testing.T) {
	w, store, client := batchWorker(20000, 16<<20), &batchStore{}, &batchClient{}
	w.Cfg.Consumer.FlushMS = 600000
	start := time.Now()
	now := start
	w.now = func() time.Time { return now }
	w.processPartition(context.Background(), fetchRecords(0, 0, 1, 2), start, false, store, client)
	now = start.Add(5 * time.Minute)
	w.processPartition(context.Background(), fetchRecords(0, 1, 1, 2), now, false, store, client)
	w.processPartition(context.Background(), fetchRecords(1, 0, 1, 2), now, false, store, client)
	now = start.Add(10*time.Minute - time.Millisecond)
	w.processPartition(context.Background(), fetchRecords(0, 0, 0, 0), now, false, store, client)
	if len(store.loads) != 0 || w.pollWait(now) != time.Millisecond {
		t.Fatal("flushed before deadline")
	}
	now = start.Add(10 * time.Minute)
	w.processPartition(context.Background(), fetchRecords(0, 0, 0, 0), now, false, store, client)
	w.processPartition(context.Background(), fetchRecords(1, 0, 0, 0), now, false, store, client)
	if !reflect.DeepEqual(client.commits, []int64{2}) || len(w.buffered[1].records) != 1 {
		t.Fatal("partition deadline reset or cold partition forced out")
	}
}

func TestSixteenMiBAccumulatedAcrossPolls(t *testing.T) {
	w, store, client := batchWorker(20000, 16<<20), &batchStore{}, &batchClient{}
	w.Cfg.Consumer.FlushMS = 600000
	start := time.Now()
	for i := 0; i < 16; i++ {
		fp := fetchRecords(0, int64(i), 1, (1<<20)-8)
		fp.Records[0].Key = []byte("12345678")
		w.processPartition(context.Background(), fp, start, false, store, client)
		if i < 15 && len(store.loads) != 0 {
			t.Fatal("flushed before 16 MiB")
		}
	}
	if !reflect.DeepEqual(store.rowCounts("ingest_rejections"), []int{16}) || !reflect.DeepEqual(client.commits, []int64{16}) {
		t.Fatal("16 MiB batch did not commit")
	}
}

func TestCooperativeAssignmentRetainsBuffers(t *testing.T) {
	w, store, client := batchWorker(20000, 16<<20), &batchStore{}, &batchClient{}
	w.Cfg.Consumer.FlushMS = 600000
	start := time.Now()
	w.processPartition(context.Background(), fetchRecords(0, 0, 1, 2), start, false, store, client)
	w.assign(map[string][]int32{w.Cfg.Kafka.Topic: {0, 1, 2}})
	if !w.ready[0] || !w.buffered[0].started.Equal(start) || w.ready[2] {
		t.Fatal("retained partition reinitialized")
	}
	w.deferPartition(fetchRecords(0, 1, 1, 2), start, w.epochs[0])
	w.revoke(map[string][]int32{w.Cfg.Kafka.Topic: {0}})
	if len(w.queued) != 0 || len(w.buffered) != 0 || w.owns(0) {
		t.Fatal("revoked work retained")
	}
	w.assign(map[string][]int32{w.Cfg.Kafka.Topic: {0}})
	if w.ownsEpoch(0, 0) {
		t.Fatal("stale ownership accepted")
	}
}

type cancelStore struct {
	batchStore
	cancel context.CancelFunc
}

func (s *cancelStore) Load(ctx context.Context, table, label string, body []byte, rows []map[string]any) error {
	s.cancel()
	<-ctx.Done()
	return ctx.Err()
}

func TestCancelledRoundRetainsFrozenBatchAndQueuedWork(t *testing.T) {
	w := batchWorker(1, 16<<20)
	ctx, cancel := context.WithCancel(context.Background())
	store, client := &cancelStore{cancel: cancel}, &batchClient{}
	w.deferPartition(fetchRecords(0, 0, 2, 2), time.Now(), 0)
	w.runRound(ctx, store, client)
	if w.pending[0] == nil || len(client.commits) != 0 || !reflect.DeepEqual(client.seeks, []int64{0}) {
		t.Fatal("cancellation lost frozen retry barrier")
	}
	w2 := batchWorker(20000, 16<<20)
	w2.deferPartition(fetchRecords(0, 0, 1, 2), time.Now(), 0)
	w2.runRound(ctx, &batchStore{}, &batchClient{})
	if len(w2.queued) != 1 {
		t.Fatal("cancelled scheduler lost unstarted poll records")
	}
}

type blockingStore struct {
	batchStore
	entered, release chan struct{}
}

func (s *blockingStore) Load(ctx context.Context, table, label string, body []byte, rows []map[string]any) error {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	select {
	case <-s.release:
		return s.batchStore.Load(ctx, table, label, body, rows)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestIdleFlushSerializesWithRevocation(t *testing.T) {
	w := batchWorker(20000, 16<<20)
	store := &blockingStore{entered: make(chan struct{}, 1), release: make(chan struct{})}
	client := &batchClient{}
	start := time.Now().Add(-time.Minute)
	w.buffered[0] = &recordBuffer{records: fetchRecords(0, 0, 1, 2).Records, started: start}
	w.deferPartition(fetchRecords(0, 0, 0, 0), start, 0)
	done := make(chan struct{})
	go func() { w.runRound(context.Background(), store, client); close(done) }()
	<-store.entered
	revoked := make(chan struct{})
	go func() { w.revoke(map[string][]int32{w.Cfg.Kafka.Topic: {0}}); close(revoked) }()
	select {
	case <-revoked:
		t.Fatal("revocation raced with commit")
	case <-time.After(20 * time.Millisecond):
	}
	close(store.release)
	<-done
	<-revoked
	if w.owns(0) || len(w.buffered) != 0 || len(w.pending) != 0 {
		t.Fatal("revoked buffers retained")
	}
	if !reflect.DeepEqual(client.commits, []int64{1}) {
		t.Fatal("in-flight confirmed flush did not commit before revoke")
	}
}

func TestFreezeContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := batchWorker(20000, 16<<20)
	if _, err := consumer.FreezeContext(ctx, fetchRecords(0, 0, 20000, 2).Records, w.Cfg.Consumer, time.Now()); err != context.Canceled {
		t.Fatalf("freeze: %v", err)
	}
}

func TestKafkaFetchWaitIndependentOfFlush(t *testing.T) {
	for _, ms := range []int{1, 600000} {
		w := batchWorker(20000, 16<<20)
		w.Cfg.Consumer.FlushMS = ms
		w.Cfg.Consumer.Group = "batch-config-test"
		w.Cfg.Kafka.Brokers = []string{"127.0.0.1:1"}
		real, err := New(w.Cfg, nil, w.Health)
		if err != nil {
			t.Fatal(err)
		}
		if real.Client.OptValue(kgo.FetchMaxWait) != (5 * time.Second) {
			t.Fatal("fetch wait follows flush interval")
		}
		real.Client.CloseAllowingRebalance()
	}
}
