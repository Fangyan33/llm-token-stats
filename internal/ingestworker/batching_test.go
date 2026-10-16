package ingestworker

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/consumer"
	"github.com/Fangyan33/llm-token-stats/internal/health"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type testLoad struct {
	table, label string
	body         string
	rows         int
}

type batchStore struct {
	loads  []testLoad
	fail   string
	onLoad func()
}

func (s *batchStore) Load(_ context.Context, table, label string, body []byte, rows []map[string]any) error {
	s.loads = append(s.loads, testLoad{table: table, label: label, body: string(body), rows: len(rows)})
	if s.onLoad != nil {
		s.onLoad()
	}
	if table == s.fail {
		return errors.New("injected_load_failure")
	}
	return nil
}

func (*batchStore) Evidence(context.Context, []string) ([]consumer.Evidence, error) {
	return nil, nil
}

func (s *batchStore) rowCounts(table string) []int {
	var counts []int
	for _, load := range s.loads {
		if load.table == table {
			counts = append(counts, load.rows)
		}
	}
	return counts
}

type batchClient struct {
	commits, seeks []int64
	paused         []int32
	commitErr      error
	commitCode     int16
	onCommit       func()
}

func (c *batchClient) SetOffsets(offsets map[string]map[int32]kgo.EpochOffset) {
	for _, partitions := range offsets {
		for _, offset := range partitions {
			c.seeks = append(c.seeks, offset.Offset)
		}
	}
}

func (c *batchClient) PauseFetchPartitions(parts map[string][]int32) map[string][]int32 {
	for _, ps := range parts {
		c.paused = append(c.paused, ps...)
	}
	return parts
}

func (c *batchClient) CommitOffsetsSync(_ context.Context, offsets map[string]map[int32]kgo.EpochOffset, done func(*kgo.Client, *kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error)) {
	if c.onCommit != nil {
		c.onCommit()
	}
	for _, partitions := range offsets {
		for _, offset := range partitions {
			c.commits = append(c.commits, offset.Offset)
		}
	}
	response := &kmsg.OffsetCommitResponse{Topics: []kmsg.OffsetCommitResponseTopic{{Partitions: []kmsg.OffsetCommitResponseTopicPartition{{ErrorCode: c.commitCode}}}}}
	done(nil, nil, response, c.commitErr)
}

func batchWorker(rows, bytes int) *Worker {
	return &Worker{
		Cfg: config.Config{Kafka: config.Kafka{Topic: "llm-metering-events"}, Consumer: config.Consumer{
			Rows: rows, Bytes: bytes, FlushMS: 1000, AcceptedConfigs: []string{"collection-v1"},
			AcceptedServices: []string{"model-service-a"},
		}},
		Health: &health.State{}, owned: map[int32]bool{0: true, 1: true}, ready: map[int32]bool{0: true, 1: true},
		pending: map[int32]*consumer.Batch{}, buffered: map[int32]*recordBuffer{},
		blocked: map[int32]bool{}, retryAt: map[int32]time.Time{},
	}
}

func fetchRecords(part int32, start int64, count, size int) kgo.FetchTopicPartition {
	rs := make([]*kgo.Record, count)
	for i := range rs {
		rs[i] = &kgo.Record{Topic: "llm-metering-events", Partition: part, Offset: start + int64(i), Timestamp: time.Now(), Value: []byte(strings.Repeat("x", size))}
	}
	return kgo.FetchTopicPartition{Topic: "llm-metering-events", FetchPartition: kgo.FetchPartition{Partition: part, Records: rs}}
}

func TestBatchRowsAcrossPolls(t *testing.T) {
	for _, rows := range []int{129, 5000} {
		t.Run(strconv.Itoa(rows), func(t *testing.T) {
			w, store, client := batchWorker(rows, 4<<20), &batchStore{}, &batchClient{}
			client.onCommit = func() {
				if len(store.loads) == 0 || store.loads[len(store.loads)-1].table != "consumer_batch_receipts" {
					t.Fatal("committed before receipt confirmation")
				}
			}
			now := time.Now()
			for offset := 0; offset < rows; {
				count := min(100, rows-offset)
				w.processPartition(context.Background(), fetchRecords(0, int64(offset), count, 2), now, false, store, client)
				offset += count
				if offset < rows && (len(store.loads) != 0 || len(client.commits) != 0) {
					t.Fatal("partial batch was flushed before its limit or deadline")
				}
			}
			if got := store.rowCounts("ingest_rejections"); !reflect.DeepEqual(got, []int{rows}) {
				t.Fatalf("batch sizes = %v, want [%d]", got, rows)
			}
			if !reflect.DeepEqual(client.commits, []int64{int64(rows)}) || len(w.buffered) != 0 || len(w.pending) != 0 {
				t.Fatalf("commits=%v, buffered=%d, pending=%d", client.commits, len(w.buffered), len(w.pending))
			}
		})
	}
}

func TestAcceptedEventsBatchExceeds128(t *testing.T) {
	body, err := os.ReadFile("../../testdata/collection/usage-known.json")
	if err != nil {
		t.Fatal(err)
	}
	w, store, client := batchWorker(500, 4<<20), &batchStore{}, &batchClient{}
	now := time.Now()
	for offset := 0; offset < 500; {
		count := min(128, 500-offset)
		fp := fetchRecords(0, int64(offset), count, 0)
		for _, r := range fp.Records {
			r.Key = []byte("7f6c2a90-4b13-4b68-9d21-67283e0c5f44")
			r.Value = body
		}
		w.processPartition(context.Background(), fp, now, false, store, client)
		offset += count
	}
	if got := store.rowCounts("events_raw"); !reflect.DeepEqual(got, []int{500}) {
		t.Fatalf("accepted event batch sizes = %v", got)
	}
	if !reflect.DeepEqual(client.commits, []int64{500}) {
		t.Fatalf("commits = %v", client.commits)
	}
}

func TestBatchByteBoundary(t *testing.T) {
	w, store, client := batchWorker(5000, 32768), &batchStore{}, &batchClient{}
	now := time.Now()
	w.processPartition(context.Background(), fetchRecords(0, 0, 1, 20000), now, false, store, client)
	w.processPartition(context.Background(), fetchRecords(0, 1, 1, 20000), now, false, store, client)
	if !reflect.DeepEqual(client.commits, []int64{1}) || len(w.buffered[0].records) != 1 {
		t.Fatalf("byte overflow did not flush the prefix: commits=%v", client.commits)
	}
	// Keys also count against the configured Kafka-record byte budget.
	fp := fetchRecords(0, 2, 1, 12760)
	fp.Records[0].Key = []byte("12345678")
	w.processPartition(context.Background(), fp, now, false, store, client)
	if got := store.rowCounts("ingest_rejections"); !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatalf("byte-limited batch sizes = %v", got)
	}
	if !reflect.DeepEqual(client.commits, []int64{1, 3}) || len(w.buffered) != 0 {
		t.Fatalf("exact byte limit was not flushed: commits=%v", client.commits)
	}
}

func TestPartialBatchDeadlineWithoutNewRecords(t *testing.T) {
	for _, ms := range []int{1000, 15000} {
		t.Run(strconv.Itoa(ms), func(t *testing.T) {
			w, store, client := batchWorker(5000, 4<<20), &batchStore{}, &batchClient{}
			w.Cfg.Consumer.FlushMS = ms
			now := time.Now()
			w.processPartition(context.Background(), fetchRecords(0, 0, 1, 2), now, false, store, client)
			w.processPartition(context.Background(), fetchRecords(0, 1, 1, 2), now.Add(100*time.Millisecond), false, store, client)
			if !w.buffered[0].started.Equal(now) || len(store.loads) != 0 {
				t.Fatal("later poll reset the oldest record's deadline or flushed early")
			}
			interval := config.Duration(ms)
			w.buffered[0].started = time.Now().Add(-interval / 2)
			w.processPartition(context.Background(), fetchRecords(0, 0, 0, 0), time.Now(), false, store, client)
			if len(store.loads) != 0 || len(client.commits) != 0 {
				t.Fatal("idle partition flushed before the configured deadline")
			}
			w.buffered[0].started = time.Now().Add(-interval)
			w.processPartition(context.Background(), fetchRecords(0, 0, 0, 0), time.Now(), false, store, client)
			if got := store.rowCounts("ingest_rejections"); !reflect.DeepEqual(got, []int{2}) || !reflect.DeepEqual(client.commits, []int64{2}) {
				t.Fatalf("idle partition did not flush: batches=%v commits=%v", got, client.commits)
			}
		})
	}
}

func TestPartitionBuffersAndRebalanceWindow(t *testing.T) {
	w, store, client := batchWorker(3, 4<<20), &batchStore{}, &batchClient{}
	now := time.Now()
	w.processPartition(context.Background(), fetchRecords(0, 0, 2, 2), now, false, store, client)
	w.processPartition(context.Background(), fetchRecords(1, 100, 1, 2), now, false, store, client)
	// A full batch on a hot partition leaves its new tail bounded, while the
	// other partition retains its own records and original deadline.
	w.processPartition(context.Background(), fetchRecords(0, 2, 2, 2), now.Add(100*time.Millisecond), false, store, client)
	if len(w.buffered[0].records) != 1 || len(w.buffered[1].records) != 1 || !reflect.DeepEqual(client.commits, []int64{3}) {
		t.Fatal("partitions were mixed or a full batch was not flushed")
	}
	w.processPartition(context.Background(), fetchRecords(1, 101, 2, 2), now.Add(200*time.Millisecond), false, store, client)
	// The release window must not move forward when hot partitions replace
	// their tails, otherwise continuous traffic could block rebalance forever.
	if wait := w.pollWait(now.Add(900*time.Millisecond), time.Time{}); wait != 200*time.Millisecond {
		t.Fatalf("hot partition's tail deadline = %v", wait)
	}
	if wait := w.pollWait(now.Add(900*time.Millisecond), now.Add(time.Second)); wait != 100*time.Millisecond {
		t.Fatalf("poll wait = %v", wait)
	}
	w.processPartition(context.Background(), fetchRecords(0, 0, 0, 0), time.Now(), true, store, client)
	w.processPartition(context.Background(), fetchRecords(1, 0, 0, 0), time.Now(), true, store, client)
	if len(w.buffered) != 0 || !reflect.DeepEqual(client.commits, []int64{3, 103, 4}) {
		t.Fatalf("window did not drain before rebalance: commits=%v", client.commits)
	}
}

func TestFrozenBatchRetryAfterPartialPoll(t *testing.T) {
	for _, failure := range []string{"load", "commit", "commit_code"} {
		t.Run(failure, func(t *testing.T) {
			w, store, client := batchWorker(200, 4<<20), &batchStore{}, &batchClient{}
			switch failure {
			case "load":
				store.fail = "consumer_batch_receipts"
			case "commit":
				client.commitErr = errors.New("injected_commit_failure")
			case "commit_code":
				client.commitCode = 22
			}
			now := time.Now()
			w.processPartition(context.Background(), fetchRecords(0, 0, 100, 2), now, false, store, client)
			w.processPartition(context.Background(), fetchRecords(0, 100, 101, 2), now, false, store, client)
			frozen := w.pending[0]
			if frozen == nil || frozen.First != 0 || frozen.Last != 199 || len(w.buffered) != 0 || !reflect.DeepEqual(client.seeks, []int64{0}) || len(client.paused) != 1 {
				t.Fatal("failed batch was not frozen and rewound to its original first offset")
			}
			first := store.loads[0]
			store.fail, client.commitErr, client.commitCode = "", nil, 0
			w.processPartition(context.Background(), fetchRecords(0, 0, 1, 2), time.Now(), false, store, client)
			var repeated *testLoad
			for i := 1; i < len(store.loads); i++ {
				if store.loads[i].table == first.table {
					repeated = &store.loads[i]
					break
				}
			}
			if repeated == nil || repeated.label != first.label || repeated.body != first.body || repeated.rows != 200 {
				t.Fatal("retry changed the frozen load content or label")
			}
			if len(w.pending) != 0 || w.blocked[0] || client.commits[len(client.commits)-1] != 200 || !reflect.DeepEqual(client.seeks, []int64{0, 200}) {
				t.Fatalf("retry did not commit the entire frozen range: commits=%v seeks=%v", client.commits, client.seeks)
			}
		})
	}
}

func TestOversizedAndUnknownRecordsPause(t *testing.T) {
	for _, value := range []string{strings.Repeat("x", 32769), `{"schema_version":2}`} {
		w, store, client := batchWorker(5000, 32768), &batchStore{}, &batchClient{}
		now := time.Now()
		fp := fetchRecords(0, 0, 1, 0)
		fp.Records[0].Value = []byte(value)
		w.processPartition(context.Background(), fp, now, true, store, client)
		if len(store.loads) != 0 || len(client.commits) != 0 || len(client.paused) != 1 || len(w.retryAt) != 0 || !reflect.DeepEqual(client.seeks, []int64{0}) {
			t.Fatal("unprocessable record was written, committed, or automatically retried")
		}
	}
}

func TestRevocationDropsUncommittedBuffers(t *testing.T) {
	w, store, client := batchWorker(2, 4<<20), &batchStore{}, &batchClient{}
	now := time.Now()
	w.processPartition(context.Background(), fetchRecords(0, 0, 1, 2), now, false, store, client)
	w.revoke(map[string][]int32{w.Cfg.Kafka.Topic: {0}})
	w.processPartition(context.Background(), fetchRecords(0, 1, 1, 2), now, true, store, client)
	if len(w.buffered) != 0 || len(store.loads) != 0 || len(client.commits) != 0 {
		t.Fatal("revoked owner retained or committed its accumulation buffer")
	}
	w.owned[0], w.ready[0] = true, true
	store.onLoad = func() { w.revoke(map[string][]int32{w.Cfg.Kafka.Topic: {0}}) }
	w.processPartition(context.Background(), fetchRecords(0, 0, 2, 2), now, true, store, client)
	if len(w.pending) != 0 || len(client.commits) != 0 {
		t.Fatal("revocation during writing retained the frozen batch or committed")
	}
}
