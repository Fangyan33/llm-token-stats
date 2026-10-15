//go:build integration

// These tests write synthetic data only to the isolated deploy/local stack.
package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/consumer"
	"github.com/Fangyan33/llm-token-stats/internal/doris"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/Fangyan33/llm-token-stats/internal/health"
	"github.com/Fangyan33/llm-token-stats/internal/ingestworker"
	"github.com/Fangyan33/llm-token-stats/internal/producer"
	"github.com/Fangyan33/llm-token-stats/pkg/observationwire"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const dsn = "root@tcp(127.0.0.1:19030)/metering?timeout=5s&readTimeout=5s&writeTimeout=5s&interpolateParams=true&time_zone=%27%2B00%3A00%27"

func uuid(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	b[6] = b[6]&15 | 64
	b[8] = b[8]&63 | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}
func database(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Ping(); err != nil {
		t.Fatalf("run make build wasm local-up first: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}
func eventually(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("local collection condition did not become true within 45s")
}
func kafka(t *testing.T) *kgo.Client {
	t.Helper()
	c, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:29092"), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.DisableIdempotentWrite(), kgo.RecordRetries(1), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}
func publish(t *testing.T, c *kgo.Client, topic string, part int32, key string, b []byte) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r := &kgo.Record{Topic: topic, Partition: part, Key: []byte(key), Value: b}
	if err := c.ProduceSync(ctx, r).FirstErr(); err != nil {
		t.Fatal(err)
	}
	return r.Offset
}
func TestCollectionEndToEnd(t *testing.T) {
	db := database(t)
	cl := kafka(t)
	for _, tc := range []struct {
		name            string
		stream, missing bool
	}{{"json", false, false}, {"sse", true, false}, {"unknown", true, true}} {
		t.Run(tc.name, func(t *testing.T) {
			xid := uuid(t)
			body := map[string]any{"model": "fixture", "messages": []any{map[string]any{"role": "user", "content": "synthetic fixture"}}, "stream": tc.stream}
			if tc.missing {
				body["test_case"] = "missing_usage"
			}
			b, _ := json.Marshal(body)
			req, _ := http.NewRequest("POST", "http://127.0.0.1:18080/v1/chat/completions", bytes.NewReader(b))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("x-request-id", xid)
			res, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
			if err != nil {
				t.Fatal(err)
			}
			out, err := io.ReadAll(io.LimitReader(res.Body, 262144))
			res.Body.Close()
			if err != nil || res.StatusCode != 200 {
				t.Fatalf("business response: status=%d err=%v", res.StatusCode, err)
			}
			if tc.stream && !bytes.Contains(out, []byte("data: [DONE]")) {
				t.Fatal("SSE did not finish")
			}
			for _, id := range res.Header.Values("x-metering-request-id") {
				if !eventsUUID(id) {
					t.Fatal("upstream forged metering header survived")
				}
			}
			var id, raw, status, observation string
			var part int32
			var eligible int
			var input, output sql.NullInt64
			eventually(t, func() bool {
				return db.QueryRow("SELECT logical_request_id,event_json,kafka_partition,usage_status,observation_status,usage_eligible,input_tokens,output_tokens FROM metering.events_raw WHERE x_request_id=? AND event_type='usage_observed'", xid).Scan(&id, &raw, &part, &status, &observation, &eligible, &input, &output) == nil
			})
			var event events.Event
			if err = json.Unmarshal([]byte(raw), &event); err != nil || event.RouteName != "unknown" {
				t.Fatal("unnamed Envoy route must be recorded as unknown")
			}
			if observation != "complete" {
				t.Fatalf("observation=%s", observation)
			}
			if tc.missing {
				if status != "unknown" || eligible != 0 || input.Valid || output.Valid {
					t.Fatal("unknown usage became zero or eligible")
				}
			} else if status != "known" || eligible != 1 || input.Int64 != 10 || output.Int64 != 4 {
				t.Fatal("reported usage mismatch")
			}
			var count int
			if err = db.QueryRow("SELECT COUNT(*) FROM metering.events_raw WHERE logical_request_id=?", id).Scan(&count); err != nil || count != 2 {
				t.Fatalf("event count=%d err=%v", count, err)
			}
			offset := publish(t, cl, "llm-metering-events", part, id, []byte(raw))
			eventually(t, func() bool {
				var n int
				return db.QueryRow("SELECT COUNT(*) FROM metering.consumer_batch_receipts WHERE kafka_topic='llm-metering-events' AND kafka_partition=? AND first_offset<=? AND next_offset>?", part, offset, offset).Scan(&n) == nil && n > 0
			})
			if err = db.QueryRow("SELECT COUNT(*) FROM metering.events_raw WHERE logical_request_id=?", id).Scan(&count); err != nil || count != 2 {
				t.Fatal("identical replay was not deduplicated")
			}
			if tc.name == "json" {
				var e events.Event
				json.Unmarshal([]byte(raw), &e)
				e.ProducerID = "synthetic-variant"
				variant, _ := events.Canonical(&e)
				publish(t, cl, "llm-metering-events", part, id, variant)
				eventually(t, func() bool {
					var n int
					return db.QueryRow("SELECT COUNT(*) FROM metering.request_conflict_ids WHERE logical_request_id=?", id).Scan(&n) == nil && n == 1
				})
				if err = db.QueryRow("SELECT has_conflict FROM metering.requests WHERE logical_request_id=?", id).Scan(&count); err != nil || count != 1 {
					t.Fatal("conflict leaked into valid request projection")
				}
				eventually(t, func() bool {
					var n int
					return db.QueryRow("SELECT COUNT(*) FROM metering.summary_dirty_notifications WHERE reason='CONFLICT' AND kafka_partition=?", part).Scan(&n) == nil && n > 0
				})
			}
		})
	}
	t.Run("ignored_methods_and_paths", func(t *testing.T) {
		before := gatewaySelectedRequests(t)
		ids := []string{}
		for _, tc := range []struct{ method, path string }{
			{"GET", "/v1/chat/completions"}, {"HEAD", "/v1/chat/completions"},
			{"OPTIONS", "/v1/chat/completions"}, {"PUT", "/v1/chat/completions"},
			{"POST", "/v1/embeddings"}, {"POST", "/v1/chat/completions/"},
			{"POST", "/v1/chat/completions/extra"}, {"GET", "/healthz"},
		} {
			id := uuid(t)
			ids = append(ids, id)
			req, _ := http.NewRequest(tc.method, "http://127.0.0.1:18080"+tc.path, strings.NewReader(`{"model":"fixture","stream":false}`))
			req.Header.Set("content-type", "application/json")
			req.Header.Set("x-request-id", id)
			res, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
		}
		if after := gatewaySelectedRequests(t); after != before {
			t.Fatalf("ignored request selected: before=%d after=%d", before, after)
		}
		var count int
		where := "'" + strings.Join(ids, "','") + "'"
		if err := db.QueryRow("SELECT COUNT(*) FROM metering.events_raw WHERE x_request_id IN (" + where + ")").Scan(&count); err != nil || count != 0 {
			t.Fatal("non-whitelisted request generated events")
		}
	})
}

func gatewaySelectedRequests(t *testing.T) uint64 {
	t.Helper()
	res, err := (&http.Client{Timeout: 5 * time.Second}).Get("http://127.0.0.1:19000/stats?filter=metering_requests_selected_total")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 65536))
	if err != nil || res.StatusCode != 200 {
		t.Fatal("cannot read gateway selected counter")
	}
	var total uint64
	found := false
	for _, line := range strings.Split(string(body), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && strings.HasSuffix(fields[0], "metering_requests_selected_total:") {
			n, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			total += n
			found = true
		}
	}
	if !found {
		t.Fatal("gateway selected counter missing")
	}
	return total
}
func eventsUUID(s string) bool { return observationwire.UUID(s) }

func localStore(t *testing.T, target string) *doris.Store {
	t.Helper()
	t.Setenv("METERING_TEST_DSN", dsn)
	t.Setenv("METERING_TEST_USER", "root")
	t.Setenv("METERING_TEST_PASS", "")
	u, _ := url.Parse(target)
	s, err := doris.New(config.Doris{FE: target, AllowedHosts: []string{u.Host}, DSNEnv: "METERING_TEST_DSN", UsernameEnv: "METERING_TEST_USER", PasswordEnv: "METERING_TEST_PASS", Database: "metering"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.DB.Close() })
	return s
}
func TestStreamLoadConfirmationLoss(t *testing.T) {
	var calls atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(out http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, err := io.ReadAll(io.LimitReader(r.Body, 32768))
		if err != nil {
			t.Error(err)
			return
		}
		req, err := http.NewRequestWithContext(r.Context(), r.Method, "http://127.0.0.1:18040"+r.URL.RequestURI(), bytes.NewReader(body))
		if err != nil {
			t.Error(err)
			return
		}
		req.Header = r.Header.Clone()
		res, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
		if err != nil {
			t.Error(err)
			return
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()
		conn, _, err := out.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
	}))
	defer proxy.Close()
	s := localStore(t, proxy.URL)
	id := uuid(t)
	rows := []map[string]any{{"group_id": "test-confirmation-" + id, "kafka_topic": "llm-metering-events", "kafka_partition": 0, "origin_id": events.Hash([]byte(id)), "initial_offset": 0, "created_at": consumer.Stamp(time.Now())}}
	body := consumer.EncodeRows(rows)
	label := "test_confirmation_" + events.Hash(body)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.Load(ctx, "consumer_partition_origins", label, body, rows); err != nil {
		t.Fatal(err)
	}
	if err := s.Load(ctx, "consumer_partition_origins", label, body, rows); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("visible label was sent %d times", calls.Load())
	}
}

func TestUnknownSchemaPausesPartition(t *testing.T) {
	db := database(t)
	cl := kafka(t)
	id := uuid(t)
	topic := "metering-schema-test-" + id
	group := "schema-test-" + id
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	req := kmsg.NewPtrCreateTopicsRequest()
	req.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}}
	resp, err := req.RequestWith(ctx, cl)
	cancel()
	if err != nil || len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("create local test topic: %v", err)
	}
	s := localStore(t, "http://127.0.0.1:18040")
	c := config.Config{Kafka: config.Kafka{Brokers: []string{"127.0.0.1:29092"}, Topic: topic}, Consumer: config.Consumer{Group: group, AcceptedConfigs: []string{"collection-v1"}, Routes: []config.Route{{RouteName: "llm-chat", ServiceID: "model-service-a"}}}, Doris: s.Cfg}
	if err = c.ConsumerDefaults(); err != nil {
		t.Fatal(err)
	}
	h := &health.State{}
	worker, err := ingestworker.New(c, s, h)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { worker.Run(runCtx); close(done) }()
	defer func() { stop(); <-done }()
	eventually(t, func() bool { return h.Ready.Load() })
	publish(t, cl, topic, 0, "synthetic", []byte(`{}`))
	eventually(t, func() bool {
		var n int
		return db.QueryRow("SELECT COUNT(*) FROM metering.consumer_batch_receipts WHERE kafka_topic=? AND next_offset=1", topic).Scan(&n) == nil && n == 1
	})
	// The version sentinel is deliberately sent after the preceding batch commits.
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	fetch := kmsg.NewPtrOffsetFetchRequest()
	fetch.Group = group
	fetch.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: topic, Partitions: []int32{0}}}
	eventually(t, func() bool {
		r, e := fetch.RequestWith(ctx, cl)
		return e == nil && len(r.Topics) > 0 && r.Topics[0].Partitions[0].Offset == 1
	})
	cancel()
	publish(t, cl, topic, 0, "synthetic", []byte(`{"schema_version":2}`))
	eventually(t, func() bool { return !h.Ready.Load() })
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := fetch.RequestWith(ctx, cl)
	if err != nil || len(r.Topics) == 0 || r.Topics[0].Partitions[0].Offset != 1 {
		t.Fatal("unknown version advanced committed offset")
	}
}

func TestProducerBlackholeIsBounded(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	closed := make(chan struct{})
	defer close(closed)
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() { <-closed; c.Close() }()
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := &health.State{}
	p, err := producer.New(ctx, config.Kafka{Brokers: []string{l.Addr().String()}, Topic: "llm-metering-events"}, h, func() { t.Error("producer failed to close within watchdog budget") })
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	e := &events.Event{LogicalRequestID: uuid(t)}
	start := time.Now()
	for i := 0; i < 1024; i++ {
		p.Emit(e)
	}
	if time.Since(start) > time.Second {
		t.Fatal("Emit blocked on blackhole")
	}
	eventually(t, func() bool {
		r := httptest.NewRecorder()
		h.Handler().ServeHTTP(r, httptest.NewRequest("GET", "/metrics", nil))
		s := r.Body.String()
		return strings.Contains(s, "producer_dropped_total") && (strings.Contains(s, "producer_timeout_total") || strings.Contains(s, "producer_watchdog_total"))
	})
}

type stalledBody struct {
	first, tail []byte
	wait        bool
}

func (r *stalledBody) Read(b []byte) (int, error) {
	if len(r.first) > 0 {
		n := copy(b, r.first)
		r.first = r.first[n:]
		return n, nil
	}
	if !r.wait {
		r.wait = true
		time.Sleep(2200 * time.Millisecond)
	}
	if len(r.tail) > 0 {
		n := copy(b, r.tail)
		r.tail = r.tail[n:]
		return n, nil
	}
	return 0, io.EOF
}
func TestHTTPRewriteBoundariesAndTimeout(t *testing.T) {
	db := database(t)
	prefix, suffix := `{"stream":false,"messages":[{"content":"`, `"}]}`
	for _, tc := range []struct {
		name   string
		size   int
		slow   bool
		stream bool
	}{{"boundary", 262144, false, false}, {"over_limit", 262145, false, false}, {"one_megabyte", 1 << 20, false, false}, {"timeout", 128, true, false}, {"start_ack_header", 128, false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(prefix + strings.Repeat("x", tc.size-len(prefix)-len(suffix)) + suffix)
			if tc.stream {
				body = []byte(`{"model":"fixture","stream":true,"stream_options":{"include_usage":true},"test_case":"slow_response","messages":[]}`)
			}
			var reader io.Reader = bytes.NewReader(body)
			if tc.slow {
				reader = &stalledBody{first: body[:40], tail: body[40:]}
			}
			xid := uuid(t)
			req, _ := http.NewRequest("POST", "http://127.0.0.1:18080/v1/chat/completions", reader)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("x-request-id", xid)
			res, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
			if err != nil {
				t.Fatal(err)
			}
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
			if res.StatusCode != 200 || res.Header.Get("x-test-body-sha256") != events.Hash(body) {
				t.Fatalf("body changed or failed: status=%d", res.StatusCode)
			}
			if tc.stream {
				id := res.Header.Get("x-metering-request-id")
				if !eventsUUID(id) {
					t.Fatal("Start ACK ID missing before deliberately delayed response")
				}
				eventually(t, func() bool {
					var n int
					return db.QueryRow("SELECT COUNT(*) FROM metering.events_raw WHERE logical_request_id=? AND x_request_id=?", id, xid).Scan(&n) == nil && n == 2
				})
			}
			if tc.slow {
				eventually(t, func() bool {
					var status string
					var input sql.NullInt64
					var terminal, eligible int
					return db.QueryRow("SELECT observation_status,input_tokens,terminal_observed,usage_eligible FROM metering.events_raw WHERE x_request_id=? AND event_type='usage_observed'", xid).Scan(&status, &input, &terminal, &eligible) == nil && status != "complete" && !input.Valid && terminal == 0 && eligible == 0
				})
			}
		})
	}
}
