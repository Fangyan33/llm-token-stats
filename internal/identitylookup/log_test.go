package identitylookup

import (
	"context"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Each capture gets its own queue. Drain it only in tests, after every producer
// has returned, so assertions on bytes.Buffer cannot race the asynchronous writer.
func testResponseLogQueue(t *testing.T, r *remote) func() {
	t.Helper()
	q := newResponseLogQueue(responseLogCapacity)
	r.logQueue = q
	return finishTestLogQueue(t, q)
}

func finishTestLogQueue(t *testing.T, q *responseLogQueue) func() {
	t.Helper()
	var once sync.Once
	finish := func() {
		t.Helper()
		once.Do(func() { close(q.entries) })
		select {
		case <-q.done:
		case <-time.After(3 * time.Second):
			t.Fatal("test log writer did not stop")
		}
	}
	t.Cleanup(finish)
	return finish
}

type blockedResponseLogWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	calls   atomic.Int64
}

func (w *blockedResponseLogWriter) Write(p []byte) (int, error) {
	w.calls.Add(1)
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(p), nil
}

func TestBlockedResponseLogsDoNotBlockResolvers(t *testing.T) {
	const capacity, queries = 2, 64
	q := newResponseLogQueue(capacity)
	finish := finishTestLogQueue(t, q)
	w := &blockedResponseLogWriter{entered: make(chan struct{}), release: make(chan struct{})}
	// Release before the queue cleanup, even when an assertion fails.
	var once sync.Once
	unblock := func() { once.Do(func() { close(w.release) }) }
	defer unblock()
	logger := log.New(w, "", 0)
	newRemote := func(kind string) *remote {
		return &remote{uri: "http://directory.invalid/" + kind, credential: "synthetic-credential", logger: logger, logQueue: q,
			client: &http.Client{Transport: lookupTransport(func(*http.Request) (*http.Response, error) {
				data := `{"service_id":"s","service_name":"服务","provider_id":"p","provider_name":"供应商"}`
				message := "synthetic-credential"
				if kind == "customer" {
					data, message = `{"api_key_id":"k","customer_id":"c","customer_name":"客户"}`, message+" synthetic-token"
				}
				body := strings.Replace(lookupSuccessJSON(data), "请求处理成功", message, 1)
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
			})}}
	}
	service, customer := serviceRemote{newRemote("service")}, customerRemote{newRemote("customer")}
	resolve := func(kind int) error {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if kind%2 == 0 {
			_, err := service.Resolve(ctx, ServiceLookupRequest{})
			return err
		}
		_, err := customer.Resolve(ctx, []byte("synthetic-token"))
		return err
	}
	first := make(chan error, 1)
	go func() { first <- resolve(0) }()
	select {
	case <-w.entered:
	case <-time.After(time.Second):
		t.Fatal("writer was not entered")
	}
	select {
	case err := <-first:
		if err != nil {
			t.Fatal("completed HTTP result changed by logging", err)
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("resolver waited on a blocked logger past its deadline")
	}
	done := make(chan error, queries)
	for i := 0; i < queries; i++ {
		go func() { done <- resolve(i) }()
	}
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for i := 0; i < queries; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal("queue saturation changed query result", err)
			}
		case <-timer.C:
			t.Fatal("saturated logger blocked concurrent resolvers")
		}
	}
	if len(q.entries) != capacity || q.dropped.Load() != queries-capacity || w.calls.Load() != 1 {
		t.Fatalf("unbounded delivery: queued=%d dropped=%d writes=%d", len(q.entries), q.dropped.Load(), w.calls.Load())
	}
	for i := 0; i < capacity; i++ {
		entry := <-q.entries
		if strings.Contains(entry.line, "synthetic-credential") || strings.Contains(entry.line, "synthetic-token") || !strings.Contains(entry.line, "[REDACTED]") {
			t.Fatal("queue retained an unredacted credential or token")
		}
	}
	// The same worker accepts another entry after recovery; no replacement
	// worker or goroutine per query is needed during the concurrent burst.
	unblock()
	q.submit(responseLogEntry{logger: logger, line: "recovered"})
	finish()
	if w.calls.Load() != 2 {
		t.Fatal("logging did not resume after backpressure cleared")
	}
}
