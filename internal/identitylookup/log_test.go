package identitylookup

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/logging"
)

func testResponseLogQueue(t *testing.T, r *remote) func() {
	t.Helper()
	t.Cleanup(r.logger.Close)
	return func() {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if !r.logger.Flush(ctx) {
			t.Fatal("test log writer did not flush")
		}
	}
}

var textLogField = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_]*)=("(?:\\.|[^"\\])*"|[^\s]+)`)

func readTextLog(b []byte, result *map[string]any) bool {
	entry := map[string]any{}
	for _, m := range textLogField.FindAllStringSubmatch(string(b), -1) {
		var value any = m[2]
		if strings.HasPrefix(m[2], `"`) {
			v, err := strconv.Unquote(m[2])
			if err != nil {
				return false
			}
			value = v
		} else if m[2] == "true" || m[2] == "false" {
			value = m[2] == "true"
		} else if n, err := strconv.ParseFloat(m[2], 64); err == nil && m[1] == "http_status" {
			value = n
		}
		entry[m[1]] = value
	}
	*result = entry
	return entry["event"] != nil
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
	w := &blockedResponseLogWriter{entered: make(chan struct{}), release: make(chan struct{})}
	logger := logging.New("observer", w, slog.LevelInfo)
	t.Cleanup(logger.Close)
	var once sync.Once
	unblock := func() { once.Do(func() { close(w.release) }) }
	defer unblock()
	r := &remote{credential: "synthetic-credential", uri: "http://directory.invalid", logger: logger, client: &http.Client{Transport: lookupTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(lookupSuccessJSON(`{"service_id":"s","service_name":"服务","provider_id":"p","provider_name":"供应商"}`)))}, nil
	})}}
	resolve := func() error {
		_, err := (serviceRemote{r}).Resolve(context.Background(), ServiceLookupRequest{})
		return err
	}
	if err := resolve(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-w.entered:
	case <-time.After(time.Second):
		t.Fatal("writer did not block")
	}
	before := DroppedResponseLogs()
	const queries = 2 * logging.Capacity
	done := make(chan error, queries)
	for i := 0; i < queries; i++ {
		go func() { done <- resolve() }()
	}
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for i := 0; i < queries; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-timer.C:
			t.Fatal("blocked logs prevented lookup completion")
		}
	}
	if logger.Dropped() != queries-logging.Capacity || DroppedResponseLogs()-before != queries-logging.Capacity || w.calls.Load() != 1 {
		t.Fatal("logging is unbounded or changed lookup drop accounting")
	}
	unblock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !logger.Flush(ctx) {
		t.Fatal("logs did not recover")
	}
	if err := resolve(); err != nil {
		t.Fatal(err)
	}
	if !logger.Flush(ctx) || w.calls.Load() < logging.Capacity+2 {
		t.Fatal("logs did not resume after writer recovery")
	}
}
