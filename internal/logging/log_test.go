package logging

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type capture struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (c *capture) Write(p []byte) (int, error) { c.mu.Lock(); defer c.mu.Unlock(); return c.b.Write(p) }
func (c *capture) String() string              { c.mu.Lock(); defer c.mu.Unlock(); return c.b.String() }
func flush(t *testing.T, l *Logger) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !l.Flush(ctx) {
		t.Fatal("flush timed out")
	}
}

func TestLevelFilteringAndTextEscaping(t *testing.T) {
	for _, level := range []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError} {
		t.Run(level.String(), func(t *testing.T) {
			var out capture
			l := New("consumer", &out, level)
			defer l.Close()
			for _, candidate := range []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError} {
				l.Log(candidate, "test_"+candidate.String(), "detail", "line\n\x1b\u0085")
			}
			flush(t, l)
			got := out.String()
			for _, candidate := range []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError} {
				if strings.Contains(got, "event=test_"+candidate.String()) != (candidate >= level) {
					t.Fatal("incorrect level filtering", got)
				}
			}
			if !strings.Contains(got, "component=consumer") || strings.ContainsAny(strings.TrimSuffix(got, "\n"), "\x1b\u0085") {
				t.Fatal("unsafe or missing text fields", got)
			}
		})
	}
}

func TestRepeatedFaultSummaryAndRecovery(t *testing.T) {
	var out capture
	l := New("observer", &out, slog.LevelInfo)
	defer l.Close()
	for i := 0; i < 4; i++ {
		l.Repeat(slog.LevelError, "external_failed", "dependency", "error_code", "refused", "attempt", i)
	}
	flush(t, l)
	if strings.Count(out.String(), "\n") != 1 {
		t.Fatal("first error was not immediate or repeats flooded")
	}
	// Summarize without another failure arriving, as the periodic worker does.
	l.summaries(time.Now().Add(Window), false)
	l.Recovered("dependency", "external_recovered")
	l.Repeat(slog.LevelError, "external_failed", "dependency", "error_code", "refused")
	flush(t, l)
	got := out.String()
	if strings.Count(got, "\n") != 4 || !strings.Contains(got, "event=external_failed_summary") || !strings.Contains(got, "suppressed_count=3") || !strings.Contains(got, "external_recovered") {
		t.Fatal(got)
	}
}

type blocked struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blocked) Write(p []byte) (int, error) {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return len(p), nil
}
func TestBlockedOutputIsBoundedAndShutdownHasDeadline(t *testing.T) {
	w := &blocked{make(chan struct{}), make(chan struct{}), sync.Once{}}
	l := newLogger("observer", w, slog.LevelDebug, 2, Window)
	defer close(w.release)
	l.Log(slog.LevelInfo, "first")
	select {
	case <-w.entered:
	case <-time.After(time.Second):
		t.Fatal("writer not entered")
	}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			l.Log(slog.LevelError, "fault", "safe", i)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("hot path waited for writer")
	}
	if l.Dropped() != 98 {
		t.Fatal("queue did not bound pending logs", l.Dropped())
	}
	started := time.Now()
	l.Close()
	if time.Since(started) > 600*time.Millisecond {
		t.Fatal("shutdown waited indefinitely")
	}
}

func TestErrorDiagnosticsExcludeURLsCredentialsAndSQLValues(t *testing.T) {
	const secret = "PRIVATE-ENDPOINT-AND-SECRET"
	for _, err := range []error{
		&url.Error{Op: "Post", URL: "https://user:" + secret + "@" + secret + "/?token=" + secret, Err: errors.New(secret)},
		&mysql.MySQLError{Number: 1045, SQLState: [5]byte{'2', '8', '0', '0', '0'}, Message: secret},
		&kerr.Error{Code: 29, Message: secret},
		&net.OpError{Op: "dial", Net: "tcp", Addr: &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 1234}, Err: syscall.ECONNREFUSED},
		&net.DNSError{Name: secret, Err: secret, IsNotFound: true},
		WithCause("safe_code", errors.New(secret)),
	} {
		var out capture
		l := New("observer", &out, slog.LevelDebug)
		l.Log(slog.LevelError, "fault", ErrorFields(err)...)
		flush(t, l)
		l.Close()
		if strings.Contains(out.String(), secret) {
			t.Fatal("unsafe error string was logged")
		}
		if _, ok := err.(*net.OpError); ok && !strings.Contains(out.String(), "connection refused") {
			t.Fatal("lost useful network cause", out.String())
		}
	}
}

func TestKafkaHooksCaptureRetriedConnectionAndProtocolFailures(t *testing.T) {
	var out capture
	l := New("consumer", &out, slog.LevelInfo)
	defer l.Close()
	restore := SetDefault(l)
	defer restore()
	h := KafkaHooks{}
	meta := kgo.BrokerMetadata{Host: "broker.invalid", Port: 9092}
	h.OnBrokerConnect(meta, time.Millisecond, nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED})
	h.OnBrokerConnect(kgo.BrokerMetadata{Host: "other-broker.invalid", Port: 9092}, time.Millisecond, nil, nil)
	flush(t, l)
	if strings.Contains(out.String(), "kafka_connection_recovered") {
		t.Fatal("a different broker incorrectly cleared the fault")
	}
	h.OnBrokerE2E(meta, 1, kgo.BrokerE2E{ReadErr: io.ErrUnexpectedEOF})
	h.OnBrokerConnect(meta, time.Millisecond, nil, nil)
	flush(t, l)
	for _, want := range []string{"kafka_connect_failed", "connection refused", "kafka_request_failed", "unexpected_eof", "kafka_connection_recovered"} {
		if !strings.Contains(out.String(), want) {
			t.Fatal("missing diagnostic", want, out.String())
		}
	}
}

func TestKafkaDeliveryFailureCauses(t *testing.T) {
	for _, tc := range []struct {
		err   error
		cause string
	}{
		{kgo.ErrRecordTimeout, "kafka_record_timeout"},
		{kgo.ErrMaxBuffered, "kafka_buffer_full"},
		{kgo.ErrClientClosed, "kafka_client_closed"},
		{&net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}, "network_timeout"},
	} {
		var out capture
		l := New("observer", &out, slog.LevelError)
		l.Log(slog.LevelError, "producer_delivery_failed", ErrorFields(tc.err)...)
		flush(t, l)
		l.Close()
		if !strings.Contains(out.String(), "error="+tc.cause) {
			t.Fatal("lost Kafka delivery cause", out.String())
		}
	}
}
