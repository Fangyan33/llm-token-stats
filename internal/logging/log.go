// Package logging provides bounded, asynchronous process logs. Callers supply
// metadata only: bodies, credentials, DSNs and SQL must never be log arguments.
package logging

import (
	"bytes"
	"context"
	"io"
	"log"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
	"github.com/Fangyan33/llm-token-stats/pkg/logsafe"
)

const (
	Capacity     = 256
	Window       = 30 * time.Second
	FlushTimeout = 200 * time.Millisecond
	maxRepeats   = 256
)

type entry struct {
	line    string
	barrier chan struct{}
}
type repeated struct {
	level        slog.Level
	event, scope string
	attrs        []slog.Attr // Sanitized scalar values only; never errors or closures.
	at           time.Time
	count        int64
}
type Logger struct {
	component string
	level     slog.Level
	entries   chan entry
	dropped   atomic.Int64
	closed    atomic.Bool
	stop      chan struct{}
	once      sync.Once
	mu        sync.Mutex
	repeats   map[string]*repeated
	window    time.Duration
}

func New(component string, out io.Writer, level slog.Level) *Logger {
	return newLogger(component, out, level, Capacity, Window)
}
func newLogger(component string, out io.Writer, level slog.Level, capacity int, window time.Duration) *Logger {
	l := &Logger{component: component, level: level, entries: make(chan entry, capacity), stop: make(chan struct{}), repeats: map[string]*repeated{}, window: window}
	go func() {
		for {
			select {
			case e := <-l.entries:
				if e.barrier != nil {
					close(e.barrier)
				} else if _, err := io.WriteString(out, e.line); err != nil {
					l.dropped.Add(1)
				}
			case <-l.stop:
				return
			}
		}
	}()
	go func() {
		ticker := time.NewTicker(min(window, time.Second))
		defer ticker.Stop()
		var reported int64
		for {
			select {
			case now := <-ticker.C:
				l.summaries(now, false)
				if n := l.Dropped(); n > reported && l.Log(slog.LevelWarn, "logs_dropped", "dropped_count", n) {
					reported = n
				}
			case <-l.stop:
				return
			}
		}
	}()
	return l
}

func (l *Logger) Enabled(level slog.Level) bool { return level >= l.level && !l.closed.Load() }
func (l *Logger) Dropped() int64                { return l.dropped.Load() }
func safeAttrs(fields []any) []slog.Attr {
	attrs := make([]slog.Attr, 0, min(len(fields)/2, 24))
	for i := 0; i+1 < len(fields) && len(attrs) < 24; i += 2 {
		key, ok := fields[i].(string)
		if !ok {
			continue
		}
		var a slog.Attr
		switch v := fields[i+1].(type) {
		case string:
			a = slog.String(key, logsafe.Clean(v))
		case bool:
			a = slog.Bool(key, v)
		case int:
			a = slog.Int(key, v)
		case int32:
			a = slog.Int64(key, int64(v))
		case int64:
			a = slog.Int64(key, v)
		case uint32:
			a = slog.Uint64(key, uint64(v))
		case uint64:
			a = slog.Uint64(key, v)
		case time.Duration:
			a = slog.String(key, v.String())
		default:
			continue // Never call String or Error on arbitrary values.
		}
		replaced := false
		for n := range attrs {
			if attrs[n].Key == key {
				attrs[n] = a
				replaced = true
				break
			}
		}
		if !replaced {
			attrs = append(attrs, a)
		}
	}
	return attrs
}
func (l *Logger) emit(level slog.Level, event string, attrs []slog.Attr) bool {
	if !l.Enabled(level) {
		return true
	}
	var b bytes.Buffer
	h := slog.NewTextHandler(&b, &slog.HandlerOptions{Level: l.level, ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == slog.MessageKey {
			a.Key = "event"
		}
		return a
	}})
	r := slog.NewRecord(time.Now().UTC(), level, event, 0)
	r.AddAttrs(slog.String("component", l.component))
	r.AddAttrs(attrs...)
	_ = h.Handle(context.Background(), r)
	select {
	case l.entries <- entry{line: b.String()}:
		return true
	default:
		l.dropped.Add(1)
		return false
	}
}

// Log returns false only if an enabled log could not enter the bounded queue.
func (l *Logger) Log(level slog.Level, event string, fields ...any) bool {
	if !l.Enabled(level) {
		return true
	}
	return l.emit(level, event, safeAttrs(fields))
}

// Repeat logs the first occurrence immediately; later occurrences are counted
// and summarized even if no subsequent call arrives. Scope is a bounded label
// such as dependency/operation, never a request or observation identifier.
func (l *Logger) Repeat(level slog.Level, event, scope string, fields ...any) bool {
	if !l.Enabled(level) {
		return true
	}
	attrs := safeAttrs(fields)
	key := scope + "/" + event
	for _, a := range attrs {
		if a.Key == "error_code" || a.Key == "error" || a.Key == "reason" || a.Key == "phase" || a.Key == "limit_kind" {
			key += "/" + a.Key + "=" + a.Value.String()
		}
	}
	l.mu.Lock()
	if _, ok := l.repeats[key]; !ok && len(l.repeats) >= maxRepeats {
		key = "overflow"
	}
	if r := l.repeats[key]; r != nil {
		r.count++
		r.attrs = attrs
		l.mu.Unlock()
		return true
	}
	l.repeats[key] = &repeated{level: level, event: event, scope: scope, attrs: attrs, at: time.Now()}
	l.mu.Unlock()
	return l.emit(level, event, attrs)
}
func (l *Logger) summaries(now time.Time, force bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, r := range l.repeats {
		if r.count > 0 && (force || now.Sub(r.at) >= l.window) {
			attrs := append(append([]slog.Attr(nil), r.attrs...), slog.Int64("suppressed_count", r.count))
			if l.emit(r.level, r.event+"_summary", attrs) {
				r.count = 0
				r.at = now
			}
		}
	}
}
func (l *Logger) Recovered(scope, event string, fields ...any) {
	l.mu.Lock()
	found := false
	for key, r := range l.repeats {
		if r.scope == scope {
			found = true
			if r.count > 0 {
				l.emit(r.level, r.event+"_summary", append(append([]slog.Attr(nil), r.attrs...), slog.Int64("suppressed_count", r.count)))
			}
			delete(l.repeats, key)
		}
	}
	l.mu.Unlock()
	if found {
		l.Log(slog.LevelInfo, event, fields...)
	}
}

func (l *Logger) Flush(ctx context.Context) bool {
	done := make(chan struct{})
	select {
	case l.entries <- entry{barrier: done}:
	case <-ctx.Done():
		return false
	}
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}
func (l *Logger) Close() {
	l.once.Do(func() {
		l.summaries(time.Now(), true)
		l.closed.Store(true)
		ctx, cancel := context.WithTimeout(context.Background(), FlushTimeout)
		defer cancel()
		l.Flush(ctx)
		close(l.stop)
	})
}

// The unconfigured default follows log.SetOutput for embedders and tests.
type defaultWriter struct{}

func (defaultWriter) Write(b []byte) (int, error) { return log.Writer().Write(b) }

var current atomic.Pointer[Logger]

func init()            { current.Store(New("observer", defaultWriter{}, slog.LevelInfo)) }
func Default() *Logger { return current.Load() }
func SetDefault(l *Logger) func() {
	previous := current.Swap(l)
	return func() { current.Store(previous) }
}
func Init(component string) {
	level, valid := logsafe.ParseLevel(os.Getenv("LOG_LEVEL"))
	l := New(component, os.Stderr, slog.Level(level))
	old := current.Swap(l)
	old.Close()
	if !valid {
		l.Log(slog.LevelWarn, "invalid_log_level", "config_field", "LOG_LEVEL", "reason", "invalid_enum", "expected", "debug, info, warn or error", "fallback", "info")
	}
	Info("starting", "log_level", level.String())
}
func Shutdown()                         { Default().Close() }
func Info(event string, fields ...any)  { Default().Log(slog.LevelInfo, event, fields...) }
func Debug(event string, fields ...any) { Default().Log(slog.LevelDebug, event, fields...) }
func Warn(event, scope string, fields ...any) {
	Default().Repeat(slog.LevelWarn, event, scope, fields...)
}
func Error(event string, err error, fields ...any) {
	if r := configdiag.Find(err); r != nil {
		for _, issue := range r.Issues {
			pairs := issue.Fields()
			detail := []any{"config_source", r.Source}
			for n := 0; n+1 < len(pairs); n += 2 {
				detail = append(detail, pairs[n], pairs[n+1])
			}
			detail = append(detail, ErrorFields(issue.Cause)...)
			Default().Log(slog.LevelError, "configuration_validation_failed", detail...)
		}
		code := "config_invalid"
		if len(r.Issues) > 0 {
			code = r.Issues[0].Code
		}
		if outer, ok := err.(*diagnostic); ok {
			code = outer.code
		}
		Default().Log(slog.LevelError, event, append(fields, "error_code", code, "config_source", r.Source, "error_count", r.Total, "omitted_count", r.Total-len(r.Issues))...)
		return
	}
	Default().Log(slog.LevelError, event, append(fields, ErrorFields(err)...)...)
}
func Failure(event, scope string, err error, fields ...any) {
	if errorsCanceled(err) {
		Debug(event+"_canceled", fields...)
		return
	}
	Default().Repeat(slog.LevelError, event, scope, append(fields, ErrorFields(err)...)...)
}
func Recovered(scope, event string, fields ...any) { Default().Recovered(scope, event, fields...) }
