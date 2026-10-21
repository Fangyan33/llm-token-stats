package identitylookup

import (
	"encoding/json"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"unicode"
	"unicode/utf8"
)

const (
	maxLogMessageBytes  = 1024
	responseLogCapacity = 256
)

type responseLogEntry struct {
	logger *log.Logger
	line   string
}

// Only this single worker may block on the destination. The process-wide queue
// retains at most responseLogCapacity sanitized entries plus the active write.
// It is never drained synchronously by queries or during process shutdown.
type responseLogQueue struct {
	entries chan responseLogEntry
	done    chan struct{}
	dropped atomic.Int64
}

func newResponseLogQueue(capacity int) *responseLogQueue {
	q := &responseLogQueue{entries: make(chan responseLogEntry, capacity), done: make(chan struct{})}
	go func() {
		defer close(q.done)
		for entry := range q.entries {
			entry.logger.Print(entry.line)
		}
	}()
	return q
}

func (q *responseLogQueue) submit(entry responseLogEntry) {
	select {
	case q.entries <- entry:
	default:
		q.dropped.Add(1)
	}
}

var defaultResponseLogs = newResponseLogQueue(responseLogCapacity)

// DroppedResponseLogs counts queue-full losses without using the log output.
func DroppedResponseLogs() int64 { return defaultResponseLogs.dropped.Load() }

type responseRecord struct {
	HTTPStatus   int
	ResponseCode string
	ResponseMsg  string
	Success      *bool
}

// Redact before truncating so a secret crossing the cutoff cannot leak a prefix.
// Longest matches take precedence when the credential and token overlap.
func logValue(value string, secrets ...string) string {
	secrets = append([]string(nil), secrets...)
	sort.Slice(secrets, func(i, j int) bool { return len(secrets[i]) > len(secrets[j]) })
	var replacements []string
	for _, secret := range secrets {
		if secret != "" {
			replacements = append(replacements, secret, "[REDACTED]")
		}
	}
	if len(replacements) != 0 {
		value = strings.NewReplacer(replacements...).Replace(value)
	}
	if len(value) > maxLogMessageBytes {
		end := maxLogMessageBytes
		for !utf8.RuneStart(value[end]) {
			end--
		}
		value = value[:end]
	}
	// JSON escapes C0 controls; escape the remaining Unicode controls too.
	var out strings.Builder
	for _, c := range value {
		if c >= 0x7f && unicode.IsControl(c) {
			quoted := strconv.QuoteRuneToASCII(c)
			out.WriteString(quoted[1 : len(quoted)-1])
		} else {
			out.WriteRune(c)
		}
	}
	return out.String()
}

// Resolve calls this once after payload validation, so partial identities are
// logged as failures. No request body, endpoint, identity fields or raw response
// is retained in the record. The default logger is the Observer's existing log.
func (r *remote) logResponse(kind string, record responseRecord, err error, secrets ...string) {
	secrets = append(secrets, r.credential)
	level, status := "INFO", "mapped"
	if err != nil {
		level, status = "WARN", Failure(err)
	}
	entry := struct {
		Event        string `json:"event"`
		Level        string `json:"level"`
		LookupType   string `json:"lookup_type"`
		HTTPStatus   int    `json:"http_status,omitempty"`
		ResponseCode string `json:"responseCode,omitempty"`
		Success      *bool  `json:"success,omitempty"`
		ResponseMsg  string `json:"responseMsg,omitempty"`
		Status       string `json:"status"`
	}{"identity_lookup_response", level, kind, record.HTTPStatus,
		logValue(record.ResponseCode, secrets...), record.Success,
		logValue(record.ResponseMsg, secrets...), status}
	encoded, _ := json.Marshal(entry)
	logger := r.logger
	if logger == nil {
		logger = log.Default()
	}
	queue := r.logQueue
	if queue == nil {
		queue = defaultResponseLogs
	}
	// Only the already-redacted serialization crosses the asynchronous boundary;
	// never enqueue the record, credential, token, request or a closure over them.
	queue.submit(responseLogEntry{logger: logger, line: string(encoded)})
}
