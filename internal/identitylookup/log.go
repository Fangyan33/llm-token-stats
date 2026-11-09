package identitylookup

import (
	"log/slog"
	"sync/atomic"

	"github.com/Fangyan33/llm-token-stats/internal/logging"
	"github.com/Fangyan33/llm-token-stats/pkg/logsafe"
)

const (
	maxLogMessageBytes = 1024
)

var droppedResponseLogs atomic.Int64

// DroppedResponseLogs counts queue-full losses without using the log output.
func DroppedResponseLogs() int64 { return droppedResponseLogs.Load() }

type responseRecord struct {
	HTTPStatus   int
	ResponseCode string
	ResponseMsg  string
	Success      *bool
	ElapsedMS    int64
	Diagnostic   []any
}

// Redact before truncating so a secret crossing the cutoff cannot leak a prefix.
// Longest matches take precedence when the credential and token overlap.
func logValue(value string, secrets ...string) string { return logsafe.Clean(value, secrets...) }

// Resolve calls this once after payload validation, so partial identities are
// logged as failures. No request body, endpoint, identity fields or raw response
// is retained in the record. The default logger is the Observer's existing log.
func (r *remote) logResponse(kind string, record responseRecord, err error, secrets ...string) {
	secrets = append(secrets, r.credential)
	logger := r.logger
	if logger == nil {
		logger = logging.Default()
	}
	fields := []any{"lookup_type", kind, "duration_ms", record.ElapsedMS}
	if record.HTTPStatus != 0 {
		fields = append(fields, "http_status", record.HTTPStatus)
	}
	if record.ResponseCode != "" {
		fields = append(fields, "responseCode", logValue(record.ResponseCode, secrets...))
	}
	if record.Success != nil {
		fields = append(fields, "success", *record.Success)
	}
	if record.ResponseMsg != "" {
		fields = append(fields, "responseMsg", logValue(record.ResponseMsg, secrets...))
	}
	fields = append(fields, record.Diagnostic...)
	accepted := true
	if err != nil {
		fields = append(fields, "status", Failure(err), "error_code", Failure(err))
		accepted = logger.Repeat(slog.LevelError, "identity_lookup_response", "lookup/"+kind, fields...)
	} else {
		logger.Recovered("lookup/"+kind, "identity_lookup_recovered", "lookup_type", kind)
		accepted = logger.Log(slog.LevelInfo, "identity_lookup_response", append(fields, "status", "mapped")...)
	}
	// Only the already-redacted serialization crosses the asynchronous boundary;
	// never enqueue the record, credential, token, request or a closure over them.
	if !accepted {
		droppedResponseLogs.Add(1)
	}
}
