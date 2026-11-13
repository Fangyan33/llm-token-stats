package main

import (
	"strconv"
	"strings"
	"time"

	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
	"github.com/Fangyan33/llm-token-stats/pkg/logsafe"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/types"
)

const maxPendingLogs = 64

type wasmLog struct {
	level logsafe.Level
	text  string
}
type wasmRepeat struct {
	level  logsafe.Level
	event  string
	fields []string
	at     time.Time
	count  int
}
type wasmLogger struct {
	level    logsafe.Level
	pending  []wasmLog
	repeated map[string]*wasmRepeat
	dropped  int
}

func (r *root) initLogging() {
	level, valid := logsafe.ParseLevel(logLevelEnvironment())
	if level < logsafe.Info {
		level = logsafe.Info
	} // This plugin has no DEBUG events.
	r.logs.level = level
	if r.logs.repeated == nil {
		r.logs.repeated = map[string]*wasmRepeat{}
	}
	if !valid {
		r.log(logsafe.Warn, "invalid_log_level", "config_field", "LOG_LEVEL", "reason", "invalid_enum", "expected", "debug, info, warn or error", "fallback", "info")
	}
}
func (r *root) log(level logsafe.Level, event string, fields ...string) {
	if level < r.logs.level {
		return
	}
	if len(r.logs.pending) >= maxPendingLogs {
		r.logs.dropped++
		return
	}
	var b strings.Builder
	b.WriteString("level=" + level.String() + " component=wasm event=" + event)
	for i := 0; i+1 < len(fields) && i < 32; i += 2 {
		b.WriteByte(' ')
		b.WriteString(fields[i])
		b.WriteByte('=')
		b.WriteString(strconv.Quote(logsafe.Clean(fields[i+1])))
	}
	r.logs.pending = append(r.logs.pending, wasmLog{level, b.String()})
}
func (r *root) logConfiguration(err error) {
	if report := configdiag.Find(err); report != nil {
		for _, issue := range report.Issues {
			r.log(logsafe.Error, "configuration_validation_failed", append(issue.Fields(), "config_source", "plugin_configuration", "action", "passthrough")...)
		}
		r.log(logsafe.Error, "plugin_configuration_failed", "error_count", strconv.Itoa(report.Total), "omitted_count", strconv.Itoa(report.Total-len(report.Issues)), "action", "passthrough")
		return
	}
	r.log(logsafe.Error, "plugin_configuration_failed", "reason", "configuration_read_failed", "host_status", hostError(err), "action", "passthrough")
}
func (r *root) repeatedLog(level logsafe.Level, event string, fields ...string) {
	if level < r.logs.level {
		return
	}
	if r.logs.repeated == nil {
		r.logs.repeated = map[string]*wasmRepeat{}
	}
	// Events are fixed call-site labels, never request IDs or dynamic error text.
	key := repeatKey(event, fields)
	if previous := r.logs.repeated[key]; previous != nil {
		previous.count++
		previous.fields = append(previous.fields[:0], fields...)
		return
	}
	if len(r.logs.repeated) >= 64 {
		r.logs.dropped++
		return
	}
	r.logs.repeated[key] = &wasmRepeat{level: level, event: event, fields: append([]string(nil), fields...), at: time.Now()}
	r.log(level, event, fields...)
}
func repeatKey(event string, fields []string) string {
	key := event
	for n := 0; n+1 < len(fields); n += 2 {
		switch fields[n] {
		case "reason", "phase", "limit_kind", "metric", "transport_slot":
			key += "/" + fields[n+1]
		}
	}
	return key
}
func (r *root) logRecovered(event string) {
	found := false
	for key, previous := range r.logs.repeated {
		if previous.event == event {
			r.logSummary(previous)
			delete(r.logs.repeated, key)
			found = true
		}
	}
	if found {
		r.log(logsafe.Info, event+"_recovered")
	}
}
func (r *root) logTransportRecovered(event string, c *channel) {
	if len(r.logs.repeated) == 0 {
		return
	}
	slot, found := r.transportSlot(c), false
	for key, previous := range r.logs.repeated {
		if previous.event != event {
			continue
		}
		for n := 0; n+1 < len(previous.fields); n += 2 {
			if previous.fields[n] == "transport_slot" && previous.fields[n+1] == slot {
				r.logSummary(previous)
				delete(r.logs.repeated, key)
				found = true
				break
			}
		}
	}
	if found {
		r.log(logsafe.Info, event+"_recovered", "transport_slot", slot)
	}
}
func (r *root) transportSlot(c *channel) string {
	for n, candidate := range r.channels {
		if candidate == c {
			return strconv.Itoa(n)
		}
	}
	return "unassigned"
}
func (r *root) logSummary(previous *wasmRepeat) {
	if previous.count > 0 {
		r.log(previous.level, previous.event+"_summary", append(previous.fields, "suppressed_count", strconv.Itoa(previous.count))...)
		previous.count = 0
	}
	previous.at = time.Now()
}
func (r *root) flushLogs(limit int) {
	for _, previous := range r.logs.repeated {
		if time.Since(previous.at) >= 30*time.Second {
			r.logSummary(previous)
		}
	}
	if r.logs.dropped > 0 && len(r.logs.pending) < maxPendingLogs {
		r.log(logsafe.Warn, "logs_dropped", "dropped_count", strconv.Itoa(r.logs.dropped))
		r.logs.dropped = 0
	}
	for i := 0; i < limit && len(r.logs.pending) > 0; i++ {
		e := r.logs.pending[0]
		r.logs.pending[0] = wasmLog{}
		r.logs.pending = r.logs.pending[1:]
		switch e.level {
		case logsafe.Error:
			proxywasm.LogError(e.text)
		case logsafe.Warn:
			proxywasm.LogWarn(e.text)
		default:
			proxywasm.LogInfo(e.text)
		}
	}
}
func hostError(err error) string {
	switch err {
	case nil:
		return "none"
	case types.ErrorStatusNotFound:
		return "not_found"
	case types.ErrorStatusBadArgument:
		return "bad_argument"
	case types.ErrorStatusEmpty:
		return "empty"
	case types.ErrorStatusCasMismatch:
		return "cas_mismatch"
	case types.ErrorInternalFailure:
		return "internal_failure"
	case types.ErrorUnimplemented:
		return "unimplemented"
	default:
		return "host_error"
	}
}
