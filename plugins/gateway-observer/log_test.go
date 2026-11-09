package main

import (
	"strings"
	"testing"
	"time"

	"github.com/Fangyan33/llm-token-stats/pkg/logsafe"
)

func TestWasmLoggingLevelsAndNoDebug(t *testing.T) {
	for _, level := range []string{"info", "debug", "warn", "error", "invalid"} {
		t.Run(level, func(t *testing.T) {
			t.Setenv("LOG_LEVEL", level)
			h, r := testHostAndRoot(t, "llm-chat", nil, "istio-system", "gateway-0")
			r.repeatedLog(logsafe.Warn, "test_warning", "action", "passthrough")
			r.repeatedLog(logsafe.Error, "test_failure", "host_status", "not_found")
			r.flushLogs(maxPendingLogs)
			if len(h.GetDebugLogs()) != 0 {
				t.Fatal("Wasm emitted DEBUG")
			}
			if len(h.GetInfoLogs()) > 0 && (level == "warn" || level == "error") {
				t.Fatal("INFO bypassed LOG_LEVEL")
			}
			if len(h.GetInfoLogs()) == 0 && level != "warn" && level != "error" {
				t.Fatal("missing startup INFO")
			}
			if len(h.GetWarnLogs()) > 0 && level == "error" {
				t.Fatal("WARN bypassed LOG_LEVEL")
			}
			if len(h.GetErrorLogs()) != 1 {
				t.Fatal("ERROR not emitted")
			}
		})
	}
}
func TestWasmRepeatedLogsAreBoundedAndSummarized(t *testing.T) {
	h, r := testHostAndRoot(t, "llm-chat", nil, "istio-system", "gateway-0")
	for i := 0; i < 200; i++ {
		r.repeatedLog(logsafe.Error, "external_failure", "host_status", "internal_failure")
	}
	r.flushLogs(maxPendingLogs)
	if len(h.GetErrorLogs()) != 1 {
		t.Fatal("repeated failures flooded host")
	}
	r.logs.repeated["external_failure"].at = time.Now().Add(-31 * time.Second)
	r.flushLogs(maxPendingLogs)
	if !strings.Contains(strings.Join(h.GetErrorLogs(), "\n"), "suppressed_count=\"199\"") {
		t.Fatal("missing repeat summary")
	}
	for i := 0; i < 200; i++ {
		r.log(logsafe.Warn, "capacity_probe")
	}
	if len(r.logs.pending) > maxPendingLogs || r.logs.dropped == 0 {
		t.Fatal("pending logs are unbounded")
	}
}
