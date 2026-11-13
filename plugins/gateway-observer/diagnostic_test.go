package main

import (
	"strings"
	"testing"

	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/types"
)

func TestInvalidConfigurationLogsFieldsAndPassesThrough(t *testing.T) {
	h, r := testHostAndRoot(t, "", func(c map[string]any) {
		c["chunk_bytes"] = 16385
		c["tick_ms"] = 11
		c["observer_cluster"] = ""
	}, "test", "gateway")
	if r.cfg.Enabled {
		t.Fatal("invalid collection configuration stayed enabled")
	}
	id := h.InitializeHttpContext()
	if h.CallOnRequestHeaders(id, [][2]string{{":method", "POST"}, {":path", "/v1/chat/completions"}}, false) != types.ActionContinue || h.CallOnRequestBody(id, []byte("private-body"), false) != types.ActionContinue || h.CallOnResponseBody(id, []byte("private-response"), true) != types.ActionContinue {
		t.Fatal("invalid config blocked business")
	}
	logs := strings.Join(h.GetErrorLogs(), "\n")
	for _, want := range []string{`config_field="observer_cluster"`, `config_field="chunk_bytes"`, `config_field="tick_ms"`, `action="passthrough"`, `error_count="3"`} {
		if !strings.Contains(logs, want) {
			t.Fatalf("missing %s: %s", want, logs)
		}
	}
	if strings.Contains(logs, "private-body") || strings.Contains(logs, "private-response") {
		t.Fatal("body leaked")
	}
}

func TestTransportCloseRetainsGRPCStatus(t *testing.T) {
	h, r := testHostAndRoot(t, "", nil, "test", "gateway")
	selectedRequest(h)
	c := r.states[1].ch
	r.OnClose(c.token, 14)
	r.flushLogs(maxPendingLogs)
	if !strings.Contains(strings.Join(h.GetErrorLogs(), "\n"), `grpc_status="14"`) {
		t.Fatal("gRPC status lost")
	}
}
