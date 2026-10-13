package main

import (
	"encoding/json"
	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	w "github.com/Fangyan33/llm-token-stats/pkg/observationwire"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/proxytest"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/types"
	"google.golang.org/protobuf/proto"
	"os"
	"strings"
	"testing"
	"time"
)

func testHost(t *testing.T) proxytest.HostEmulator {
	t.Helper()
	proxytest.ResetGrpc()
	b, err := os.ReadFile("../../configs/gateway-observer.example.json")
	if err != nil {
		t.Fatal(err)
	}
	var c map[string]any
	json.Unmarshal(b, &c)
	c["enabled"] = true
	c["gateway_instance_id"] = "test-gateway"
	b, _ = json.Marshal(c)
	host, reset := proxytest.NewHostEmulator(proxytest.NewEmulatorOption().WithVMContext(&vm{}).WithPluginConfiguration(b).WithProperty([]string{"route_name"}, []byte("llm-chat")))
	t.Cleanup(reset)
	if host.StartVM() != types.OnVMStartStatusOK || host.StartPlugin() != types.OnPluginStartStatusOK {
		t.Fatal("start")
	}
	return host
}

func TestRewriteTimeoutResumesAndStopsAmbiguousObservation(t *testing.T) {
	h := testHost(t)
	id := h.InitializeHttpContext()
	h.CallOnRequestHeaders(id, [][2]string{{":method", "POST"}, {":path", "/v1/chat/completions"}, {"content-type", "application/json"}}, false)
	prefix := []byte(`{"stream":false,`)
	calls := proxytest.GrpcCalls()
	a := w.String(nil, 1, "7f6c2a90-4b13-4b68-9d21-67283e0c5f44")
	a = w.UInt(a, 2, 1)
	a = w.String(a, 4, "accepted")
	proxytest.EmitGrpcReceive(proxytest.PluginContextID, calls[0].Token, a)
	if h.CallOnRequestBody(id, prefix, false) != types.ActionPause {
		t.Fatal("body did not pause")
	}
	time.Sleep(2100 * time.Millisecond)
	h.Tick()
	if h.GetCurrentHttpStreamAction(id) != types.ActionContinue {
		t.Fatal("timeout did not resume")
	}
	tail := []byte(`"messages":[]}`)
	if h.CallOnRequestBody(id, tail, true) != types.ActionContinue {
		t.Fatal("tail did not continue")
	}
	var seen []byte
	for _, b := range proxytest.GrpcCalls()[0].Messages {
		var f pb.ObservationFrame
		if proto.Unmarshal(b, &f) != nil {
			t.Fatal("wire")
		}
		if c := f.GetBodyChunk(); c != nil && c.Direction == pb.Direction_REQUEST {
			seen = append(seen, c.Data...)
		}
	}
	if string(seen) != string(prefix) || !calls[0].Cancelled {
		t.Fatalf("prefix duplicated or lost: bytes=%d cancelled=%t", len(seen), calls[0].Cancelled)
	}
}
func TestFullRequestBoundaryAndControlReserve(t *testing.T) {
	h := testHost(t)
	id := h.InitializeHttpContext()
	h.CallOnRequestHeaders(id, [][2]string{{":method", "POST"}, {":path", "/v1/chat/completions"}, {"content-type", "application/json"}}, false)
	prefix, suffix := `{"stream":false,"messages":[{"content":"`, `"}]}`
	body := []byte(prefix + strings.Repeat("x", 262144-len(prefix)-len(suffix)) + suffix)
	if h.CallOnRequestBody(id, body, true) != types.ActionContinue {
		t.Fatal("large request paused")
	}
	if string(h.GetCurrentRequestBody(id)) != string(body) {
		t.Fatal("non-stream body changed")
	}
	calls := proxytest.GrpcCalls()
	if len(calls) != 1 || len(calls[0].Messages) != 17 || calls[0].Cancelled {
		t.Fatal("full request did not fit reserved budget")
	}
	a := w.String(nil, 1, "7f6c2a90-4b13-4b68-9d21-67283e0c5f44")
	a = w.UInt(a, 2, 1)
	a = w.String(a, 4, "accepted")
	proxytest.EmitGrpcReceive(proxytest.PluginContextID, calls[0].Token, a)
	h.CallOnResponseHeaders(id, [][2]string{{":status", "200"}, {"content-type", "application/json"}, {"x-metering-request-id", "forged"}, {"x-metering-request-id", "second"}}, true)
	var values []string
	for _, p := range h.GetCurrentResponseHeaders(id) {
		if p[0] == "x-metering-request-id" {
			values = append(values, p[1])
		}
	}
	if len(values) != 1 || values[0] != "7f6c2a90-4b13-4b68-9d21-67283e0c5f44" {
		t.Fatalf("response ID not replaced: values=%v cancelled=%t", values, calls[0].Cancelled)
	}
	h.CompleteHttpContext(id)
	if !calls[0].End[len(calls[0].End)-1] {
		t.Fatal("End did not fit")
	}
}
func TestIgnoreBeforeAllocation(t *testing.T) {
	h := testHost(t)
	id := h.InitializeHttpContext()
	h.CallOnRequestHeaders(id, [][2]string{{":method", "GET"}, {":path", "/v1/chat/completions"}, {"content-type", "application/json"}}, false)
	h.CallOnRequestBody(id, []byte("invalid"), true)
	if len(proxytest.GrpcCalls()) != 0 {
		t.Fatal("ignored opened grpc")
	}
}
func TestRewriteEOSAndLateACK(t *testing.T) {
	h := testHost(t)
	id := h.InitializeHttpContext()
	if h.CallOnRequestHeaders(id, [][2]string{{":method", "POST"}, {":path", "/v1/chat/completions?x=y"}, {"content-type", "application/json"}, {"content-length", "1"}}, false) != types.ActionContinue {
		t.Fatal("headers not continued")
	}
	h.CallOnRequestBody(id, []byte(`{"stream":true,"messages":[],"n":9007199254740993}`), true)
	calls := proxytest.GrpcCalls()
	if len(calls) != 1 || len(calls[0].Messages) != 2 {
		t.Fatal("frames", len(calls))
	}
	if string(h.GetCurrentRequestBody(id)) != `{"stream":true,"messages":[],"n":9007199254740993,"stream_options":{"include_usage":true}}` {
		t.Fatal("rewrite")
	}
	h.CallOnResponseHeaders(id, [][2]string{{":status", "200"}, {"content-type", "text/event-stream"}}, false)
	h.CallOnResponseBody(id, []byte("data: [DONE]\n\n"), false)
	h.CallOnResponseTrailers(id, [][2]string{{"secret", "ignored"}})
	last := calls[0].Messages[len(calls[0].Messages)-1]
	fs, _ := w.Read(last)
	if fs[len(fs)-1].N != 12 {
		t.Fatal("not EOS frame")
	}
	h.CompleteHttpContext(id)
	if !calls[0].End[len(calls[0].End)-1] {
		t.Fatal("not half closed")
	}
	a := w.String(nil, 1, "7f6c2a90-4b13-4b68-9d21-67283e0c5f44")
	a = w.UInt(a, 2, uint64(len(calls[0].Messages)))
	a = w.UInt(a, 3, 1)
	a = w.String(a, 4, "finalized")
	proxytest.EmitGrpcReceive(proxytest.PluginContextID, calls[0].Token, a)
	proxytest.EmitGrpcReceive(proxytest.PluginContextID, calls[0].Token, a)
	if !calls[0].Cancelled {
		t.Fatal("not released")
	}
}
