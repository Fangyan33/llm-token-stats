package main

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"strconv"
	"testing"
	"time"

	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	w "github.com/Fangyan33/llm-token-stats/pkg/observationwire"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/proxytest"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/types"
	"google.golang.org/protobuf/proto"
)

func selectedRequest(h proxytest.HostEmulator) uint32 {
	id := h.InitializeHttpContext()
	h.CallOnRequestHeaders(id, [][2]string{{":method", "POST"}, {":path", "/v1/chat/completions"}, {":authority", "model.example.invalid"}, {"content-type", "application/json"}}, false)
	return id
}
func TestRequestBodyNeverReadOrPaused(t *testing.T) {
	for _, size := range []int{29, 262145, 1 << 20, 32 << 20, 32<<20 + 1} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			h, r := testHostAndRoot(t, "", nil, "test", "gateway")
			id := selectedRequest(h)
			before := r.inflight
			for _, op := range []string{"read_request_body", "replace_request_body", "continue_request"} {
				proxytest.FailNextHostCalls(op, 1)
			}
			body := bytes.Repeat([]byte("x"), size)
			if h.CallOnRequestBody(id, body, true) != types.ActionContinue || !bytes.Equal(h.GetCurrentRequestBody(id), body) {
				t.Fatal("body changed or paused")
			}
			for _, op := range []string{"read_request_body", "replace_request_body", "continue_request"} {
				if proxytest.RemainingHostFailures(op) != 1 {
					t.Fatalf("forbidden hostcall %s", op)
				}
			}
			if r.inflight != before || len(r.states) != 1 {
				t.Fatal("request size consumed observation budget")
			}
			for _, call := range captureCalls(t) {
				for _, raw := range call.Messages {
					var f pb.ObservationFrame
					proto.Unmarshal(raw, &f)
					if f.GetBodyChunk() != nil {
						t.Fatal("request body entered transport")
					}
				}
			}
		})
	}
}
func TestCollectionFaultsNeverPauseBusiness(t *testing.T) {
	for _, failure := range []string{"grpc_open", "grpc_send", "define_metric", "increment_metric", "tick_period"} {
		t.Run(failure, func(t *testing.T) {
			h, r := testHostAndRoot(t, "", nil, "test", "gateway")
			proxytest.FailNextHostCalls(failure, 1000)
			if failure == "tick_period" {
				h.StartPlugin()
			}
			id := selectedRequest(h)
			body := []byte(`{"stream":true,"stream_options":{"include_usage":false}}`)
			if h.CallOnRequestBody(id, body, true) != types.ActionContinue || !bytes.Equal(h.GetCurrentRequestBody(id), body) {
				t.Fatal("fault changed business request")
			}
			if h.CallOnResponseHeaders(id, [][2]string{{":status", "200"}, {"content-type", "text/event-stream"}}, false) != types.ActionContinue {
				t.Fatal("headers blocked")
			}
			if h.CallOnResponseBody(id, []byte("data: [DONE]\n\n"), true) != types.ActionContinue {
				t.Fatal("response blocked")
			}
			h.CompleteHttpContext(id)
			h.Tick()
			r.cleanup()
			if len(r.states) != 0 || r.inflight != 0 || r.messages != 0 || len(r.q) != 0 {
				t.Fatal("fault retained resources")
			}
		})
	}
}
func TestMultiplexedBatchesAndCumulativeACK(t *testing.T) {
	h, r := testHostAndRoot(t, "", nil, "test", "gateway")
	for i := 0; i < 600; i++ {
		selectedRequest(h)
	}
	h.Tick()
	calls := proxytest.GrpcCalls()
	if len(calls) != 4 || len(r.states) != 600 {
		t.Fatalf("channels=%d observations=%d", len(calls), len(r.states))
	}
	seen := map[uint64]bool{}
	for _, c := range calls {
		var seq uint64
		for i, raw := range c.Messages {
			if len(raw) > 32768 || w.ValidateBatch(raw, false) != nil || c.End[i] {
				t.Fatal("invalid/half-closed batch")
			}
			var b pb.ObservationBatch
			proto.Unmarshal(raw, &b)
			if b.BatchSeq != seq+1 || len(b.Frames) > 128 {
				t.Fatal("batch sequence/bound")
			}
			seq = b.BatchSeq
			for _, f := range b.Frames {
				if seen[f.CaptureId] {
					t.Fatal("duplicate capture key")
				}
				seen[f.CaptureId] = true
			}
		}
		raw, _ := proto.Marshal(&pb.ObservationAckBatch{AcceptedBatchSeq: seq})
		proxytest.EmitGrpcReceive(proxytest.PluginContextID, c.Token, raw)
	}
	if len(seen) != 600 || r.inflight != 0 || r.messages != 0 {
		t.Fatal("cumulative ACK did not release writes")
	}
	s := r.states[1]
	raw, _ := proto.Marshal(&pb.ObservationAck{ObservationId: "7f6c2a90-4b13-4b68-9d21-67283e0c5f44", AcceptedFrameSeq: 1, CloseStream: true, Reason: "finalized"})
	emitCaptureAck(t, s.ch.token, 1, raw)
	if len(r.states) != 599 {
		t.Fatal("close affected unrelated observations")
	}
	for _, c := range calls {
		if c.Cancelled {
			t.Fatal("capture close cancelled shared channel")
		}
	}
	// Duplicate and late ACKs are harmless.
	emitCaptureAck(t, s.ch.token, 1, raw)
	r.cleanup()
}
func TestACKTimeoutAndObservationTimeoutRelease(t *testing.T) {
	for _, mode := range []string{"ack", "observation"} {
		t.Run(mode, func(t *testing.T) {
			h, r := testHostAndRoot(t, "", nil, "test", "gateway")
			id := selectedRequest(h)
			h.Tick()
			if mode == "ack" {
				for _, c := range r.channels {
					if c != nil && len(c.records) > 0 {
						c.records[0].at = time.Now().Add(-3 * time.Second)
					}
				}
			} else {
				r.q[0].at = time.Now().Add(-time.Second)
				heap.Fix(&r.q, 0)
			}
			h.Tick()
			if len(r.states) != 0 || len(r.q) != 0 {
				t.Fatal("expired observation retained")
			}
			if h.CallOnRequestBody(id, []byte("unchanged"), true) != types.ActionContinue {
				t.Fatal("timeout blocked business")
			}
			r.cleanup()
			if r.inflight != 0 || r.messages != 0 {
				t.Fatal("cleanup leak")
			}
		})
	}
}
func TestPodBudgetSharedAcrossOwners(t *testing.T) {
	_, r := testHostAndRoot(t, "", nil, "test", "gateway")
	c := r.cfg
	c.PodInflightBytes = 2 << 20
	c.PodInflightMessages = 2048
	c.PodMaxObservations = 512
	a, err := newPodBudget(c, "test/shared")
	if err != nil {
		t.Fatal(err)
	}
	b, err := newPodBudget(c, "test/shared")
	if err != nil {
		t.Fatal(err)
	}
	if !a.take(1<<20, 1024, 256) || !b.take(1<<20, 1024, 256) || a.take(1, 0, 0) || b.take(0, 1, 0) || a.take(0, 0, 1) {
		t.Fatal("pod-wide limit violated")
	}
	a.give(1<<20, 1024, 256)
	if !b.take(1<<20, 1024, 256) {
		t.Fatal("released blocks unavailable")
	}
	b.close()
	a.close()
	data, _, err := proxywasm.GetSharedData(a.key)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if binary.LittleEndian.Uint64(data[24+i*8:]) != 0 {
			t.Fatal("shared lease leaked")
		}
	}
	c.PodMaxObservations++
	if _, err = newPodBudget(c, "test/shared"); err == nil {
		t.Fatal("mismatched budget reset live ledger")
	}
}

func TestFailedNativeCancelRetainsCreditsUntilRelease(t *testing.T) {
	h, r := testHostAndRoot(t, "", nil, "test", "gateway")
	id := selectedRequest(h)
	h.Tick()
	var c *channel
	for _, s := range r.states {
		c = s.ch
		break
	}
	reserved := r.budget.used[0]
	proxytest.FailNextHostCalls("grpc_cancel", 1)
	r.failChannel(c, "synthetic_failure")
	if !c.closing || len(r.states) != 0 || r.budget.used[0] != reserved || r.openChannel(c) {
		t.Fatal("uncertain native buffers were credited/reopened")
	}
	if h.CallOnRequestBody(id, []byte("unchanged"), true) != types.ActionContinue {
		t.Fatal("cancel fault blocked business")
	}
	c.retryAt = time.Now().Add(-time.Second)
	h.Tick()
	if c.closing || c.token != 0 || r.inflight != 0 || r.messages != 0 || r.budget.used[0] != 0 {
		t.Fatal("successful retry did not release credits")
	}
}
