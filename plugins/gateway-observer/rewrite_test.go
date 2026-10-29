package main

import (
	"bytes"
	"container/heap"
	"strings"
	"testing"
	"time"

	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/proxytest"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/types"
)

func rewriteHost(t *testing.T) (proxytest.HostEmulator, *root, uint32) {
	t.Helper()
	h, r := testHostAndRoot(t, "", nil, "test", "gateway")
	id := h.InitializeHttpContext()
	h.CallOnRequestHeaders(id, [][2]string{{":method", "POST"}, {":path", "/v1/chat/completions"}, {":authority", "model.example.invalid"}, {"content-type", "application/json"}}, false)
	return h, r, id
}

// Expire deadlines without sleeps; real tick cadence is covered in Envoy E2E.
func expireRewrite(r *root, id uint32) {
	h := r.requests[id]
	h.rewriteUntil = time.Now().Add(-time.Second)
	h.timer.at = h.rewriteUntil
	heap.Fix(&r.rewriteQ, h.timer.idx)
}

func assertReleased(t *testing.T, r *root) {
	t.Helper()
	if len(r.requests) != 0 || len(r.rewriteQ) != 0 || r.rewrites != 0 || r.rewriteBytes != 0 {
		t.Fatal("rewrite reservation or timer leaked")
	}
}

func TestExpiredBodyCallbackPassesOriginalWithoutTick(t *testing.T) {
	for _, eos := range []bool{false, true} {
		t.Run(map[bool]string{false: "fragment", true: "eos"}[eos], func(t *testing.T) {
			h, r, id := rewriteHost(t)
			prefix := []byte(`{"stream":true,`)
			tail := []byte(`"messages":[]}`)
			if h.CallOnRequestBody(id, prefix, false) != types.ActionPause {
				t.Fatal("expected buffered prefix")
			}
			expireRewrite(r, id)
			if h.CallOnRequestBody(id, tail, eos) != types.ActionContinue || !bytes.Equal(h.GetCurrentRequestBody(id), append(prefix, tail...)) {
				t.Fatal("late callback must release original accumulated body")
			}
			assertReleased(t, r)
		})
	}
}

func TestRewriteHostFailuresPassOriginal(t *testing.T) {
	for _, failure := range []string{"read_request_body", "replace_request_body"} {
		t.Run(failure, func(t *testing.T) {
			h, r, id := rewriteHost(t)
			prefix, tail := []byte(`{"stream":true,`), []byte(`"messages":[]}`)
			h.CallOnRequestBody(id, prefix, false)
			proxytest.FailNextHostCalls(failure, 1)
			if h.CallOnRequestBody(id, tail, true) != types.ActionContinue || !bytes.Equal(h.GetCurrentRequestBody(id), append(prefix, tail...)) {
				t.Fatal("host failure changed or blocked original body")
			}
			assertReleased(t, r)
		})
	}
}

func TestBufferedRewriteSurvivesObservationFailure(t *testing.T) {
	h, r, id := rewriteHost(t)
	if h.GetTickPeriod() != 10 {
		t.Fatal("rewrite recovery tick must be 10ms")
	}
	prefix, tail := []byte(`{"stream":true,`), []byte(`"messages":[],"stream_options":{"include_usage":false}}`)
	if h.CallOnRequestBody(id, prefix, false) != types.ActionPause {
		t.Fatal("prefix was not buffered")
	}
	call := proxytest.GrpcCalls()[0]
	proxytest.EmitGrpcClose(proxytest.PluginContextID, call.Token, 14)
	if h.CallOnRequestBody(id, tail, true) != types.ActionContinue {
		t.Fatal("observation failure blocked rewrite")
	}
	want := []byte(`{"stream":true,"messages":[],"stream_options":{"include_usage":true}}`)
	if !bytes.Equal(h.GetCurrentRequestBody(id), want) {
		t.Fatal("buffered request must still overwrite include_usage after observer failure")
	}
	assertReleased(t, r)
}

func TestRewriteInvalidOrOversizedBodyPassesOriginal(t *testing.T) {
	for name, body := range map[string][]byte{"invalid": []byte(`{"stream":true,broken`), "oversized": []byte(strings.Repeat("x", 262145))} {
		t.Run(name, func(t *testing.T) {
			h, r, id := rewriteHost(t)
			if h.CallOnRequestBody(id, body, true) != types.ActionContinue || !bytes.Equal(h.GetCurrentRequestBody(id), body) {
				t.Fatal("invalid/oversized body was changed or blocked")
			}
			assertReleased(t, r)
		})
	}
}

func TestResumeFailureRetriedOrReleasedByNextBody(t *testing.T) {
	for _, failure := range []string{"continue_request", "effective_context"} {
		for _, next := range []string{"tick", "body", "delete", "reload"} {
			t.Run(failure+"/"+next, func(t *testing.T) {
				h, r, id := rewriteHost(t)
				prefix := []byte(`{"stream":true,`)
				h.CallOnRequestBody(id, prefix, false)
				expireRewrite(r, id)
				proxytest.FailNextHostCalls(failure, 1)
				h.Tick()
				if h.GetCurrentHttpStreamAction(id) != types.ActionPause || r.requests[id] == nil || !r.requests[id].passthrough || len(r.rewriteQ) != 1 {
					t.Fatal("failed resume lost its pending recovery")
				}
				switch next {
				case "tick":
					expireRewrite(r, id)
					h.Tick()
					if h.GetCurrentHttpStreamAction(id) != types.ActionContinue {
						t.Fatal("retry did not resume")
					}
				case "body":
					tail := []byte(`"messages":[]}`)
					if h.CallOnRequestBody(id, tail, true) != types.ActionContinue || !bytes.Equal(h.GetCurrentRequestBody(id), append(prefix, tail...)) {
						t.Fatal("pending recovery must not rewrite or pause again")
					}
				case "delete":
					h.CompleteHttpContext(id)
				case "reload":
					h.StartPlugin()
					if h.GetCurrentHttpStreamAction(id) != types.ActionContinue {
						t.Fatal("reload did not resume")
					}
				}
				assertReleased(t, r)
			})
		}
	}
}

func TestTimeoutBeforeBodyNeverCallsResume(t *testing.T) {
	h, r, id := rewriteHost(t)
	expireRewrite(r, id)
	proxytest.FailNextHostCalls("continue_request", 1)
	h.Tick()
	assertReleased(t, r)
	body := []byte(`{"stream":true,"messages":[]}`)
	if h.CallOnRequestBody(id, body, true) != types.ActionContinue || !bytes.Equal(h.GetCurrentRequestBody(id), body) {
		t.Fatal("late first body must pass through")
	}
	if _, err := h.GetCounterMetric("metering_rewrite_resume_failed_total"); err == nil {
		t.Fatal("timer tried to resume a request that was never paused")
	}
}

func TestRewriteRecoveryPrecedesObservationTimeoutBacklog(t *testing.T) {
	h, r, id := rewriteHost(t)
	h.CallOnRequestBody(id, []byte(`{"stream":true,`), false)
	for i := 0; i < 300; i++ {
		heap.Push(&r.q, &deadline{at: time.Now().Add(-time.Hour), id: uint32(10000 + i), idx: -1})
	}
	expireRewrite(r, id)
	h.Tick()
	if h.GetCurrentHttpStreamAction(id) != types.ActionContinue || len(r.q) == 0 {
		t.Fatal("observation backlog delayed the business resume")
	}
	assertReleased(t, r)
}

func TestMetricFailuresCannotPreventRewriteOrResume(t *testing.T) {
	for _, failure := range []string{"define_metric", "increment_metric"} {
		for _, timeout := range []bool{false, true} {
			t.Run(failure+map[bool]string{false: "/rewrite", true: "/timeout"}[timeout], func(t *testing.T) {
				h, r, id := rewriteHost(t)
				proxytest.FailNextHostCalls(failure, 1000)
				if timeout {
					h.CallOnRequestBody(id, []byte(`{"stream":true,`), false)
					expireRewrite(r, id)
					h.Tick()
				} else {
					h.CallOnRequestBody(id, []byte(`{"stream":true,"messages":[]}`), true)
					if !bytes.Contains(h.GetCurrentRequestBody(id), []byte(`"include_usage":true`)) {
						t.Fatal("metric failure prevented mandatory rewrite")
					}
				}
				if h.GetCurrentHttpStreamAction(id) != types.ActionContinue {
					t.Fatal("metric failure blocked business")
				}
				assertReleased(t, r)
			})
		}
	}
}

func TestInvalidReloadAndTickFailureReleasePendingRequests(t *testing.T) {
	for _, failure := range []string{"identity", "tick_period"} {
		t.Run(failure, func(t *testing.T) {
			h, r, id := rewriteHost(t)
			h.CallOnRequestBody(id, []byte(`{"stream":true,`), false)
			if failure == "identity" {
				h.SetProperty([]string{"node", "metadata", "NAME"}, []byte("invalid/name"))
			} else {
				proxytest.FailNextHostCalls("tick_period", 1)
			}
			h.StartPlugin()
			if h.GetCurrentHttpStreamAction(id) != types.ActionContinue || r.cfg.Enabled {
				t.Fatal("bad reload failed to resume old request and disable new collection")
			}
			assertReleased(t, r)
		})
	}
}
