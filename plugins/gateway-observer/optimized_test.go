package main

import (
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/proxytest"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/types"
	"testing"
	"time"
)

func TestMetricsDisabledMakesNoMetricHostcalls(t *testing.T) {
	h, r := testHostAndRoot(t, "", func(c map[string]any) { c["metrics_enabled"] = false }, "test", "gateway")
	proxytest.FailNextHostCalls("define_metric", 1000)
	proxytest.FailNextHostCalls("increment_metric", 1000)
	id := selectedRequest(h)
	h.CallOnResponseHeaders(id, [][2]string{{":status", "200"}, {"content-type", "text/event-stream"}}, false)
	for i := 0; i < 10; i++ {
		if h.CallOnResponseBody(id, []byte("data: {}\n\n"), false) != types.ActionContinue {
			t.Fatal("blocked business")
		}
	}
	h.CompleteHttpContext(id)
	h.Tick()
	r.cleanup()
	if proxytest.RemainingHostFailures("define_metric") != 1000 || proxytest.RemainingHostFailures("increment_metric") != 1000 || len(r.metrics) != 0 || len(r.gauges) != 0 {
		t.Fatal("disabled metrics invoked host")
	}
}

func TestFaultReclamationHasFixedWorkAndPreservesOtherChannels(t *testing.T) {
	h, r := testHostAndRoot(t, "", nil, "test", "gateway")
	for i := 0; i < 3000; i++ {
		selectedRequest(h)
	}
	h.Tick()
	c := r.states[1].ch
	neighbor := r.states[2]
	before := len(r.states)
	r.failChannel(c, "synthetic_failure")
	if before-len(r.states) > 64 || !c.failed || r.active(c.head) || !r.active(neighbor) {
		t.Fatal("cleanup not bounded or invalidation affected neighbor")
	}
	for i := 0; i < 100 && c.failed; i++ {
		h.Tick()
	}
	if c.failed || c.head != nil {
		t.Fatal("deferred cleanup did not finish")
	}
	for _, s := range r.states {
		if s.ch == c {
			t.Fatal("channel index retained observation")
		}
	}
	r.cleanup()
	if r.inflight != 0 || r.messages != 0 || len(r.states) != 0 || len(r.q) != 0 {
		t.Fatal("reclamation leaked budget")
	}
}

func TestResourceGaugesPublishPeriodically(t *testing.T) {
	h, r := testHostAndRoot(t, "", nil, "test", "gateway")
	r.metricsAt = time.Now()
	id := selectedRequest(h)
	if n, _ := h.GetGaugeMetric("metering_active_streams"); n != 0 {
		t.Fatal("gauge updated in callback")
	}
	r.metricsAt = time.Now().Add(-time.Second)
	h.Tick()
	if n, _ := h.GetGaugeMetric("metering_active_streams"); n != 1 {
		t.Fatal("gauge not published")
	}
	h.CompleteHttpContext(id)
	r.cleanup()
}
