package main

import (
	"container/heap"
	"strconv"
	"testing"
	"time"

	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/types"
	"google.golang.org/protobuf/proto"
)

func TestLongObservationDeadlineAndElapsedTime(t *testing.T) {
	for _, ms := range []int64{1800000, 3600000} {
		t.Run(strconv.FormatInt(ms, 10), func(t *testing.T) {
			h, r := testHostAndRoot(t, "", func(c map[string]any) { c["observation_max_ms"] = ms }, "test", "gateway")
			id := selectedRequest(h)
			var neighbors []uint32
			for i := 0; i < 4; i++ {
				neighbors = append(neighbors, selectedRequest(h))
			}
			s, other := r.states[1], r.states[5]
			if s == nil || other == nil || s.ch != other.ch || !s.timer.at.Equal(s.started.Add(time.Duration(ms)*time.Millisecond)) {
				t.Fatal("observation deadline does not match configured duration")
			}
			// Advance this observation to its final minute. Native ACK deadlines
			// still use actual host time and remain independent of this clock.
			s.started = time.Now().Add(-time.Duration(ms)*time.Millisecond + time.Minute)
			s.timer.at = s.started.Add(time.Duration(ms) * time.Millisecond)
			heap.Fix(&r.q, s.timer.idx)
			if h.CallOnResponseHeaders(id, [][2]string{{":status", "200"}, {"content-type", "text/event-stream"}}, false) != types.ActionContinue {
				t.Fatal("long observation blocked response headers")
			}
			if h.CallOnResponseBody(id, []byte("data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n"), false) != types.ActionContinue {
				t.Fatal("long observation blocked response body")
			}
			found := false
			for _, call := range captureCalls(t) {
				for _, data := range call.Messages {
					var f pb.ObservationFrame
					if err := proto.Unmarshal(data, &f); err != nil {
						t.Fatal(err)
					}
					if b := f.GetBodyChunk(); b != nil {
						want := (ms - 60000) * 1000
						if b.ObservedElapsedUs == nil || *b.ObservedElapsedUs < want || *b.ObservedElapsedUs > want+5000000 {
							t.Fatal("long response callback timestamp was lost or overflowed")
						}
						found = true
					}
				}
			}
			if !found || !r.active(s) {
				t.Fatal("long observation ended prematurely")
			}
			s.timer.at = time.Now().Add(-time.Second)
			heap.Fix(&r.q, s.timer.idx)
			h.Tick()
			if r.active(s) || !r.active(other) {
				t.Fatal("timeout failed to isolate the expired observation")
			}
			if h.CallOnResponseBody(id, []byte("data: [DONE]\n\n"), true) != types.ActionContinue {
				t.Fatal("expired observation blocked business completion")
			}
			h.CompleteHttpContext(id)
			for _, neighbor := range neighbors {
				h.CompleteHttpContext(neighbor)
			}
			r.cleanup()
			if len(r.states) != 0 || len(r.q) != 0 || r.inflight != 0 {
				t.Fatal("timeout retained observation resources")
			}
		})
	}
}
