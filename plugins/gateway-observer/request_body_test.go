package main

import (
	"bytes"
	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/proxytest"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/types"
	"google.golang.org/protobuf/proto"
	"strings"
	"testing"
)

func TestOptionalRequestBodyCaptureAndFailOpen(t *testing.T) {
	for _, mode := range []string{"disabled", "enabled", "quota", "observer_stop", "read_failure", "trailers"} {
		t.Run(mode, func(t *testing.T) {
			h := testHostWithConfig(t, "", func(c map[string]any) {
				c["request_body_capture_enabled"] = mode != "disabled"
				if mode == "quota" {
					c["pod_inflight_bytes"] = 65536
				}
			})
			id := h.InitializeHttpContext()
			h.CallOnRequestHeaders(id, [][2]string{{":method", "POST"}, {":path", "/v1/chat/completions"}, {":authority", "model.example.invalid"}}, false)
			if mode == "observer_stop" {
				r := testRoot
				for _, s := range r.states {
					s.requestStopped = true
				}
			}
			if mode == "read_failure" {
				proxytest.FailNextHostCalls("read_request_body", 1)
			}
			body := []byte(`{"messages":[{"content":"` + strings.Repeat(" word", 10000) + `"}]}`)
			for from := 0; from < len(body); {
				n := min(17001, len(body)-from)
				piece := body[from : from+n]
				if h.CallOnRequestBody(id, piece, from+n == len(body) && mode != "trailers") != types.ActionContinue || !bytes.Equal(piece, h.GetCurrentRequestBody(id)) {
					t.Fatal("business modified or paused")
				}
				from += n
			}
			if mode == "trailers" {
				h.CallOnRequestTrailers(id, nil)
			}
			h.CallOnResponseHeaders(id, [][2]string{{":status", "200"}, {"content-type", "application/json"}}, false)
			h.CallOnResponseBody(id, []byte(`{"usage":{"prompt_tokens":123,"completion_tokens":4}}`), true)
			h.CompleteHttpContext(id)
			var captured []byte
			responses := 0
			requestEOS := false
			stopped := false
			for _, call := range captureCalls(t) {
				for _, message := range call.Messages {
					var f pb.ObservationFrame
					if proto.Unmarshal(message, &f) != nil {
						t.Fatal("decode")
					}
					if f.GetRequestBodyStopped() != nil {
						stopped = true
					}
					if b := f.GetBodyChunk(); b != nil {
						if len(b.Data) > 16384 {
							t.Fatal("oversize chunk")
						}
						if b.Direction == pb.Direction_REQUEST {
							captured = append(captured, b.Data...)
							requestEOS = b.EndStream
						} else {
							responses++
						}
					}
				}
			}
			if responses == 0 {
				t.Fatal("response capture lost")
			}
			if mode == "read_failure" && (!stopped || len(captured) != 0 || proxytest.RemainingHostFailures("read_request_body") != 0) {
				t.Fatal("read failure canceled response or did not stop input")
			}
			if mode == "enabled" || mode == "trailers" {
				if !bytes.Equal(captured, body) || !requestEOS {
					t.Fatal("incomplete capture", len(captured), len(body))
				}
			} else if mode == "disabled" && len(captured) > 0 {
				t.Fatal("disabled read body")
			} else if mode == "quota" && (!stopped || len(captured) >= len(body)) {
				t.Fatal("quota did not stop request only")
			}
		})
	}
}
