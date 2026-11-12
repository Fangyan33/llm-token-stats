package main

import (
	"reflect"
	"strconv"
	"strings"
	"testing"

	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	w "github.com/Fangyan33/llm-token-stats/pkg/observationwire"
	"github.com/Fangyan33/llm-token-stats/pkg/requestmeta"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/types"
	"google.golang.org/protobuf/proto"
)

func TestRequestHeaderSnapshotAndBodyRemainUnchanged(t *testing.T) {
	for _, body := range []string{`{"stream":true,"messages":[]}`, `{"stream":true,broken`} {
		t.Run(body, func(t *testing.T) {
			h, _ := testHostAndRoot(t, "", func(c map[string]any) {
				c["user_id_header"] = []string{"content-length"}
				c["logical_request_id_headers"] = []string{"content-length"}
				c["extra_request_headers"] = []string{"Content-Length"}
			}, "test", "gateway")
			id := h.InitializeHttpContext()
			length := strconv.Itoa(len(body))
			headers := [][2]string{{":method", "POST"}, {":path", "/v1/chat/completions"}, {":authority", "model.example.invalid"}, {"content-type", "application/json"}, {"content-length", length}}
			if h.CallOnRequestHeaders(id, headers, false) != types.ActionContinue {
				t.Fatal("header snapshot blocked business traffic")
			}
			calls := captureCalls(t)
			if len(calls) != 1 || len(calls[0].Messages) != 1 {
				t.Fatal("missing Start")
			}
			var frame pb.ObservationFrame
			if proto.Unmarshal(calls[0].Messages[0], &frame) != nil || frame.GetStart() == nil {
				t.Fatal("invalid Start")
			}
			st := frame.GetStart()
			if st.RequestHeaders["content-length"] != length || st.GetUserId() != length || st.GetLogicalRequestId() != length {
				t.Fatal("snapshot lost original Content-Length")
			}
			if !reflect.DeepEqual(h.GetCurrentRequestHeaders(id), headers) {
				t.Fatal("headers changed")
			}
			if h.CallOnRequestBody(id, []byte(body), true) != types.ActionContinue {
				t.Fatal("body forwarding blocked")
			}
			if string(h.GetCurrentRequestBody(id)) != body {
				t.Fatal("body changed")
			}

		})
	}
}

func TestRequestHeaderSnapshots(t *testing.T) {
	for _, tc := range []struct {
		name    string
		config  any
		headers [][2]string
		want    string
	}{
		{"default", nil, [][2]string{{"user-id", "默认用户"}}, "默认用户"},
		{"priority", []string{"X-User", "user-id"}, [][2]string{{"x-user", "first"}, {"user-id", "second"}}, "first"},
		{"missing", []string{"x-user", "user-id"}, [][2]string{{"user-id", "fallback"}}, "fallback"},
		{"empty", []string{"x-user", "user-id"}, [][2]string{{"x-user", ""}, {"user-id", "fallback"}}, "fallback"},
		{"long", []string{"x-user", "user-id"}, [][2]string{{"x-user", strings.Repeat("x", 513)}, {"user-id", "fallback"}}, "fallback"},
		{"duplicate", []string{"x-user", "user-id"}, [][2]string{{"x-user", "a"}, {"x-user", "b"}, {"user-id", "fallback"}}, "fallback"},
		{"disabled", []string{}, [][2]string{{"user-id", "ignored"}}, ""},
		{"none", []string{"missing"}, nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := testHostWithConfig(t, "", func(c map[string]any) {
				delete(c, "user_id_header")
				if tc.config != nil {
					c["user_id_header"] = tc.config
				}
				c["extra_request_headers"] = []string{"X-Team", "x-repeat", "missing"}
			})
			id := h.InitializeHttpContext()
			headers := append([][2]string{{":method", "POST"}, {":path", "/v1/chat/completions"}, {":authority", "model.example.invalid"}, {"user-agent", "sdk/1.0"}, {"x-team", "team-a"}, {"x-repeat", "one"}, {"x-repeat", "two"}}, tc.headers...)
			if h.CallOnRequestHeaders(id, headers, true) != types.ActionContinue {
				t.Fatal("business blocked")
			}
			calls := captureCalls(t)
			if len(calls) != 1 || len(calls[0].Messages) != 1 {
				t.Fatal("missing Start")
			}
			raw := calls[0].Messages[0]
			var f pb.ObservationFrame
			if err := proto.Unmarshal(raw, &f); err != nil || w.Validate(raw, "frame") != nil {
				t.Fatal("invalid wire")
			}
			st := f.GetStart()
			if st.GetUserId() != tc.want || st.GetUserAgent() != "sdk/1.0" || !reflect.DeepEqual(st.RequestHeaders, map[string]string{"x-team": "team-a", "x-repeat": "one,two"}) {
				t.Fatalf("snapshot mismatch: uid=%q agent=%q extra=%v", st.GetUserId(), st.GetUserAgent(), st.RequestHeaders)
			}
			if !requestmeta.Valid(st.UserId, st.UserAgent, st.RequestHeaders) || !reflect.DeepEqual(h.GetCurrentRequestHeaders(id), headers) {
				t.Fatal("invalid metadata or changed business headers")
			}
		})
	}
}

func TestRequestMetadataBudgetPreservesStart(t *testing.T) {
	h := testHostWithConfig(t, "", func(c map[string]any) {
		c["extra_request_headers"] = []string{"x-first", "x-second", "x-third", "x-last"}
	})
	id := h.InitializeHttpContext()
	headers := [][2]string{{":method", "POST"}, {":path", "/v1/chat/completions"}, {":authority", "model.example.invalid"}, {"authorization", "Bearer " + strings.Repeat("a", 4096)}, {"user-id", "u"}, {"user-agent", strings.Repeat("<", 2048)}, {"x-first", strings.Repeat("a", 1024)}, {"x-second", strings.Repeat("a", 1024)}, {"x-third", strings.Repeat("a", 1024)}, {"x-last", "small"}}
	if h.CallOnRequestHeaders(id, headers, true) != types.ActionContinue {
		t.Fatal("business blocked")
	}
	calls := captureCalls(t)
	if len(calls) != 1 || len(calls[0].Messages) != 1 {
		t.Fatal("optional fields dropped observation")
	}
	raw := calls[0].Messages[0]
	var f pb.ObservationFrame
	if proto.Unmarshal(raw, &f) != nil || len(raw) > 6144 {
		t.Fatal("frame budget exceeded")
	}
	st := f.GetStart()
	if st.GetUserId() != "u" || st.UserAgent != nil || st.RequestHeaders["x-last"] != "small" || !requestmeta.Valid(st.UserId, st.UserAgent, st.RequestHeaders) {
		t.Fatal("optional budget handling")
	}
}
