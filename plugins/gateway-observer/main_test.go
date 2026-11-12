package main

import (
	"bytes"
	"encoding/json"
	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	w "github.com/Fangyan33/llm-token-stats/pkg/observationwire"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/proxytest"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/types"
	"google.golang.org/protobuf/proto"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestBearerStartAndUnchangedAuthorization(t *testing.T) {
	for _, tc := range []struct {
		name          string
		authorization []string
		want          []byte
	}{
		{"single", []string{"Bearer Synthetic-MixedCase.Key"}, []byte("Synthetic-MixedCase.Key")},
		{"scheme_case", []string{"bEaReR  Synthetic-MixedCase.Key"}, []byte("Synthetic-MixedCase.Key")},
		{"maximum", []string{"Bearer " + strings.Repeat("a", 4096)}, bytes.Repeat([]byte{'a'}, 4096)},
		{"missing", nil, nil}, {"empty", []string{"Bearer "}, nil},
		{"other_scheme", []string{"Basic secret"}, nil},
		{"duplicate", []string{"Bearer first", "Bearer second"}, nil},
		{"duplicate_same", []string{"Bearer first", "Bearer first"}, nil},
		{"joined", []string{"Bearer first,Bearer second"}, nil},
		{"too_long", []string{"Bearer " + strings.Repeat("a", 4097)}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := testHost(t)
			id := h.InitializeHttpContext()
			headers := [][2]string{{":method", "POST"}, {":path", "/v1/chat/completions"}, {":authority", "MODEL.example.invalid:443"}, {"x-forwarded-host", "forged.invalid"}, {"forwarded", "host=forged.invalid"}, {"x-api-key", "must-be-ignored"}, {"customer_id", "forged"}}
			for _, v := range tc.authorization {
				headers = append(headers, [2]string{"authorization", v})
			}
			h.CallOnRequestHeaders(id, headers, false)
			calls := captureCalls(t)
			if len(calls) != 1 || len(calls[0].Messages) != 1 {
				t.Fatal("missing Start")
			}
			var f pb.ObservationFrame
			if proto.Unmarshal(calls[0].Messages[0], &f) != nil || f.GetStart() == nil {
				t.Fatal("invalid Start")
			}
			st := f.GetStart()
			if !bytes.Equal(st.ApiKey, tc.want) || st.RequestAuthority != "MODEL.example.invalid:443" {
				t.Fatal("wrong transient identity input")
			}
			got := h.GetCurrentRequestHeaders(id)
			var authorizations []string
			for _, pair := range got {
				if pair[0] == "authorization" {
					authorizations = append(authorizations, pair[1])
				}
			}
			if strings.Join(authorizations, "\n") != strings.Join(tc.authorization, "\n") {
				t.Fatal("business Authorization changed")
			}
		})
	}
}

func TestClientRequestIDPriorityAndMissingValues(t *testing.T) {
	for _, tc := range []struct {
		name              string
		headers           [][2]string
		want              string
		disabled, omitted bool
	}{
		{name: "priority", headers: [][2]string{{"request_id", "Second"}, {"x-client-request-id", "First.Case:42_abc-1"}}, want: "First.Case:42_abc-1"},
		{name: "fallback", headers: [][2]string{{"request_id", "Second"}}, want: "Second"},
		{name: "empty", headers: [][2]string{{"x-client-request-id", ""}, {"request_id", "Second"}}, want: "Second"},
		{name: "invalid", headers: [][2]string{{"x-client-request-id", "bad id"}, {"request_id", "Second"}}, want: "Second"},
		{name: "too_long", headers: [][2]string{{"x-client-request-id", strings.Repeat("a", 129)}, {"request_id", "Second"}}, want: "Second"},
		{name: "maximum", headers: [][2]string{{"x-client-request-id", strings.Repeat("a", 128)}}, want: strings.Repeat("a", 128)},
		{name: "duplicate", headers: [][2]string{{"x-client-request-id", "First"}, {"x-client-request-id", "Other"}, {"request_id", "Second"}}, want: "Second"},
		{name: "duplicate_same", headers: [][2]string{{"x-client-request-id", "First"}, {"x-client-request-id", "First"}, {"request_id", "Second"}}, want: "Second"},
		{name: "joined", headers: [][2]string{{"x-client-request-id", "First,Other"}}},
		{name: "non_ascii", headers: [][2]string{{"x-client-request-id", "客户端"}}},
		{name: "missing"},
		{name: "empty_list", headers: [][2]string{{"x-client-request-id", "First"}}, disabled: true},
		{name: "missing_setting", headers: [][2]string{{"x-client-request-id", "First"}}, omitted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := testHostWithConfig(t, "", func(c map[string]any) {
				c["logical_request_id_headers"] = []string{"X-Client-Request-ID", "request_id"}
				if tc.disabled {
					c["logical_request_id_headers"] = []string{}
				}
				if tc.omitted {
					delete(c, "logical_request_id_headers")
				}
			})
			id := h.InitializeHttpContext()
			headers := append([][2]string{{":method", "POST"}, {":path", "/v1/chat/completions"}, {":authority", "model.example.invalid"}, {"x-request-id", "gateway-visible"}}, tc.headers...)
			if h.CallOnRequestHeaders(id, headers, true) != types.ActionContinue {
				t.Fatal("client ID extraction blocked business traffic")
			}
			calls := captureCalls(t)
			if len(calls) != 1 || len(calls[0].Messages) != 1 {
				t.Fatal("missing Start")
			}
			var f pb.ObservationFrame
			if err := proto.Unmarshal(calls[0].Messages[0], &f); err != nil || f.GetStart() == nil {
				t.Fatal("invalid Start", err)
			}
			st := f.GetStart()
			if st.GetLogicalRequestId() != tc.want || (st.LogicalRequestId == nil) != (tc.want == "") || st.GetXRequestId() != "gateway-visible" {
				t.Fatalf("client ID=%v, gateway ID=%q", st.LogicalRequestId, st.GetXRequestId())
			}
			if !reflect.DeepEqual(h.GetCurrentRequestHeaders(id), headers) {
				t.Fatal("client ID extraction changed request headers")
			}
		})
	}
}

func TestResponseDoesNotExposeObservationID(t *testing.T) {
	h := testHost(t)
	id := h.InitializeHttpContext()
	h.CallOnRequestHeaders(id, [][2]string{{":method", "POST"}, {":path", "/v1/chat/completions"}, {":authority", "model.example.invalid"}}, true)
	calls := captureCalls(t)
	a, err := proto.Marshal(&pb.ObservationAck{ObservationId: "7f6c2a90-4b13-4b68-9d21-67283e0c5f44", AcceptedFrameSeq: 1, Reason: "accepted"})
	if err != nil {
		t.Fatal(err)
	}
	emitCaptureAck(t, calls[0].Token, 1, a)
	headers := [][2]string{{":status", "200"}, {"content-type", "application/json"}, {"x-business", "unchanged"}}
	h.CallOnResponseHeaders(id, headers, true)
	if !reflect.DeepEqual(h.GetCurrentResponseHeaders(id), headers) {
		t.Fatal("observation injected or changed response headers")
	}
}

func TestHostAllowlistBeforeAllocation(t *testing.T) {
	for _, tc := range []struct {
		name, authority              string
		patterns                     []string
		want, omitted, invalidConfig bool
	}{
		{name: "exact", authority: "MODEL.Example.invalid.:443", patterns: []string{"Model.Example.invalid."}, want: true},
		{name: "suffix", authority: "a.example.invalid", patterns: []string{"*.example.invalid"}, want: true},
		{name: "nested", authority: "a.b.example.invalid", patterns: []string{"*.example.invalid"}, want: true},
		{name: "root", authority: "example.invalid", patterns: []string{"*.example.invalid"}},
		{name: "boundary", authority: "badexample.invalid", patterns: []string{"*.example.invalid"}},
		{name: "trailing_domain", authority: "a.example.invalid.evil", patterns: []string{"*.example.invalid"}},
		{name: "different", authority: "other.invalid", patterns: []string{"model.example.invalid"}},
		{name: "all", authority: "any.valid", patterns: []string{"*"}, want: true},
		{name: "empty", authority: "model.example.invalid", patterns: []string{}},
		{name: "missing_list", authority: "model.example.invalid", omitted: true},
		{name: "missing_host", patterns: []string{"*"}},
		{name: "ip", authority: "127.0.0.1:80", patterns: []string{"*"}},
		{name: "invalid", authority: "bad_host", patterns: []string{"*"}},
		{name: "unicode_exact", authority: "K.example.invalid", patterns: []string{"k.example.invalid"}},
		{name: "unicode_suffix", authority: "K.example.invalid.:443", patterns: []string{"*.example.invalid"}},
		{name: "unicode_all", authority: "K.example.invalid", patterns: []string{"*"}},
		{name: "invalid_config", authority: "model.example.invalid", patterns: []string{"model-*.example.invalid"}, invalidConfig: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := testHostWithConfig(t, "", func(c map[string]any) {
				if tc.omitted {
					delete(c, "accepted_request_hosts")
				} else {
					c["accepted_request_hosts"] = tc.patterns
				}
			})
			id := h.InitializeHttpContext()
			body := []byte(`{"stream":true,"messages":[]}`)
			headers := [][2]string{{":method", "POST"}, {":path", "/v1/chat/completions"}, {":authority", tc.authority}, {"content-type", "application/json"}, {"content-length", "29"}, {"authorization", "Bearer synthetic-private"}, {"x-forwarded-host", "model.example.invalid"}, {"forwarded", "host=model.example.invalid"}}
			if h.CallOnRequestHeaders(id, headers, false) != types.ActionContinue {
				t.Fatal("headers paused")
			}
			if tc.want {
				if len(captureCalls(t)) != 1 {
					t.Fatal("accepted host did not open observation")
				}
				h.CallOnRequestBody(id, body, true)
				if !bytes.Equal(h.GetCurrentRequestBody(id), body) {
					t.Fatal("request body changed")
				}
				return
			}
			if !reflect.DeepEqual(h.GetCurrentRequestHeaders(id), headers) {
				t.Fatal("ignored request headers changed")
			}
			if h.CallOnRequestBody(id, body, false) != types.ActionContinue || !bytes.Equal(h.GetCurrentRequestBody(id), body) {
				t.Fatal("ignored request paused or body changed")
			}
			h.CallOnResponseHeaders(id, [][2]string{{":status", "200"}}, false)
			h.CallOnResponseBody(id, []byte("unchanged"), true)
			h.CompleteHttpContext(id)
			if len(captureCalls(t)) != 0 {
				t.Fatal("ignored host created observation")
			}
			for _, name := range []string{"active_streams", "inflight_bytes", "inflight_messages"} {
				if n, err := h.GetGaugeMetric("metering_" + name); err != nil || n != 0 {
					t.Fatalf("resource %s=%d: %v", name, n, err)
				}
			}
			metric := "metering_requests_ignored_host_total"
			if tc.authority == "" || tc.name == "ip" || tc.name == "invalid" || strings.HasPrefix(tc.name, "unicode_") {
				metric = "metering_requests_ignored_invalid_host_total"
			}
			if tc.invalidConfig {
				metric = "metering_config_invalid_total"
			}
			if n, err := h.GetCounterMetric(metric); err != nil || n != 1 {
				t.Fatalf("missing fixed rejection counter %s: %d %v", metric, n, err)
			}
		})
	}
}

func testHost(t *testing.T) proxytest.HostEmulator {
	return testHostWithRoute(t, "llm-chat")
}

func testHostWithRoute(t *testing.T, route string) proxytest.HostEmulator {
	return testHostWithConfig(t, route, nil)
}

func testHostWithConfig(t *testing.T, route string, change func(map[string]any)) proxytest.HostEmulator {
	return testHostWithIdentity(t, route, change, "test", "test-gateway")
}

func testHostWithIdentity(t *testing.T, route string, change func(map[string]any), namespace, name string) proxytest.HostEmulator {
	h, _ := testHostAndRoot(t, route, change, namespace, name)
	return h
}

type capturedVM struct {
	vm
	root *root
}

func (v *capturedVM) NewPluginContext(id uint32) types.PluginContext {
	v.root = v.vm.NewPluginContext(id).(*root)
	return v.root
}

func testHostAndRoot(t *testing.T, route string, change func(map[string]any), namespace, name string) (proxytest.HostEmulator, *root) {
	t.Helper()
	proxytest.ResetGrpc()
	b, err := os.ReadFile("../../configs/gateway-observer.example.json")
	if err != nil {
		t.Fatal(err)
	}
	var c map[string]any
	json.Unmarshal(b, &c)
	c["enabled"] = true
	c["metrics_enabled"] = true
	if change != nil {
		change(c)
	}
	b, _ = json.Marshal(c)
	v := &capturedVM{}
	option := proxytest.NewEmulatorOption().WithVMContext(v).WithPluginConfiguration(b)
	if namespace != "" {
		option = option.WithProperty([]string{"node", "metadata", "NAMESPACE"}, []byte(namespace))
	}
	if name != "" {
		option = option.WithProperty([]string{"node", "metadata", "NAME"}, []byte(name))
	}
	if route != "" {
		option = option.WithProperty([]string{"route_name"}, []byte(route))
	}
	host, reset := proxytest.NewHostEmulator(option)
	t.Cleanup(reset)
	if host.StartVM() != types.OnVMStartStatusOK || host.StartPlugin() != types.OnPluginStartStatusOK {
		t.Fatal("start")
	}
	testRoot = v.root
	return host, v.root
}

func TestWildcardSelectsWithoutRouteMetadata(t *testing.T) {
	for _, route := range []string{"", "other.named-match", "llm-chat", "*", strings.Repeat("r", 129), string([]byte{0xff})} {
		t.Run(route, func(t *testing.T) {
			h := testHostWithRoute(t, route)
			id := h.InitializeHttpContext()
			h.CallOnRequestHeaders(id, [][2]string{{":method", "POST"}, {":path", "/v1/chat/completions?x=y"}, {":authority", "model.example.invalid"}, {"content-type", "application/json"}}, false)
			calls := captureCalls(t)
			if len(calls) != 1 || len(calls[0].Messages) != 1 {
				t.Fatal("matching method/path did not send Start")
			}
			var frame pb.ObservationFrame
			if err := proto.Unmarshal(calls[0].Messages[0], &frame); err != nil {
				t.Fatal(err)
			}
			start := frame.GetStart()
			want := route
			if route == "" || route == "*" || len(route) > 128 || route == string([]byte{0xff}) {
				want = "unknown"
			}
			if start == nil || start.RouteName != want {
				t.Fatalf("Start must report actual route or unknown: %+v", start)
			}
		})
	}
}

func TestGrpcUpstreamAuthority(t *testing.T) {
	for _, tc := range []struct{ cluster, authority string }{
		{"observer", "observer"},
		{"outbound|9090||metering-observer.metering.svc.cluster.local", "metering-observer.metering.svc.cluster.local:9090"},
		{"outbound|9090|canary|metering-observer.metering.svc.cluster.local", "metering-observer.metering.svc.cluster.local:9090"},
	} {
		service, err := w.Read(grpcUpstream(tc.cluster))
		if err != nil || len(service) != 1 || service[0].N != 1 {
			t.Fatal("invalid GrpcService EnvoyGrpc")
		}
		fields, err := w.Read(service[0].B)
		if err != nil || len(fields) != 2 || fields[0].N != 1 || string(fields[0].B) != tc.cluster || fields[1].N != 2 || string(fields[1].B) != tc.authority {
			t.Fatalf("routing cluster or HTTP/2 authority mismatch: %q", tc.cluster)
		}
	}
}

func TestIgnoreBeforeAllocation(t *testing.T) {
	for _, tc := range []struct{ method, path string }{
		{"GET", "/v1/chat/completions"}, {"HEAD", "/v1/chat/completions"},
		{"OPTIONS", "/v1/chat/completions"}, {"PUT", "/v1/chat/completions"},
		{"POST", "/v1/embeddings"}, {"POST", "/healthz"},
		{"POST", "/v1/chat/completions/"}, {"POST", "/v1/chat/completions/extra"},
	} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			h := testHostWithRoute(t, "")
			id := h.InitializeHttpContext()
			h.CallOnRequestHeaders(id, [][2]string{{":method", tc.method}, {":path", tc.path}, {":authority", "model.example.invalid"}, {"content-type", "application/json"}}, false)
			h.CallOnRequestBody(id, []byte("invalid"), true)
			h.CallOnResponseHeaders(id, [][2]string{{":status", "200"}}, false)
			h.CallOnResponseBody(id, []byte("ignored"), true)
			if len(captureCalls(t)) != 0 || string(h.GetCurrentRequestBody(id)) != "invalid" {
				t.Fatal("ignored request opened grpc or changed business body")
			}
			h.CompleteHttpContext(id)
			if len(captureCalls(t)) != 0 {
				t.Fatal("ignored request completion sent event")
			}
		})
	}
}
func TestRuntimeGatewayIdentity(t *testing.T) {
	for _, tc := range []struct {
		namespace, name string
		valid           bool
	}{
		{"gateways", "gateway-a", true},
		{"gateways", "gateway-b", true}, // same configuration, a newly scaled replica
		{"other", "gateway-a", true},
		{"gateways", "gateway-a", true},                                      // same name reused: intentionally the same identity
		{"n", strings.Repeat("a", 63) + "." + strings.Repeat("b", 62), true}, // 128 bytes
		{"test", strings.Repeat("a", 64), true},                              // Kubernetes DNS subdomains have no per-label length limit.
		{"test", strings.Repeat("a", 64) + ".pod", true},
		{strings.Repeat("n", 63), strings.Repeat("a", 64), true}, // exactly 128 bytes
		{"n", strings.Repeat("a", 126), true},                    // exactly 128 bytes, one long Pod-name label
		{"n", strings.Repeat("a", 127), false},                   // 129 bytes: do not truncate
		{strings.Repeat("n", 64), "pod", false},                  // namespaces still use DNS label limits
		{"test.ns", "pod", false},
		{"", "gateway-a", false}, {"gateways", "", false},
		{"bad/namespace", "gateway-a", false}, {"gateways", "bad/name", false},
		{"gateways", "UPPER", false}, {"gateways", "a..b", false},
		{"gateways", "-pod", false}, {"gateways", "pod-", false},
		{"gateways", ".pod", false}, {"gateways", "pod.", false},
		{"gateways", "pod.-name", false}, {"gateways", "pod-.name", false},
		{"gateways", string([]byte{0xff}), false},
		{"nn", strings.Repeat("a", 63) + "." + strings.Repeat("b", 62), false}, // no truncation
	} {
		t.Run(tc.namespace+"/"+tc.name, func(t *testing.T) {
			h := testHostWithIdentity(t, "llm-chat", nil, tc.namespace, tc.name)
			id := h.InitializeHttpContext()
			headers := [][2]string{{":method", "POST"}, {":path", "/v1/chat/completions"}, {":authority", "model.example.invalid"}, {"gateway_instance_id", "forged"}, {"x-pod-name", "forged"}}
			if action := h.CallOnRequestHeaders(id, headers, false); action != types.ActionContinue {
				t.Fatalf("business request blocked: %v", action)
			}
			calls := captureCalls(t)
			if !tc.valid {
				if len(calls) != 0 {
					t.Fatal("invalid identity emitted observations")
				}
				if n, err := h.GetCounterMetric("metering_gateway_identity_unavailable_total"); err != nil || n != 1 {
					t.Fatalf("missing identity rejection metric: %d %v", n, err)
				}
				if !reflect.DeepEqual(h.GetCurrentRequestHeaders(id), headers) {
					t.Fatal("business headers changed")
				}
				return
			}
			var frame pb.ObservationFrame
			if len(calls) != 1 || len(calls[0].Messages) != 1 || proto.Unmarshal(calls[0].Messages[0], &frame) != nil || frame.GetStart() == nil {
				t.Fatal("missing valid Start")
			}
			if got := frame.GetStart().GatewayInstanceId; got != tc.namespace+"/"+tc.name {
				t.Fatalf("identity = %q", got)
			}
		})
	}
}

func TestReloadCannotRetainInvalidGatewayIdentity(t *testing.T) {
	h := testHost(t)
	if err := h.SetProperty([]string{"node", "metadata", "NAME"}, []byte("invalid/name")); err != nil {
		t.Fatal(err)
	}
	if h.StartPlugin() != types.OnPluginStartStatusOK {
		t.Fatal("reload must fail open")
	}
	id := h.InitializeHttpContext()
	h.CallOnRequestHeaders(id, [][2]string{{":method", "POST"}, {":path", "/v1/chat/completions"}, {":authority", "model.example.invalid"}}, false)
	if len(captureCalls(t)) != 0 {
		t.Fatal("stale identity emitted observations after reload")
	}
}

func TestConfiguredGatewayIdentityRejected(t *testing.T) {
	h := testHostWithConfig(t, "llm-chat", func(c map[string]any) { c["gateway_instance_id"] = "legacy-shared-id" })
	id := h.InitializeHttpContext()
	h.CallOnRequestHeaders(id, [][2]string{{":method", "POST"}, {":path", "/v1/chat/completions"}, {":authority", "model.example.invalid"}}, false)
	if len(captureCalls(t)) != 0 {
		t.Fatal("configured identity was accepted")
	}
	if n, err := h.GetCounterMetric("metering_config_invalid_total"); err != nil || n != 1 {
		t.Fatalf("missing rejection metric: %d %v", n, err)
	}
}

func TestResponseChunksShareCallbackTime(t *testing.T) {
	h := testHost(t)
	id := h.InitializeHttpContext()
	h.CallOnRequestHeaders(id, [][2]string{{":method", "POST"}, {":path", "/v1/chat/completions"}, {":authority", "model.example.invalid"}, {"content-type", "application/json"}}, false)
	h.CallOnRequestBody(id, []byte(`{"stream":true}`), true)
	h.CallOnResponseHeaders(id, [][2]string{{":status", "200"}, {"content-type", "text/event-stream"}}, false)
	if h.CallOnResponseBody(id, bytes.Repeat([]byte("x"), 200000), true) != types.ActionContinue {
		t.Fatal("timing paused response")
	}
	var elapsed *int64
	count := 0
	calls := captureCalls(t)
	if len(calls) != 1 {
		t.Fatalf("observation streams=%d", len(calls))
	}
	for _, b := range calls[0].Messages {
		var f pb.ObservationFrame
		if err := proto.Unmarshal(b, &f); err != nil {
			t.Fatal(err)
		}
		c := f.GetBodyChunk()
		if c == nil {
			continue
		}
		if c.Direction == pb.Direction_REQUEST {
			if c.ObservedElapsedUs != nil {
				t.Fatal("request got response time")
			}
			continue
		}
		if c.ObservedElapsedUs == nil || *c.ObservedElapsedUs < 0 {
			t.Fatal("response time missing")
		}
		if elapsed != nil && *elapsed != *c.ObservedElapsedUs {
			t.Fatal("split callback timings differ")
		}
		elapsed = c.ObservedElapsedUs
		count++
	}
	if count != 13 {
		t.Fatalf("chunks=%d", count)
	}
}

// Decode batches for tests whose assertions concern Start metadata rather than
// transport multiplexing. Transport tests inspect the original batches directly.
var testRoot *root

func captureCalls(t *testing.T) []*proxytest.GrpcCall {
	t.Helper()
	for _, c := range testRoot.channels {
		if c != nil {
			testRoot.flush(c)
		}
	}
	var calls []*proxytest.GrpcCall
	for _, c := range proxytest.GrpcCalls() {
		copy := *c
		copy.Messages = nil
		copy.End = nil
		for _, data := range c.Messages {
			var batch pb.ObservationBatch
			if err := proto.Unmarshal(data, &batch); err != nil {
				t.Fatal(err)
			}
			for _, item := range batch.Frames {
				b, err := proto.Marshal(item.Frame)
				if err != nil {
					t.Fatal(err)
				}
				copy.Messages = append(copy.Messages, b)
			}
		}
		calls = append(calls, &copy)
	}
	return calls
}
func emitCaptureAck(t *testing.T, token uint32, capture uint64, data []byte) {
	t.Helper()
	var ack pb.ObservationAck
	if err := proto.Unmarshal(data, &ack); err != nil {
		t.Fatal(err)
	}
	var seq uint64
	for _, c := range testRoot.channels {
		if c != nil && c.token == token {
			seq = c.seq
		}
	}
	b, err := proto.Marshal(&pb.ObservationAckBatch{AcceptedBatchSeq: seq, Acks: []*pb.CaptureAck{{CaptureId: capture, Ack: &ack}}})
	if err != nil {
		t.Fatal(err)
	}
	proxytest.EmitGrpcReceive(proxytest.PluginContextID, token, b)
}
