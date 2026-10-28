package main

import (
	"container/heap"
	"github.com/Fangyan33/llm-token-stats/pkg/credentials"
	"github.com/Fangyan33/llm-token-stats/pkg/gatewayconfig"
	w "github.com/Fangyan33/llm-token-stats/pkg/observationwire"
	"github.com/Fangyan33/llm-token-stats/pkg/requesthost"
	"github.com/Fangyan33/llm-token-stats/pkg/requestid"
	"github.com/Fangyan33/llm-token-stats/pkg/routeinfo"
	"github.com/Fangyan33/llm-token-stats/pkg/strictjson"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/types"
	"mime"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

func main() { proxywasm.SetVMContext(&vm{}) }

type vm struct{ types.DefaultVMContext }

func (*vm) NewPluginContext(id uint32) types.PluginContext {
	return &root{id: id, states: map[uint32]*state{}, requests: map[uint32]*httpContext{}, metrics: map[string]proxywasm.MetricCounter{}}
}

type deadline struct {
	at      time.Time
	id      uint32
	rewrite bool
	idx     int
}
type deadlines []*deadline

func (q deadlines) Len() int           { return len(q) }
func (q deadlines) Less(i, j int) bool { return q[i].at.Before(q[j].at) }
func (q deadlines) Swap(i, j int)      { q[i], q[j] = q[j], q[i]; q[i].idx = i; q[j].idx = j }
func (q *deadlines) Push(x any)        { n := x.(*deadline); n.idx = len(*q); *q = append(*q, n) }
func (q *deadlines) Pop() any {
	old := *q
	n := old[len(old)-1]
	old[len(old)-1] = nil
	*q = old[:len(old)-1]
	n.idx = -1
	return n
}

type frame struct {
	seq   uint64
	bytes int64
	at    time.Time
}
type state struct {
	token           uint32
	seq             uint64
	id              string
	frames          []frame
	bytes           int64
	bodySeq, offset [2]uint64
	eos             [2]bool
	ending          bool
	started         time.Time
	timer           *deadline
}
type root struct {
	types.DefaultPluginContext
	id           uint32
	cfg          gatewayconfig.Config
	instanceID   string
	states       map[uint32]*state
	requests     map[uint32]*httpContext
	inflight     int64
	messages     int64
	rewrites     int64
	rewriteBytes int64
	q            deadlines
	metrics      map[string]proxywasm.MetricCounter
	gauges       map[string]proxywasm.MetricGauge
	gaugeValues  [5]int64
}

func (r *root) resourceMetrics() {
	if r.gauges == nil {
		r.gauges = map[string]proxywasm.MetricGauge{}
	}
	values := [5]int64{int64(len(r.states)), r.inflight, r.messages, r.rewrites, r.rewriteBytes}
	for i, name := range []string{"active_streams", "inflight_bytes", "inflight_messages", "active_rewrites", "rewrite_reserved_bytes"} {
		value := values[i]
		g, ok := r.gauges[name]
		if !ok {
			g = proxywasm.DefineGaugeMetric("metering_" + name)
			r.gauges[name] = g
		}
		if value != r.gaugeValues[i] {
			g.Add(value - r.gaugeValues[i])
			r.gaugeValues[i] = value
		}
	}
}

func (r *root) count(name string) {
	m, ok := r.metrics[name]
	if !ok {
		m = proxywasm.DefineCounterMetric("metering_" + name + "_total")
		r.metrics[name] = m
	}
	m.Increment(1)
}
func (r *root) OnPluginStart(_ int) types.OnPluginStartStatus {
	r.cleanup()
	r.instanceID = ""
	r.resourceMetrics()
	b, err := proxywasm.GetPluginConfiguration()
	if err == nil {
		r.cfg, err = gatewayconfig.Decode(b)
	}
	if err != nil {
		r.cfg.Enabled = false
		r.count("config_invalid")
		return types.OnPluginStartStatusOK
	}
	if r.cfg.Enabled {
		// Istio 1.13 exposes these bootstrap properties without changing the gateway
		// Pod template. Resolve once per root, never from request headers or config.
		r.instanceID = gatewayInstanceID(property("node", "metadata", "NAMESPACE"), property("node", "metadata", "NAME"))
		if r.instanceID == "" {
			r.cfg.Enabled = false
			r.count("gateway_identity_unavailable")
			return types.OnPluginStartStatusOK
		}
	}
	if proxywasm.SetTickPeriodMilliSeconds(uint32(r.cfg.TickMS)) != nil {
		r.cfg.Enabled = false
		r.count("config_invalid")
	}
	return types.OnPluginStartStatusOK
}
func (r *root) NewHttpContext(id uint32) types.HttpContext { return &httpContext{root: r, id: id} }
func (r *root) drop(s *state, reason string) {
	if s == nil {
		return
	}
	if _, ok := r.states[s.token]; !ok {
		return
	}
	delete(r.states, s.token)
	if s.timer != nil && s.timer.idx >= 0 {
		heap.Remove(&r.q, s.timer.idx)
	}
	r.inflight -= s.bytes
	r.messages -= int64(len(s.frames))
	s.bytes = 0
	s.frames = nil
	r.resourceMetrics()
	proxywasm.CancelGrpcStream(r.id, s.token)
	r.count("dropped_" + reason)
}
func (r *root) cleanup() {
	for _, h := range r.requests {
		h.downgrade(true)
	}
	for _, s := range r.states {
		r.drop(s, "shutdown")
	}
}
func (r *root) OnPluginDone() bool   { r.cleanup(); return true }
func (r *root) OnDelete()            { r.cleanup() }
func (r *root) active(s *state) bool { return s != nil && r.states[s.token] == s }
func (r *root) schedule(s *state) {
	if !r.active(s) {
		return
	}
	at := s.started.Add(time.Duration(r.cfg.ObservationMaxMS) * time.Millisecond)
	if len(s.frames) > 0 {
		d := s.frames[0].at.Add(time.Duration(r.cfg.AckTimeoutMS) * time.Millisecond)
		if d.Before(at) {
			at = d
		}
		if s.ending {
			d = s.frames[len(s.frames)-1].at.Add(time.Duration(r.cfg.EndAckTimeoutMS) * time.Millisecond)
			if d.Before(at) {
				at = d
			}
		}
	}
	if s.timer == nil {
		s.timer = &deadline{at: at, id: s.token, idx: -1}
		heap.Push(&r.q, s.timer)
	} else {
		s.timer.at = at
		if s.timer.idx >= 0 {
			heap.Fix(&r.q, s.timer.idx)
		} else {
			heap.Push(&r.q, s.timer)
		}
	}
}
func (r *root) send(s *state, kind int, b []byte, end bool) bool {
	if !r.active(s) || s.ending {
		return false
	}
	encoded := w.Frame(s.seq+1, kind, b)
	if kind == 10 {
		defer clear(encoded)
	}
	n := int64(len(encoded) + 5)
	maxB, maxM := r.cfg.StreamInflightBytes, r.cfg.StreamInflightMessages
	if kind == 12 {
		maxB -= 8192
		maxM -= 4
	}
	if len(encoded) > 32768 || (kind != 12 && int64(len(encoded)) > r.cfg.MetadataMaxBytes) || s.bytes+n > maxB || int64(len(s.frames))+1 > maxM || r.inflight+n > r.cfg.RootInflightBytes || r.messages+1 > r.cfg.RootInflightMessages {
		r.drop(s, "resource_limit")
		return false
	}
	r.inflight += n
	r.messages++
	s.bytes += n
	s.seq++
	s.frames = append(s.frames, frame{s.seq, n, time.Now()})
	r.resourceMetrics()
	if proxywasm.SendGrpcMessage(s.token, encoded, end) != nil {
		r.drop(s, "send_failed")
		return false
	}
	s.ending = end
	r.schedule(s)
	return true
}
func (r *root) OnReceive(token uint32, size int) {
	s := r.states[token]
	if s == nil {
		return
	}
	if size < 1 || int64(size) > r.cfg.AckMaxBytes {
		r.drop(s, "protocol_error")
		return
	}
	b, err := proxywasm.GetGrpcReceiveBuffer(size)
	if err != nil {
		r.drop(s, "protocol_error")
		return
	}
	a, err := w.DecodeAck(b)
	if err != nil || a.Seq > s.seq || s.id != "" && s.id != a.ID {
		r.drop(s, "protocol_error")
		return
	}
	s.id = a.ID
	for len(s.frames) > 0 && s.frames[0].seq <= a.Seq {
		n := s.frames[0].bytes
		s.frames = s.frames[1:]
		s.bytes -= n
		r.inflight -= n
		r.messages--
	}
	if a.Close {
		r.drop(s, "closed")
		return
	}
	r.resourceMetrics()
	r.schedule(s)
}
func (r *root) OnClose(token, status uint32) { r.drop(r.states[token], "transport_lost") }
func (r *root) OnTick() {
	for i := 0; i < 256 && len(r.q) > 0 && !r.q[0].at.After(time.Now()); i++ {
		d := heap.Pop(&r.q).(*deadline)
		if d.rewrite {
			if h := r.requests[d.id]; h != nil {
				h.downgrade(true)
			}
		} else {
			r.drop(r.states[d.id], "timeout")
		}
	}
}

type httpContext struct {
	types.DefaultHttpContext
	root       *root
	id         uint32
	selected   bool
	cfg        gatewayconfig.Config
	instanceID string
	s          *state
	rewrite    bool
	timer      *deadline
	status     uint64
	closed     bool
}

func header(k string) string         { v, _ := proxywasm.GetHttpRequestHeader(k); return v }
func responseHeader(k string) string { v, _ := proxywasm.GetHttpResponseHeader(k); return v }
func property(path ...string) string {
	b, err := proxywasm.GetProperty(path)
	if err != nil {
		return ""
	}
	return string(b)
}
func idValid(s string) bool { return len(s) > 0 && len(s) <= 128 && utf8.ValidString(s) }

// Keep the existing event/wire limit; reject rather than truncate identities.
func gatewayInstanceID(namespace, name string) string {
	validLabel := func(s string) bool {
		if len(s) == 0 || s[0] == '-' || s[len(s)-1] == '-' {
			return false
		}
		for _, c := range s {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
				return false
			}
		}
		return true
	}
	// Namespaces use DNS labels (63 bytes); Pod names use DNS subdomains
	// (253 bytes total, with no per-label length limit in Kubernetes validation).
	if len(namespace) > 63 || !validLabel(namespace) || len(name) > 253 || len(namespace)+1+len(name) > 128 {
		return ""
	}
	for _, label := range strings.Split(name, ".") {
		if !validLabel(label) {
			return ""
		}
	}
	return namespace + "/" + name
}
func (h *httpContext) OnHttpRequestHeaders(_ int, eos bool) types.Action {
	now := time.Now()
	r := h.root
	actualRoute := property("route_name")
	route := actualRoute
	if route == "" {
		route = property("route_metadata", "filter_metadata", "metering", "route_name")
	}
	rule, reason, ok := r.cfg.Match(route, header(":method"), header(":path"))
	if !ok {
		r.count("requests_ignored_" + reason)
		return types.ActionContinue
	}
	// Select before allocating rewrite/observation resources or reading credentials.
	authority := header(":authority")
	host := requesthost.Normalize(authority)
	if host == "" {
		r.count("requests_ignored_invalid_host")
		return types.ActionContinue
	}
	if !requesthost.Match(r.cfg.AcceptedRequestHosts, host) {
		r.count("requests_ignored_host")
		return types.ActionContinue
	}
	h.selected = true
	h.cfg = r.cfg
	h.instanceID = r.instanceID
	r.count("requests_selected")
	ct, enc := header("content-type"), header("content-encoding")
	mt, _, err := mime.ParseMediaType(strings.ToLower(ct))
	action := types.ActionContinue
	if !eos && err == nil && mt == "application/json" && (enc == "" || strings.EqualFold(enc, "identity")) {
		if r.rewrites < h.cfg.RootMaxRewrites && r.rewriteBytes+h.cfg.RewriteReservationBytes <= h.cfg.RootRewriteBudgetBytes {
			r.rewrites++
			r.rewriteBytes += h.cfg.RewriteReservationBytes
			r.resourceMetrics()
			h.rewrite = true
			r.requests[h.id] = h
			h.timer = &deadline{at: now.Add(time.Duration(h.cfg.RewriteTimeoutMS) * time.Millisecond), id: h.id, rewrite: true, idx: -1}
			heap.Push(&r.q, h.timer)
			if err := proxywasm.RemoveHttpRequestHeader("content-length"); err == nil {
				action = types.ActionContinue
			} else {
				h.releaseRewrite()
				r.count("rewrite_header_failed")
			}
		} else {
			r.count("rewrite_resource_limit")
		}
	}
	if int64(len(r.states)) >= h.cfg.RootMaxStreams {
		return action
	}
	upstream := grpcUpstream(h.cfg.ObserverCluster)
	token, err := proxywasm.OpenGrpcStream(r.id, upstream, "metering.v1.ObservationService", "Observe", r)
	if err != nil {
		r.count("dropped_open_failed")
		return action
	}
	s := &state{token: token, started: now}
	h.s = s
	r.states[token] = s
	r.resourceMetrics()
	s.eos[0] = eos
	b := w.UInt(nil, 1, uint64(now.UnixMilli()))
	b = w.String(b, 2, h.instanceID)
	b = w.String(b, 3, h.cfg.ConfigVersion)
	b = w.String(b, 4, routeinfo.Name(actualRoute))
	b = w.String(b, 6, rule.AdapterID)
	if len(authority) <= 512 && utf8.ValidString(authority) {
		b = w.String(b, 16, authority)
	}
	// Unlike get_header_map_value, this property joins duplicate header values.
	// A comma is rejected by Bearer; never fall back to the first header value.
	authorization, _ := proxywasm.GetProperty([]string{"request", "headers", "authorization"})
	apiKey := credentials.Bearer(authorization)
	if len(apiKey) > 0 {
		b = w.Bytes(b, 17, apiKey)
	}
	clear(authorization)
	b = w.String(b, 11, "POST")
	b = w.String(b, 12, ct)
	b = w.String(b, 13, enc)
	if eos {
		b = w.UInt(b, 14, 1)
	}
	x := header("x-request-id")
	valid := idValid(x)
	for _, c := range x {
		valid = valid && (c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("._:-", c))
	}
	if valid {
		b = w.String(b, 15, x)
	}
	for _, name := range h.cfg.LogicalRequestIDHeaders {
		// Envoy joins repeated field values with commas. Reject the candidate
		// rather than silently selecting one of several client IDs.
		value, err := proxywasm.GetProperty([]string{"request", "headers", name})
		if err == nil && requestid.Valid(string(value)) {
			b = w.Bytes(b, 18, value)
			clear(value)
			break
		}
		clear(value)
	}
	r.send(s, 10, b, false)
	clear(b)
	return action
}

// Istio cluster names contain '|', which is not a valid HTTP/2 authority for Go gRPC.
// Preserve the routing cluster and use its service DNS/port as :authority; SNI stays on the cluster.
func grpcUpstream(cluster string) []byte {
	authority := cluster
	parts := strings.Split(cluster, "|")
	if len(parts) == 4 && parts[0] == "outbound" && parts[1] != "" && parts[3] != "" {
		authority = parts[3] + ":" + parts[1]
	}
	envoy := w.String(nil, 1, cluster)
	return w.Bytes(nil, 1, w.String(envoy, 2, authority))
}
func (h *httpContext) releaseRewrite() {
	if !h.rewrite {
		return
	}
	h.rewrite = false
	delete(h.root.requests, h.id)
	h.root.rewrites--
	h.root.rewriteBytes -= h.cfg.RewriteReservationBytes
	h.root.resourceMetrics()
	if h.timer != nil && h.timer.idx >= 0 {
		heap.Remove(&h.root.q, h.timer.idx)
	}
}
func (h *httpContext) downgrade(resume bool) {
	if !h.rewrite {
		return
	}
	old := proxywasm.CurrentEffectiveContext()
	defer proxywasm.SetEffectiveContext(old)
	if proxywasm.SetEffectiveContext(h.id) != nil {
		h.releaseRewrite()
		h.root.drop(h.s, "context_lost")
		return
	}
	b, err := proxywasm.GetHttpRequestBody(0, int(h.cfg.RequestBodyMaxBytes)+1)
	if err == nil && len(b) <= int(h.cfg.RequestBodyMaxBytes) {
		h.observeBytes(0, b, false, true)
	} else {
		h.root.drop(h.s, "body_limit")
	}
	h.releaseRewrite()
	h.root.count("rewrite_passthrough")
	if resume {
		// Async resume in the fixed Envoy may bypass later body callbacks.
		// Stop after the bounded prefix when tail boundaries cannot be proven.
		h.root.drop(h.s, "rewrite_timeout_boundary")
		if proxywasm.ResumeHttpRequest() != nil {
			h.root.count("rewrite_resume_failed")
		}
	}
}
func (h *httpContext) OnHttpRequestBody(n int, eos bool) types.Action {
	if !h.selected {
		return types.ActionContinue
	}
	h.root.count("request_body_callbacks")
	if eos {
		h.root.count("request_body_eos")
	} else {
		h.root.count("request_body_not_eos")
	}
	if n > 0 {
		h.root.count("request_body_nonempty")
	}
	if h.rewrite {
		if n > int(h.cfg.RequestBodyMaxBytes) {
			h.releaseRewrite()
			h.root.drop(h.s, "body_limit")
			h.root.count("rewrite_body_limit")
			return types.ActionContinue
		}
		if !eos {
			return types.ActionPause
		}
		// Include conservative growth for the usage field before reading the
		// final buffer; rewrite itself remains independent of observation.
		if h.root.active(h.s) {
			h.bodyCapacity(0, min(n+128, int(h.cfg.RequestBodyMaxBytes)), eos)
		}
		b, err := proxywasm.GetHttpRequestBody(0, n)
		if err != nil {
			h.releaseRewrite()
			h.root.drop(h.s, "body_read_failed")
			return types.ActionContinue
		}
		out, err := strictjson.RewriteUsage(b, int(h.cfg.RequestBodyMaxBytes))
		if err != nil {
			out = b
			h.root.count("rewrite_invalid_json")
		}
		if err == nil {
			if proxywasm.ReplaceHttpRequestBody(out) != nil {
				h.root.drop(h.s, "rewrite_replace_failed")
				h.releaseRewrite()
				return types.ActionContinue
			}
		}
		h.observeBytes(0, out, eos, true)
		h.releaseRewrite()
		return types.ActionContinue
	}
	h.observeBuffer(0, n, eos)
	return types.ActionContinue
}
func (h *httpContext) observeBuffer(d, n int, eos bool) {
	if !h.root.active(h.s) {
		return
	}
	if int64(n) > h.cfg.CallbackMaxBytes || (n+int(h.cfg.ChunkBytes)-1)/int(h.cfg.ChunkBytes) > int(h.cfg.CallbackMaxChunks) {
		h.root.drop(h.s, "callback_limit")
		return
	}
	if !h.bodyCapacity(d, n, eos) {
		return
	}
	var b []byte
	var err error
	if d == 0 {
		b, err = proxywasm.GetHttpRequestBody(0, n)
	} else {
		b, err = proxywasm.GetHttpResponseBody(0, n)
	}
	if err != nil && n > 0 {
		h.root.drop(h.s, "body_read_failed")
		return
	}
	h.observeBytes(d, b, eos, false)
}
func (h *httpContext) observeBytes(d int, b []byte, eos, full bool) {
	s, r := h.s, h.root
	if !r.active(s) || s.eos[d] {
		return
	}
	if !h.bodyCapacity(d, len(b), eos) {
		return
	}
	chunks := (len(b) + int(h.cfg.ChunkBytes) - 1) / int(h.cfg.ChunkBytes)
	if chunks == 0 && eos {
		chunks = 1
	}
	for i := 0; i < chunks; i++ {
		from := i * int(h.cfg.ChunkBytes)
		to := min(from+int(h.cfg.ChunkBytes), len(b))
		v := w.UInt(nil, 1, uint64(d+1))
		v = w.UInt(v, 2, s.bodySeq[d]+1)
		v = w.UInt(v, 3, s.offset[d])
		v = w.Bytes(v, 4, b[from:to])
		end := eos && i == chunks-1
		if end {
			v = w.UInt(v, 5, 1)
		}
		if !r.send(s, 12, v, false) {
			return
		}
		s.bodySeq[d]++
		s.offset[d] += uint64(to - from)
		s.eos[d] = end
	}
}

// Call before copying streaming buffers. Root callbacks execute serially, so
// this check reserves capacity for this callback until the exact send ledger
// replaces the conservative estimate. The rewrite copy has its own budget.
func (h *httpContext) bodyCapacity(d, n int, eos bool) bool {
	s, r := h.s, h.root
	if !r.active(s) || s.eos[d] || n < 0 {
		return false
	}
	max := h.cfg.RequestBodyMaxBytes
	if d == 1 {
		max = h.cfg.ResponseBodyMaxBytes
	}
	if s.offset[d] > uint64(max) || uint64(n) > uint64(max)-s.offset[d] {
		r.drop(s, "body_limit")
		return false
	}
	chunks := (n + int(h.cfg.ChunkBytes) - 1) / int(h.cfg.ChunkBytes)
	if chunks == 0 && eos {
		chunks = 1
	}
	estimate := int64(n) + int64(chunks)*256
	if s.bytes+estimate > h.cfg.StreamInflightBytes-8192 || int64(len(s.frames))+int64(chunks) > h.cfg.StreamInflightMessages-4 || r.inflight+estimate > h.cfg.RootInflightBytes || r.messages+int64(chunks) > h.cfg.RootInflightMessages {
		r.drop(s, "resource_limit")
		return false
	}
	return true
}
func (h *httpContext) OnHttpResponseHeaders(_ int, eos bool) types.Action {
	if !h.selected {
		return types.ActionContinue
	}
	n, err := strconv.ParseUint(responseHeader(":status"), 10, 32)
	if err != nil || n < 200 || n > 599 {
		return types.ActionContinue
	}
	h.status = n
	b := w.UInt(nil, 1, n)
	b = w.String(b, 2, responseHeader("content-type"))
	b = w.String(b, 3, responseHeader("content-encoding"))
	if eos {
		b = w.UInt(b, 4, 1)
	}
	if h.root.send(h.s, 11, b, false) {
		h.s.eos[1] = eos
	}
	return types.ActionContinue
}
func (h *httpContext) OnHttpResponseBody(n int, eos bool) types.Action {
	if h.selected {
		h.observeBuffer(1, n, eos)
	}
	return types.ActionContinue
}
func (h *httpContext) OnHttpRequestTrailers(_ int) types.Action {
	if h.selected {
		if h.rewrite {
			h.downgrade(false)
			h.root.count("rewrite_trailers_unsupported")
		}
		h.observeBytes(0, nil, true, false)
	}
	return types.ActionContinue
}
func (h *httpContext) OnHttpResponseTrailers(_ int) types.Action {
	if h.selected {
		h.observeBytes(1, nil, true, false)
	}
	return types.ActionContinue
}
func (h *httpContext) OnHttpStreamDone() {
	if h.closed {
		return
	}
	h.closed = true
	h.releaseRewrite()
	if !h.selected || !h.root.active(h.s) {
		return
	}
	s := h.s
	b := w.UInt(nil, 1, uint64(time.Now().UnixMilli()))
	b = w.UInt(b, 2, s.bodySeq[0])
	b = w.UInt(b, 3, s.bodySeq[1])
	b = w.UInt(b, 4, s.offset[0])
	b = w.UInt(b, 5, s.offset[1])
	if s.eos[0] {
		b = w.UInt(b, 6, 1)
	}
	if s.eos[1] {
		b = w.UInt(b, 7, 1)
	}
	if h.status >= 200 {
		b = w.UInt(b, 8, h.status)
	}
	h.root.send(s, 13, b, true)
	h.s = nil
}
func (h *httpContext) OnDelete() {
	h.releaseRewrite()
	if !h.closed {
		h.root.drop(h.s, "context_deleted")
	}
	h.s = nil
}
