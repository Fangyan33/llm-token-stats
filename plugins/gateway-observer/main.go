package main

import (
	"container/heap"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Fangyan33/llm-token-stats/pkg/credentials"
	"github.com/Fangyan33/llm-token-stats/pkg/gatewayconfig"
	"github.com/Fangyan33/llm-token-stats/pkg/logsafe"
	w "github.com/Fangyan33/llm-token-stats/pkg/observationwire"
	"github.com/Fangyan33/llm-token-stats/pkg/requesthost"
	"github.com/Fangyan33/llm-token-stats/pkg/requestid"
	"github.com/Fangyan33/llm-token-stats/pkg/requestmeta"
	"github.com/Fangyan33/llm-token-stats/pkg/routeinfo"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm/types"
)

func main() { proxywasm.SetVMContext(&vm{}) }

type vm struct{ types.DefaultVMContext }

func (*vm) NewPluginContext(id uint32) types.PluginContext {
	return &root{id: id, states: map[uint64]*state{}, metrics: map[string]proxywasm.MetricCounter{}}
}

type deadline struct {
	at  time.Time
	id  uint64
	idx int
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

type state struct {
	capture         uint64
	seq             uint64
	id              string
	ch              *channel
	bodySeq, offset [2]uint64
	eos             [2]bool
	ending          bool
	started         time.Time
	timer           *deadline
}
type batchRecord struct {
	seq             uint64
	bytes, messages int64
	at              time.Time
}
type channel struct {
	token           uint32
	closing         bool
	seq             uint64
	batch           []byte
	bytes, messages int64
	records         []batchRecord
	retryAt         time.Time
}
type root struct {
	types.DefaultPluginContext
	id                 uint32
	cfg                gatewayconfig.Config
	instanceID         string
	states             map[uint64]*state
	channels           []*channel
	next               uint64
	inflight, messages int64
	q                  deadlines
	budget             *podBudget
	metrics            map[string]proxywasm.MetricCounter
	gauges             map[string]proxywasm.MetricGauge
	gaugeValues        [3]int64
	logs               wasmLogger
}

func (r *root) resourceMetrics() {
	if r.gauges == nil {
		r.gauges = map[string]proxywasm.MetricGauge{}
	}
	values := [3]int64{int64(len(r.states)), r.inflight, r.messages}
	for i, name := range []string{"active_streams", "inflight_bytes", "inflight_messages"} {
		value := values[i]
		g, ok := r.gauges[name]
		if !ok {
			var err error
			g, err = proxywasm.TryDefineGaugeMetric("metering_" + name)
			if err != nil {
				r.repeatedLog(logsafe.Warn, "metric_definition_failed", "metric", name, "host_status", hostError(err))
				continue
			}
			r.gauges[name] = g
		}
		if value != r.gaugeValues[i] {
			if err := g.TryAdd(value - r.gaugeValues[i]); err == nil {
				r.gaugeValues[i] = value
			} else {
				r.repeatedLog(logsafe.Warn, "metric_update_failed", "metric", name, "host_status", hostError(err))
			}
		}
	}
}

func (r *root) count(name string) {
	m, ok := r.metrics[name]
	if !ok {
		var err error
		m, err = proxywasm.TryDefineCounterMetric("metering_" + name + "_total")
		if err != nil {
			r.repeatedLog(logsafe.Warn, "metric_definition_failed", "metric", name, "host_status", hostError(err))
			return
		}
		r.metrics[name] = m
	}
	if err := m.TryIncrement(1); err != nil {
		r.repeatedLog(logsafe.Warn, "metric_update_failed", "metric", name, "host_status", hostError(err))
	}
}
func (r *root) OnPluginStart(_ int) types.OnPluginStartStatus {
	r.initLogging()
	defer r.flushLogs(maxPendingLogs)
	r.cleanup()
	r.instanceID = ""
	r.resourceMetrics()
	b, err := proxywasm.GetPluginConfiguration()
	if err == nil {
		r.cfg, err = gatewayconfig.Decode(b)
	}
	if err != nil {
		r.log(logsafe.Error, "plugin_configuration_failed", "reason", "invalid_configuration", "action", "passthrough")
		r.cfg.Enabled = false
		r.count("config_invalid")
		return types.OnPluginStartStatusOK
	}
	if r.cfg.Enabled {
		// Istio 1.13 exposes these bootstrap properties without changing the gateway
		// Pod template. Resolve once per root, never from request headers or config.
		r.instanceID = gatewayInstanceID(property("node", "metadata", "NAMESPACE"), property("node", "metadata", "NAME"))
		if r.instanceID == "" {
			r.log(logsafe.Error, "gateway_identity_unavailable", "action", "passthrough")
			r.cfg.Enabled = false
			r.count("gateway_identity_unavailable")
			return types.OnPluginStartStatusOK
		}
	}
	if err := proxywasm.SetTickPeriodMilliSeconds(uint32(r.cfg.TickMS)); err != nil {
		r.log(logsafe.Error, "plugin_tick_failed", "host_status", hostError(err), "action", "passthrough")
		r.cfg.Enabled = false
		r.count("config_invalid")
	}
	if r.cfg.Enabled {
		var budgetErr error
		r.budget, budgetErr = newPodBudget(r.cfg, r.instanceID)
		if budgetErr != nil {
			r.cfg.Enabled = false
			r.count("budget_unavailable")
		}
		r.channels = make([]*channel, int(r.cfg.TransportStreamsPerWorker))
	}
	r.log(logsafe.Info, "plugin_configured", "enabled", strconv.FormatBool(r.cfg.Enabled), "config_version", r.cfg.ConfigVersion, "observer_cluster", r.cfg.ObserverCluster, "log_level", r.logs.level.String())
	return types.OnPluginStartStatusOK
}
func (r *root) NewHttpContext(id uint32) types.HttpContext { return &httpContext{root: r, id: id} }

func (r *root) active(s *state) bool { return s != nil && r.states[s.capture] == s }
func (r *root) drop(s *state, reason string) {
	if !r.active(s) {
		return
	}
	delete(r.states, s.capture)
	if s.timer != nil && s.timer.idx >= 0 {
		heap.Remove(&r.q, s.timer.idx)
	}
	r.budget.give(0, 0, 1)
	r.count("dropped_" + reason)
	r.resourceMetrics()
}
func (r *root) cleanup() {
	for _, c := range r.channels {
		if c != nil {
			r.failChannel(c, "shutdown")
		}
	}
	for _, s := range r.states {
		r.drop(s, "shutdown")
	}
	if r.budget != nil {
		if r.inflight == 0 && r.messages == 0 {
			r.budget.close()
		} else {
			// A failed native cancel does not prove its send buffers are gone.
			// Keep those shared credits claimed across reloads; a Pod restart
			// clears the uncertain native state and the ledger together.
			r.budget.give(0, 0, 0)
		}
	}
	r.inflight = 0
	r.messages = 0
	r.channels = nil
	r.budget = nil
}
func (r *root) OnPluginDone() bool {
	r.cleanup()
	r.log(logsafe.Info, "plugin_stopped")
	r.flushLogs(maxPendingLogs)
	return true
}
func (r *root) OnDelete() { r.cleanup(); r.flushLogs(maxPendingLogs) }
func (r *root) openChannel(c *channel) bool {
	if c.closing {
		return false
	}
	if c.token != 0 {
		return true
	}
	if time.Now().Before(c.retryAt) {
		return false
	}
	token, err := proxywasm.OpenGrpcStream(r.id, grpcUpstream(r.cfg.ObserverCluster), "metering.v1.ObservationService", "Observe", r)
	if err != nil {
		c.retryAt = time.Now().Add(100 * time.Millisecond)
		r.count("dropped_open_failed")
		r.repeatedLog(logsafe.Error, "grpc_open_failed", "host_status", hostError(err))
		return false
	}
	c.token = token
	c.seq = 0
	r.logRecovered("grpc_open_failed")
	return true
}
func (r *root) failChannel(c *channel, reason string) {
	if c == nil {
		return
	}
	if c.token != 0 {
		if err := proxywasm.CancelGrpcStream(r.id, c.token); err != nil {
			c.closing = true
			c.retryAt = time.Now().Add(100 * time.Millisecond)
			for _, s := range r.states {
				if s.ch == c {
					r.drop(s, reason)
				}
			}
			r.repeatedLog(logsafe.Error, "grpc_cancel_failed", "host_status", hostError(err), "action", "retain_budget")
			return
		}
	}
	r.releaseChannel(c, reason)
}
func (r *root) releaseChannel(c *channel, reason string) {
	c.token = 0
	c.closing = false
	c.retryAt = time.Now().Add(100 * time.Millisecond)
	n, m := c.bytes, c.messages
	for _, rec := range c.records {
		n += rec.bytes
		m += rec.messages
	}
	r.inflight -= n
	r.messages -= m
	r.budget.give(n, m, 0)
	clear(c.batch)
	c.batch = nil
	c.bytes = 0
	c.messages = 0
	c.records = nil
	for _, s := range r.states {
		if s.ch == c {
			r.drop(s, reason)
		}
	}
	if reason != "shutdown" {
		r.repeatedLog(logsafe.Error, "observation_"+reason, "action", "skip_observation")
	}
	r.resourceMetrics()
}
func (r *root) flush(c *channel) bool {
	if c == nil || c.messages == 0 {
		return true
	}
	if c.token == 0 {
		return false
	}
	b := w.UInt(nil, 1, c.seq+1)
	b = append(b, c.batch...)
	if err := proxywasm.SendGrpcMessage(c.token, b, false); err != nil {
		clear(b)
		r.failChannel(c, "send_failed")
		return false
	}
	clear(b)
	clear(c.batch)
	c.batch = c.batch[:0]
	c.seq++
	c.records = append(c.records, batchRecord{c.seq, c.bytes, c.messages, time.Now()})
	c.bytes = 0
	c.messages = 0
	return true
}
func (r *root) send(s *state, kind int, b []byte, end bool) bool {
	if !r.active(s) || s.ending {
		return false
	}
	f := w.Frame(s.seq+1, kind, b)
	defer clear(f)
	if len(f) > 32700 || (kind != 12 && int64(len(f)) > r.cfg.MetadataMaxBytes) {
		r.cancel(s, "metadata_limit")
		return false
	}
	item := w.Bytes(w.UInt(nil, 1, s.capture), 2, f)
	defer clear(item)
	enc := w.Bytes(nil, 2, item)
	defer clear(enc)
	c := s.ch
	if int64(len(c.batch)+len(enc)+16) > r.cfg.TransportBatchMaxBytes || c.messages >= r.cfg.TransportBatchMaxFrames {
		if !r.flush(c) {
			return false
		}
	}
	n := int64(len(enc) + 16)
	if !r.budget.take(n, 1, 0) {
		r.count("resource_limit")
		r.repeatedLog(logsafe.Warn, "observation_resource_limit", "limit_kind", "pod_budget", "action", "skip_observation")
		r.cancel(s, "resource_limit")
		return false
	}
	c.batch = append(c.batch, enc...)
	c.bytes += n
	c.messages++
	r.inflight += n
	r.messages++
	s.seq++
	s.ending = end
	if s.timer == nil {
		s.timer = &deadline{at: s.started.Add(time.Duration(r.cfg.ObservationMaxMS) * time.Millisecond), id: s.capture, idx: -1}
		heap.Push(&r.q, s.timer)
	}
	if end {
		s.timer.at = time.Now().Add(time.Duration(r.cfg.EndAckTimeoutMS) * time.Millisecond)
		heap.Fix(&r.q, s.timer.idx)
	}
	r.resourceMetrics()
	if c.messages >= r.cfg.TransportBatchMaxFrames {
		return r.flush(c)
	}
	return true
}
func (r *root) cancel(s *state, reason string) {
	if !r.active(s) {
		return
	}
	// A control-only cancel does not claim EOS or a business End. Reserve a
	// separate bounded transport write; on failure close this channel.
	c := s.ch
	if c.token != 0 {
		if r.flush(c) {
			f := w.Frame(s.seq+1, 14, nil)
			item := w.Bytes(w.UInt(nil, 1, s.capture), 2, f)
			b := w.Bytes(w.UInt(nil, 1, c.seq+1), 2, item)
			n := int64(len(b) + 5)
			if r.budget.take(n, 1, 0) {
				c.seq++
				c.records = append(c.records, batchRecord{c.seq, n, 1, time.Now()})
				r.inflight += n
				r.messages++
				if err := proxywasm.SendGrpcMessage(c.token, b, false); err != nil {
					r.failChannel(c, "send_failed")
				}
			} else {
				r.failChannel(c, "resource_limit")
			}
			clear(b)
		}
	}
	r.drop(s, reason)
}
func (r *root) OnReceive(token uint32, size int) {
	var c *channel
	for _, x := range r.channels {
		if x != nil && x.token == token {
			c = x
			break
		}
	}
	if c == nil || c.closing {
		return
	}
	if size < 1 || int64(size) > r.cfg.AckMaxBytes {
		r.failChannel(c, "protocol_error")
		return
	}
	b, err := proxywasm.GetGrpcReceiveBuffer(size)
	if err != nil {
		r.failChannel(c, "protocol_error")
		return
	}
	a, err := w.DecodeBatchAck(b)
	if err != nil || a.Seq > c.seq {
		r.failChannel(c, "protocol_error")
		return
	}
	for len(c.records) > 0 && c.records[0].seq <= a.Seq {
		rec := c.records[0]
		c.records = c.records[1:]
		r.inflight -= rec.bytes
		r.messages -= rec.messages
		r.budget.give(rec.bytes, rec.messages, 0)
	}
	for _, ack := range a.Acks {
		s := r.states[ack.Capture]
		if s == nil {
			continue
		}
		if s.ch != c {
			r.failChannel(c, "protocol_error")
			return
		}
		if ack.Seq > s.seq || (s.id != "" && s.id != ack.ID) {
			r.cancel(s, "protocol_error")
			continue
		}
		s.id = ack.ID
		if ack.Close {
			r.drop(s, "closed")
		}
	}
	r.resourceMetrics()
}
func (r *root) OnClose(token, status uint32) {
	for _, c := range r.channels {
		if c != nil && c.token == token {
			r.releaseChannel(c, "transport_lost")
			return
		}
	}
}
func (r *root) OnTick() {
	for _, c := range r.channels {
		if c == nil {
			continue
		}
		if c.closing {
			if !time.Now().Before(c.retryAt) {
				r.failChannel(c, "cancel_retry")
			}
			continue
		}
		if len(c.records) > 0 && time.Since(c.records[0].at) > time.Duration(r.cfg.AckTimeoutMS)*time.Millisecond {
			r.failChannel(c, "timeout")
		} else {
			r.flush(c)
		}
	}
	for i := 0; i < 256 && len(r.q) > 0 && !r.q[0].at.After(time.Now()); i++ {
		d := heap.Pop(&r.q).(*deadline)
		r.cancel(r.states[d.id], "timeout")
	}
	r.flushLogs(8)
}

type httpContext struct {
	types.DefaultHttpContext
	root       *root
	id         uint32
	selected   bool
	cfg        gatewayconfig.Config
	instanceID string
	s          *state
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
	if r.budget == nil || !r.budget.take(0, 0, 1) {
		r.count("dropped_stream_limit")
		return types.ActionContinue
	}
	r.next++
	var c *channel
	for i := 0; i < len(r.channels); i++ {
		k := int((r.next + uint64(i)) % uint64(len(r.channels)))
		if r.channels[k] == nil {
			r.channels[k] = &channel{}
		}
		x := r.channels[k]
		if r.openChannel(x) {
			c = x
			break
		}
	}
	if c == nil {
		r.budget.give(0, 0, 1)
		return types.ActionContinue
	}
	s := &state{capture: r.next, ch: c, started: now}
	h.s = s
	r.states[s.capture] = s
	s.eos[0] = eos
	b := h.startPayload(now, eos, authority, actualRoute, rule.AdapterID, header("content-type"), header("content-encoding"))
	r.send(s, 10, b, false)
	clear(b)
	return types.ActionContinue
}
func (h *httpContext) OnHttpRequestBody(n int, eos bool) types.Action {
	if n >= 0 && h.root.active(h.s) {
		h.s.offset[0] += uint64(n)
		h.s.eos[0] = eos
	}
	return types.ActionContinue
}
func (h *httpContext) OnHttpRequestTrailers(_ int) types.Action {
	if h.root.active(h.s) {
		h.s.eos[0] = true
	}
	return types.ActionContinue
}
func (h *httpContext) OnHttpResponseHeaders(_ int, eos bool) types.Action {
	if !h.root.active(h.s) {
		return types.ActionContinue
	}
	n, err := strconv.ParseUint(responseHeader(":status"), 10, 32)
	if err != nil || n < 200 || n > 599 {
		return types.ActionContinue
	}
	h.status = n
	b := w.String(w.String(w.UInt(nil, 1, n), 2, responseHeader("content-type")), 3, responseHeader("content-encoding"))
	if eos {
		b = w.UInt(b, 4, 1)
	}
	if h.root.send(h.s, 11, b, false) {
		h.s.eos[1] = eos
	}
	return types.ActionContinue
}
func (h *httpContext) observeResponse(n int, eos bool, at time.Time) {
	s := h.s
	if !h.root.active(s) || s.eos[1] {
		return
	}
	if n < 0 || s.offset[1] > uint64(h.cfg.ResponseBodyMaxBytes) || uint64(n) > uint64(h.cfg.ResponseBodyMaxBytes)-s.offset[1] {
		h.root.cancel(s, "body_limit")
		return
	}
	for from := 0; from < n || (n == 0 && eos && from == 0); {
		count := min(int(h.cfg.ChunkBytes), n-from)
		var b []byte
		var err error
		if count > 0 {
			b, err = proxywasm.GetHttpResponseBody(from, count)
			if err != nil {
				h.root.cancel(s, "body_read_failed")
				return
			}
		}
		v := w.UInt(w.UInt(w.UInt(nil, 1, 2), 2, s.bodySeq[1]+1), 3, s.offset[1])
		v = w.Bytes(v, 4, b)
		last := eos && from+count == n
		if last {
			v = w.UInt(v, 5, 1)
		}
		v = w.UInt(v, 6, uint64(at.Sub(s.started).Microseconds()))
		if !h.root.send(s, 12, v, false) {
			return
		}
		s.bodySeq[1]++
		s.offset[1] += uint64(count)
		s.eos[1] = last
		from += count
		if count == 0 {
			break
		}
	}
}
func (h *httpContext) OnHttpResponseBody(n int, eos bool) types.Action {
	h.observeResponse(n, eos, time.Now())
	return types.ActionContinue
}
func (h *httpContext) OnHttpResponseTrailers(_ int) types.Action {
	h.observeResponse(0, true, time.Now())
	return types.ActionContinue
}
func (h *httpContext) OnHttpStreamDone() {
	if h.closed {
		return
	}
	h.closed = true
	s := h.s
	if !h.root.active(s) {
		return
	}
	b := w.UInt(nil, 1, uint64(time.Now().UnixMilli()))
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
	if !h.closed {
		h.root.cancel(h.s, "context_deleted")
	}
	h.s = nil
}
func (h *httpContext) startPayload(now time.Time, eos bool, authority, route, adapter, ct, enc string) []byte {
	b := w.UInt(nil, 1, uint64(now.UnixMilli()))
	b = w.String(b, 2, h.instanceID)
	b = w.String(b, 3, h.cfg.ConfigVersion)
	b = w.String(b, 4, routeinfo.Name(route))
	b = w.String(b, 6, adapter)
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
	// Start is always the first frame in a new observation stream.
	return h.requestMetadata(b, 1)
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

// requestMetadata appends optional fields only while both budgets allow them.
// Failed optional reads never drop the existing Start or pause business traffic.
func (h *httpContext) requestMetadata(b []byte, seq uint64) []byte {
	var uid, agent *string
	extras := map[string]string{}
	fits := func(next []byte) bool {
		return len(w.Frame(seq, 10, next)) <= int(h.cfg.MetadataMaxBytes) && requestmeta.JSONSize(uid, agent, extras) <= requestmeta.MaxJSONBytes
	}
	if len(h.cfg.UserIDHeader) > 0 {
		headers, err := proxywasm.GetHttpRequestHeaders()
		if err == nil {
			for _, name := range h.cfg.UserIDHeader {
				count, value := 0, ""
				for _, pair := range headers {
					if strings.EqualFold(pair[0], name) {
						count++
						value = pair[1]
					}
				}
				if count != 1 || !requestmeta.Value(value, requestmeta.MaxUserID) {
					continue
				}
				uid = &value
				next := w.String(b, 19, value)
				if fits(next) {
					b = next
					break
				}
				uid = nil
			}
		}
	}
	value, err := proxywasm.GetProperty([]string{"request", "headers", "user-agent"})
	if err == nil && requestmeta.Value(string(value), requestmeta.MaxUserAgent) {
		v := string(value)
		agent = &v
		next := w.String(b, 20, v)
		if fits(next) {
			b = next
		} else {
			agent = nil
		}
	}
	clear(value)
	for _, name := range h.cfg.ExtraRequestHeaders {
		value, err := proxywasm.GetProperty([]string{"request", "headers", name})
		if err == nil && requestmeta.Value(string(value), requestmeta.MaxValue) {
			v := string(value)
			extras[name] = v
			entry := w.String(w.String(nil, 1, name), 2, v)
			next := w.Bytes(b, 21, entry)
			if fits(next) {
				b = next
			} else {
				delete(extras, name)
			}
			clear(entry)
		}
		clear(value)
	}
	return b
}
