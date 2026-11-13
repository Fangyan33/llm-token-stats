package main

import (
	"container/heap"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
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
	return &root{id: id, states: map[uint64]*state{}}
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
	prev, next      *state
	bodySeq, offset [2]uint64
	eos             [2]bool
	ending          bool
	started         time.Time
	timer           *deadline
	requestCaptured uint64
	requestStopped  bool
	requestStopSent bool
	requestComplete bool
}
type batchRecord struct {
	seq                           uint64
	bytes, messages               int64
	at                            time.Time
	requestBytes, requestMessages int64
}
type channel struct {
	token                         uint32
	closing                       bool
	seq                           uint64
	batch                         []byte
	bytes, messages               int64
	records                       []batchRecord
	recordHead                    int
	retryAt                       time.Time
	head                          *state
	failed                        bool
	released                      bool
	cleanupReason                 string
	requestBytes, requestMessages int64
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
	requestBudget      *podBudget
	metrics            map[string]proxywasm.MetricCounter
	gauges             map[string]proxywasm.MetricGauge
	gaugeValues        [3]int64
	logs               wasmLogger
	ackStorage         [128]w.CaptureAck
	metricsDirty       bool
	metricsAt          time.Time
}

func (r *root) resourceMetrics() {
	if r.cfg.MetricsEnabled {
		r.metricsDirty = true
	}
}
func (r *root) publishResourceMetrics() {
	if !r.cfg.MetricsEnabled || !r.metricsDirty {
		return
	}
	r.metricsDirty = false
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
	if !r.cfg.MetricsEnabled {
		return
	}
	if r.metrics == nil {
		r.metrics = map[string]proxywasm.MetricCounter{}
	}
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
	defer func() { r.resourceMetrics(); r.publishResourceMetrics() }()
	r.cleanup()
	r.instanceID = ""
	r.resourceMetrics()
	b, err := proxywasm.GetPluginConfiguration()
	if err == nil {
		r.cfg, err = gatewayconfig.Decode(b)
	}
	if err != nil {
		r.logConfiguration(err)
		r.cfg.Enabled = false
		r.count("config_invalid")
		return types.OnPluginStartStatusOK
	}
	if r.cfg.Enabled {
		// Istio 1.13 exposes these bootstrap properties without changing the gateway
		// Pod template. Resolve once per root, never from request headers or config.
		r.instanceID = gatewayInstanceID(property("node", "metadata", "NAMESPACE"), property("node", "metadata", "NAME"))
		if r.instanceID == "" {
			r.log(logsafe.Error, "gateway_identity_unavailable", "reason", "missing_or_invalid_node_metadata", "required_metadata", "node.metadata.NAMESPACE,node.metadata.NAME", "action", "passthrough")
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
			r.logBudgetInitialization(budgetErr, "observation", "passthrough")
			r.cfg.Enabled = false
			r.count("budget_unavailable")
		} else {
			r.budget.owner = r
		}
		if r.cfg.RequestBodyCaptureEnabled && r.cfg.Enabled {
			requestCfg := r.cfg
			requestCfg.PodInflightBytes /= 4
			requestCfg.PodInflightMessages /= 4
			r.requestBudget, budgetErr = newPodBudget(requestCfg, r.instanceID+"/request-body")
			if budgetErr != nil {
				r.logBudgetInitialization(budgetErr, "request_body", "skip_request_body_capture")
				r.cfg.RequestBodyCaptureEnabled = false
			} else {
				r.requestBudget.owner = r
			}
		}
		r.channels = make([]*channel, int(r.cfg.TransportStreamsPerWorker))
	}
	r.log(logsafe.Info, "plugin_configured", "enabled", strconv.FormatBool(r.cfg.Enabled), "config_version", r.cfg.ConfigVersion, "observer_cluster", r.cfg.ObserverCluster, "log_level", r.logs.level.String())
	return types.OnPluginStartStatusOK
}
func (r *root) NewHttpContext(id uint32) types.HttpContext { return &httpContext{root: r, id: id} }

func (r *root) active(s *state) bool { return s != nil && !s.ch.failed && r.states[s.capture] == s }
func (r *root) drop(s *state, reason string) {
	if s == nil || r.states[s.capture] != s {
		return
	}
	delete(r.states, s.capture)
	if s.prev != nil {
		s.prev.next = s.next
	} else {
		s.ch.head = s.next
	}
	if s.next != nil {
		s.next.prev = s.prev
	}
	s.prev, s.next = nil, nil
	if s.timer != nil && s.timer.idx >= 0 {
		heap.Remove(&r.q, s.timer.idx)
	}
	r.budget.give(0, 0, 1)
	r.count("dropped_" + reason)
	switch reason {
	case "body_limit", "metadata_limit", "resource_limit", "observation_timeout":
		r.repeatedLog(logsafe.Warn, "observation_dropped", "reason", reason, "action", "skip_observation")
	case "body_read_failed", "protocol_error":
		r.repeatedLog(logsafe.Error, "observation_dropped", "reason", reason, "action", "skip_observation")
	}
	r.resourceMetrics()
}
func (r *root) cleanup() {
	for _, c := range r.channels {
		if c != nil {
			r.failChannel(c, "shutdown")
			// Teardown has no future ticks. Drain owned state, retaining native
			// credits if cancellation is uncertain.
			for c.failed && (c.head != nil || c.released && c.recordHead < len(c.records)) {
				r.reapChannel(c, 256)
			}
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
	if r.requestBudget != nil {
		if r.inflight == 0 && r.messages == 0 {
			r.requestBudget.close()
		} else {
			r.requestBudget.give(0, 0, 0)
		}
		r.requestBudget = nil
	}
	r.inflight = 0
	r.messages = 0
	r.channels = nil
	r.budget = nil
	r.publishResourceMetrics()
}
func (r *root) OnPluginDone() bool {
	r.cleanup()
	r.log(logsafe.Info, "plugin_stopped")
	r.flushLogs(maxPendingLogs)
	return true
}
func (r *root) OnDelete() { r.cleanup(); r.flushLogs(maxPendingLogs) }
func (r *root) openChannel(c *channel) bool {
	if c.closing || c.failed {
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
		r.repeatedLog(logsafe.Error, "grpc_open_failed", "transport_slot", r.transportSlot(c), "host_status", hostError(err), "action", "skip_observation")
		return false
	}
	c.token = token
	c.seq = 0
	c.released = false
	r.logTransportRecovered("grpc_open_failed", c)
	return true
}
func (r *root) failChannel(c *channel, reason string) {
	if c == nil {
		return
	}
	c.failed = true
	if c.cleanupReason == "" {
		c.cleanupReason = reason
	}
	if c.token != 0 {
		if err := proxywasm.CancelGrpcStream(r.id, c.token); err != nil {
			c.closing = true
			c.retryAt = time.Now().Add(100 * time.Millisecond)
			r.reapChannel(c, 64)
			r.repeatedLog(logsafe.Error, "grpc_cancel_failed", "host_status", hostError(err), "action", "retain_budget")
			return
		}
	}
	r.releaseChannel(c, reason)
}
func (r *root) releaseChannel(c *channel, reason string) {
	c.failed = true
	if c.cleanupReason == "" {
		c.cleanupReason = reason
	}
	c.token = 0
	c.closing = false
	c.retryAt = time.Now().Add(100 * time.Millisecond)
	if !c.released {
		r.inflight -= c.bytes
		r.messages -= c.messages
		r.budget.give(c.bytes, c.messages, 0)
		if r.requestBudget != nil {
			r.requestBudget.give(c.requestBytes, c.requestMessages, 0)
		}
		clear(c.batch)
		if c.batch != nil {
			c.batch = c.batch[:11]
		}
		c.bytes, c.messages = 0, 0
		c.requestBytes, c.requestMessages = 0, 0
		c.released = true
	}
	r.reapChannel(c, 64)
	if reason != "shutdown" && c.cleanupReason != "shutdown" {
		r.repeatedLog(logsafe.Error, "observation_"+reason, "action", "skip_observation")
	}
	r.resourceMetrics()
}

// Invalidation is immediate; reclamation has a fixed work budget per callback.
// Native write credits remain held until cancellation/closure is confirmed.
func (r *root) reapChannel(c *channel, limit int) int {
	used := 0
	for c.head != nil && used < limit {
		r.drop(c.head, c.cleanupReason)
		used++
	}
	if !c.released {
		return used
	}
	var n, m int64
	var rn, rm int64
	for c.recordHead < len(c.records) && used < limit {
		rec := c.records[c.recordHead]
		c.records[c.recordHead] = batchRecord{}
		c.recordHead++
		n += rec.bytes
		m += rec.messages
		rn += rec.requestBytes
		rm += rec.requestMessages
		used++
	}
	if n != 0 || m != 0 {
		r.inflight -= n
		r.messages -= m
		r.budget.give(n, m, 0)
		if r.requestBudget != nil {
			r.requestBudget.give(rn, rm, 0)
		}
	}
	if c.head == nil && c.recordHead == len(c.records) {
		c.records = c.records[:0]
		c.recordHead = 0
		c.failed = false
		c.cleanupReason = ""
	}
	return used
}
func (r *root) flush(c *channel) bool {
	if c == nil || c.messages == 0 {
		return true
	}
	if c.token == 0 {
		return false
	}
	var header [11]byte
	prefix := w.UInt(header[:0], 1, c.seq+1)
	start := 11 - len(prefix)
	copy(c.batch[start:11], prefix)
	b := c.batch[start:]
	if err := proxywasm.SendGrpcMessage(c.token, b, false); err != nil {
		r.repeatedLog(logsafe.Error, "grpc_send_failed", "transport_slot", r.transportSlot(c), "host_status", hostError(err), "action", "skip_observation")
		clear(b)
		r.failChannel(c, "send_failed")
		return false
	}
	r.logTransportRecovered("grpc_send_failed", c)
	clear(c.batch)
	c.batch = c.batch[:11]
	c.seq++
	if c.recordHead > 0 && len(c.records) == cap(c.records) {
		n := copy(c.records, c.records[c.recordHead:])
		clear(c.records[n:])
		c.records = c.records[:n]
		c.recordHead = 0
	}
	c.records = append(c.records, batchRecord{seq: c.seq, bytes: c.bytes, messages: c.messages, at: time.Now(), requestBytes: c.requestBytes, requestMessages: c.requestMessages})
	c.bytes = 0
	c.messages = 0
	c.requestBytes, c.requestMessages = 0, 0
	return true
}
func (r *root) send(s *state, kind int, b []byte, end bool) bool {
	c, n, ok := r.prepare(s, kind, len(b))
	if !ok {
		return false
	}
	c.batch = w.AppendCaptureHeader(c.batch, s.capture, s.seq+1, kind, len(b))
	c.batch = append(c.batch, b...)
	return r.commit(s, end, n)
}

// prepare claims Pod credits before encoding or copying a response fragment.
func (r *root) prepare(s *state, kind, payload int) (*channel, int64, bool) {
	return r.prepareFrame(s, kind, payload, false)
}
func (r *root) prepareFrame(s *state, kind, payload int, request bool) (*channel, int64, bool) {
	if !r.active(s) || s.ending && kind != 14 {
		return nil, 0, false
	}
	f := w.FrameSize(s.seq+1, kind, payload)
	if f > 32700 || (kind != 12 && int64(f) > r.cfg.MetadataMaxBytes) {
		r.cancel(s, "metadata_limit")
		return nil, 0, false
	}
	encoded := w.CaptureFrameSize(s.capture, s.seq+1, kind, payload)
	c := s.ch
	if int64(max(11, len(c.batch))+encoded) > r.cfg.TransportBatchMaxBytes || c.messages >= r.cfg.TransportBatchMaxFrames {
		if !r.flush(c) {
			return nil, 0, false
		}
	}
	n := int64(encoded + 16)
	if request && (r.requestBudget == nil || !r.requestBudget.take(n, 1, 0)) {
		r.requestBudget.logFailure(r, "request_capture_resource_limit", "skip_request_body_capture")
		return nil, 0, false
	}
	if !r.budget.take(n, 1, 0) {
		if request {
			r.budget.logFailure(r, "request_capture_resource_limit", "skip_request_body_capture")
			r.requestBudget.give(n, 1, 0)
			return nil, 0, false
		}
		r.count("resource_limit")
		r.budget.logFailure(r, "observation_resource_limit", "skip_observation")
		if kind == 14 {
			r.failChannel(c, "resource_limit")
		} else {
			r.cancel(s, "resource_limit")
		}
		return nil, 0, false
	}
	if c.batch == nil {
		c.batch = make([]byte, 11, int(r.cfg.TransportBatchMaxBytes))
	}
	return c, n, true
}
func (r *root) commit(s *state, end bool, n int64) bool {
	c := s.ch
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
	// Cancels share the bounded batch and credits; they never fabricate EOS/End.
	if c, n, ok := r.prepare(s, 14, 0); ok {
		c.batch = w.AppendCaptureHeader(c.batch, s.capture, s.seq+1, 14, 0)
		c.bytes += n
		c.messages++
		r.inflight += n
		r.messages++
		s.seq++
		if c.messages >= r.cfg.TransportBatchMaxFrames {
			r.flush(c)
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
		r.repeatedLog(logsafe.Error, "grpc_ack_rejected", "transport_slot", r.transportSlot(c), "reason", "invalid_ack_size", "actual_bytes", strconv.Itoa(size), "limit", strconv.FormatInt(r.cfg.AckMaxBytes, 10), "action", "skip_observation")
		r.failChannel(c, "protocol_error")
		return
	}
	b, err := proxywasm.GetGrpcReceiveBuffer(size)
	if err != nil {
		r.repeatedLog(logsafe.Error, "grpc_receive_failed", "transport_slot", r.transportSlot(c), "phase", "read_receive_buffer", "host_status", hostError(err), "action", "skip_observation")
		r.failChannel(c, "protocol_error")
		return
	}
	a, err := w.DecodeBatchAckInto(b, r.ackStorage[:0])
	clear(b)
	defer clear(r.ackStorage[:])
	if err != nil || a.Seq > c.seq {
		reason := "invalid_ack_encoding"
		if err == nil {
			reason = "ack_sequence_ahead"
		}
		r.repeatedLog(logsafe.Error, "grpc_ack_rejected", "transport_slot", r.transportSlot(c), "reason", reason, "action", "skip_observation")
		r.failChannel(c, "protocol_error")
		return
	}
	r.logTransportRecovered("grpc_receive_failed", c)
	r.logTransportRecovered("grpc_ack_rejected", c)
	r.logTransportRecovered("grpc_stream_closed", c)
	var freedBytes, freedMessages, requestBytes, requestMessages int64
	for c.recordHead < len(c.records) && c.records[c.recordHead].seq <= a.Seq {
		rec := c.records[c.recordHead]
		c.records[c.recordHead] = batchRecord{}
		c.recordHead++
		freedBytes += rec.bytes
		freedMessages += rec.messages
		requestBytes += rec.requestBytes
		requestMessages += rec.requestMessages
	}
	r.inflight -= freedBytes
	r.messages -= freedMessages
	r.budget.give(freedBytes, freedMessages, 0)
	if r.requestBudget != nil {
		r.requestBudget.give(requestBytes, requestMessages, 0)
	}
	if c.recordHead == len(c.records) {
		c.records = c.records[:0]
		c.recordHead = 0
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
		if ack.StopRequestBody {
			s.requestStopped = true
		}
		if ack.Close {
			r.drop(s, "closed")
		}
	}
	r.resourceMetrics()
}
func (r *root) OnClose(token, status uint32) {
	for _, c := range r.channels {
		if c != nil && c.token == token {
			if !c.closing && !c.failed && (status != 0 || c.head != nil || c.recordHead < len(c.records)) {
				r.repeatedLog(logsafe.Error, "grpc_stream_closed", "transport_slot", r.transportSlot(c), "grpc_status", strconv.FormatUint(uint64(status), 10), "action", "skip_observation")
			}
			r.releaseChannel(c, "transport_lost")
			return
		}
	}
}
func (r *root) OnTick() {
	remaining := 256
	for _, c := range r.channels {
		if c == nil {
			continue
		}
		if c.closing {
			if !time.Now().Before(c.retryAt) {
				r.failChannel(c, "cancel_retry")
			}
			if c.failed {
				remaining -= r.reapChannel(c, remaining)
			}
			continue
		}
		if c.failed {
			remaining -= r.reapChannel(c, remaining)
			continue
		}
		if c.recordHead < len(c.records) && time.Since(c.records[c.recordHead].at) > time.Duration(r.cfg.AckTimeoutMS)*time.Millisecond {
			r.failChannel(c, "timeout")
		} else {
			r.flush(c)
		}
	}
	if r.cfg.MetricsEnabled && time.Since(r.metricsAt) >= time.Second {
		r.metricsAt = time.Now()
		r.publishResourceMetrics()
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
		r.budget.logFailure(r, "observation_admission_failed", "skip_observation")
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
	s.next = c.head
	if c.head != nil {
		c.head.prev = s
	}
	c.head = s
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
		if h.cfg.RequestBodyCaptureEnabled {
			h.observeRequest(n, eos)
		}
		h.s.offset[0] += uint64(n)
		h.s.eos[0] = eos
	}
	return types.ActionContinue
}
func (h *httpContext) OnHttpRequestTrailers(_ int) types.Action {
	if h.root.active(h.s) {
		if h.cfg.RequestBodyCaptureEnabled {
			h.observeRequest(0, true)
		}
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
	h.sendRequestStop()
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
		last := eos && from+count == n
		elapsed := uint64(at.Sub(s.started).Microseconds())
		payload := 2 + 1 + w.VarSize(s.bodySeq[1]+1) + 1 + w.VarSize(s.offset[1]) + 1 + w.VarSize(uint64(count)) + count + 1 + w.VarSize(elapsed)
		if last {
			payload += 2
		}
		c, credit, ok := h.root.prepare(s, 12, payload)
		if !ok {
			return
		}
		var b []byte
		var err error
		if count > 0 {
			b, err = proxywasm.GetHttpResponseBody(from, count)
			if err != nil || len(b) != count {
				clear(b)
				h.root.budget.give(credit, 1, 0)
				h.root.repeatedLog(logsafe.Error, "response_body_read_failed", "host_status", hostError(err), "expected_bytes", strconv.Itoa(count), "actual_bytes", strconv.Itoa(len(b)), "action", "skip_observation")
				h.root.cancel(s, "body_read_failed")
				return
			}
		}
		v := w.AppendCaptureHeader(c.batch, s.capture, s.seq+1, 12, payload)
		v = w.UInt(w.UInt(w.UInt(v, 1, 2), 2, s.bodySeq[1]+1), 3, s.offset[1])
		v = w.Bytes(v, 4, b)
		clear(b)
		if last {
			v = w.UInt(v, 5, 1)
		}
		c.batch = w.UInt(v, 6, elapsed)
		if !h.root.commit(s, false, credit) {
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
	if h.cfg.RequestBodyCaptureEnabled {
		if s.requestComplete && !s.requestStopped {
			b = w.UInt(b, 12, 1)
		}
		b = w.UInt(b, 13, s.bodySeq[0])
	}
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
	if h.cfg.RequestBodyCaptureEnabled {
		b = w.UInt(b, 22, 1)
	}
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

func (r *root) logBudgetInitialization(err error, kind, action string) {
	if report := configdiag.Find(err); report != nil && len(report.Issues) > 0 {
		r.log(logsafe.Error, "budget_initialization_failed", append(report.Issues[0].Fields(), "budget_kind", kind, "action", action)...)
	}
}
