package observation

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/Fangyan33/llm-token-stats/internal/producer"
	"github.com/Fangyan33/llm-token-stats/internal/requesttokens"
)

// A request event joins the frozen identity with an independent tokenizer result.
// Jobs retain this small snapshot, never the observation or its response parser.
// Either side may arrive first, including after the response event was emitted.
type requestEvent struct {
	mu             sync.Mutex
	sink           producer.Sink
	captureEnabled bool
	snapshot       *events.Event
	result         requesttokens.Result
	ready, emitted bool
}

func (r *requestEvent) takeLocked() *events.Event {
	if r.emitted || r.snapshot == nil || !r.ready {
		return nil
	}
	r.emitted = true
	e := r.snapshot
	r.snapshot = nil
	if r.captureEnabled {
		e.RequestModel = r.result.RequestModel
		e.Usage = events.Unknown()
		e.UsageStatus = "unknown"
		if r.result.Value != nil {
			e.Usage.InputTokens = events.Token{Value: r.result.Value, Source: "estimated"}
			e.UsageStatus = "partial"
		}
	}
	e.EventCreatedAtMs = time.Now().UnixMilli()
	return e
}

func (r *requestEvent) freeze(e *events.Event) {
	r.mu.Lock()
	e.AnomalyCodes = append([]string{}, e.AnomalyCodes...)
	r.snapshot = e
	ready := r.takeLocked()
	r.mu.Unlock()
	if ready != nil {
		r.sink.Emit(ready)
	}
}

func (r *requestEvent) complete(result requesttokens.Result) {
	r.mu.Lock()
	if r.ready {
		r.mu.Unlock()
		return
	}
	r.result, r.ready = result, true
	ready := r.takeLocked()
	r.mu.Unlock()
	if ready != nil {
		r.sink.Emit(ready)
	}
}

type requestCapture struct {
	chunks           [][]byte
	bytes, reserved  int64
	maxBytes         int64
	done, notifyStop bool
}

func takeAtomic(a *atomic.Int64, n, max int64) bool {
	for {
		old := a.Load()
		if n < 0 || n > max-old {
			return false
		}
		if a.CompareAndSwap(old, old+n) {
			return true
		}
	}
}
func (s *Server) reserveRequest(n int64) bool {
	if !takeAtomic(&s.requestBytes, n, s.Cfg.ParserBudget/2) {
		return false
	}
	if !s.reserve(n) {
		s.requestBytes.Add(-n)
		return false
	}
	return true
}
func (s *Server) releaseRequest(n int64) { s.requestBytes.Add(-n); s.bytes.Add(-n) }
func (s *Server) reportToken(r requesttokens.Result) {
	s.Health.Count("request_tokens_" + r.Reason)
}
func (o *observation) unknownRequestTokens(reason string) {
	r := requesttokens.Result{Reason: reason}
	o.s.reportToken(r)
	o.requestEvent.complete(r)
}
func (o *observation) startRequest(st *pb.Start) {
	if !st.RequestBodyCaptureEnabled {
		o.unknownRequestTokens("disabled")
		return
	}
	c := o.s.Cfg
	if c.RequestTokenDefaults() != nil {
		o.unknownRequestTokens("resource_limit")
		return
	}
	o.s.tokenOnce.Do(func() {
		o.s.tokenPool = requesttokens.NewPool(c.RequestTokenizerWorkers, c.RequestTokenizerQueueRequests, c.RequestTokenizerQueueBytes, o.s.reserveRequest, o.s.releaseRequest, o.s.reportToken)
		o.s.Health.GaugeFunc("observer_request_reserved_bytes", o.s.requestBytes.Load)
		o.s.Health.Gauge("observer_tokenizer_workers", int64(c.RequestTokenizerWorkers))
		o.s.Health.GaugeFunc("observer_tokenizer_queue_requests", func() int64 { n, _ := o.s.tokenPool.Stats(); return int64(n) })
		o.s.Health.GaugeFunc("observer_tokenizer_queue_bytes", func() int64 { _, n := o.s.tokenPool.Stats(); return n })
	})
	o.request = &requestCapture{maxBytes: c.RequestBodyMaxBytes}
	enc := strings.TrimSpace(strings.ToLower(st.RequestContentEncoding))
	if enc != "" && enc != "identity" {
		o.stopRequest("unsupported_encoding")
	}
	if st.RequestEndStream {
		o.stopRequest("missing_body")
	}
}
func (o *observation) stopRequest(reason string) {
	r := o.request
	if r == nil || r.done {
		return
	}
	r.done = true
	r.notifyStop = true
	for _, b := range r.chunks {
		clear(b)
	}
	r.chunks = nil
	o.s.releaseRequest(r.reserved)
	r.reserved = 0
	o.unknownRequestTokens(reason)
}
func (o *observation) feedRequest(b *pb.BodyChunk) {
	r := o.request
	if r == nil || r.done {
		return
	}
	if o.eos[0] || b.BodySeq != o.bodySeq[0]+1 || b.Offset != o.offset[0] || len(b.Data) > 16384 || len(b.Data) == 0 && !b.EndStream {
		o.stopRequest("capture_incomplete")
		return
	}
	if int64(len(b.Data)) > r.maxBytes-r.bytes {
		o.stopRequest("body_limit")
		return
	}
	// Charge each segment and backing pointer storage before copying it.
	n := int64(len(b.Data) + 64)
	if !o.s.reserveRequest(n) {
		o.stopRequest("resource_limit")
		return
	}
	r.chunks = append(r.chunks, append([]byte(nil), b.Data...))
	r.reserved += n
	r.bytes += int64(len(b.Data))
	o.bodySeq[0] = b.BodySeq
	o.offset[0] += uint64(len(b.Data))
	o.eos[0] = b.EndStream
	if b.EndStream {
		j := requesttokens.Job{Chunks: r.chunks, Reserved: r.reserved, BodyBytes: r.bytes, Complete: o.requestEvent.complete}
		if !o.s.tokenPool.Submit(j) {
			o.stopRequest("queue_full")
			return
		}
		r.done = true
		r.chunks = nil
		r.reserved = 0
	}
}
func (s *Server) CloseRequestTokens() {
	if s.tokenPool != nil {
		s.tokenPool.Close()
	}
}
