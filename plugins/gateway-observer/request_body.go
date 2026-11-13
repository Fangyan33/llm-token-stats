package main

import (
	w "github.com/Fangyan33/llm-token-stats/pkg/observationwire"
	"github.com/tetratelabs/proxy-wasm-go-sdk/proxywasm"
)

func (h *httpContext) sendRequestStop() {
	s := h.s
	if !h.root.active(s) || !s.requestStopped || s.requestStopSent {
		return
	}
	// Best effort, without canceling the observation if no credit is available.
	c, n, ok := h.root.prepareFrame(s, 15, 0, true)
	if !ok {
		return
	}
	c.batch = w.AppendCaptureHeader(c.batch, s.capture, s.seq+1, 15, 0)
	c.requestBytes += n
	c.requestMessages++
	h.root.commit(s, false, n)
	s.requestStopSent = true
}
func (h *httpContext) observeRequest(size int, eos bool) {
	s := h.s
	r := h.root
	if !r.active(s) || s.requestComplete {
		return
	}
	if s.requestStopped {
		h.sendRequestStop()
		return
	}
	for from := 0; from < size || size == 0 && eos && from == 0; {
		count := min(int(h.cfg.ChunkBytes), size-from)
		last := eos && from+count == size
		payload := 2 + 1 + w.VarSize(s.bodySeq[0]+1) + 1 + w.VarSize(s.requestCaptured) + 1 + w.VarSize(uint64(count)) + count
		if last {
			payload += 2
		}
		c, credit, ok := r.prepareFrame(s, 12, payload, true)
		if !ok {
			s.requestStopped = true
			h.sendRequestStop()
			return
		}
		var b []byte
		var err error
		if count > 0 {
			b, err = proxywasm.GetHttpRequestBody(from, count)
		}
		if err != nil || len(b) != count {
			clear(b)
			r.budget.give(credit, 1, 0)
			r.requestBudget.give(credit, 1, 0)
			s.requestStopped = true
			h.sendRequestStop()
			return
		}
		v := w.AppendCaptureHeader(c.batch, s.capture, s.seq+1, 12, payload)
		v = w.UInt(w.UInt(w.UInt(v, 1, 1), 2, s.bodySeq[0]+1), 3, s.requestCaptured)
		v = w.Bytes(v, 4, b)
		clear(b)
		if last {
			v = w.UInt(v, 5, 1)
		}
		c.batch = v
		c.requestBytes += credit
		c.requestMessages++
		if !r.commit(s, false, credit) {
			return
		}
		s.bodySeq[0]++
		s.requestCaptured += uint64(count)
		s.requestComplete = last
		from += count
		if count == 0 {
			break
		}
	}
}
