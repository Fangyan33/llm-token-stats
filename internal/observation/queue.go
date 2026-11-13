package observation

import (
	"github.com/Fangyan33/llm-token-stats/internal/logging"
	"google.golang.org/protobuf/proto"
	"sync"
)

// One bounded queue per transport, with a single Pod-wide count/byte budget.
// AcceptedBatchSeq acknowledges this ownership, not parsing or Kafka durability.
type receiveQueue struct {
	s      *Server
	mu     sync.Mutex
	closed bool
	ch     chan batchReceived
}

func (s *Server) newReceiveQueue() *receiveQueue {
	n := s.Cfg.ReceiveQueueBatches
	if n == 0 {
		n = 4096
	}
	return &receiveQueue{s: s, ch: make(chan batchReceived, min(n, 128))}
}
func (q *receiveQueue) push(rec batchReceived) bool {
	// Charge protobuf objects, strings, slice backing arrays and wire payload,
	// not just payload lengths. gRPC's in-progress Recv has the stream reserve.
	n := int64(256 + 4*proto.Size(rec.batch) + 256*len(rec.batch.Frames))
	maxBytes, maxBatches := q.s.Cfg.ReceiveQueueBytes, q.s.Cfg.ReceiveQueueBatches
	if maxBytes == 0 {
		maxBytes = 128 << 20
	}
	if maxBatches == 0 {
		maxBatches = 4096
	}
	q.s.queueMu.Lock()
	if q.s.queueBytes+n > maxBytes || q.s.queueBatches >= maxBatches {
		usedBytes, usedBatches := q.s.queueBytes, q.s.queueBatches
		q.s.queueMu.Unlock()
		reason := "byte_limit"
		if usedBatches >= maxBatches {
			reason = "batch_limit"
		}
		logging.Warn("observer_receive_queue_full", "observer_receive_queue", "reason", reason, "queued_bytes", usedBytes, "batch_bytes", n, "byte_limit", maxBytes, "queued_batches", usedBatches, "batch_limit", maxBatches, "action", "close_observation_transport")
		return false
	}
	q.s.queueBytes += n
	q.s.queueBatches++
	q.s.queueMu.Unlock()
	rec.reserved = n
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		select {
		case q.ch <- rec:
			logging.Recovered("observer_receive_queue", "observer_receive_queue_recovered")
			return true
		default:
			logging.Warn("observer_receive_queue_full", "observer_receive_queue", "reason", "transport_batch_limit", "batch_limit", cap(q.ch), "action", "close_observation_transport")
		}
	}
	q.release(rec)
	return false
}
func clearBatch(rec batchReceived) {
	if rec.batch == nil {
		return
	}
	for _, x := range rec.batch.Frames {
		if st := x.GetFrame().GetStart(); st != nil {
			clear(st.ApiKey)
			st.ApiKey = nil
		}
		if b := x.GetFrame().GetBodyChunk(); b != nil {
			clear(b.Data)
			b.Data = nil
		}
	}
}
func (q *receiveQueue) release(rec batchReceived) {
	if rec.reserved == 0 {
		return
	}
	q.s.queueMu.Lock()
	q.s.queueBytes -= rec.reserved
	q.s.queueBatches--
	q.s.queueMu.Unlock()
}
func (q *receiveQueue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	for {
		select {
		case rec := <-q.ch:
			clearBatch(rec)
			q.release(rec)
		default:
			return
		}
	}
}
