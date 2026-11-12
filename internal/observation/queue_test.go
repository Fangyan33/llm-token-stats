package observation

import (
	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	"github.com/Fangyan33/llm-token-stats/internal/config"
	"testing"
)

func TestReceiveQueueGlobalBudgetAndCredentialCleanup(t *testing.T) {
	s := &Server{Cfg: config.Observer{ReceiveQueueBatches: 2, ReceiveQueueBytes: 32768}}
	a, b := s.newReceiveQueue(), s.newReceiveQueue()
	key := []byte("synthetic-private")
	rec := batchReceived{batch: &pb.ObservationBatch{BatchSeq: 1, Frames: []*pb.CaptureFrame{{CaptureId: 1, Frame: &pb.ObservationFrame{Payload: &pb.ObservationFrame_Start{Start: &pb.Start{ApiKey: key}}}}}}}
	if !a.push(rec) || !b.push(rec) || a.push(rec) {
		t.Fatal("global count not enforced")
	}
	a.close()
	b.close()
	if s.queueBatches != 0 || s.queueBytes != 0 {
		t.Fatal("queue ownership leaked")
	}
	for _, v := range key {
		if v != 0 {
			t.Fatal("queued credential not erased")
		}
	}
	if a.push(rec) || s.queueBytes != 0 || s.queueBatches != 0 {
		t.Fatal("enqueue after close leaked reservation")
	}
	s.Cfg.ReceiveQueueBytes = 32768
	large := batchReceived{batch: &pb.ObservationBatch{Frames: []*pb.CaptureFrame{{Frame: &pb.ObservationFrame{Payload: &pb.ObservationFrame_BodyChunk{BodyChunk: &pb.BodyChunk{Data: make([]byte, 16384)}}}}}}}
	if s.newReceiveQueue().push(large) {
		t.Fatal("byte budget not enforced")
	}
}
