package observationwire

import (
	"bytes"
	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestBatchWireBoundsAndSingularFields(t *testing.T) {
	frame := Frame(1, 14, nil)
	item := Bytes(UInt(nil, 1, 9), 2, frame)
	good := Bytes(UInt(nil, 1, 1), 2, item)
	if ValidateBatch(good, false) != nil {
		t.Fatal("valid cancel batch rejected")
	}
	var decoded pb.ObservationBatch
	if proto.Unmarshal(good, &decoded) != nil || decoded.BatchSeq != 1 || decoded.Frames[0].CaptureId != 9 || decoded.Frames[0].Frame.GetCancel() == nil {
		t.Fatal("protobuf disagreement")
	}
	many := UInt(nil, 1, 1)
	for i := 0; i < 129; i++ {
		many = Bytes(many, 2, item)
	}
	for name, raw := range map[string][]byte{
		"duplicate_batch_seq": UInt(good, 1, 2), "missing_seq": Bytes(nil, 2, item), "missing_capture": Bytes(UInt(nil, 1, 1), 2, Bytes(nil, 2, frame)),
		"duplicate_capture": Bytes(UInt(nil, 1, 1), 2, UInt(item, 1, 10)), "duplicate_payload": Bytes(UInt(nil, 1, 1), 2, Bytes(item, 2, frame)),
		"too_many": many, "too_large": append(good, bytes.Repeat([]byte{0}, 32769)...), "invalid_varint": {8, 128},
	} {
		t.Run(name, func(t *testing.T) {
			if ValidateBatch(raw, false) == nil {
				t.Fatal("invalid batch accepted")
			}
		})
	}
	if Validate(Frame(1, 13, UInt(nil, 2, 1)), "frame") == nil {
		t.Fatal("reserved End field accepted")
	}
}
func TestCumulativeBatchACKWithCaptureClose(t *testing.T) {
	a := &pb.ObservationAckBatch{AcceptedBatchSeq: 42, Acks: []*pb.CaptureAck{{CaptureId: 7, Ack: &pb.ObservationAck{ObservationId: "7f6c2a90-4b13-4b68-9d21-67283e0c5f44", AcceptedFrameSeq: 12, CloseStream: true, Reason: "finalized"}}}}
	b, _ := proto.Marshal(a)
	got, err := DecodeBatchAck(b)
	if err != nil || got.Seq != 42 || len(got.Acks) != 1 || got.Acks[0].Capture != 7 || !got.Acks[0].Close {
		t.Fatal(got, err)
	}
	b, _ = proto.Marshal(&pb.ObservationAckBatch{AcceptedBatchSeq: 43})
	got, err = DecodeBatchAck(b)
	if err != nil || got.Seq != 43 || len(got.Acks) != 0 {
		t.Fatal("batch-only ACK rejected")
	}
}
