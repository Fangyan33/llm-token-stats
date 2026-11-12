package observationwire

import (
	"bytes"
	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestSinglePassACKStrictValidationAndStorage(t *testing.T) {
	id := "7f6c2a90-4b13-4b68-9d21-67283e0c5f44"
	ack := String(UInt(String(nil, 1, id), 2, 1), 4, "accepted")
	item := Bytes(UInt(nil, 1, 7), 2, ack)
	good := Bytes(UInt(nil, 1, 1), 2, item)
	for name, b := range map[string][]byte{
		"duplicate_seq":     UInt(good, 1, 2),
		"zero_seq":          UInt(nil, 1, 0),
		"missing_seq":       Bytes(nil, 2, item),
		"duplicate_capture": Bytes(UInt(nil, 1, 1), 2, UInt(item, 1, 8)),
		"duplicate_ack":     Bytes(UInt(nil, 1, 1), 2, Bytes(item, 2, ack)),
		"duplicate_id":      Bytes(UInt(nil, 1, 1), 2, Bytes(UInt(nil, 1, 7), 2, String(ack, 1, id))),
		"bad_bool":          Bytes(UInt(nil, 1, 1), 2, Bytes(UInt(nil, 1, 7), 2, UInt(ack, 3, 2))),
		"wrong_wire":        Bytes(UInt(nil, 1, 1), 2, Bytes(UInt(nil, 1, 7), 2, Bytes(ack, 3, nil))),
		"invalid_utf8":      Bytes(UInt(nil, 1, 1), 2, Bytes(UInt(nil, 1, 7), 2, String(UInt(String(nil, 1, id), 2, 1), 4, string([]byte{255})))),
		"missing_payload":   Bytes(UInt(nil, 1, 1), 2, UInt(nil, 1, 7)),
		"unknown_reason":    Bytes(UInt(nil, 1, 1), 2, Bytes(UInt(nil, 1, 7), 2, String(UInt(String(nil, 1, id), 2, 1), 4, "other"))),
		"too_many_fields":   append(UInt(nil, 1, 1), bytes.Repeat(UInt(nil, 100, 1), 256)...),
		"noncanonical":      {8, 128, 0},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeBatchAck(b); err == nil {
				t.Fatal("invalid ACK accepted")
			}
		})
	}
	storage := make([]CaptureAck, 0, 128)
	got, err := DecodeBatchAckInto(good, storage)
	if err != nil || len(got.Acks) != 1 || &got.Acks[0] != &storage[:1][0] {
		t.Fatal("storage not reused", err)
	}
	clear(good)
	if got.Acks[0].ID != id || got.Acks[0].Reason != "accepted" {
		t.Fatal("ACK retained receive buffer")
	}
}

func TestDirectCaptureEncodingInteroperability(t *testing.T) {
	for _, seq := range []uint64{1, 127, 128, 1 << 32, ^uint64(0)} {
		for _, size := range []int{0, 100, 16384} {
			payload := Bytes(nil, 4, bytes.Repeat([]byte("x"), size))
			b := AppendCaptureHeader(nil, seq, seq, 12, len(payload))
			b = append(b, payload...)
			if len(b) != CaptureFrameSize(seq, seq, 12, len(payload)) {
				t.Fatal("length mismatch")
			}
			var batch pb.ObservationBatch
			if proto.Unmarshal(append(UInt(nil, 1, 1), b...), &batch) != nil || batch.Frames[0].CaptureId != seq || batch.Frames[0].Frame.FrameSeq != seq || !bytes.Equal(batch.Frames[0].Frame.GetBodyChunk().Data, bytes.Repeat([]byte("x"), size)) {
				t.Fatal("protobuf disagreement")
			}
		}
	}
}

func BenchmarkBatchACKDecode(b *testing.B) {
	for _, n := range []int{0, 1, 32, 128} {
		a := &pb.ObservationAckBatch{AcceptedBatchSeq: 42}
		for i := 0; i < n; i++ {
			a.Acks = append(a.Acks, &pb.CaptureAck{CaptureId: uint64(i + 1), Ack: &pb.ObservationAck{ObservationId: "7f6c2a90-4b13-4b68-9d21-67283e0c5f44", AcceptedFrameSeq: 12, Reason: "accepted"}})
		}
		raw, _ := proto.Marshal(a)
		b.Run([]string{"batch", "one", "32", "128"}[map[int]int{0: 0, 1: 1, 32: 2, 128: 3}[n]], func(b *testing.B) {
			storage := make([]CaptureAck, 0, 128)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := DecodeBatchAckInto(raw, storage); err != nil {
					b.Fatal(err)
				}
				clear(storage[:cap(storage)])
			}
		})
	}
}
