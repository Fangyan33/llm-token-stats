package observationwire

import (
	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestStandardInterop(t *testing.T) {
	b := Frame(1, 10, String(UInt(nil, 1, 1234), 2, "gateway"))
	var f pb.ObservationFrame
	if proto.Unmarshal(b, &f) != nil || f.GetStart().GatewayInstanceId != "gateway" {
		t.Fatal("decode")
	}
	a, _ := proto.Marshal(&pb.ObservationAck{LogicalRequestId: "7f6c2a90-4b13-4b68-9d21-67283e0c5f44", AcceptedFrameSeq: 4, Reason: "accepted"})
	if ack, err := DecodeAck(a); err != nil || ack.Seq != 4 {
		t.Fatal("ack")
	}
	for _, bad := range [][]byte{append(b, UInt(nil, 1, 1)...), Frame(1, 10, append(UInt(nil, 1, 1), UInt(nil, 1, 2)...)), append(b, Bytes(nil, 13, nil)...), {0x88, 0, 1}, Frame(1, 10, String(nil, 2, string([]byte{255}))), Frame(1, 11, UInt(nil, 1, 1<<32)), Frame(1, 11, UInt(nil, 4, 2)), Frame(1, 13, UInt(nil, 7, 2)), Frame(1, 13, UInt(nil, 9, 1<<32))} {
		if Validate(bad, "frame") == nil {
			t.Fatal("invalid wire accepted")
		}
	}
}

func TestReservedIdentityAndCredentialWireLimits(t *testing.T) {
	for _, field := range []int{5, 7, 8, 9, 10} {
		if Validate(Frame(1, 10, String(nil, field, "old-identity")), "frame") == nil {
			t.Fatal("reserved identity accepted")
		}
	}
	for _, payload := range [][]byte{Bytes(nil, 17, make([]byte, 4097)), String(nil, 16, string(make([]byte, 513))), append(Bytes(nil, 17, []byte("first")), Bytes(nil, 17, []byte("second"))...)} {
		if Validate(Frame(1, 10, payload), "frame") == nil {
			t.Fatal("unbounded or repeated credential accepted")
		}
	}
}
