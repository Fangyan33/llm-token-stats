package observationwire

import "testing"

func TestRequestBodyStopWireAndACK(t *testing.T) {
	id := "11111111-1111-4111-8111-111111111111"
	b := UInt(String(UInt(String(nil, 1, id), 2, 3), 4, "accepted"), 5, 1)
	a, err := DecodeAck(b)
	if err != nil || !a.StopRequestBody || a.Close {
		t.Fatal(a, err)
	}
	for _, bad := range [][]byte{UInt(b, 5, 1), UInt(String(UInt(String(nil, 1, id), 2, 3), 4, "accepted"), 5, 2)} {
		if _, err := DecodeAck(bad); err == nil {
			t.Fatal("invalid request stop ack")
		}
	}
	if Validate(Frame(2, 15, nil), "frame") != nil {
		t.Fatal("stop frame")
	}
	if Validate(UInt(nil, 22, 2), "start") == nil {
		t.Fatal("invalid bool accepted")
	}
}
