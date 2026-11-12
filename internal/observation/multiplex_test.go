package observation

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestMultiplexedCaptureFailureAndConnectionReuse(t *testing.T) {
	k := &sink{}
	srv := lookupServer(k)
	srv.Cfg.MaxActive = 2
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.ForceServerCodec(Codec{}))
	pb.RegisterObservationServiceServer(server, srv)
	go server.Serve(listener)
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///multiplex", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := pb.NewObservationServiceClient(conn).Observe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var batch uint64
	send := func(items ...*pb.CaptureFrame) {
		t.Helper()
		batch++
		if e := stream.Send(&pb.ObservationBatch{BatchSeq: batch, Frames: items}); e != nil {
			t.Fatal(e)
		}
	}
	start := func(id uint64) *pb.CaptureFrame {
		st := lookupStart()
		same := "same-client-id"
		st.LogicalRequestId = &same
		return &pb.CaptureFrame{CaptureId: id, Frame: &pb.ObservationFrame{SchemaVersion: 1, FrameSeq: 1, Payload: &pb.ObservationFrame_Start{Start: st}}}
	}
	closed := map[uint64]string{}
	awaitClosed := func(id uint64) {
		t.Helper()
		for closed[id] == "" {
			a, e := stream.Recv()
			if e != nil {
				t.Fatal(e)
			}
			for _, v := range a.Acks {
				if v.Ack.CloseStream {
					closed[v.CaptureId] = v.Ack.ObservationId
				}
			}
		}
	}
	send(start(1), start(2), start(3)) // Admission rejects only capture 3, keeps 1/2.
	awaitClosed(3)
	headers := func(id uint64) *pb.CaptureFrame {
		return &pb.CaptureFrame{CaptureId: id, Frame: &pb.ObservationFrame{SchemaVersion: 1, FrameSeq: 2, Payload: &pb.ObservationFrame_ResponseHeaders{ResponseHeaders: &pb.ResponseHeaders{HttpStatus: 200, ContentType: "application/json"}}}}
	}
	body := func(id uint64, data []byte) *pb.CaptureFrame {
		return &pb.CaptureFrame{CaptureId: id, Frame: &pb.ObservationFrame{SchemaVersion: 1, FrameSeq: 3, Payload: &pb.ObservationFrame_BodyChunk{BodyChunk: &pb.BodyChunk{Direction: pb.Direction_RESPONSE, BodySeq: 1, Data: data, EndStream: true}}}}
	}
	response := []byte(`{"model":"fixture","usage":{"prompt_tokens":1000000,"completion_tokens":1024,"total_tokens":1001024}}`)
	end := func(id uint64) *pb.CaptureFrame {
		return &pb.CaptureFrame{CaptureId: id, Frame: &pb.ObservationFrame{SchemaVersion: 1, FrameSeq: 4, Payload: &pb.ObservationFrame_End{End: &pb.End{RequestFinishedAtMs: time.Now().UnixMilli(), LastResponseBodySeq: 1, ResponseBytesSeen: uint64(len(response)), RequestEndStreamSeen: true, ResponseEndStreamSeen: true}}}}
	}
	send(headers(1), headers(2), body(1, []byte("{")), body(2, response), end(2))
	awaitClosed(1)
	awaitClosed(2)
	// Late frames for closed IDs and a new valid capture share the same stream.
	send(end(1), start(4))
	send(headers(4), body(4, response), end(4))
	awaitClosed(4)
	if closed[1] == closed[2] || closed[2] == closed[4] {
		t.Fatal("client ID collapsed observations")
	}
	stream.CloseSend()
	for {
		_, err = stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	awaitLookup(t, func() bool { return srv.active.Load() == 0 && srv.bytes.Load() == 0 })
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(k.es) != 6 {
		t.Fatalf("events=%d; rejected capture must not fabricate events", len(k.es))
	}
	known := 0
	for _, e := range k.es {
		if e.EventType == "usage_observed" && e.UsageEligible {
			known++
			if e.RequestModel != nil || e.Usage.InputTokens.Value == nil || *e.Usage.InputTokens.Value != 1000000 {
				t.Fatal("response-only usage changed")
			}
		}
	}
	if known != 2 {
		t.Fatalf("valid captures affected by neighbor: known=%d", known)
	}
}
