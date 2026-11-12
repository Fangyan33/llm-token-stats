package observation

import (
	"context"
	"github.com/Fangyan33/llm-token-stats/internal/observation/testutil"
	"io"
	"net"
	"testing"
	"time"

	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/Fangyan33/llm-token-stats/internal/health"
	"github.com/Fangyan33/llm-token-stats/internal/identitylookup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestResponseTimingEvents(t *testing.T) {
	for _, mode := range []string{"sse", "hour", "missing_usage", "missing_first_time", "regressing_time", "json", "missing_end", "unsupported"} {
		t.Run(mode, func(t *testing.T) {
			k := &sink{}
			srv := &Server{Cfg: config.Observer{ProducerID: "test", ClusterName: "test-cluster", DeploymentEnvironment: "dev", MaxActive: 4, ParserBudget: 64 << 20, AcceptedConfigs: []string{"v1"}, AcceptedAdapters: []string{"chat_completions_v1"}}, Sink: k, Health: &health.State{}, CustomerResolver: customerFake{}, ServiceResolver: serviceFunc(func(ctx context.Context, r identitylookup.ServiceLookupRequest) (identitylookup.ServiceIdentity, error) {
				time.Sleep(40 * time.Millisecond)
				return serviceFake{}.Resolve(ctx, r)
			})}
			if mode == "hour" {
				// Exercise the fixed safety net near one hour without a wall-clock
				// wait; it must still accept the final usage and int64 timings.
				srv.observationNow = func() time.Time { return time.Now().Add(time.Hour) }
			}
			l := bufconn.Listen(1 << 20)
			g := grpc.NewServer(grpc.ForceServerCodec(Codec{}))
			pb.RegisterObservationServiceServer(g, srv)
			go g.Serve(l)
			defer g.Stop()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, err := grpc.NewClient("passthrough:///test", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return l.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			stream, err := testutil.New(c).Observe(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var seq uint64
			send := func(f *pb.ObservationFrame) {
				t.Helper()
				seq++
				f.SchemaVersion = 1
				f.FrameSeq = seq
				if err := stream.Send(f); err != nil {
					t.Fatal(err)
				}
			}
			now := time.Now().UnixMilli()
			send(&pb.ObservationFrame{Payload: &pb.ObservationFrame_Start{Start: &pb.Start{RequestStartedAtMs: now, GatewayInstanceId: "gateway", ConfigVersion: "v1", AdapterId: "chat_completions_v1", RequestAuthority: "model.example.invalid", ApiKey: []byte("synthetic-key"), Method: "POST", RequestContentType: "application/json"}}})
			if _, err = stream.Recv(); err != nil {
				t.Fatal(err)
			}
			req := []byte(`{"stream":true,"model":"test"}`)
			ct := "text/event-stream"
			if mode == "json" {
				ct = "application/json"
			}
			if mode == "unsupported" {
				ct = "text/plain"
			}
			send(&pb.ObservationFrame{Payload: &pb.ObservationFrame_ResponseHeaders{ResponseHeaders: &pb.ResponseHeaders{HttpStatus: 200, ContentType: ct}}})
			pieces := []string{"data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n", "data: {\"choices\":[{\"delta\":{\"content\":\"last\"}}]}\n\n", "data: {\"choices\":[],\"usage\":{\"completion_tokens\":5}}\n\ndata: [DONE]\n\n"}
			if mode == "missing_usage" {
				pieces[2] = "data: [DONE]\n\n"
			}
			if mode == "json" {
				pieces = []string{`{"usage":{"completion_tokens":5}}`}
			}
			var bytes uint64
			elapsed := []int64{100000, 300000, 900000}
			if mode == "hour" {
				elapsed = []int64{3540000000, 3599000000, 3600000000}
			}
			for i, b := range pieces {
				us := events.Ptr(elapsed[i])
				if mode == "missing_first_time" && i == 0 {
					us = nil
				}
				if mode == "regressing_time" && i == 1 {
					us = events.Ptr(int64(50000))
				}
				send(&pb.ObservationFrame{Payload: &pb.ObservationFrame_BodyChunk{BodyChunk: &pb.BodyChunk{Direction: pb.Direction_RESPONSE, BodySeq: uint64(i + 1), Offset: bytes, Data: []byte(b), EndStream: i == len(pieces)-1, ObservedElapsedUs: us}}})
				bytes += uint64(len(b))
			}
			if mode != "missing_end" {
				finished := now + 1000
				if mode == "hour" {
					finished = now + 3600000
				}
				send(&pb.ObservationFrame{Payload: &pb.ObservationFrame_End{End: &pb.End{RequestFinishedAtMs: finished, LastResponseBodySeq: uint64(len(pieces)), RequestBytesSeen: uint64(len(req)), ResponseBytesSeen: bytes, RequestEndStreamSeen: true, ResponseEndStreamSeen: true}}})
			}
			stream.CloseSend()
			for {
				_, err := stream.Recv()
				if err != nil {
					if err != io.EOF {
						t.Fatal(err)
					}
					break
				}
			}
			k.mu.Lock()
			defer k.mu.Unlock()
			if len(k.es) != 2 {
				t.Fatalf("events=%d", len(k.es))
			}
			for _, e := range k.es {
				raw, _ := events.Canonical(e)
				if _, err := events.Decode(raw, []byte(e.ObservationID), time.Now()); err != nil {
					t.Fatal(err)
				}
			}
			start, e := k.es[0], k.es[1]
			if start.ResponseStreaming != nil || start.FirstOutputElapsedUS != nil || start.LastOutputElapsedUS != nil {
				t.Fatal("response metadata leaked into start snapshot")
			}
			wantFirst := mode != "json" && mode != "missing_first_time" && mode != "unsupported"
			wantLast := wantFirst && mode != "regressing_time"
			if (e.FirstOutputElapsedUS != nil) != wantFirst || (e.LastOutputElapsedUS != nil) != wantLast {
				t.Fatalf("timing nullability wrong: %#v", e)
			}
			if wantFirst && *e.FirstOutputElapsedUS != elapsed[0] {
				t.Fatal("first time changed")
			}
			if wantLast && *e.LastOutputElapsedUS != elapsed[1] {
				t.Fatal("usage tail counted as generation")
			}
			if mode == "unsupported" {
				if e.ResponseStreaming != nil {
					t.Fatal("unsupported response type guessed")
				}
			} else if e.ResponseStreaming == nil || *e.ResponseStreaming != (mode != "json") {
				t.Fatal("streaming flag")
			}
			if mode != "missing_usage" && mode != "unsupported" && (e.Usage.OutputTokens.Value == nil || *e.Usage.OutputTokens.Value != 5) {
				t.Fatal("lost reported output")
			}
		})
	}
}
