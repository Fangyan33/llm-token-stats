package observation

import (
	"context"
	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/Fangyan33/llm-token-stats/internal/health"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

type sink struct {
	mu sync.Mutex
	es []*events.Event
}

func (s *sink) Emit(e *events.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := *e
	s.es = append(s.es, &copy)
}
func TestObserveEOSAndOnceFinalization(t *testing.T) {
	for _, mode := range []string{"trailers_eos", "bad_end", "resource_limit", "missing_route", "other_route", "legacy_wildcard", "overlong_route"} {
		t.Run(mode, func(t *testing.T) {
			badEnd, limited := mode == "bad_end", mode == "resource_limit"
			l := bufconn.Listen(1 << 20)
			h := &health.State{}
			k := &sink{}
			srv := &Server{Cfg: config.Observer{ProducerID: "test", MaxActive: 4, ParserBudget: 64 << 20, MaxObservationMS: 5000, AcceptedConfigs: []string{"v1"}, AcceptedAdapters: []string{"chat_completions_v1"}, Routes: []config.Route{{RouteName: "chat", ServiceID: "a"}}}, Sink: k, Health: h}
			if limited {
				srv.Cfg.ParserBudget = 3 * 32768
			}
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
			stream, err := pb.NewObservationServiceClient(c).Observe(ctx)
			if err != nil {
				t.Fatal(err)
			}
			st := &pb.Start{RequestStartedAtMs: time.Now().UnixMilli(), GatewayInstanceId: "gateway", ConfigVersion: "v1", RouteName: "chat", ServiceId: "a", AdapterId: "chat_completions_v1", IdentityStatus: "known", ApiKeyId: events.Ptr("k"), CustomerId: events.Ptr("c"), ApplicationId: events.Ptr("a"), Method: "POST", RequestContentType: "application/json"}
			wantRoute := "chat"
			switch mode {
			case "missing_route":
				st.RouteName, wantRoute = "", "unknown"
			case "other_route":
				st.RouteName, wantRoute = "different.actual-route", "different.actual-route"
			case "legacy_wildcard":
				st.RouteName, wantRoute = "*", "unknown"
			case "overlong_route":
				st.RouteName, wantRoute = strings.Repeat("r", 129), "unknown"
			}
			if err = stream.Send(&pb.ObservationFrame{SchemaVersion: 1, FrameSeq: 1, Payload: &pb.ObservationFrame_Start{Start: st}}); err != nil {
				t.Fatal(err)
			}
			ack, err := stream.Recv()
			if err != nil || ack.AcceptedFrameSeq != 1 {
				t.Fatal("start ack", err)
			}
			req := []byte(`{"model":"test","stream":false}`)
			resp := []byte(`{"model":"test","usage":{"prompt_tokens":0,"completion_tokens":4,"total_tokens":4}}`)
			frames := []*pb.ObservationFrame{{Payload: &pb.ObservationFrame_BodyChunk{BodyChunk: &pb.BodyChunk{Direction: pb.Direction_REQUEST, BodySeq: 1, Data: req, EndStream: true}}}, {Payload: &pb.ObservationFrame_ResponseHeaders{ResponseHeaders: &pb.ResponseHeaders{HttpStatus: 200, ContentType: "application/json"}}}, {Payload: &pb.ObservationFrame_BodyChunk{BodyChunk: &pb.BodyChunk{Direction: pb.Direction_RESPONSE, BodySeq: 1, Data: resp}}}, {Payload: &pb.ObservationFrame_BodyChunk{BodyChunk: &pb.BodyChunk{Direction: pb.Direction_RESPONSE, BodySeq: 2, Offset: uint64(len(resp)), EndStream: true}}}, {Payload: &pb.ObservationFrame_End{End: &pb.End{RequestFinishedAtMs: time.Now().UnixMilli(), LastRequestBodySeq: 1, LastResponseBodySeq: 2, RequestBytesSeen: uint64(len(req)), ResponseBytesSeen: uint64(len(resp)), RequestEndStreamSeen: true, ResponseEndStreamSeen: !badEnd}}}}
			if limited {
				frames = frames[:1]
			}
			for i, f := range frames {
				f.SchemaVersion = 1
				f.FrameSeq = uint64(i + 2)
				if stream.Send(f) != nil {
					t.Fatal("send")
				}
			}
			stream.CloseSend()
			for {
				_, err = stream.Recv()
				if err != nil {
					break
				}
			}
			k.mu.Lock()
			defer k.mu.Unlock()
			if len(k.es) != 2 {
				t.Fatal("not exactly two", len(k.es))
			}
			e := k.es[1]
			if k.es[0].RouteName != wantRoute || e.RouteName != wantRoute {
				t.Fatalf("diagnostic route must be fixed across events: %q / %q", k.es[0].RouteName, e.RouteName)
			}
			b, _ := events.Canonical(e)
			if _, err = events.Decode(b, []byte(e.LogicalRequestID), time.Now()); err != nil {
				t.Fatal(err)
			}
			if !badEnd && !limited && !e.UsageEligible {
				t.Fatal("not eligible", e.AnomalyCodes)
			}
			if (badEnd || limited) && (e.TerminalObserved || e.UsageEligible) {
				t.Fatal("bad end accepted")
			}
			if limited && !events.Has(e.AnomalyCodes, "parse_limit") {
				t.Fatal("resource limit did not map to Kafka anomaly enum")
			}
		})
	}
}
