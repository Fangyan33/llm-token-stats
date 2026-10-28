package observation

import (
	"context"
	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/Fangyan33/llm-token-stats/internal/health"
	"github.com/Fangyan33/llm-token-stats/internal/identitylookup"
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
	for _, mode := range []string{"trailers_eos", "bad_end", "resource_limit", "missing_route", "other_route", "legacy_wildcard", "overlong_route", "unimplemented", "new_service", "legacy_invalid_host", "client_id"} {
		t.Run(mode, func(t *testing.T) {
			badEnd, limited := mode == "bad_end", mode == "resource_limit"
			l := bufconn.Listen(1 << 20)
			h := &health.State{}
			k := &sink{}
			srv := &Server{Cfg: config.Observer{ProducerID: "test", ClusterName: "test-cluster", DeploymentEnvironment: "stg", MaxActive: 4, ParserBudget: 64 << 20, MaxObservationMS: 5000, AcceptedConfigs: []string{"v1"}, AcceptedAdapters: []string{"chat_completions_v1"}}, Sink: k, Health: h, ServiceResolver: serviceFake{}, CustomerResolver: customerFake{}}
			if mode == "unimplemented" {
				srv.ServiceResolver = nil
				srv.CustomerResolver = nil
			}
			if mode == "new_service" {
				srv.ServiceResolver = serviceFunc(func(ctx context.Context, r identitylookup.ServiceLookupRequest) (identitylookup.ServiceIdentity, error) {
					v, err := serviceFake{}.Resolve(ctx, r)
					v.ServiceID = "new-service-without-registration"
					return v, err
				})
			}
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
			st := &pb.Start{RequestStartedAtMs: time.Now().UnixMilli(), GatewayInstanceId: "gateway", ConfigVersion: "v1", RouteName: "chat", AdapterId: "chat_completions_v1", RequestAuthority: "Model.Example.invalid:443", ApiKey: []byte("synthetic-key"), Method: "POST", RequestContentType: "application/json"}
			if mode == "client_id" {
				st.LogicalRequestId = events.Ptr("Client-Correlation:42")
			}
			if mode == "legacy_invalid_host" {
				st.RequestAuthority = "127.0.0.1"
			}
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
			for _, event := range k.es {
				if event.ObservationID != ack.ObservationId || event.EventID != event.ObservationID+map[string]string{"request_started": ":start", "usage_observed": ":usage"}[event.EventType] {
					t.Fatal("events and ACK do not share an observation ID")
				}
				if (event.LogicalRequestID == nil) != (st.LogicalRequestId == nil) || event.LogicalRequestID != nil && *event.LogicalRequestID != *st.LogicalRequestId {
					t.Fatal("client ID was replaced or not frozen across events")
				}
			}
			if mode == "new_service" && (e.ServiceID == nil || *e.ServiceID != "new-service-without-registration") {
				t.Fatal("service ID still filtered")
			}
			if mode == "legacy_invalid_host" && (e.RequestHost != nil || e.MappingStatus != "invalid_host") {
				t.Fatal("legacy diagnostic contract changed")
			}
			if k.es[0].RouteName != wantRoute || e.RouteName != wantRoute {
				t.Fatalf("diagnostic route must be fixed across events: %q / %q", k.es[0].RouteName, e.RouteName)
			}
			b, _ := events.Canonical(e)
			if _, err = events.Decode(b, []byte(e.ObservationID), time.Now()); err != nil {
				t.Fatal(err)
			}
			if !badEnd && !limited && !e.UsageEligible {
				t.Fatal("not eligible", e.AnomalyCodes)
			}
			if mode == "unimplemented" && (e.MappingStatus != "lookup_not_implemented" || e.CustomerMappingStatus != "lookup_not_implemented" || e.IdentityStatus != "invalid" || e.UsageStatus != "known") {
				t.Fatal("stub changed usage or invented identity")
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

type serviceFake struct{}

func (serviceFake) Resolve(context.Context, identitylookup.ServiceLookupRequest) (identitylookup.ServiceIdentity, error) {
	return identitylookup.ServiceIdentity{ServiceID: "a", ProviderID: "p", ServiceName: "服务", ProviderName: "供应商"}, nil
}

type customerFake struct{}

func (customerFake) Resolve(context.Context, []byte) (identitylookup.CustomerIdentity, error) {
	return identitylookup.CustomerIdentity{APIKeyID: "k", CustomerID: "c", CustomerName: "客户"}, nil
}

func TestLookupDoesNotDelayAcksAndHasNoQueue(t *testing.T) {
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	entered := make(chan struct{})
	k := &sink{}
	srv := &Server{Cfg: config.Observer{ProducerID: "test", ClusterName: "cluster", DeploymentEnvironment: "stg", MaxActive: 4, ParserBudget: 64 << 20, MaxObservationMS: 5000, LookupTimeoutMS: 1500, LookupConcurrency: 1, AcceptedConfigs: []string{"v1"}, AcceptedAdapters: []string{"chat_completions_v1"}}, Sink: k, Health: &health.State{}, CustomerResolver: customerFake{}}
	disableLookupCache(srv)
	srv.ServiceResolver = serviceFunc(func(ctx context.Context, _ identitylookup.ServiceLookupRequest) (identitylookup.ServiceIdentity, error) {
		close(entered)
		select {
		case <-release:
			return serviceFake{}.Resolve(ctx, identitylookup.ServiceLookupRequest{})
		case <-ctx.Done():
			return identitylookup.ServiceIdentity{}, ctx.Err()
		}
	})
	l := bufconn.Listen(1 << 20)
	g := grpc.NewServer(grpc.ForceServerCodec(Codec{}))
	pb.RegisterObservationServiceServer(g, srv)
	go g.Serve(l)
	defer g.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	c, err := grpc.NewClient("passthrough:///async", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return l.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	stream, err := pb.NewObservationServiceClient(c).Observe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	st := &pb.Start{RequestStartedAtMs: time.Now().UnixMilli(), GatewayInstanceId: "gateway", ConfigVersion: "v1", AdapterId: "chat_completions_v1", Method: "POST", RequestAuthority: "model.example.com", ApiKey: []byte("private"), RequestEndStream: true}
	stream.Send(&pb.ObservationFrame{SchemaVersion: 1, FrameSeq: 1, Payload: &pb.ObservationFrame_Start{Start: st}})
	ackWithinBudget := func(wantSeq uint64) {
		t.Helper()
		done := make(chan bool, 1)
		go func() {
			for {
				a, err := stream.Recv()
				if err != nil {
					done <- false
					return
				}
				if a.CloseStream {
					done <- false
					return
				}
				if a.AcceptedFrameSeq >= wantSeq {
					done <- a.AcceptedFrameSeq >= 1
					return
				}
			}
		}()
		select {
		case ok := <-done:
			if !ok {
				t.Error("ACK failed")
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatal("ACK waited for lookup")
		}
	}
	ackWithinBudget(1)
	<-entered
	extra := &pb.Start{RequestAuthority: "model.example.com", ApiKey: []byte("second-private")}
	overload := <-srv.beginResolve(ctx, extra)
	if overload.MappingStatus != "lookup_overloaded" || overload.CustomerMappingStatus != "lookup_overloaded" || extra.ApiKey != nil {
		t.Fatal("unbounded lookup queue or retained credential")
	}
	k.mu.Lock()
	n := len(k.es)
	k.mu.Unlock()
	if n != 0 {
		t.Fatal("published unresolved identity")
	}
	stream.Send(&pb.ObservationFrame{SchemaVersion: 1, FrameSeq: 2, Payload: &pb.ObservationFrame_ResponseHeaders{ResponseHeaders: &pb.ResponseHeaders{HttpStatus: 200, EndStream: true}}})
	stream.Send(&pb.ObservationFrame{SchemaVersion: 1, FrameSeq: 3, Payload: &pb.ObservationFrame_End{End: &pb.End{RequestFinishedAtMs: time.Now().UnixMilli(), RequestEndStreamSeen: true, ResponseEndStreamSeen: true}}})
	ackWithinBudget(3)
	close(release)
	for {
		if _, err := stream.Recv(); err != nil {
			break
		}
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(k.es) != 2 || k.es[0].ServiceName == nil || k.es[1].ServiceName == nil || *k.es[0].ServiceName != *k.es[1].ServiceName {
		t.Fatal("snapshot not frozen across events")
	}
}
