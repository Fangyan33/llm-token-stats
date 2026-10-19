package observation

import (
	"bytes"
	"context"
	"errors"
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

func lookupServer(k *sink) *Server {
	return &Server{Cfg: config.Observer{ProducerID: "test", ClusterName: "cluster", DeploymentEnvironment: "dev", MaxActive: 4, ParserBudget: 64 << 20, MaxObservationMS: 5000, LookupTimeoutMS: 150, LookupConcurrency: 1, AcceptedConfigs: []string{"v1"}, AcceptedAdapters: []string{"chat_completions_v1"}, AcceptedServices: []string{"a"}}, Sink: k, Health: &health.State{}, ServiceResolver: serviceFake{}, CustomerResolver: customerFake{}}
}

func lookupStart() *pb.Start {
	return &pb.Start{RequestStartedAtMs: time.Now().UnixMilli(), GatewayInstanceId: "gateway", ConfigVersion: "v1", AdapterId: "chat_completions_v1", Method: "POST", RequestAuthority: "model.example.com", ApiKey: []byte("synthetic-private"), RequestEndStream: true}
}

func awaitLookup(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("lookup did not finish within its bounded budget")
		}
		time.Sleep(time.Millisecond)
	}
}

func assertFrozenPair(t *testing.T, k *sink, service, customer string) {
	t.Helper()
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(k.es) != 2 {
		t.Fatalf("expected one start and one result, got %d", len(k.es))
	}
	for _, e := range k.es {
		if e.MappingStatus != service || e.CustomerMappingStatus != customer {
			t.Fatalf("attribution discarded: %s / %s", e.MappingStatus, e.CustomerMappingStatus)
		}
		if (e.ServiceName != nil) != (service == "mapped") || (e.CustomerName != nil) != (customer == "mapped") {
			t.Fatal("name snapshot lost or manufactured")
		}
		if events.IdentityHash(e) != events.IdentityHash(k.es[0]) {
			t.Fatal("identity differs between start and result")
		}
		b, err := events.Canonical(e)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := events.Decode(b, []byte(e.LogicalRequestID), time.Now()); err != nil {
			t.Fatal("invalid finalized event", err)
		}
	}
	if k.es[1].UsageEligible {
		t.Fatal("cancelled observation became eligible")
	}
}

func TestCancelledStreamRetainsIndependentResults(t *testing.T) {
	for _, mode := range []string{"customer_success", "service_success", "both_success_after_cancel", "both_timeout", "observation_deadline"} {
		t.Run(mode, func(t *testing.T) {
			k := &sink{}
			srv := lookupServer(k)
			if mode == "observation_deadline" {
				srv.Cfg.MaxObservationMS = 40
			}
			entered := make(chan struct{}, 2)
			release := make(chan struct{})
			srv.ServiceResolver = serviceFunc(func(ctx context.Context, request identitylookup.ServiceLookupRequest) (identitylookup.ServiceIdentity, error) {
				entered <- struct{}{}
				if mode != "service_success" {
					select {
					case <-release:
					case <-ctx.Done():
						return identitylookup.ServiceIdentity{}, ctx.Err()
					}
				}
				return serviceFake{}.Resolve(ctx, request)
			})
			srv.CustomerResolver = customerFunc(func(ctx context.Context, key []byte) (identitylookup.CustomerIdentity, error) {
				entered <- struct{}{}
				if mode != "customer_success" && mode != "observation_deadline" {
					select {
					case <-release:
					case <-ctx.Done():
						return identitylookup.CustomerIdentity{}, ctx.Err()
					}
				}
				return customerFake{}.Resolve(ctx, key)
			})
			listener := bufconn.Listen(1 << 20)
			server := grpc.NewServer(grpc.ForceServerCodec(Codec{}))
			pb.RegisterObservationServiceServer(server, srv)
			go server.Serve(listener)
			defer server.Stop()
			conn, err := grpc.NewClient("passthrough:///cancel", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stream, err := pb.NewObservationServiceClient(conn).Observe(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := stream.Send(&pb.ObservationFrame{SchemaVersion: 1, FrameSeq: 1, Payload: &pb.ObservationFrame_Start{Start: lookupStart()}}); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("queries not admitted")
				}
			}
			if mode != "observation_deadline" {
				cancel()
			}
			if mode == "both_success_after_cancel" {
				close(release)
			}
			awaitLookup(t, func() bool { k.mu.Lock(); defer k.mu.Unlock(); return len(k.es) == 2 && srv.active.Load() == 0 })
			service, customer := "lookup_timeout", "lookup_timeout"
			if mode == "service_success" || mode == "both_success_after_cancel" {
				service = "mapped"
			}
			if mode == "customer_success" || mode == "observation_deadline" || mode == "both_success_after_cancel" {
				customer = "mapped"
			}
			assertFrozenPair(t, k, service, customer)
			if srv.bytes.Load() != 0 {
				t.Fatal("parser reservation leaked")
			}
		})
	}
}

func TestLateResolverKeepsAdmissionAndCannotRewriteSnapshot(t *testing.T) {
	srv := lookupServer(&sink{})
	srv.Cfg.LookupTimeoutMS = 20
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	srv.CustomerResolver = customerFunc(func(ctx context.Context, key []byte) (identitylookup.CustomerIdentity, error) {
		<-release // Deliberately ignores cancellation until the test releases it.
		if string(key) != "synthetic-private" {
			t.Error("credential cleared while resolver still borrowed it")
		}
		return customerFake{}.Resolve(ctx, key)
	})
	ctx, cancel := context.WithCancel(context.Background())
	result := srv.beginResolve(ctx, lookupStart())
	cancel()
	var snapshot events.Event
	select {
	case snapshot = <-result:
	case <-time.After(time.Second):
		t.Fatal("coordinator waited indefinitely for resolver")
	}
	before, _ := events.Canonical(&snapshot)
	if snapshot.MappingStatus != "mapped" || snapshot.CustomerMappingStatus != "lookup_timeout" || len(srv.lookupSlots) != 1 {
		t.Fatal("timeout lost result or prematurely released admission")
	}
	if e := <-srv.beginResolve(context.Background(), lookupStart()); e.MappingStatus != "lookup_overloaded" {
		t.Fatal("late task escaped concurrency limit")
	}
	// Underlying work still owns the admission slot until it returns.
	close(release)
	awaitLookup(t, func() bool { return len(srv.lookupSlots) == 0 })
	after, _ := events.Canonical(&snapshot)
	if !bytes.Equal(before, after) {
		t.Fatal("late result modified frozen snapshot")
	}
}

func TestCompletedCustomerClearsTokenBeforeWorkerCleanup(t *testing.T) {
	srv := lookupServer(&sink{})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	release, finished := make(chan struct{}), make(chan struct{})
	key := []byte("synthetic-private")
	srv.CustomerResolver = customerFunc(func(ctx context.Context, token []byte) (identitylookup.CustomerIdentity, error) {
		<-release
		return customerFake{}.Resolve(ctx, token)
	})
	e := srv.resolveQueries(ctx, &pb.Start{RequestAuthority: "model.example.com", ApiKey: key}, func() { close(finished) })
	if e.CustomerMappingStatus != "lookup_timeout" || string(key) != "synthetic-private" {
		t.Fatal("early cleanup")
	}
	close(release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("worker cleanup did not finish")
	}
	if !bytes.Equal(key, make([]byte, len(key))) {
		t.Fatal("worker retained credential")
	}
}

func TestAlreadyCancelledParentDoesNotCancelAdmittedQueries(t *testing.T) {
	for i := 0; i < 20; i++ {
		srv := lookupServer(&sink{})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		e := <-srv.beginResolve(ctx, lookupStart())
		if e.MappingStatus != "mapped" || e.CustomerMappingStatus != "mapped" {
			t.Fatal("parent cancellation raced away completed attribution")
		}
	}
}

type ackFailureStream struct {
	grpc.ServerStream
	ctx    context.Context
	frames chan *pb.ObservationFrame
	send   func(*pb.ObservationAck) error
}

func (s *ackFailureStream) Context() context.Context        { return s.ctx }
func (s *ackFailureStream) Send(a *pb.ObservationAck) error { return s.send(a) }
func (s *ackFailureStream) Recv() (*pb.ObservationFrame, error) {
	select {
	case f := <-s.frames:
		return f, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func TestEndACKFailurePreservesEndAndFinalizesOnce(t *testing.T) {
	for _, mode := range []string{"write_error", "cancel", "ack_timeout"} {
		t.Run(mode, func(t *testing.T) {
			k := &sink{}
			srv := lookupServer(k)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stream := &ackFailureStream{ctx: ctx, frames: make(chan *pb.ObservationFrame, 3)}
			stream.send = func(a *pb.ObservationAck) error {
				if a.AcceptedFrameSeq < 3 {
					return nil
				}
				if mode == "ack_timeout" {
					<-ctx.Done()
				} else if mode == "cancel" {
					cancel()
				}
				return errors.New("synthetic ACK failure")
			}
			finish := time.Now().UnixMilli()
			frames := []*pb.ObservationFrame{
				{Payload: &pb.ObservationFrame_Start{Start: lookupStart()}},
				{Payload: &pb.ObservationFrame_ResponseHeaders{ResponseHeaders: &pb.ResponseHeaders{HttpStatus: 200, EndStream: true}}},
				{Payload: &pb.ObservationFrame_End{End: &pb.End{RequestFinishedAtMs: finish, RequestEndStreamSeen: true, ResponseEndStreamSeen: true}}},
			}
			for i, f := range frames {
				f.SchemaVersion, f.FrameSeq = 1, uint64(i+1)
				stream.frames <- f
			}
			done := make(chan error, 1)
			go func() { done <- srv.Observe(stream) }()
			select {
			case <-done:
			case <-time.After(4 * time.Second):
				t.Fatal("ACK failure blocked finalization")
			}
			cancel() // Release the deliberately blocked test Send/Recv.
			assertFrozenPair(t, k, "mapped", "mapped")
			k.mu.Lock()
			defer k.mu.Unlock()
			if !k.es[1].TerminalObserved || k.es[1].RequestFinishedAtMs == nil || *k.es[1].RequestFinishedAtMs != finish {
				t.Fatal("valid End discarded on ACK failure")
			}
		})
	}
}
