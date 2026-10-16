package observation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	"github.com/Fangyan33/llm-token-stats/internal/adapters/chatcompletions"
	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/Fangyan33/llm-token-stats/internal/health"
	"github.com/Fangyan33/llm-token-stats/internal/identitylookup"
	"github.com/Fangyan33/llm-token-stats/internal/producer"
	"github.com/Fangyan33/llm-token-stats/pkg/credentials"
	"github.com/Fangyan33/llm-token-stats/pkg/observationwire"
	"github.com/Fangyan33/llm-token-stats/pkg/requesthost"
	"github.com/Fangyan33/llm-token-stats/pkg/routeinfo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"io"
	"math"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

type Codec struct{}

func (Codec) Name() string                  { return "proto" }
func (Codec) Marshal(v any) ([]byte, error) { return proto.Marshal(v.(proto.Message)) }
func (Codec) Unmarshal(b []byte, v any) error {
	if _, ok := v.(*pb.ObservationFrame); ok {
		if observationwire.Validate(b, "frame") != nil {
			return errors.New("protocol_error")
		}
	}
	return proto.Unmarshal(b, v.(proto.Message))
}

type Server struct {
	pb.UnimplementedObservationServiceServer
	Cfg              config.Observer
	Sink             producer.Sink
	Health           *health.State
	active           atomic.Int64
	bytes            atomic.Int64
	Random           io.Reader
	ServiceResolver  identitylookup.ServiceResolver
	CustomerResolver identitylookup.CustomerResolver
}

func (s *Server) reserve(n int64) bool {
	for {
		old := s.bytes.Load()
		if n > s.Cfg.ParserBudget-old {
			return false
		}
		if s.bytes.CompareAndSwap(old, old+n) {
			return true
		}
	}
}
func (s *Server) newID() (string, error) {
	var b [16]byte
	r := s.Random
	if r == nil {
		r = rand.Reader
	}
	if _, err := io.ReadFull(r, b[:]); err != nil {
		s.Health.Ready.Store(false)
		s.Health.Count("random_failure")
		return "", errors.New("random_source")
	}
	s.Health.Ready.Store(true)
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	h := hex.EncodeToString(b[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:], nil
}

type received struct {
	f   *pb.ObservationFrame
	err error
}
type ackWrite struct {
	a    *pb.ObservationAck
	done chan error
}

func (s *Server) Observe(stream pb.ObservationService_ObserveServer) error {
	s.Health.GaugeFunc("observer_active_streams", s.active.Load)
	s.Health.GaugeFunc("observer_parser_reserved_bytes", s.bytes.Load)
	if s.active.Add(1) > int64(s.Cfg.MaxActive) {
		s.active.Add(-1)
		return status.Error(codes.ResourceExhausted, "resource_limit")
	}
	defer s.active.Add(-1)
	// Allow for the decoded frame, one queued frame, and a receive in progress.
	reservation := int64(3 * 32768)
	if !s.reserve(reservation) {
		return status.Error(codes.ResourceExhausted, "resource_limit")
	}
	defer func() { s.bytes.Add(-reservation) }()
	ctx, cancel := context.WithTimeout(stream.Context(), time.Duration(s.Cfg.MaxObservationMS)*time.Millisecond)
	defer cancel()
	recv := make(chan received, 1)
	go func() {
		for {
			f, err := stream.Recv()
			select {
			case recv <- received{f, err}:
			case <-ctx.Done():
				if st := f.GetStart(); st != nil {
					clear(st.ApiKey)
					st.ApiKey = nil
				}
				return
			}
			if err != nil {
				return
			}
		}
	}()
	acks := make(chan ackWrite, 1)
	var writing atomic.Int64
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case a := <-acks:
				writing.Store(time.Now().UnixNano())
				err := stream.Send(a.a)
				writing.Store(0)
				if a.done != nil {
					a.done <- err
				}
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var start *pb.Start
	var e *events.Event
	p := chatcompletions.New()
	var seq uint64
	var bodySeq, offset [2]uint64
	var eos [2]bool
	headers := false
	var statusCode *int64
	var latest *pb.ObservationAck
	work := time.Duration(0)
	queue := func(a *pb.ObservationAck) {
		select {
		case acks <- ackWrite{a: a}:
		default:
			select {
			case <-acks:
			default:
			}
			select {
			case acks <- ackWrite{a: a}:
			default:
			}
		}
	}
	final := func(reason string, end *pb.End) {
		if e == nil {
			return
		}
		for _, a := range e.AnomalyCodes {
			p.Anomaly(a)
		}
		p.Finalize()
		if reason != "finalized" {
			// ACK reasons and Kafka anomaly codes are separate closed enums.
			if reason == "resource_limit" {
				p.Anomaly("parse_limit")
			} else {
				p.Anomaly(reason)
			}
		}
		e.EventType = "usage_observed"
		e.EventID = e.LogicalRequestID + ":usage"
		e.ObservationEndedAtMs = events.Ptr(time.Now().UnixMilli())
		e.EventCreatedAtMs = time.Now().UnixMilli()
		e.RequestModel = p.RequestModel
		e.ResponseModel = p.ResponseModel
		e.HTTPStatus = statusCode
		e.Usage = p.Usage
		e.RawUsageJSON = p.Raw
		e.RawUsageTruncated = p.Truncated
		if end != nil {
			e.TerminalObserved = true
			e.RequestFinishedAtMs = events.Ptr(end.RequestFinishedAtMs)
			if end.UpstreamAttemptCount != nil {
				e.UpstreamAttemptCount = events.Ptr(int64(*end.UpstreamAttemptCount))
			}
			if end.ResponseFlags != nil && events.Has(s.Cfg.Flags, *end.ResponseFlags) {
				e.ResponseFlags = end.ResponseFlags
			}
			if end.ResponseCodeDetails != nil && events.Has(s.Cfg.Details, *end.ResponseCodeDetails) {
				e.ResponseCodeDetails = end.ResponseCodeDetails
			}
			if end.RequestFinishedAtMs < e.RequestStartedAtMs {
				p.Anomaly("clock_anomaly")
			}
		} else {
			p.Anomaly("missing_end")
		}
		observation := "unknown"
		business := "unknown"
		if offset[0]+offset[1] > 0 || headers {
			observation = "partial"
			business = "stream_incomplete"
		}
		if end != nil && eos[0] && eos[1] {
			observation = "complete"
			business = "response_complete"
		}
		for _, a := range p.Anomalies {
			if events.Has([]string{"protocol_error", "protocol_incomplete", "parse_limit", "transport_lost", "observation_timeout"}, a) {
				if observation == "complete" {
					observation = "partial"
				}
			}
		}
		e.ObservationStatus = &observation
		e.BusinessOutcome = &business
		e.AnomalyCodes = append([]string{}, p.Anomalies...)
		if e.IdentityStatus != "known" {
			e.AnomalyCodes = events.Add(e.AnomalyCodes, "identity_invalid")
		}
		e.UsageStatus, _ = events.Status(p.Usage)
		_, conflict := events.Status(p.Usage)
		if conflict || p.Conflict {
			e.UsageStatus = "conflict"
		}
		eligible := observation == "complete" && e.UsageStatus == "known" && e.IdentityStatus == "known"
		for _, a := range e.AnomalyCodes {
			if a != "invalid_model" {
				eligible = false
			}
		}
		e.UsageEligible = eligible
		s.Sink.Emit(e)
		s.Health.Count("observer_finalized")
	}
	for {
		select {
		case <-ctx.Done():
			reason := "transport_lost"
			if ctx.Err() == context.DeadlineExceeded {
				reason = "observation_timeout"
			}
			final(reason, nil)
			return status.Error(codes.DeadlineExceeded, reason)
		case <-ticker.C:
			if at := writing.Load(); at > 0 && time.Now().UnixNano()-at > int64(2*time.Second) {
				final("transport_lost", nil)
				s.Health.Count("ack_write_failure")
				return status.Error(codes.DeadlineExceeded, "ack_timeout")
			}
			if latest != nil {
				queue(latest)
				latest = nil
			}
		case r := <-recv:
			if r.err != nil {
				final("transport_lost", nil)
				if r.err == io.EOF {
					return nil
				}
				return status.Error(codes.InvalidArgument, "protocol_error")
			}
			f := r.f
			before := time.Now()
			valid := f.SchemaVersion == 1 && f.FrameSeq == seq+1 && f.Payload != nil
			reason := "protocol_error"
			var end *pb.End
			if start == nil {
				st := f.GetStart()
				if st != nil {
					defer func() { clear(st.ApiKey); st.ApiKey = nil }()
				}
				valid = valid && st != nil && s.validStart(st)
				if !valid {
					return status.Error(codes.InvalidArgument, "protocol_error")
				}
				id, err := s.newID()
				if err != nil {
					return status.Error(codes.Unavailable, "random_source")
				}
				snapshot := s.resolve(ctx, st)
				if snapshot.ServiceID != nil && !config.AcceptsService(s.Cfg.AcceptedServices, *snapshot.ServiceID) {
					s.Health.Count("lookup_service_unregistered")
					return status.Error(codes.InvalidArgument, "service_registration")
				}
				start = st
				st.RouteName = routeinfo.Name(st.RouteName)
				e = &snapshot
				e.SchemaVersion, e.EventID, e.EventType = 1, id+":start", "request_started"
				e.LogicalRequestID, e.ObservationID, e.ObservationScope = id, id, "gateway_response"
				e.XRequestID, e.ProducerID, e.GatewayInstanceID = st.XRequestId, s.Cfg.ProducerID, st.GatewayInstanceId
				e.ConfigVersion, e.AdapterID, e.RouteName = st.ConfigVersion, st.AdapterId, st.RouteName
				e.RequestStartedAtMs, e.EventCreatedAtMs = st.RequestStartedAtMs, time.Now().UnixMilli()
				e.UsageStatus, e.RawUsageJSON = "not_applicable", "{}"
				e.AnomalyCodes = []string{}
				if e.IdentityStatus != "known" {
					e.AnomalyCodes = events.Add(e.AnomalyCodes, "identity_invalid")
				}
				if st.RequestStartedAtMs < time.Now().UnixMilli()-300000 {
					e.AnomalyCodes = events.Add(e.AnomalyCodes, "clock_anomaly")
				}
				eos[0] = st.RequestEndStream
				p.Content(true, st.RequestContentType, st.RequestContentEncoding)
				seq = f.FrameSeq
				queue(&pb.ObservationAck{LogicalRequestId: id, AcceptedFrameSeq: seq, Reason: "accepted"})
				copy := *e
				s.Sink.Emit(&copy)
				continue
			}
			if st := f.GetStart(); st != nil {
				clear(st.ApiKey)
				st.ApiKey = nil
				valid = false
			}
			if h := f.GetResponseHeaders(); h != nil {
				valid = valid && !headers && h.HttpStatus >= 200 && h.HttpStatus <= 599 && len(h.ContentType) <= 128 && len(h.ContentEncoding) <= 128
				if valid {
					headers = true
					eos[1] = h.EndStream
					statusCode = events.Ptr(int64(h.HttpStatus))
					p.Content(false, h.ContentType, h.ContentEncoding)
				}
			}
			if b := f.GetBodyChunk(); b != nil {
				d := int(b.Direction) - 1
				valid = valid && d >= 0 && d < 2
				if valid {
					valid = !eos[d] && b.BodySeq == bodySeq[d]+1 && b.Offset == offset[d] && uint64(len(b.Data)) <= math.MaxUint64-offset[d] && len(b.Data) <= 16384 && (len(b.Data) > 0 || b.EndStream) && (d == 0 || headers)
					if valid {
						needed := int64(3*32768 + 16*(len(p.Request)+len(p.Response)+len(b.Data)+262144))
						if needed > reservation {
							if !s.reserve(needed - reservation) {
								valid = false
								reason = "resource_limit"
							} else {
								reservation = needed
							}
						}
						if valid {
							err := p.Feed(d == 0, b.Data, b.EndStream)
							if err != nil {
								valid = false
								if err == chatcompletions.ErrLimit {
									reason = "parse_limit"
								}
							}
							bodySeq[d] = b.BodySeq
							offset[d] += uint64(len(b.Data))
							eos[d] = b.EndStream
						}
					}
				}
			}
			if t := f.GetEnd(); t != nil {
				valid = valid && t.LastRequestBodySeq == bodySeq[0] && t.LastResponseBodySeq == bodySeq[1] && t.RequestBytesSeen == offset[0] && t.ResponseBytesSeen == offset[1] && t.RequestEndStreamSeen == eos[0] && t.ResponseEndStreamSeen == eos[1] && t.RequestFinishedAtMs > 0 && t.RequestFinishedAtMs <= 253402300799999
				if t.HttpStatus != nil {
					valid = valid && statusCode != nil && int64(*t.HttpStatus) == *statusCode
				}
				if t.UpstreamAttemptCount != nil {
					valid = valid && *t.UpstreamAttemptCount >= 1 && *t.UpstreamAttemptCount <= 2147483647
				}
				if valid {
					end = t
				}
			}
			work += time.Since(before)
			if work > 100*time.Millisecond {
				valid = false
				reason = "parse_limit"
			}
			if !valid {
				final(reason, nil)
				queue(&pb.ObservationAck{LogicalRequestId: e.LogicalRequestID, AcceptedFrameSeq: seq, CloseStream: true, Reason: reason})
				return status.Error(codes.InvalidArgument, reason)
			}
			seq = f.FrameSeq
			latest = &pb.ObservationAck{LogicalRequestId: e.LogicalRequestID, AcceptedFrameSeq: seq, Reason: "accepted"}
			if end != nil {
				final("finalized", end)
				done := make(chan error, 1)
				a := ackWrite{a: &pb.ObservationAck{LogicalRequestId: e.LogicalRequestID, AcceptedFrameSeq: seq, CloseStream: true, Reason: "finalized"}, done: done}
				select {
				case <-acks:
				default:
				}
				select {
				case acks <- a:
				case <-ctx.Done():
					return ctx.Err()
				}
				select {
				case <-done:
					return nil
				case <-time.After(2 * time.Second):
					s.Health.Count("ack_write_failure")
					return status.Error(codes.DeadlineExceeded, "ack_timeout")
				}
			}
			if seq%8 == 0 {
				queue(latest)
				latest = nil
			}
		}
	}
}
func validIdentity(s *string) bool     { return s != nil && validString(*s, 128) }
func validString(s string, n int) bool { return len(s) > 0 && len(s) <= n && utf8.ValidString(s) }
func (s *Server) validStart(st *pb.Start) bool {
	if st.RequestStartedAtMs < 1 || st.RequestStartedAtMs > 253402300799999 || st.RequestStartedAtMs > time.Now().UnixMilli()+300000 || st.Method != "POST" || !validString(st.GatewayInstanceId, 128) || !validString(st.ConfigVersion, 128) || st.AdapterId != "chat_completions_v1" || !events.Has(s.Cfg.AcceptedConfigs, st.ConfigVersion) || !events.Has(s.Cfg.AcceptedAdapters, st.AdapterId) || len(st.RequestContentType) > 128 || len(st.RequestContentEncoding) > 128 {
		return false
	}
	if st.XRequestId != nil {
		if !validString(*st.XRequestId, 128) {
			return false
		}
		for _, c := range *st.XRequestId {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == ':' || c == '-') {
				return false
			}
		}
	}
	return true
}

func (s *Server) resolve(ctx context.Context, st *pb.Start) events.Event {
	defer func() { clear(st.ApiKey); st.ApiKey = nil }()
	e := events.Event{ClusterName: s.Cfg.ClusterName, DeploymentEnvironment: s.Cfg.DeploymentEnvironment, IdentityStatus: "invalid", MappingStatus: "invalid_host", CustomerMappingStatus: "invalid_api_key"}
	host := requesthost.Normalize(st.RequestAuthority)
	st.RequestAuthority = ""
	if host != "" {
		e.RequestHost = events.Ptr(host)
		resolver := s.ServiceResolver
		if resolver == nil {
			resolver = identitylookup.UnimplementedServiceResolver{}
		}
		result, err := resolver.Resolve(ctx, identitylookup.ServiceLookupRequest{RequestHost: host, ClusterName: s.Cfg.ClusterName, DeploymentEnvironment: s.Cfg.DeploymentEnvironment})
		if err != nil {
			e.MappingStatus = identitylookup.Failure(err)
		} else if !identitylookup.ValidID(result.ServiceID) || !identitylookup.ValidID(result.ProviderID) {
			e.MappingStatus = "lookup_invalid_response"
		} else {
			e.ServiceID, e.ProviderID = events.Ptr(result.ServiceID), events.Ptr(result.ProviderID)
			e.MappingStatus = "mapped"
			e.MappingVersion = events.Ptr(events.Tuple([]any{1, e.ClusterName, e.DeploymentEnvironment, host, result.ServiceID, result.ProviderID}))
		}
	}
	if credentials.ValidToken(st.ApiKey) {
		resolver := s.CustomerResolver
		if resolver == nil {
			resolver = identitylookup.UnimplementedCustomerResolver{}
		}
		result, err := resolver.Resolve(ctx, st.ApiKey)
		if err != nil {
			e.CustomerMappingStatus = identitylookup.Failure(err)
		} else if !identitylookup.ValidID(result.APIKeyID) || !identitylookup.ValidID(result.CustomerID) {
			e.CustomerMappingStatus = "lookup_invalid_response"
		} else {
			e.ApiKeyID, e.CustomerID = events.Ptr(result.APIKeyID), events.Ptr(result.CustomerID)
			e.CustomerMappingStatus, e.IdentityStatus = "mapped", "known"
			e.CustomerMappingVersion = events.Ptr(events.Tuple([]any{1, e.ClusterName, e.DeploymentEnvironment, result.APIKeyID, result.CustomerID}))
		}
	}
	s.Health.Count("service_mapping_" + e.MappingStatus)
	s.Health.Count("customer_mapping_" + e.CustomerMappingStatus)
	return e
}
