package observation

import (
	"container/heap"
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
	"github.com/Fangyan33/llm-token-stats/internal/logging"
	"github.com/Fangyan33/llm-token-stats/internal/producer"
	"github.com/Fangyan33/llm-token-stats/pkg/credentials"
	"github.com/Fangyan33/llm-token-stats/pkg/observationwire"
	"github.com/Fangyan33/llm-token-stats/pkg/requesthost"
	"github.com/Fangyan33/llm-token-stats/pkg/requestid"
	"github.com/Fangyan33/llm-token-stats/pkg/requestmeta"
	"github.com/Fangyan33/llm-token-stats/pkg/routeinfo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"io"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

type Codec struct{}

func (Codec) Name() string                  { return "proto" }
func (Codec) Marshal(v any) ([]byte, error) { return proto.Marshal(v.(proto.Message)) }
func (Codec) Unmarshal(b []byte, v any) error {
	switch v.(type) {
	case *pb.ObservationBatch:
		if observationwire.ValidateBatch(b, false) != nil {
			return errors.New("protocol_error")
		}
	case *pb.ObservationAckBatch:
		if observationwire.ValidateBatch(b, true) != nil {
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
	active, bytes    atomic.Int64
	Random           io.Reader
	ServiceResolver  identitylookup.ServiceResolver
	CustomerResolver identitylookup.CustomerResolver
	lookupOnce       sync.Once
	lookupSlots      chan struct{}
	lookups          *lookupState
	lookupInitErr    error
	lookupNow        func() time.Time
}

func (s *Server) reserve(n int64) bool {
	for {
		old := s.bytes.Load()
		if n < 0 || n > s.Cfg.ParserBudget-old {
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

type observation struct {
	s                 *Server
	capture           uint64
	e                 *events.Event
	p                 *chatcompletions.Parser
	lookup            <-chan events.Event
	frozen            bool
	started           time.Time
	seq               uint64
	bodySeq, offset   [2]uint64
	eos               [2]bool
	headers           bool
	statusCode        *int64
	end               *pb.End
	reason            string
	reservation, base int64
	timer             *observationDeadline
}
type observationDeadline struct {
	at      time.Time
	capture uint64
	idx     int
}
type observationDeadlines []*observationDeadline

func (q observationDeadlines) Len() int           { return len(q) }
func (q observationDeadlines) Less(i, j int) bool { return q[i].at.Before(q[j].at) }
func (q observationDeadlines) Swap(i, j int)      { q[i], q[j] = q[j], q[i]; q[i].idx = i; q[j].idx = j }
func (q *observationDeadlines) Push(x any) {
	d := x.(*observationDeadline)
	d.idx = len(*q)
	*q = append(*q, d)
}
func (q *observationDeadlines) Pop() any {
	a := *q
	d := a[len(a)-1]
	a[len(a)-1] = nil
	*q = a[:len(a)-1]
	d.idx = -1
	return d
}
func (s *Server) start(ctx context.Context, id uint64, f *pb.ObservationFrame) (*observation, error) {
	st := f.GetStart()
	if st == nil || f.SchemaVersion != 1 || f.FrameSeq != 1 || proto.Size(f) > 6144 || !s.validStart(st) {
		if st != nil {
			clear(st.ApiKey)
		}
		return nil, errors.New("protocol_error")
	}
	if s.active.Add(1) > int64(s.Cfg.MaxActive) {
		s.active.Add(-1)
		clear(st.ApiKey)
		return nil, errors.New("resource_limit")
	}
	base := int64(2048 + 2*proto.Size(st))
	if !s.reserve(base) {
		s.active.Add(-1)
		clear(st.ApiKey)
		return nil, errors.New("resource_limit")
	}
	uuid, err := s.newID()
	if err != nil {
		s.active.Add(-1)
		s.bytes.Add(-base)
		clear(st.ApiKey)
		return nil, err
	}
	e := s.emptySnapshot(st, "lookup_timeout")
	lookup := s.beginResolve(ctx, st)
	clear(st.ApiKey)
	st.ApiKey = nil
	st.RouteName = routeinfo.Name(st.RouteName)
	e.SchemaVersion, e.EventID, e.EventType = events.SchemaVersion, uuid+":start", "request_started"
	e.LogicalRequestID, e.ObservationID, e.ObservationScope = st.LogicalRequestId, uuid, "gateway_response"
	e.UserID, e.UserAgent = st.UserId, st.UserAgent
	e.RequestHeaders = st.RequestHeaders
	e.XRequestID, e.ProducerID, e.GatewayInstanceID = st.XRequestId, s.Cfg.ProducerID, st.GatewayInstanceId
	e.ConfigVersion, e.AdapterID, e.RouteName = st.ConfigVersion, st.AdapterId, st.RouteName
	e.RequestStartedAtMs, e.EventCreatedAtMs = st.RequestStartedAtMs, time.Now().UnixMilli()
	e.UsageStatus, e.RawUsageJSON = "not_applicable", "{}"
	e.AnomalyCodes = []string{}
	if st.RequestStartedAtMs < time.Now().UnixMilli()-300000 {
		e.AnomalyCodes = events.Add(e.AnomalyCodes, "clock_anomaly")
	}
	o := &observation{s: s, capture: id, e: &e, p: chatcompletions.New(), lookup: lookup, started: time.Now(), seq: 1, base: base, reservation: base}
	o.eos[0] = st.RequestEndStream
	return o, nil
}
func (o *observation) pollLookup() bool {
	if o.frozen {
		return true
	}
	select {
	case snapshot := <-o.lookup:
		e := o.e
		e.ServiceID, e.ServiceName, e.ProviderID, e.ProviderName = snapshot.ServiceID, snapshot.ServiceName, snapshot.ProviderID, snapshot.ProviderName
		e.ApiKeyID, e.CustomerID, e.CustomerName = snapshot.ApiKeyID, snapshot.CustomerID, snapshot.CustomerName
		e.MappingStatus, e.MappingVersion = snapshot.MappingStatus, snapshot.MappingVersion
		e.CustomerMappingStatus, e.CustomerMappingVersion, e.IdentityStatus = snapshot.CustomerMappingStatus, snapshot.CustomerMappingVersion, snapshot.IdentityStatus
		if e.IdentityStatus != "known" {
			e.AnomalyCodes = events.Add(e.AnomalyCodes, "identity_invalid")
		}
		copy := *e
		o.s.Sink.Emit(&copy)
		o.frozen = true
		o.lookup = nil
		return true
	default:
		return false
	}
}
func (o *observation) feed(f *pb.ObservationFrame) string {
	if f == nil || f.SchemaVersion != 1 || f.FrameSeq != o.seq+1 || f.Payload == nil {
		return "protocol_error"
	}
	if st := f.GetStart(); st != nil {
		clear(st.ApiKey)
		return "protocol_error"
	}
	if f.GetCancel() != nil {
		o.seq = f.FrameSeq
		return "transport_lost"
	}
	if h := f.GetResponseHeaders(); h != nil {
		if o.headers || h.HttpStatus < 200 || h.HttpStatus > 599 || len(h.ContentType) > 128 || len(h.ContentEncoding) > 128 {
			return "protocol_error"
		}
		o.headers = true
		o.eos[1] = h.EndStream
		o.statusCode = events.Ptr(int64(h.HttpStatus))
		o.p.Content(h.ContentType, h.ContentEncoding)
	}
	if b := f.GetBodyChunk(); b != nil {
		if b.Direction != pb.Direction_RESPONSE || !o.headers || o.eos[1] || b.BodySeq != o.bodySeq[1]+1 || b.Offset != o.offset[1] || len(b.Data) > 16384 || (len(b.Data) == 0 && !b.EndStream) {
			return "protocol_error"
		}
		p := o.p
		needed := o.base + 2*int64(int(p.BufferedBytes())+len(b.Data))
		if needed > o.reservation && !o.s.reserve(needed-o.reservation) {
			return "resource_limit"
		}
		if needed > o.reservation {
			o.reservation = needed
		}
		if len(b.Data) > 0 {
			p.SetElapsed(b.ObservedElapsedUs)
		}
		err := p.Feed(b.Data, b.EndStream)
		target := o.base + p.BufferedBytes()
		if target < o.reservation {
			o.s.bytes.Add(target - o.reservation)
			o.reservation = target
		}
		if err != nil {
			if errors.Is(err, chatcompletions.ErrLimit) {
				return "parse_limit"
			}
			return "protocol_error"
		}
		o.bodySeq[1] = b.BodySeq
		o.offset[1] += uint64(len(b.Data))
		o.eos[1] = b.EndStream
	}
	if end := f.GetEnd(); end != nil {
		if end.LastResponseBodySeq != o.bodySeq[1] || end.ResponseBytesSeen != o.offset[1] || end.ResponseEndStreamSeen != o.eos[1] || end.RequestFinishedAtMs < 1 || end.RequestFinishedAtMs > 253402300799999 {
			return "protocol_error"
		}
		if end.HttpStatus != nil && (o.statusCode == nil || int64(*end.HttpStatus) != *o.statusCode) {
			return "protocol_error"
		}
		if end.UpstreamAttemptCount != nil && (*end.UpstreamAttemptCount < 1 || *end.UpstreamAttemptCount > 2147483647) {
			return "protocol_error"
		}
		o.end = end
		o.eos[0] = end.RequestEndStreamSeen
		o.offset[0] = end.RequestBytesSeen
	}
	o.seq = f.FrameSeq
	if o.end != nil {
		return "finalized"
	}
	return ""
}
func (o *observation) finalize() {
	s, e, p := o.s, o.e, o.p
	end, reason := o.end, o.reason
	bodySeq, offset, eos, headers, statusCode := o.bodySeq, o.offset, o.eos, o.headers, o.statusCode
	_ = bodySeq
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
	e.EventID = e.ObservationID + ":usage"
	e.ObservationEndedAtMs = events.Ptr(time.Now().UnixMilli())
	e.EventCreatedAtMs = time.Now().UnixMilli()
	e.RequestModel = nil // Request bodies are deliberately never observed.
	e.ResponseModel = p.ResponseModel
	e.HTTPStatus = statusCode
	e.ResponseStreaming = p.ResponseStreaming
	e.FirstOutputElapsedUS, e.LastOutputElapsedUS = p.FirstOutputUS, p.LastOutputUS
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
	eligible := observation == "complete" && e.UsageStatus == "known"
	for _, a := range e.AnomalyCodes {
		if a != "invalid_model" && a != "identity_invalid" {
			eligible = false
		}
	}
	e.UsageEligible = eligible
	s.Sink.Emit(e)
	s.Health.Count("observer_finalized")
	logging.Debug("observation_finalized", "observation_id", e.ObservationID, "reason", reason, "usage_status", e.UsageStatus, "duration_ms", time.Since(o.started).Milliseconds())
}

type batchReceived struct {
	batch *pb.ObservationBatch
	err   error
}

func (s *Server) Observe(stream pb.ObservationService_ObserveServer) error {
	s.Health.GaugeFunc("observer_active_observations", s.active.Load)
	s.Health.GaugeFunc("observer_parser_reserved_bytes", s.bytes.Load)
	if !s.reserve(3 * 32768) {
		return status.Error(codes.ResourceExhausted, "resource_limit")
	}
	defer s.bytes.Add(-3 * 32768)
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	recv := make(chan batchReceived, 1)
	go func() {
		for {
			b, err := stream.Recv()
			select {
			case recv <- batchReceived{b, err}:
			case <-ctx.Done():
				if b != nil {
					for _, x := range b.Frames {
						if st := x.GetFrame().GetStart(); st != nil {
							clear(st.ApiKey)
						}
					}
				}
				return
			}
			if err != nil {
				return
			}
		}
	}()
	out := make(chan *pb.ObservationAckBatch, 1)
	writeErrors := make(chan error, 1)
	var writing atomic.Int64
	var queued atomic.Int64
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case a := <-out:
				writing.Store(time.Now().UnixNano())
				err := stream.Send(a)
				writing.Store(0)
				queued.Add(-1)
				if err != nil {
					select {
					case writeErrors <- err:
					default:
					}
					return
				}
			}
		}
	}()
	states := map[uint64]*observation{}
	pending := map[uint64]*observation{}
	acks := map[uint64]*pb.ObservationAck{}
	var deadlines observationDeadlines
	var batchSeq, sentSeq, lastCapture uint64
	finish := func(o *observation, reason string) {
		if o.reason == "" {
			o.reason = reason
		}
		pending[o.capture] = o
		if o.timer != nil && o.timer.idx >= 0 {
			heap.Remove(&deadlines, o.timer.idx)
		}
	}
	drain := func() {
		for id, o := range pending {
			if o.reason != "" && o.pollLookup() {
				o.finalize()
				acks[id] = &pb.ObservationAck{ObservationId: o.e.ObservationID, AcceptedFrameSeq: o.seq, CloseStream: true, Reason: ackReason(o.reason)}
				delete(states, id)
				delete(pending, id)
				s.bytes.Add(-o.reservation)
				s.active.Add(-1)
			} else if o.reason == "" && o.pollLookup() {
				delete(pending, id)
			}
		}
	}
	flush := func() {
		if batchSeq == 0 || (len(acks) == 0 && sentSeq == batchSeq) {
			return
		}
		a := &pb.ObservationAckBatch{AcceptedBatchSeq: batchSeq}
		ids := []uint64{}
		for id, ack := range acks {
			item := &pb.CaptureAck{CaptureId: id, Ack: ack}
			a.Acks = append(a.Acks, item)
			if len(a.Acks) > 128 || proto.Size(a) > 32768 {
				a.Acks = a.Acks[:len(a.Acks)-1]
				break
			}
			ids = append(ids, id)
		}
		queued.Add(1)
		select {
		case out <- a:
			sentSeq = batchSeq
			for _, id := range ids {
				delete(acks, id)
			}
		default:
			queued.Add(-1)
		}
	}
	defer func() {
		for _, o := range states {
			finish(o, "transport_lost")
		}
		deadline := time.Now().Add(2100 * time.Millisecond)
		for len(states) > 0 && time.Now().Before(deadline) {
			drain()
			if len(states) > 0 {
				time.Sleep(time.Millisecond)
			}
		}
		for id, o := range states {
			delete(states, id)
			s.active.Add(-1)
			s.bytes.Add(-o.reservation)
		}
	}()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-writeErrors:
			s.Health.Count("ack_write_failure")
			return err
		case <-ticker.C:
			if at := writing.Load(); at > 0 && time.Now().UnixNano()-at > int64(2*time.Second) {
				s.Health.Count("ack_write_failure")
				return status.Error(codes.DeadlineExceeded, "ack_timeout")
			}
			now := time.Now()
			for len(deadlines) > 0 && !deadlines[0].at.After(now) {
				d := heap.Pop(&deadlines).(*observationDeadline)
				if o := states[d.capture]; o != nil {
					finish(o, "observation_timeout")
				}
			}
			drain()
			flush()
		case rec := <-recv:
			if rec.err != nil {
				if rec.err == io.EOF {
					for _, o := range states {
						finish(o, "transport_lost")
					}
					for len(states) > 0 {
						drain()
						flush()
						if len(states) > 0 {
							select {
							case <-ctx.Done():
								return ctx.Err()
							case <-time.After(time.Millisecond):
							}
						}
					}
					// Drain the bounded ACK writer before closing the transport.
					until := time.Now().Add(2 * time.Second)
					for {
						flush()
						if len(acks) == 0 && sentSeq == batchSeq && queued.Load() == 0 {
							return nil
						}
						if time.Now().After(until) {
							return status.Error(codes.DeadlineExceeded, "ack_timeout")
						}
						select {
						case <-ctx.Done():
							return ctx.Err()
						case err := <-writeErrors:
							return err
						case <-time.After(time.Millisecond):
						}
					}
				}
				return rec.err
			}
			b := rec.batch
			if b == nil || b.BatchSeq != batchSeq+1 || len(b.Frames) == 0 || len(b.Frames) > 128 || proto.Size(b) > 32768 {
				return status.Error(codes.InvalidArgument, "protocol_error")
			}
			batchSeq = b.BatchSeq
			for _, item := range b.Frames {
				id, f := item.CaptureId, item.Frame
				if id == 0 || f == nil {
					return status.Error(codes.InvalidArgument, "protocol_error")
				}
				o := states[id]
				if o == nil {
					// Capture keys increase within each transport and are never reused.
					// Late frames after a close/cancel need no unbounded tombstone map.
					if id <= lastCapture {
						if st := f.GetStart(); st != nil {
							clear(st.ApiKey)
						}
						continue
					}
					lastCapture = id
					var err error
					o, err = s.start(ctx, id, f)
					if err != nil {
						uuid, randomErr := s.newID()
						if randomErr != nil {
							return status.Error(codes.Unavailable, "random_source")
						}
						acks[id] = &pb.ObservationAck{ObservationId: uuid, AcceptedFrameSeq: f.FrameSeq, CloseStream: true, Reason: ackReason(err.Error())}
						continue
					}
					states[id] = o
					pending[id] = o
					o.timer = &observationDeadline{at: time.Now().Add(time.Duration(s.Cfg.MaxObservationMS) * time.Millisecond), capture: id, idx: -1}
					heap.Push(&deadlines, o.timer)
					acks[id] = &pb.ObservationAck{ObservationId: o.e.ObservationID, AcceptedFrameSeq: 1, Reason: "accepted"}
				} else if o.reason == "" {
					if reason := o.feed(f); reason != "" {
						finish(o, reason)
					}
					acks[id] = &pb.ObservationAck{ObservationId: o.e.ObservationID, AcceptedFrameSeq: o.seq, Reason: "accepted"}
				}
			}
			drain()
			flush()
		}
	}
}
func ackReason(reason string) string {
	switch reason {
	case "finalized", "protocol_error", "parse_limit", "resource_limit", "observation_timeout":
		return reason
	default:
		return "protocol_error"
	}
}
func validIdentity(s *string) bool     { return s != nil && validString(*s, 128) }
func validString(s string, n int) bool { return len(s) > 0 && len(s) <= n && utf8.ValidString(s) }
func (s *Server) validStart(st *pb.Start) bool {
	if !requestmeta.Valid(st.UserId, st.UserAgent, st.RequestHeaders) {
		return false
	}
	if st.RequestStartedAtMs < 1 || st.RequestStartedAtMs > 253402300799999 || st.RequestStartedAtMs > time.Now().UnixMilli()+300000 || st.Method != "POST" || !validString(st.GatewayInstanceId, 128) || !validString(st.ConfigVersion, 128) || st.AdapterId != "chat_completions_v1" || !events.Has(s.Cfg.AcceptedConfigs, st.ConfigVersion) || !events.Has(s.Cfg.AcceptedAdapters, st.AdapterId) || len(st.RequestContentType) > 128 || len(st.RequestContentEncoding) > 128 {
		return false
	}
	if st.LogicalRequestId != nil && !requestid.Valid(*st.LogicalRequestId) {
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

func (s *Server) emptySnapshot(st *pb.Start, reason string) events.Event {
	e := events.Event{ClusterName: s.Cfg.ClusterName, DeploymentEnvironment: s.Cfg.DeploymentEnvironment, IdentityStatus: "invalid", MappingStatus: "invalid_host", CustomerMappingStatus: "invalid_api_key"}
	if host := requesthost.Normalize(st.RequestAuthority); host != "" {
		e.RequestHost = events.Ptr(host)
		e.MappingStatus = reason
	}
	if credentials.ValidToken(st.ApiKey) {
		e.CustomerMappingStatus = reason
	}
	return e
}

// Admission is applied inside resolveQueries after cache hits and shared flights
// have been found. Only an immutable snapshot crosses into the receive loop.
func (s *Server) beginResolve(ctx context.Context, st *pb.Start) <-chan events.Event {
	result := make(chan events.Event, 1)
	input := &pb.Start{RequestAuthority: st.RequestAuthority, ApiKey: st.ApiKey}
	st.RequestAuthority, st.ApiKey = "", nil
	ms := s.Cfg.LookupTimeoutMS
	if ms == 0 {
		ms = 200
	}
	deadline, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Duration(ms)*time.Millisecond)
	go func() {
		defer cancel()
		result <- s.resolveQueries(deadline, input, nil)
	}()
	return result
}
