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
	"github.com/Fangyan33/llm-token-stats/internal/requesttokens"
	"github.com/Fangyan33/llm-token-stats/pkg/credentials"
	"github.com/Fangyan33/llm-token-stats/pkg/observationwire"
	"github.com/Fangyan33/llm-token-stats/pkg/requesthost"
	"github.com/Fangyan33/llm-token-stats/pkg/requestid"
	"github.com/Fangyan33/llm-token-stats/pkg/requestmeta"
	"github.com/Fangyan33/llm-token-stats/pkg/routeinfo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"io"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

type Codec struct{}

// Independent safety net for observations whose End/Cancel never arrives on a
// still-live shared transport. Wasm alone configures the collection duration.
const maxObservationDuration = time.Hour + 5*time.Second

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
	observationNow   func() time.Time // Test clock for deadline expiration only.
	queueMu          sync.Mutex
	queueBytes       int64
	queueBatches     int
	tokenOnce        sync.Once
	tokenPool        *requesttokens.Pool
	requestBytes     atomic.Int64
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
	request           *requestCapture
	requestEvent      *requestEvent
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
		logging.Warn("observation_admission_failed", "observer_admission", "reason", "active_observation_limit", "active_observations", s.active.Load(), "limit", s.Cfg.MaxActive, "action", "skip_observation")
		return nil, errors.New("resource_limit")
	}
	base := int64(2048 + 2*proto.Size(st))
	if !s.reserve(base) {
		s.active.Add(-1)
		clear(st.ApiKey)
		logging.Warn("observation_admission_failed", "observer_admission", "reason", "parser_byte_limit", "reserved_bytes", s.bytes.Load(), "requested_bytes", base, "limit", s.Cfg.ParserBudget, "action", "skip_observation")
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
	o := &observation{s: s, capture: id, e: &e, p: chatcompletions.New(), lookup: lookup, started: time.Now(), seq: 1, base: base, reservation: base,
		requestEvent: &requestEvent{sink: s.Sink, captureEnabled: st.RequestBodyCaptureEnabled}}
	o.eos[0] = st.RequestEndStream
	o.startRequest(st)
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
		o.requestEvent.freeze(&copy)
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
	if f.GetRequestBodyStopped() != nil {
		o.stopRequest("capture_incomplete")
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
		if b.Direction == pb.Direction_REQUEST {
			o.feedRequest(b)
			o.seq = f.FrameSeq
			return ""
		}
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
		if o.request != nil && (!end.RequestBodyCaptureComplete || end.LastRequestBodySeq != o.bodySeq[0] || end.RequestBytesSeen != o.offset[0]) {
			o.stopRequest("capture_incomplete")
		}
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
	o.stopRequest("capture_incomplete")
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
	e.RequestModel = nil // Request-body metadata is kept only in the :start event.
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
	batch    *pb.ObservationBatch
	err      error
	reserved int64
}

func (s *Server) Observe(stream pb.ObservationService_ObserveServer) (result error) {
	phase := "reserve_transport"
	defer func() {
		if result == nil || stream.Context().Err() != nil {
			return
		}
		code := status.Code(result)
		if code == codes.Canceled {
			return
		}
		if code == codes.ResourceExhausted || code == codes.InvalidArgument {
			logging.Warn("observation_transport_rejected", "observer_transport/"+phase, "phase", phase, "grpc_status", int(code), "action", "close_observation_transport")
		} else {
			logging.Failure("observation_transport_failed", "observer_transport/"+phase, result, "phase", phase, "action", "close_observation_transport")
		}
	}()
	s.Health.GaugeFunc("observer_active_observations", s.active.Load)
	s.Health.GaugeFunc("observer_parser_reserved_bytes", s.bytes.Load)
	s.Health.GaugeFunc("observer_receive_queue_bytes", func() int64 { s.queueMu.Lock(); defer s.queueMu.Unlock(); return s.queueBytes })
	s.Health.GaugeFunc("observer_receive_queue_batches", func() int64 { s.queueMu.Lock(); defer s.queueMu.Unlock(); return int64(s.queueBatches) })
	if !s.reserve(3 * 32768) {
		logging.Warn("observer_transport_reservation_failed", "observer_transport_reserve", "reserved_bytes", s.bytes.Load(), "requested_bytes", 3*32768, "limit", s.Cfg.ParserBudget, "action", "close_observation_transport")
		return status.Error(codes.ResourceExhausted, "resource_limit")
	}
	defer s.bytes.Add(-3 * 32768)
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()
	q := s.newReceiveQueue()
	defer q.close()
	recv := q.ch
	receiveErrors := make(chan error, 1)
	receiptReady := make(chan struct{}, 1)
	var accepted atomic.Uint64
	go func() {
		var seq uint64
		for {
			b, err := stream.Recv()
			if err != nil {
				select {
				case recv <- batchReceived{err: err}:
				case <-ctx.Done():
				}
				return
			}
			if b == nil || b.BatchSeq != seq+1 || len(b.Frames) == 0 || len(b.Frames) > 128 || proto.Size(b) > 32768 {
				clearBatch(batchReceived{batch: b})
				receiveErrors <- status.Error(codes.InvalidArgument, "protocol_error")
				return
			}
			if !q.push(batchReceived{batch: b}) {
				clearBatch(batchReceived{batch: b})
				s.Health.Count("observer_receive_queue_full")
				receiveErrors <- status.Error(codes.ResourceExhausted, "resource_limit")
				return
			}
			seq = b.BatchSeq
			accepted.Store(seq)
			select {
			case receiptReady <- struct{}{}:
			default:
			}
		}
	}()
	out := make(chan *pb.ObservationAckBatch, 1)
	writeErrors := make(chan error, 1)
	var writing atomic.Int64
	var queued atomic.Int64
	go func() {
		var lastSent uint64
		for {
			var a *pb.ObservationAckBatch
			fromOut := false
			select {
			case <-ctx.Done():
				return
			case a = <-out:
				fromOut = true
			case <-receiptReady:
				a = &pb.ObservationAckBatch{}
			}
			a.AcceptedBatchSeq = max(a.AcceptedBatchSeq, accepted.Load(), lastSent)
			if a.AcceptedBatchSeq == 0 || a.AcceptedBatchSeq == lastSent && len(a.Acks) == 0 {
				if fromOut {
					queued.Add(-1)
				}
				continue
			}
			writing.Store(time.Now().UnixNano())
			err := stream.Send(a)
			writing.Store(0)
			if fromOut {
				queued.Add(-1)
			}
			if err != nil {
				select {
				case writeErrors <- err:
				default:
				}
				return
			}
			lastSent = a.AcceptedBatchSeq
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
		size := proto.Size(a)
		ids := []uint64{}
		for id, ack := range acks {
			item := &pb.CaptureAck{CaptureId: id, Ack: ack}
			n := proto.Size(item)
			n += 1 + protowire.SizeVarint(uint64(n))
			if len(a.Acks) >= 128 || size+n > 32768 {
				break
			}
			size += n
			a.Acks = append(a.Acks, item)
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
			o.stopRequest("transport_lost")
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
		case err := <-receiveErrors:
			phase = "receive_batch"
			return err
		case err := <-writeErrors:
			phase = "write_ack"
			s.Health.Count("ack_write_failure")
			return err
		case <-ticker.C:
			if at := writing.Load(); at > 0 && time.Now().UnixNano()-at > int64(2*time.Second) {
				phase = "write_ack_timeout"
				s.Health.Count("ack_write_failure")
				return status.Error(codes.DeadlineExceeded, "ack_timeout")
			}
			now := time.Now()
			if s.observationNow != nil {
				now = s.observationNow()
			}
			for len(deadlines) > 0 && !deadlines[0].at.After(now) {
				d := heap.Pop(&deadlines).(*observationDeadline)
				if o := states[d.capture]; o != nil {
					finish(o, "observation_timeout")
				}
			}
			drain()
			flush()
		case rec := <-recv:
			phase = "receive_batch"
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
			phase = "process_batch"
			processErr := func() error {
				defer q.release(rec)
				defer clearBatch(rec)
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
						o.timer = &observationDeadline{at: time.Now().Add(maxObservationDuration), capture: id, idx: -1}
						heap.Push(&deadlines, o.timer)
						acks[id] = &pb.ObservationAck{ObservationId: o.e.ObservationID, AcceptedFrameSeq: 1, Reason: "accepted"}
					} else if o.reason == "" {
						if reason := o.feed(f); reason != "" {
							finish(o, reason)
							acks[id] = &pb.ObservationAck{ObservationId: o.e.ObservationID, AcceptedFrameSeq: o.seq, Reason: "accepted"}
						}
					}
					if o != nil && o.request != nil && o.request.notifyStop {
						o.request.notifyStop = false
						acks[id] = &pb.ObservationAck{ObservationId: o.e.ObservationID, AcceptedFrameSeq: o.seq, Reason: "accepted", StopRequestBody: true}
					}
				}
				return nil
			}()
			if processErr != nil {
				return processErr
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
