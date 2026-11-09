package producer

import (
	"context"
	"errors"
	"fmt"
	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/Fangyan33/llm-token-stats/internal/health"
	"github.com/Fangyan33/llm-token-stats/internal/logging"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"os"
	"sync"
	"time"
)

type Sink interface{ Emit(*events.Event) }
type Producer struct {
	mu      sync.Mutex
	c       *kgo.Client
	pending map[*kgo.Record]time.Time
	closed  bool
	cfg     config.Kafka
	h       *health.State
	fatal   func()
}

func Options(k config.Kafka) ([]kgo.Opt, error) {
	o := []kgo.Opt{kgo.SeedBrokers(k.Brokers...), kgo.WithHooks(logging.KafkaHooks{})}
	t, err := k.TLS()
	if err != nil {
		return nil, err
	}
	if t != nil {
		o = append(o, kgo.DialTLSConfig(t))
	}
	if k.UsernameEnv != "" {
		u, p := os.Getenv(k.UsernameEnv), os.Getenv(k.PasswordEnv)
		if u == "" || p == "" {
			return nil, fmt.Errorf("kafka_sasl")
		}
		o = append(o, kgo.SASL(plain.Auth{User: u, Pass: p}.AsMechanism()))
	}
	return o, nil
}
func New(ctx context.Context, cfg config.Kafka, h *health.State, fatal func()) (*Producer, error) {
	p := &Producer{cfg: cfg, h: h, fatal: fatal, pending: map[*kgo.Record]time.Time{}}
	c, err := p.client()
	if err != nil {
		return nil, err
	}
	p.c = c
	logging.Info("kafka_producer_configured", "topic", cfg.Topic, "broker_count", len(cfg.Brokers))
	go p.watch(ctx)
	return p, nil
}
func (p *Producer) client() (*kgo.Client, error) {
	o, err := Options(p.cfg)
	if err != nil {
		return nil, err
	}
	o = append(o, kgo.RequiredAcks(kgo.AllISRAcks()), kgo.DisableIdempotentWrite(), kgo.RecordRetries(1), kgo.UnknownTopicRetries(0), kgo.ProducerLinger(0), kgo.MaxBufferedRecords(256), kgo.MaxBufferedBytes(4<<20), kgo.RecordDeliveryTimeout(2*time.Second), kgo.ProduceRequestTimeout(time.Second), kgo.RequestTimeoutOverhead(time.Second), kgo.DialTimeout(time.Second))
	return kgo.NewClient(o...)
}
func (p *Producer) Emit(e *events.Event) {
	b, err := events.Canonical(e)
	if err != nil {
		p.h.Count("producer_dropped")
		logging.Warn("producer_event_rejected", "producer_event", "reason", "invalid_event")
		return
	}
	r := &kgo.Record{Topic: p.cfg.Topic, Key: []byte(e.ObservationID), Value: b}
	p.mu.Lock()
	if p.closed || p.c == nil || len(p.pending) >= 256 {
		p.mu.Unlock()
		p.h.Count("producer_dropped")
		logging.Warn("producer_event_dropped", "producer_capacity", "reason", "unavailable_or_full", "capacity", 256)
		return
	}
	c := p.c
	p.pending[r] = time.Now()
	p.h.Gauge("producer_pending_events", int64(len(p.pending)))
	p.mu.Unlock()
	c.TryProduce(context.Background(), r, func(r *kgo.Record, err error) {
		p.mu.Lock()
		delete(p.pending, r)
		p.h.Gauge("producer_pending_events", int64(len(p.pending)))
		p.mu.Unlock()
		if err == nil {
			p.h.Count("producer_success")
			logging.Recovered("producer_delivery", "producer_delivery_recovered", "topic", p.cfg.Topic)
			logging.Debug("producer_delivered", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset)
		} else {
			logging.Failure("producer_delivery_failed", "producer_delivery", err, "topic", r.Topic, "partition", r.Partition)
			if errors.Is(err, kgo.ErrMaxBuffered) || errors.Is(err, kerr.UnknownTopicOrPartition) {
				p.h.Count("producer_definite_failure")
			} else {
				p.h.Count("producer_uncertain")
			}
			if errors.Is(err, kgo.ErrRecordTimeout) {
				p.h.Count("producer_timeout")
			}
			if errors.Is(err, kerr.UnknownTopicOrPartition) {
				p.h.Count("producer_unknown_topic")
			}
			if errors.Is(err, kgo.ErrMaxBuffered) {
				p.h.Count("producer_buffer_full")
			}
			if errors.Is(err, kerr.NotEnoughReplicas) {
				p.h.Count("producer_insufficient_replicas")
			}
		}
	})
}
func (p *Producer) watch(ctx context.Context) {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			p.Close()
			return
		case <-t.C:
			p.mu.Lock()
			old := p.c
			stuck := false
			for _, at := range p.pending {
				if time.Since(at) > 3*time.Second {
					stuck = true
					break
				}
			}
			if stuck {
				p.c = nil
			}
			p.mu.Unlock()
			if stuck {
				p.h.Count("producer_watchdog")
				logging.Error("producer_watchdog_rebuilding", nil, "pending_age_ms", 3000)
				done := make(chan struct{})
				go func() { old.Close(); close(done) }()
				select {
				case <-done:
				case <-time.After(time.Second):
					logging.Error("producer_watchdog_close_timeout", nil)
					p.fatal()
					return
				}
				c, err := p.client()
				if err != nil {
					logging.Error("producer_rebuild_failed", err)
				} else {
					logging.Info("producer_rebuilt")
				}
				p.mu.Lock()
				if !p.closed && err == nil {
					p.c = c
				} else if c != nil {
					c.Close()
				}
				p.mu.Unlock()
			}
		}
	}
}
func (p *Producer) Close() {
	p.mu.Lock()
	p.closed = true
	c := p.c
	p.c = nil
	p.mu.Unlock()
	if c != nil {
		done := make(chan struct{})
		go func() { c.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			p.fatal()
		}
	}
}
