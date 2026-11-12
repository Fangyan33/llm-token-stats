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
	mu           sync.Mutex
	c            *kgo.Client
	pending      map[*kgo.Record]pendingRecord
	pendingCount int
	pendingBytes int64
	closed       bool
	cfg          config.Kafka
	h            *health.State
	fatal        func()
}
type pendingRecord struct {
	at    time.Time
	bytes int64
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
	if err := cfg.ProducerDefaults(); err != nil {
		return nil, err
	}
	p := &Producer{cfg: cfg, h: h, fatal: fatal, pending: map[*kgo.Record]pendingRecord{}}
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
	o = append(o, kgo.RequiredAcks(kgo.AllISRAcks()), kgo.DisableIdempotentWrite(), kgo.RecordRetries(1), kgo.UnknownTopicRetries(0), kgo.ProducerLinger(time.Duration(p.cfg.ProducerLingerMS)*time.Millisecond), kgo.MaxBufferedRecords(p.cfg.ProducerMaxRecords), kgo.MaxBufferedBytes(p.cfg.ProducerMaxBytes), kgo.ProducerBatchMaxBytes(int32(p.cfg.ProducerBatchBytes)), kgo.RecordDeliveryTimeout(time.Duration(p.cfg.ProducerDeliveryMS)*time.Millisecond), kgo.ProduceRequestTimeout(time.Second), kgo.RequestTimeoutOverhead(time.Second), kgo.DialTimeout(time.Second))
	return kgo.NewClient(o...)
}
func (p *Producer) Emit(e *events.Event) {
	// Claim the record slot before JSON encoding, so a full sink does no work.
	p.mu.Lock()
	if p.closed || p.c == nil || p.pendingCount >= p.cfg.ProducerMaxRecords {
		p.mu.Unlock()
		p.h.Count("producer_dropped")
		logging.Warn("producer_event_dropped", "producer_capacity", "reason", "unavailable_or_full", "capacity", p.cfg.ProducerMaxRecords)
		return
	}
	p.pendingCount++
	c := p.c
	p.mu.Unlock()
	b, err := events.Canonical(e)
	if err != nil {
		p.mu.Lock()
		p.pendingCount--
		p.mu.Unlock()
		p.h.Count("producer_dropped")
		logging.Warn("producer_event_rejected", "producer_event", "reason", "invalid_event")
		return
	}
	r := &kgo.Record{Topic: p.cfg.Topic, Key: []byte(e.ObservationID), Value: b}
	p.mu.Lock()
	n := int64(len(r.Key) + len(r.Value))
	if p.closed || p.c != c || p.pendingBytes+n > int64(p.cfg.ProducerMaxBytes) {
		p.pendingCount--
		p.mu.Unlock()
		p.h.Count("producer_dropped")
		logging.Warn("producer_event_dropped", "producer_capacity", "reason", "unavailable_or_full", "capacity", p.cfg.ProducerMaxBytes)
		return
	}
	p.pending[r] = pendingRecord{time.Now(), n}
	p.pendingBytes += n
	p.h.Gauge("producer_pending_events", int64(p.pendingCount))
	p.h.Gauge("producer_pending_bytes", p.pendingBytes)
	p.mu.Unlock()
	c.TryProduce(context.Background(), r, func(r *kgo.Record, err error) {
		p.mu.Lock()
		if rec, ok := p.pending[r]; ok {
			delete(p.pending, r)
			p.pendingCount--
			p.pendingBytes -= rec.bytes
		}
		p.h.Gauge("producer_pending_events", int64(p.pendingCount))
		p.h.Gauge("producer_pending_bytes", p.pendingBytes)
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
			for _, rec := range p.pending {
				if time.Since(rec.at) > time.Duration(p.cfg.ProducerWatchdogMS)*time.Millisecond {
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
				logging.Error("producer_watchdog_rebuilding", nil, "pending_age_ms", p.cfg.ProducerWatchdogMS)
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
