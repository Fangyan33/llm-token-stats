package producer

import (
	"encoding/json"
	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/Fangyan33/llm-token-stats/internal/health"
	"github.com/twmb/franz-go/pkg/kgo"
	"os"
	"testing"
)

func TestFullProducerRejectsBeforeEncoding(t *testing.T) {
	c := config.Kafka{}
	if err := c.ProducerDefaults(); err != nil {
		t.Fatal(err)
	}
	p := &Producer{cfg: c, h: &health.State{}, c: &kgo.Client{}, pendingCount: c.ProducerMaxRecords}
	// A nil event would panic if Canonical were called before capacity admission.
	p.Emit(nil)
	if p.pendingCount != c.ProducerMaxRecords || len(p.pending) != 0 {
		t.Fatal("full queue admitted work")
	}
}

func TestProducerByteLimitReleasesEncodingSlot(t *testing.T) {
	c := config.Kafka{}
	if err := c.ProducerDefaults(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile("../../testdata/collection/usage-known.json")
	if err != nil {
		t.Fatal(err)
	}
	var e events.Event
	if json.Unmarshal(raw, &e) != nil {
		t.Fatal("fixture decode")
	}
	if _, err = events.Canonical(&e); err != nil {
		t.Fatal(err)
	}
	p := &Producer{cfg: c, h: &health.State{}, c: &kgo.Client{}, pendingBytes: int64(c.ProducerMaxBytes)}
	p.Emit(&e)
	if p.pendingCount != 0 || p.pendingBytes != int64(c.ProducerMaxBytes) || len(p.pending) != 0 {
		t.Fatal("byte rejection leaked a slot or changed pending ownership")
	}
}
