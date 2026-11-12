package chatcompletions

import (
	"strings"
	"testing"
)

func TestSSEEverySplit(t *testing.T) {
	b := []byte(": comment\r\ndata: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"你好\"}},{\"index\":1,\"delta\":{\"tool_calls\":[]}}]}\r\n\r\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":0,\"completion_tokens\":4,\"total_tokens\":4}}\n\ndata: [DONE]\n\n")
	for i := 0; i <= len(b); i++ {
		p := New()
		p.Content("text/event-stream; charset=utf-8", "")
		if p.Feed(b[:i], false) != nil || p.Feed(b[i:], true) != nil {
			t.Fatalf("split %d", i)
		}
		p.Finalize()
		if p.Usage.InputTokens.Value == nil || *p.Usage.InputTokens.Value != 0 || !p.Done || len(p.Anomalies) > 0 {
			t.Fatalf("split %d: %#v", i, p.Anomalies)
		}
	}
}
func TestConflictingCandidates(t *testing.T) {
	p := New()
	p.Content("text/event-stream", "")
	b := []byte("data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2}}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":2}}\n\ndata: [DONE]\n\n")
	if p.Feed(b, true) != nil {
		t.Fatal("parse")
	}
	p.Finalize()
	if !p.Conflict || p.Usage.InputTokens.Value != nil || p.Raw == "{}" {
		t.Fatal("conflict not isolated")
	}
}
func TestRawAllowlistAndInt64(t *testing.T) {
	p := New()
	p.Content("application/json", "")
	if p.Feed([]byte(`{"usage":{"prompt_tokens":9223372036854775807,"completion_tokens":"2","secret":"body","prompt_tokens_details":{"cached_tokens":0,"text":"x"}}}`), true) != nil {
		t.Fatal("parse")
	}
	p.Finalize()
	if p.Usage.InputTokens.Value == nil || p.Usage.OutputTokens.Value != nil || !p.Truncated {
		t.Fatal("normalization")
	}
}

func TestLargeSSEEventReleasesBuffersBeforeNextEvent(t *testing.T) {
	p := New()
	p.Content("text/event-stream", "")
	data := []byte("data: {\"model\":\"fixture\",\"padding\":\"" + strings.Repeat("x", 180000) + "\"}\n\n")
	for len(data) > 0 {
		n := min(len(data), 16384)
		if e := p.Feed(data[:n], false); e != nil {
			t.Fatal(e)
		}
		data = data[n:]
	}
	if p.BufferedBytes() > 32768 {
		t.Fatalf("completed large event retained %d bytes", p.BufferedBytes())
	}
	for i := 0; i < 4096; i++ {
		if e := p.Feed([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"}}]}\n\n"), false); e != nil {
			t.Fatal(e)
		}
	}
	if e := p.Feed([]byte("data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1000000,\"completion_tokens\":4096}}\n\ndata: [DONE]\n\n"), true); e != nil {
		t.Fatal(e)
	}
	p.Finalize()
	if p.Usage.InputTokens.Value == nil || *p.Usage.InputTokens.Value != 1000000 || len(p.Anomalies) != 0 || p.BufferedBytes() != 0 {
		t.Fatal("long stream lost usage or retained buffers", p.Anomalies)
	}
}
