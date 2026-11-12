package chatcompletions

import (
	"strings"
	"testing"

	"github.com/Fangyan33/llm-token-stats/internal/events"
)

func feedAt(t *testing.T, p *Parser, b string, us *int64, eos bool) {
	t.Helper()
	p.SetElapsed(us)
	if err := p.Feed([]byte(b), eos); err != nil {
		t.Fatal(err)
	}
}

func TestOutputTimingEverySplit(t *testing.T) {
	first := "data: {\"choices\":[{\"delta\":{\"content\":\"你好\"}}]}\r\n\r\n"
	second := "data: {\"choices\":[{\"delta\":{\"content\":\"世界\"}}]}\n\n"
	body := first + second
	for split := 0; split <= len(body); split++ {
		p := New()
		p.Content("text/event-stream", "")
		feedAt(t, p, body[:split], events.Ptr(int64(100000)), false)
		feedAt(t, p, body[split:], events.Ptr(int64(300000)), false)
		feedAt(t, p, "data: {\"choices\":[],\"usage\":{\"completion_tokens\":5}}\n\ndata: [DONE]\n\n", events.Ptr(int64(900000)), true)
		p.Finalize()
		wantFirst, wantLast := int64(300000), int64(300000)
		if split >= len(first) {
			wantFirst = 100000
		}
		if split == len(body) {
			wantLast = 100000
		}
		if p.FirstOutputUS == nil || *p.FirstOutputUS != wantFirst || p.LastOutputUS == nil || *p.LastOutputUS != wantLast {
			t.Fatalf("split=%d: first=%v last=%v", split, p.FirstOutputUS, p.LastOutputUS)
		}
		if *p.Usage.OutputTokens.Value != 5 || p.Usage.InputTokens.Value != nil {
			t.Fatal("partial reported usage changed")
		}
	}
}

func TestGeneratedContentKinds(t *testing.T) {
	for _, delta := range []string{
		`{"content":" "}`, `{"reasoning_content":"思考"}`, `{"reasoning":"thinking"}`, `{"refusal":"no"}`,
		`{"tool_calls":[{"index":1,"function":{"name":"lookup"}}]}`, `{"tool_calls":[{"index":0,"function":{"arguments":"{}"}}]}`, `{"function_call":{"arguments":"{}"}}`,
	} {
		t.Run(delta, func(t *testing.T) {
			p := New()
			p.Content("text/event-stream", "")
			feedAt(t, p, ": heartbeat\n\ndata: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}\n\n", events.Ptr(int64(0)), false)
			if p.FirstOutputUS != nil {
				t.Fatal("metadata treated as output")
			}
			feedAt(t, p, `data: {"choices":[{"index":3,"delta":`+delta+"}]}\n\n", events.Ptr(int64(0)), false)
			feedAt(t, p, "data: [DONE]\n\n", events.Ptr(int64(1000)), true)
			if p.FirstOutputUS == nil || *p.FirstOutputUS != 0 || p.LastOutputUS == nil || *p.LastOutputUS != 0 {
				t.Fatal("generated content not detected or zero time lost")
			}
		})
	}
}

func TestTimingFailureDoesNotChangeUsage(t *testing.T) {
	for _, bad := range []*int64{nil, events.Ptr(int64(-1)), events.Ptr(int64(99))} {
		p := New()
		p.Content("text/event-stream", "")
		feedAt(t, p, "data: {\"choices\":[{\"delta\":{\"content\":\"one\"}}]}\n\n", events.Ptr(int64(100)), false)
		feedAt(t, p, "data: {\"choices\":[{\"delta\":{\"content\":\"two\"}}]}\n\n", bad, false)
		feedAt(t, p, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":4}}\n\ndata: [DONE]\n\n", events.Ptr(int64(200)), true)
		p.Finalize()
		if p.FirstOutputUS == nil || *p.FirstOutputUS != 100 || p.LastOutputUS != nil || !p.Done || len(p.Anomalies) != 0 || *p.Usage.OutputTokens.Value != 4 {
			t.Fatal("clock error affected usage or recovered uncertain last time")
		}
	}
	p := New()
	p.Content("text/event-stream", "")
	feedAt(t, p, "data: {\"choices\":[{\"delta\":{\"content\":\"one\"}}]}\n\n", nil, false)
	feedAt(t, p, "data: {\"choices\":[{\"delta\":{\"content\":\"two\"}}]}\n\n", events.Ptr(int64(200)), true)
	if p.FirstOutputUS != nil || p.LastOutputUS != nil {
		t.Fatal("missing first timing replaced by later output")
	}
}

func TestNonStreamingAndIncompleteTimings(t *testing.T) {
	p := New()
	p.Content("application/json", "")
	feedAt(t, p, `{"choices":[{"message":{"content":"text"}}],"usage":{"completion_tokens":4}}`, events.Ptr(int64(123)), true)
	if p.ResponseStreaming == nil || *p.ResponseStreaming || p.FirstOutputUS != nil || p.LastOutputUS != nil {
		t.Fatal("JSON got token timing")
	}
	p = New()
	p.Content("text/event-stream", "")
	feedAt(t, p, "data: {\"choices\":[{\"delta\":{\"content\":\"prefix\"}}]}\n\n", events.Ptr(int64(123)), false)
	p.Finalize()
	if p.FirstOutputUS == nil || *p.FirstOutputUS != 123 || !events.Has(p.Anomalies, "protocol_incomplete") {
		t.Fatal("lost measured TTFT on incomplete response")
	}
	if strings.Contains(p.Raw, "prefix") {
		t.Fatal("content persisted")
	}
}
