package observation

import (
	"context"
	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/Fangyan33/llm-token-stats/internal/health"
	"github.com/Fangyan33/llm-token-stats/internal/requesttokens"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRequestBodyCaptureIndependentOfUsage(t *testing.T) {
	for _, mode := range []string{"disabled", "known", "invalid_json", "invalid_messages", "limit", "missing_chunk", "abort", "compressed", "empty", "trailers", "missing_body", "missing_usage", "queue_bytes", "scratch_limit", "no_eos"} {
		t.Run(mode, func(t *testing.T) {
			out := &sink{}
			cfg := config.Observer{ParserBudget: 64 << 20, MaxActive: 4, ProducerID: "test", ClusterName: "test", DeploymentEnvironment: "dev", AcceptedConfigs: []string{"v1"}, AcceptedAdapters: []string{"chat_completions_v1"}, RequestTokenizerWorkers: 1}
			if mode == "limit" {
				cfg.RequestBodyMaxBytes = 1
			}
			if mode == "queue_bytes" {
				cfg.RequestTokenizerQueueBytes = 1
			}
			if mode == "scratch_limit" {
				cfg.ParserBudget = 8 << 10
			}
			s := &Server{Cfg: cfg, Health: &health.State{}, Sink: out}
			defer s.CloseRequestTokens()
			st := &pb.Start{RequestStartedAtMs: time.Now().UnixMilli(), GatewayInstanceId: "test", ConfigVersion: "v1", AdapterId: "chat_completions_v1", Method: "POST", RequestBodyCaptureEnabled: mode != "disabled"}
			if mode == "compressed" {
				st.RequestContentEncoding = "gzip"
			}
			st.RequestEndStream = mode == "missing_body"
			o, err := s.start(context.Background(), 1, &pb.ObservationFrame{SchemaVersion: 1, FrameSeq: 1, Payload: &pb.ObservationFrame_Start{Start: st}})
			if err != nil {
				t.Fatal(err)
			}
			body := []byte(`{"model":"requested-model","messages":[{"content":" hello中文🙂"}]}`)
			if mode == "invalid_json" {
				body = []byte(`{"messages":`)
			}
			if mode == "empty" {
				body = []byte(`{"model":"requested-model","messages":[{"content":""}]}`)
			}
			if mode == "invalid_messages" {
				body = []byte(`{"model":"requested-model","messages":42}`)
			}
			feed := func(f *pb.ObservationFrame) {
				f.SchemaVersion = 1
				f.FrameSeq = o.seq + 1
				if reason := o.feed(f); reason != "" && reason != "finalized" {
					t.Fatal(reason)
				}
			}
			if mode != "disabled" && mode != "compressed" && mode != "missing_body" {
				off := uint64(0)
				if mode == "missing_chunk" {
					off = 1
				}
				eos := mode != "abort" && mode != "trailers" && mode != "no_eos"
				feed(&pb.ObservationFrame{Payload: &pb.ObservationFrame_BodyChunk{BodyChunk: &pb.BodyChunk{Direction: pb.Direction_REQUEST, BodySeq: 1, Offset: off, Data: body, EndStream: eos}}})
				if mode == "abort" {
					feed(&pb.ObservationFrame{Payload: &pb.ObservationFrame_RequestBodyStopped{RequestBodyStopped: &pb.RequestBodyStopped{}}})
				}
				if mode == "trailers" {
					feed(&pb.ObservationFrame{Payload: &pb.ObservationFrame_BodyChunk{BodyChunk: &pb.BodyChunk{Direction: pb.Direction_REQUEST, BodySeq: 2, Offset: uint64(len(body)), EndStream: true}}})
				}
			}
			feed(&pb.ObservationFrame{Payload: &pb.ObservationFrame_ResponseHeaders{ResponseHeaders: &pb.ResponseHeaders{HttpStatus: 200, ContentType: "application/json"}}})
			resp := []byte(`{"usage":{"prompt_tokens":123,"completion_tokens":4,"total_tokens":127}}`)
			if mode == "missing_usage" {
				resp = []byte(`{"choices":[]}`)
			}
			feed(&pb.ObservationFrame{Payload: &pb.ObservationFrame_BodyChunk{BodyChunk: &pb.BodyChunk{Direction: pb.Direction_RESPONSE, BodySeq: 1, Data: resp, EndStream: true}}})
			feed(&pb.ObservationFrame{Payload: &pb.ObservationFrame_End{End: &pb.End{RequestFinishedAtMs: time.Now().UnixMilli(), LastResponseBodySeq: 1, ResponseBytesSeen: uint64(len(resp)), RequestBytesSeen: uint64(len(body)), RequestEndStreamSeen: true, ResponseEndStreamSeen: true, RequestBodyCaptureComplete: true, LastRequestBodySeq: o.bodySeq[0]}}})
			until := time.Now().Add(time.Second)
			for !o.pollLookup() && time.Now().Before(until) {
				time.Sleep(time.Millisecond)
			}
			o.reason = "finalized"
			o.finalize()
			until = time.Now().Add(5 * time.Second)
			var got []*events.Event
			for time.Now().Before(until) {
				out.mu.Lock()
				got = append([]*events.Event(nil), out.es...)
				out.mu.Unlock()
				if len(got) == 2 {
					break
				}
				time.Sleep(time.Millisecond)
			}
			if len(got) != 2 {
				t.Fatal("missing request/response event", len(got))
			}
			byType := map[string]*events.Event{}
			for _, e := range got {
				b, err := events.Canonical(e)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = events.Decode(b, []byte(e.ObservationID), time.Now()); err != nil {
					t.Fatal(e.EventType, err)
				}
				byType[e.EventType] = e
			}
			start, response := byType["request_started"], byType["usage_observed"]
			if start == nil || response == nil || start.UsageEligible || response.RequestModel != nil {
				t.Fatal("invalid request/response separation")
			}
			modelKnown := mode == "known" || mode == "empty" || mode == "trailers" || mode == "missing_usage" || mode == "invalid_messages"
			if (start.RequestModel != nil) != modelKnown || modelKnown && *start.RequestModel != "requested-model" {
				t.Fatal("request model missing or leaked from incomplete/unparsed body")
			}
			if mode == "disabled" {
				if start.Usage != nil || start.UsageStatus != "not_applicable" {
					t.Fatal("disabled capture changed start event")
				}
			} else {
				known := mode == "known" || mode == "empty" || mode == "trailers" || mode == "missing_usage"
				if start.Usage == nil || (start.Usage.InputTokens.Value != nil) != known {
					t.Fatal("wrong request estimate", start.Usage)
				}
				if known {
					want := int64(3) // Official o200k_base: " hello中文🙂".
					if mode == "empty" {
						want = 0
					}
					if *start.Usage.InputTokens.Value != want || start.Usage.InputTokens.Source != "estimated" || start.UsageStatus != "partial" {
						t.Fatal(start.Usage, start.UsageStatus)
					}
				} else if start.UsageStatus != "unknown" || start.Usage.InputTokens.Source != "unknown" {
					t.Fatal(start.Usage, start.UsageStatus)
				}
			}
			if mode == "missing_usage" {
				if response.Usage.InputTokens.Value != nil || response.UsageEligible || response.UsageStatus != "unknown" || !events.Has(response.AnomalyCodes, "missing_usage") {
					t.Fatal("request estimate leaked into missing response usage")
				}
			} else if response.Usage.InputTokens.Value == nil || *response.Usage.InputTokens.Value != 123 || response.Usage.InputTokens.Source != "reported" || !response.UsageEligible {
				t.Fatal("request estimate changed reported usage")
			}
			s.CloseRequestTokens()
			if s.requestBytes.Load() != 0 {
				t.Fatal("request credits leaked")
			}
		})
	}
}

func TestRequestEventWaitsForBothInputsAndEmitsOnce(t *testing.T) {
	for _, resultFirst := range []bool{false, true} {
		out := &sink{}
		r := &requestEvent{sink: out, captureEnabled: true}
		e := &events.Event{EventType: "request_started", AnomalyCodes: []string{}}
		result := requesttokens.Result{Value: events.Ptr(int64(42)), Reason: "known", RequestModel: events.Ptr("requested-model")}
		if resultFirst {
			r.complete(result)
		} else {
			r.freeze(e)
		}
		if len(out.es) != 0 {
			t.Fatal("request event emitted before identity/token rendezvous")
		}
		// A response may be finalized first; no correction of :start is required.
		out.Emit(&events.Event{EventType: "usage_observed"})
		if resultFirst {
			r.freeze(e)
		} else {
			r.complete(result)
		}
		r.complete(requesttokens.Result{Reason: "late_duplicate"})
		if len(out.es) != 2 || out.es[1].EventType != "request_started" || *out.es[1].Usage.InputTokens.Value != 42 || out.es[1].RequestModel == nil || *out.es[1].RequestModel != "requested-model" || r.snapshot != nil {
			t.Fatal("late result lost or duplicated request event")
		}
	}
}

func TestRequestEventConcurrentIdentityAndCount(t *testing.T) {
	for i := 0; i < 50; i++ {
		out := &sink{}
		r := &requestEvent{sink: out, captureEnabled: true}
		start := make(chan struct{})
		var done sync.WaitGroup
		done.Add(2)
		go func() {
			defer done.Done()
			<-start
			r.freeze(&events.Event{EventType: "request_started", AnomalyCodes: []string{}})
		}()
		go func() {
			defer done.Done()
			<-start
			result := requesttokens.Result{Value: events.Ptr(int64(0)), Reason: "known"}
			r.complete(result)
			r.complete(result)
		}()
		close(start)
		done.Wait()
		if len(out.es) != 1 || out.es[0].Usage.InputTokens.Value == nil || *out.es[0].Usage.InputTokens.Value != 0 || out.es[0].Usage.InputTokens.Source != "estimated" {
			t.Fatal("concurrent completion changed or duplicated event")
		}
	}
}
func TestRequestCaptureSizeConfiguration(t *testing.T) {
	for _, n := range []int64{32 << 20, 128 << 20} {
		c := config.Observer{RequestBodyMaxBytes: n}
		if c.RequestTokenDefaults() != nil {
			t.Fatal(n)
		}
	}
	c := config.Observer{RequestBodyMaxBytes: (128 << 20) + 1}
	if c.RequestTokenDefaults() == nil {
		t.Fatal("limit accepted")
	}
	// Split UTF-8 in the middle of a code point. Only the complete body is parsed.
	parts, err := requesttokens.Parts([]byte(`{"messages":[{"content":"` + strings.Repeat("中文", 10) + `"}]}`))
	if err != nil || len(parts) != 1 {
		t.Fatal(err)
	}
}
