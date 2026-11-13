package consumer

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRequestInputEstimateWrittenToTypedColumns(t *testing.T) {
	for _, name := range []string{"request-estimated", "request-unknown", "request-started"} {
		t.Run(name, func(t *testing.T) {
			b, err := os.ReadFile("../../testdata/collection/" + name + ".json")
			if err != nil {
				t.Fatal(err)
			}
			var e events.Event
			if err := json.Unmarshal(b, &e); err != nil {
				t.Fatal(err)
			}
			now := time.UnixMilli(e.EventCreatedAtMs)
			batch, err := Freeze([]*kgo.Record{{Topic: "test", Key: []byte(e.ObservationID), Value: b, Timestamp: now}}, config.Consumer{Bytes: 4 << 20, AcceptedConfigs: []string{e.ConfigVersion}}, now)
			if err != nil || len(batch.Events) != 1 || len(batch.Rejected) != 0 {
				t.Fatal("request rejected", err)
			}
			var row map[string]any
			if err := json.Unmarshal(batch.Loads["events_raw"], &row); err != nil {
				t.Fatal(err)
			}
			if row["output_tokens"] != nil || row["usage_eligible"] != float64(0) {
				t.Fatal("request input changed response columns or eligibility")
			}
			if e.RequestModel != nil && row["request_model"] != *e.RequestModel || e.RequestModel == nil && row["request_model"] != nil {
				t.Fatal("request model changed in typed load")
			}
			if name == "request-estimated" {
				if row["input_tokens"] != float64(1000000) || row["input_tokens_source"] != "estimated" {
					t.Fatal("estimate missing in Doris load", row["input_tokens"], row["input_tokens_source"])
				}
			} else if row["input_tokens"] != nil {
				t.Fatal("unknown request input was filled")
			}
			if _, err := events.Decode([]byte(row["event_json"].(string)), []byte(e.ObservationID), now); err != nil {
				t.Fatal("event JSON disagrees with typed load", err)
			}
		})
	}
}
