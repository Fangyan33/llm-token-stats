package gatewayconfig

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestConfigurationDiagnosticsAcrossJSONAndStruct(t *testing.T) {
	b, err := os.ReadFile("../../configs/gateway-observer.example.json")
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	raw["chunk_bytes"], raw["tick_ms"], raw["ack_timeout_ms"] = float64(16385), float64(11), float64(5)
	raw["user_id_header"] = []any{"authorization"}
	raw["logical_request_id_headers"] = []any{"X-Id", "x-id"}
	route := raw["routes"].([]any)[0].(map[string]any)
	route["paths"] = []any{"/private/secret?x=1", "/ok", "/ok"}
	jsonData, _ := json.Marshal(raw)
	value, _ := structpb.NewStruct(raw)
	structData, _ := proto.Marshal(value)
	for _, data := range [][]byte{jsonData, structData} {
		_, err := Decode(data)
		r := configdiag.Find(err)
		if r == nil {
			t.Fatal("missing diagnostic", err)
		}
		for _, field := range []string{"chunk_bytes", "tick_ms", "user_id_header[0]", "logical_request_id_headers[1]", "routes[0].paths[0]", "routes[0].paths[2]"} {
			found := false
			for _, i := range r.Issues {
				if i.Field == field {
					found = true
				}
				if strings.Contains(strings.Join(i.Fields(), " "), "private/secret") {
					t.Fatal("configuration value leaked")
				}
			}
			if !found {
				t.Errorf("missing %s: %+v", field, r)
			}
		}
	}
}

func TestStructInvalidIntegerIdentifiesField(t *testing.T) {
	v, _ := structpb.NewStruct(map[string]any{"chunk_bytes": 1.5})
	b, _ := proto.Marshal(v)
	_, err := Decode(b)
	r := configdiag.Find(err)
	if r == nil || r.Issues[0].Field != "chunk_bytes" || r.Issues[0].Reason != "invalid_integer" {
		t.Fatalf("wrong Struct diagnostic: %+v", r)
	}
}
