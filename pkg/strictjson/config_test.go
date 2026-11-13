package strictjson

import (
	"strings"
	"testing"

	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
)

func TestConfigurationDiagnosticsLocateFailuresWithoutInput(t *testing.T) {
	type route struct {
		Paths []string `json:"paths"`
	}
	type configuration struct {
		Routes []route `json:"routes"`
	}
	for _, tc := range []struct{ name, input, field, reason string }{
		{"type", `{"routes":[{"paths":["ok",123]}]}`, "routes[0].paths[1]", "type_mismatch"},
		{"unknown", `{"routes":[{}, {"typo":"https://private.invalid/secret"}]}`, "routes[1].typo", "unknown_field"},
		{"duplicate", `{"routes":[{"paths":[],"paths":[]}]}`, "routes[0].paths", "duplicate_key"},
		{"syntax", "{\n\"routes\": [ }", "routes[0]", "invalid_json"},
		{"unicode", `{"routes":[{"paths":["\ud800"]}]}`, "routes[0].paths[0]", "invalid_json_string"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var c configuration
			err := DecodeConfig([]byte(tc.input), &c)
			r := configdiag.Find(err)
			if r == nil || len(r.Issues) != 1 {
				t.Fatalf("missing diagnostic: %v", err)
			}
			i := r.Issues[0]
			if i.Field != tc.field || i.Reason != tc.reason || i.Offset < 1 || i.Line < 1 || i.Column < 1 {
				t.Fatalf("wrong diagnostic: %+v", i)
			}
			if strings.Contains(strings.Join(i.Fields(), " "), "private.invalid") {
				t.Fatal("input leaked")
			}
			if Decode([]byte(tc.input), &c) != ErrInvalid {
				t.Fatal("business decoder exposed detailed diagnostic")
			}
		})
	}
}
