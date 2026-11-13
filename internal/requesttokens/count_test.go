package requesttokens

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPartsAndOrdinaryText(t *testing.T) {
	c, err := NewCounter()
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{`{"messages":[{"content":""}]}`, `{"messages":[{"content":null,"tool_calls":[{"function":{"arguments":"{}","name":"weather"}}]}],"tools":[{"function":{"name":"weather"},"type":"function"}]}`, `{"messages":[{"content":[{"type":"text","text":"你好"},{"type":"image_url","image_url":{"url":"ignored"}}]}]}`} {
		if _, err := c.Count([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	for _, body := range []string{`{}`, `{"messages":[]}`, `{"messages":[null]}`, `{"messages":[{"content":42}]}`, `{"messages":[{"content":"a","content":"b"}]}`, `{"messages":[{"content":"a"}],"tools":{}}`, `{"messages":[{"content":[{"type":"image_url"}]}]}`, `{"messages":[{"content":"\ud800"}]}`} {
		if _, err := c.Count([]byte(body)); err == nil {
			t.Fatalf("accepted invalid test body: %s", body)
		}
	}
	parts, err := Parts([]byte(`{"messages":[{"content":"hello","role":"user","tool_calls":[{"z":1,"a":"<x>"}]}],"tools":[{"z":2,"a":1}]}`))
	if err != nil || len(parts) != 3 || parts[1] != `[{"a":"<x>","z":1}]` || parts[2] != `[{"a":1,"z":2}]` {
		t.Fatal(parts, err)
	}
	if n, err := c.Count([]byte(`{"messages":[{"content":"` + strings.Repeat(" word", 100000) + `"}]}`)); err != nil || n != 100000 {
		t.Fatal(n, err)
	}
}

func TestRequestModelSharesValidationAndDoesNotChangeCount(t *testing.T) {
	c, err := NewCounter()
	if err != nil {
		t.Fatal(err)
	}
	for _, model := range []string{"model-a", "模型/variant-1", strings.Repeat("m", 1024), strings.Repeat("你", 341) + "a"} {
		raw, _ := json.Marshal(model)
		n, got, err := c.Analyze([]byte(`{"model":` + string(raw) + `,"messages":[{"content":"hello"}]}`))
		if err != nil || n != 1 || got == nil || *got != model {
			t.Fatal("valid model/count", n, got, err)
		}
	}
	// Maximum escaped representation of a valid 1024-byte model remains accepted.
	_, model, err := c.Analyze([]byte(`{"model":"` + strings.Repeat(`\u006d`, 1024) + `","messages":[{"content":"hello"}]}`))
	if err != nil || model == nil || *model != strings.Repeat("m", 1024) {
		t.Fatal("escaped model rejected", err)
	}
	for _, raw := range []string{`null`, `42`, `{}`, `[]`, `""`, `" \t "`, `"model\u0000"`, `"line\nfeed"`, `"` + strings.Repeat("m", 1025) + `"`, `"` + strings.Repeat("你", 342) + `"`, `"` + strings.Repeat(`\u006d`, 1025) + `"`} {
		n, model, err := c.Analyze([]byte(`{"model":` + raw + `,"messages":[{"content":"hello"}]}`))
		if err != nil || n != 1 || model != nil {
			t.Fatal("invalid model changed token count", n, model, err)
		}
	}
	for _, body := range []string{`{"model":"model-a","messages":[]}`, `{"model":"model-a","messages":42}`, `{"model":"model-a"}`} {
		_, model, err := c.Analyze([]byte(body))
		if err == nil || model == nil || *model != "model-a" {
			t.Fatal("valid model lost with uncountable content", model, err)
		}
	}
	for _, body := range []string{`{"model":"a","model":"b","messages":[]}`, `{"model":"a","messages":`, `{"model":"\ud800","messages":[]}`} {
		_, model, err := c.Analyze([]byte(body))
		if err == nil || model != nil {
			t.Fatal("model extracted from invalid JSON")
		}
	}
}
