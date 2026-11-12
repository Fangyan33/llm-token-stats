package gatewayconfig

import (
	"encoding/json"
	"os"
	"testing"
)

func TestMetricsSwitchDefaultAndType(t *testing.T) {
	b, err := os.ReadFile("../../configs/gateway-observer.example.json")
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if json.Unmarshal(b, &raw) != nil {
		t.Fatal("config fixture")
	}
	for _, enabled := range []bool{false, true} {
		raw["metrics_enabled"] = enabled
		b, _ = json.Marshal(raw)
		c, e := Decode(b)
		if e != nil || c.MetricsEnabled != enabled {
			t.Fatal("metrics switch", c, e)
		}
	}
	delete(raw, "metrics_enabled")
	b, _ = json.Marshal(raw)
	c, e := Decode(b)
	if e != nil || c.MetricsEnabled {
		t.Fatal("default must be off", e)
	}
	for _, bad := range []any{nil, "true", 1} {
		raw["metrics_enabled"] = bad
		b, _ = json.Marshal(raw)
		if _, e := Decode(b); e == nil {
			t.Fatal("non-boolean metrics switch accepted", bad)
		}
	}
}
