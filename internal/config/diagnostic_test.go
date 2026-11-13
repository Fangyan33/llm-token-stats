package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
)

func TestReadConsumerAggregatesCommonFieldsAndOffsetIndices(t *testing.T) {
	c, err := Read("../../configs/consumer.local.json")
	if err != nil {
		t.Fatal(err)
	}
	c.Kafka.Topic, c.Kafka.Brokers = "private/topic", nil
	c.Consumer.Rows, c.Consumer.Bytes = 20001, -1
	c.Consumer.Initial = []Position{{Partition: -1, Offset: -2}, {Partition: -1}}
	raw, _ := json.Marshal(c)
	path := filepath.Join(t.TempDir(), "consumer.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	_, err = ReadConsumer(path)
	r := configdiag.Find(err)
	if r == nil || r.Total != 8 || r.Source != path {
		t.Fatalf("missing aggregation: %+v", r)
	}
	for _, field := range []string{"kafka.topic", "kafka.bootstrap_servers", "consumer.max_batch_rows", "consumer.max_batch_bytes", "consumer.initial_offsets[0].partition", "consumer.initial_offsets[0].offset", "consumer.initial_offsets[1].partition"} {
		found := false
		for _, i := range r.Issues {
			if i.Field == field {
				found = true
			}
		}
		if !found {
			t.Errorf("missing %s", field)
		}
	}
	if strings.Contains(err.Error(), "private") {
		t.Fatal("configuration value leaked")
	}
}

func TestDorisDSNDiagnosticsDistinguishCauses(t *testing.T) {
	for _, tc := range []struct{ value, reason string }{
		{"", "environment_variable_empty"},
		{"secret-without-database-separator", "invalid_dsn"},
		{"user:private-password@tcp(localhost:9030)/other", "database_mismatch"},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			t.Setenv("TEST_DIAGNOSTIC_DSN", tc.value)
			_, err := (Doris{Database: "metering", DSNEnv: "TEST_DIAGNOSTIC_DSN"}).ParseDSN("doris_dsn")
			r := configdiag.Find(err)
			if r == nil || r.Issues[0].Reason != tc.reason || r.Issues[0].Field != "doris.dsn_env" || r.Issues[0].EnvName != "TEST_DIAGNOSTIC_DSN" {
				t.Fatalf("wrong DSN diagnostic: %+v", r)
			}
			if strings.Contains(strings.Join(r.Issues[0].Fields(), " "), "private-password") {
				t.Fatal("password leaked")
			}
		})
	}
}
