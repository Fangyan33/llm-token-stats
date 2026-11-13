package logging

import (
	"errors"
	"log/slog"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
)

func TestConfigurationLogsAreBoundedAndPreserveIndividualCauses(t *testing.T) {
	var out capture
	l := New("consumer", &out, slog.LevelError)
	defer l.Close()
	restore := SetDefault(l)
	defer restore()
	r := &configdiag.Report{Source: "configs/consumer.json"}
	r.Merge(configdiag.New("kafka_tls", "kafka.ca_file", "file_read_failed", "readable CA file", &os.PathError{Op: "open", Path: "never-log-this-secret", Err: syscall.ENOENT}))
	for n := 0; n < 35; n++ {
		r.Number(-1, 1, 20000, "config_consumer", configdiag.Index("consumer.test", n))
	}
	Error("consumer_failed", WithCause("configuration_failed", r))
	flush(t, l)
	got := out.String()
	if strings.Count(got, "event=configuration_validation_failed") != 32 || !strings.Contains(got, "error_count=36 omitted_count=4") {
		t.Fatal(got)
	}
	if strings.Contains(got, "never-log-this-secret") {
		t.Fatal("file path leaked")
	}
	if strings.Count(got, "errno=") != 1 {
		t.Fatal("file error attached to unrelated fields", got)
	}
	if !strings.Contains(got, "config_field=consumer.test[0]") || !strings.Contains(got, "actual=-1") || !strings.Contains(got, "error_code=configuration_failed") {
		t.Fatal(got)
	}
}

func TestNestedErrorCodesRemainDistinctAndUnknownErrorsStayPrivate(t *testing.T) {
	err := WithCause("outer", WithCause("middle", WithCause("inner", errors.New("private-password"))))
	fields := ErrorFields(err)
	seen := map[string]bool{}
	for n := 0; n+1 < len(fields); n += 2 {
		key := fields[n].(string)
		if seen[key] {
			t.Fatalf("duplicate field %s", key)
		}
		seen[key] = true
		if fields[n+1] == "private-password" {
			t.Fatal("raw error leaked")
		}
	}
	if fields[3] != "outer" {
		t.Fatal("outer error code lost", fields)
	}
}
