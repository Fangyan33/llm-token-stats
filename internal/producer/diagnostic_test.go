package producer

import (
	"testing"

	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
)

func TestOptionsReportsBothMissingCredentialsAndCA(t *testing.T) {
	t.Setenv("DIAGNOSTIC_KAFKA_USER", "")
	t.Setenv("DIAGNOSTIC_KAFKA_PASSWORD", "")
	_, err := Options(config.Kafka{CAFile: t.TempDir() + "/missing.pem", UsernameEnv: "DIAGNOSTIC_KAFKA_USER", PasswordEnv: "DIAGNOSTIC_KAFKA_PASSWORD"})
	r := configdiag.Find(err)
	if r == nil || r.Total != 3 {
		t.Fatalf("missing combined TLS/SASL diagnostic: %+v", r)
	}
	want := []string{"kafka.ca_file", "kafka.username_env", "kafka.password_env"}
	for n, field := range want {
		if r.Issues[n].Field != field {
			t.Fatalf("missing field %s: %+v", field, r)
		}
	}
}
