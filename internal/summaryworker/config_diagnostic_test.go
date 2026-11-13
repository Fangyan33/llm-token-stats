package summaryworker

import (
	"testing"

	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
)

func TestWorkerConfigurationAggregatesFields(t *testing.T) {
	c := Config{Doris: config.Doris{Database: "invalid/name"}, Worker: Settings{CollectionStartDate: "not-a-date", RefreshTimeoutSeconds: 5, MaxRunSeconds: 1, LockMode: "invalid"}}
	r := configdiag.Find(c.Defaults())
	if r == nil || r.Total != 5 {
		t.Fatalf("missing worker errors: %+v", r)
	}
	for _, field := range []string{"doris.database", "doris.dsn_env", "summary_worker.collection_start_date", "summary_worker.max_run_seconds", "summary_worker.lock_mode"} {
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
}
