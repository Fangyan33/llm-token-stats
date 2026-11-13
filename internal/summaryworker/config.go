// Package summaryworker refreshes ended UTC dates and their nested monthly MVs.
package summaryworker

import (
	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
	"os"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/pkg/strictjson"
)

type Settings struct {
	CollectionStartDate   string `json:"collection_start_date"`
	RefreshTimeoutSeconds int    `json:"refresh_timeout_seconds"`
	PollIntervalSeconds   int    `json:"poll_interval_seconds"`
	MaxRunSeconds         int    `json:"max_run_seconds"`
	LockMode              string `json:"lock_mode"`
	LockFile              string `json:"lock_file"`
	LeaseName             string `json:"lease_name"`
}
type Config struct {
	Doris  config.Doris `json:"doris"`
	Worker Settings     `json:"summary_worker"`
}

func ReadConfig(path string) (Config, error) {
	var c Config
	b, err := os.ReadFile(path)
	if err != nil {
		return c, configdiag.Source(configdiag.New("summary_config_file", "$", "file_read_failed", "readable configuration file", err), path)
	}
	if len(b) > 65536 {
		r := &configdiag.Report{Source: path}
		r.Number(int64(len(b)), 0, 65536, "summary_config_json", "$")
		return c, r
	}
	if err := strictjson.DecodeConfig(b, &c); err != nil {
		return c, configdiag.Source(err, path)
	}
	if err := c.Defaults(); err != nil {
		return c, configdiag.Source(err, path)
	}
	return c, nil
}
func (c *Config) Defaults() error {
	w := &c.Worker
	r := &configdiag.Report{}
	r.Check(config.DatabaseName(c.Doris.Database), "summary_database", "doris.database", "invalid_identifier", "ASCII letter followed by up to 63 letters, digits or underscores")
	r.Check(c.Doris.DSNEnv != "", "summary_database", "doris.dsn_env", "required", "environment variable containing Doris DSN")
	_, dateErr := ParseDate(w.CollectionStartDate)
	r.Check(dateErr == nil, "collection_start_date_required", "summary_worker.collection_start_date", "invalid_date", "valid UTC date in YYYY-MM-DD format")
	if w.RefreshTimeoutSeconds == 0 {
		w.RefreshTimeoutSeconds = 1800
	}
	if w.PollIntervalSeconds == 0 {
		w.PollIntervalSeconds = 2
	}
	if w.MaxRunSeconds == 0 {
		w.MaxRunSeconds = 21600
	}
	r.Number(int64(w.RefreshTimeoutSeconds), 1, 9223372036854775807, "summary_timeouts", "summary_worker.refresh_timeout_seconds")
	r.Number(int64(w.PollIntervalSeconds), 1, 9223372036854775807, "summary_timeouts", "summary_worker.poll_interval_seconds")
	r.Relation(w.MaxRunSeconds >= w.RefreshTimeoutSeconds, "summary_timeouts", "summary_worker.max_run_seconds", "summary_worker.refresh_timeout_seconds", "max_run_seconds >= refresh_timeout_seconds", int64(w.MaxRunSeconds))
	r.Check(w.LockMode == "local" || w.LockMode == "kubernetes", "summary_lock_mode_required", "summary_worker.lock_mode", "invalid_enum", "local or kubernetes")
	if w.LockMode == "local" {
		r.Check(w.LockFile != "", "summary_lock_file_required", "summary_worker.lock_file", "required", "nonempty local lock file path")
	}
	if w.LockMode == "kubernetes" {
		r.Check(identifier.MatchString(w.LeaseName), "summary_lease_name", "summary_worker.lease_name", "invalid_identifier", "ASCII letter followed by up to 62 letters, digits, underscores or hyphens")
	}
	return r.Err()
}

func ParseDate(s string) (time.Time, error) {
	t, e := time.Parse("2006-01-02", s)
	if e != nil || t.Format("2006-01-02") != s {
		return time.Time{}, configdiag.New("invalid_date", "$", "invalid_date", "valid UTC date in YYYY-MM-DD format", nil)
	}
	return t, nil
}
