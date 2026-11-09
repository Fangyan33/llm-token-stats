// Package summaryworker refreshes ended UTC dates and their nested monthly MVs.
package summaryworker

import (
	"github.com/Fangyan33/llm-token-stats/internal/logging"
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
		return c, logging.WithCause("summary_config_file", err)
	}
	if len(b) > 65536 || strictjson.Decode(b, &c) != nil {
		return c, logging.WithCause("summary_config_json", nil)
	}
	if err := c.Defaults(); err != nil {
		return c, err
	}
	return c, nil
}
func (c *Config) Defaults() error {
	w := &c.Worker
	if !config.DatabaseName(c.Doris.Database) || c.Doris.DSNEnv == "" {
		return logging.WithCause("summary_database", nil)
	}
	if _, err := ParseDate(w.CollectionStartDate); err != nil {
		return logging.WithCause("collection_start_date_required", nil)
	}
	if w.RefreshTimeoutSeconds == 0 {
		w.RefreshTimeoutSeconds = 1800
	}
	if w.PollIntervalSeconds == 0 {
		w.PollIntervalSeconds = 2
	}
	if w.MaxRunSeconds == 0 {
		w.MaxRunSeconds = 21600
	}
	if w.RefreshTimeoutSeconds < 1 || w.PollIntervalSeconds < 1 || w.MaxRunSeconds < w.RefreshTimeoutSeconds {
		return logging.WithCause("summary_timeouts", nil)
	}
	if w.LockMode != "local" && w.LockMode != "kubernetes" {
		return logging.WithCause("summary_lock_mode_required", nil)
	}
	if w.LockMode == "local" && w.LockFile == "" {
		return logging.WithCause("summary_lock_file_required", nil)
	}
	if w.LockMode == "kubernetes" && !identifier.MatchString(w.LeaseName) {
		return logging.WithCause("summary_lease_name", nil)
	}
	return nil
}
func ParseDate(s string) (time.Time, error) {
	t, e := time.Parse("2006-01-02", s)
	if e != nil || t.Format("2006-01-02") != s {
		return time.Time{}, logging.WithCause("invalid_date", nil)
	}
	return t, nil
}
