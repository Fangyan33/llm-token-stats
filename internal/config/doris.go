package config

import (
	"os"

	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
	"github.com/go-sql-driver/mysql"
)

// ParseDSN snapshots credentials once. Driver messages may contain the DSN, so
// expose only a fixed reason and the name of the environment variable.
func (c Doris) ParseDSN(code string) (*mysql.Config, error) {
	raw := os.Getenv(c.DSNEnv)
	if raw == "" {
		return nil, configdiag.Environment(code, "doris.dsn_env", c.DSNEnv, "environment_variable_empty", "nonempty Doris DSN")
	}
	parsed, err := mysql.ParseDSN(raw)
	if err != nil {
		return nil, configdiag.Environment(code, "doris.dsn_env", c.DSNEnv, "invalid_dsn", "valid MySQL DSN")
	}
	if parsed.DBName != c.Database {
		r := configdiag.Environment(code, "doris.dsn_env", c.DSNEnv, "database_mismatch", "DSN database equals doris.database")
		r.Issues[0].RelatedField = "doris.database"
		return nil, r
	}
	return parsed, nil
}
