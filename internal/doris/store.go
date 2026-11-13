package doris

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/consumer"
	"github.com/Fangyan33/llm-token-stats/internal/logging"
	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
	"github.com/go-sql-driver/mysql"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Store struct {
	DB       *sql.DB
	Cfg      config.Doris
	HTTP     *http.Client
	username string
	password string
}

func New(c config.Doris) (*Store, error) {
	if !config.DatabaseName(c.Database) {
		return nil, configdiag.New("doris_database", "doris.database", "invalid_identifier", "valid database identifier", nil)
	}
	parsed, err := c.ParseDSN("doris_dsn")
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("mysql", parsed.FormatDSN())
	if err != nil {
		return nil, configdiag.Environment("doris_dsn", "doris.dsn_env", c.DSNEnv, "driver_initialization_failed", "supported MySQL connection options")
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(time.Minute)
	logging.Info("doris_client_configured", "database", c.Database)
	// Freeze the same credentials for SQL and HTTP for this Store's lifetime.
	s := &Store{DB: db, Cfg: c, username: parsed.User, password: parsed.Passwd}
	s.HTTP = &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(r *http.Request, via []*http.Request) error {
		if len(via) > 3 || !s.allowed(r.URL) || r.Method != "PUT" || len(via) > 0 && via[0].URL.Scheme == "https" && r.URL.Scheme != "https" {
			return logging.WithFields("doris_redirect", nil, "config_field", "doris.allowed_load_hosts", "reason", "redirect_policy_rejected", "expected", "allowed HTTP(S) PUT target without downgrade, at most 3 redirects")
		}
		r.SetBasicAuth(s.username, s.password)
		return nil
	}}
	return s, nil
}
func (s *Store) allowed(u *url.URL) bool {
	if u.Scheme != "http" && u.Scheme != "https" || u.User != nil {
		return false
	}
	for _, h := range s.Cfg.AllowedHosts {
		if u.Host == h {
			return true
		}
	}
	return false
}
func quote(v any) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(fmt.Sprint(v), "\\", "\\\\"), "'", "''") + "'"
}

var keys = map[string][]string{"events_raw": {"request_date", "observation_id", "event_id", "payload_hash"}, "ingest_rejections": {"kafka_record_date", "kafka_topic", "kafka_partition", "kafka_offset", "payload_hash"}, "request_conflict_ids": {"observation_id"}, "summary_dirty_notifications": {"stat_date", "notification_id"}, "consumer_batch_receipts": {"kafka_topic", "kafka_partition", "first_offset", "last_offset", "batch_id"}, "consumer_partition_origins": {"group_id", "kafka_topic", "kafka_partition", "origin_id"}, "consumer_partition_gaps": {"group_id", "kafka_topic", "kafka_partition", "gap_id"}}

func (s *Store) visible(ctx context.Context, label string) (bool, error) {
	rows, err := s.DB.QueryContext(ctx, "SHOW TRANSACTION FROM "+s.Database()+" WHERE LABEL = "+quote(label))
	if err != nil {
		// A new label normally has no transaction yet. Doris reports that as
		// 1105, the same generic code used for real SQL failures. Match only
		// the known absence diagnostic; other 1105 errors must remain visible.
		var mysqlErr *mysql.MySQLError
		missing := "transaction with label " + label + " does not exist"
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1105 && (mysqlErr.Message == missing || mysqlErr.Message == "errCode = 2, detailMessage = "+missing) {
			return false, nil
		}
		logging.Failure("doris_sql_failed", "doris_transaction", err, "operation", "transaction_status", "label", label)
		return false, logging.WithCause("transaction_unknown", err)
	}
	logging.Recovered("doris_transaction", "doris_sql_recovered", "operation", "transaction_status")
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return false, logging.WithCause("transaction_unknown", err)
	}
	for rows.Next() {
		vals := make([]sql.RawBytes, len(cols))
		ptr := make([]any, len(cols))
		for i := range vals {
			ptr[i] = &vals[i]
		}
		if err := rows.Scan(ptr...); err != nil {
			logging.Failure("doris_sql_failed", "doris_transaction", err, "operation", "scan_transaction", "label", label)
			return false, logging.WithCause("transaction_unknown", err)
		}
		for i, c := range cols {
			if c == "TransactionStatus" {
				if string(vals[i]) == "ABORTED" {
					logging.Failure("doris_transaction_aborted", "doris_transaction", nil, "label", label)
					return false, logging.WithCause("transaction_aborted", nil)
				}
				return string(vals[i]) == "VISIBLE", nil
			}
		}
	}
	if err := rows.Err(); err != nil {
		logging.Failure("doris_sql_failed", "doris_transaction", err, "operation", "read_transaction", "label", label)
		return false, logging.WithCause("transaction_unknown", err)
	}
	return false, nil
}
func (s *Store) verify(ctx context.Context, table string, rows []map[string]any) error {
	ks, ok := keys[table]
	if !ok {
		return logging.WithCause("doris_table", nil)
	}
	if table == "consumer_batch_receipts" {
		ks = append(append([]string(nil), ks...), "next_offset", "notification_count", "notification_digest")
	}
	seen := map[string]bool{}
	values := [][]string{}
	for _, r := range rows {
		parts, vals := []string{}, []string{}
		for _, k := range ks {
			val := quote(r[k])
			vals = append(vals, val)
			parts = append(parts, "`"+k+"`="+val)
		}
		key := strings.Join(parts, " AND ")
		if !seen[key] {
			seen[key] = true
			values = append(values, vals)
		}
	}
	for i := 0; i < len(values); i += 128 {
		end := min(i+128, len(values))
		var n int
		predicate := visibilityPredicate(ks, values[i:end])
		if err := s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+s.Database()+"."+table+" WHERE "+predicate).Scan(&n); err != nil || n != end-i {
			logging.Failure("doris_visibility_failed", "doris_visibility", err, "table", table, "expected_rows", end-i, "visible_rows", n)
			return logging.WithCause("doris_visibility", err)
		}
	}
	logging.Recovered("doris_visibility", "doris_visibility_recovered", "table", table)
	return nil
}
func (s *Store) Load(ctx context.Context, table, label string, body []byte, rows []map[string]any) error {
	started := time.Now()
	if _, ok := keys[table]; !ok {
		return logging.WithCause("doris_table", nil)
	}
	if v, err := s.visible(ctx, label); err == nil && v {
		return s.verify(ctx, table, rows)
	}
	u, err := url.Parse(strings.TrimRight(s.Cfg.FE, "/") + "/api/" + s.Cfg.Database + "/" + table + "/_stream_load")
	if err != nil || !s.allowed(u) {
		return configdiag.New("doris_target", "doris.fe_url", "invalid_or_disallowed_target", "HTTP(S) target included in doris.allowed_load_hosts", nil)
	}
	r, err := http.NewRequestWithContext(ctx, "PUT", u.String(), bytes.NewReader(body))
	if err != nil {
		return logging.WithFields("doris_request", err, "phase", "create_stream_load_request")
	}
	r.SetBasicAuth(s.username, s.password)
	for k, v := range map[string]string{"label": label, "Expect": "100-continue", "format": "json", "read_json_by_line": "true", "max_filter_ratio": "0", "timezone": "+00:00", "strict_mode": "true", "timeout": "10"} {
		r.Header.Set(k, v)
	}
	res, err := s.HTTP.Do(r)
	if err != nil {
		logging.Failure("doris_stream_load_failed", "doris_http", err, "table", table, "label", label, "row_count", len(rows), "duration_ms", time.Since(started).Milliseconds())
	}
	if err == nil {
		defer res.Body.Close()
		b, readErr := io.ReadAll(io.LimitReader(res.Body, 65536))
		var result struct {
			Status                                                                      string
			NumberTotalRows, NumberLoadedRows, NumberFilteredRows, NumberUnselectedRows int
		}
		decodeErr := json.Unmarshal(b, &result)
		if res.StatusCode != 200 || readErr != nil || decodeErr != nil {
			cause := readErr
			if cause == nil {
				cause = decodeErr
			}
			logging.Failure("doris_stream_load_response_failed", "doris_http", cause, "table", table, "label", label, "http_status", res.StatusCode, "phase", "read_or_decode_response", "duration_ms", time.Since(started).Milliseconds())
			result.Status = "uncertain"
		}
		if result.Status == "Success" || result.Status == "Publish Timeout" {
			if result.NumberTotalRows != len(rows) || result.NumberLoadedRows != len(rows) || result.NumberFilteredRows != 0 || result.NumberUnselectedRows != 0 {
				logging.Failure("doris_stream_load_row_count_failed", "doris_load_rows", nil, "table", table, "label", label, "expected_rows", len(rows), "loaded_rows", result.NumberLoadedRows, "filtered_rows", result.NumberFilteredRows, "total_rows", result.NumberTotalRows, "unselected_rows", result.NumberUnselectedRows, "load_status", loadStatus(result.Status))
				return logging.WithCause("doris_row_count", nil)
			}
		} else if result.Status != "Label Already Exists" && result.Status != "uncertain" {
			logging.Failure("doris_stream_load_rejected", "doris_http", nil, "table", table, "label", label, "http_status", res.StatusCode, "load_status", loadStatus(result.Status), "reason", "load_rejected", "action", "pause_partition")
			return logging.WithCause("doris_load", nil)
		}
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		v, e := s.visible(ctx, label)
		if e != nil && e.Error() == "transaction_aborted" {
			return e
		}
		if v {
			err := s.verify(ctx, table, rows)
			if err == nil {
				logging.Recovered("doris_http", "doris_stream_load_recovered", "table", table)
				logging.Recovered("doris_load_rows", "doris_row_counts_recovered", "table", table)
				logging.Debug("doris_load_visible", "table", table, "label", label, "row_count", len(rows), "duration_ms", time.Since(started).Milliseconds())
			}
			return err
		}
		select {
		case <-ctx.Done():
			logging.Failure("doris_load_visibility_timeout", "doris_visibility", ctx.Err(), "table", table, "label", label, "duration_ms", time.Since(started).Milliseconds())
			return logging.WithCause("transaction_unknown", ctx.Err())
		case <-ticker.C:
		}
	}
}
func (s *Store) Evidence(ctx context.Context, ids []string) ([]consumer.Evidence, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	out := []consumer.Evidence{}
	for start := 0; start < len(ids); start += 128 {
		end := start + 128
		if end > len(ids) {
			end = len(ids)
		}
		qs := []string{}
		for _, id := range ids[start:end] {
			qs = append(qs, quote(id))
		}
		// Compare the single contract using typed identity columns.
		r, err := s.DB.QueryContext(ctx, "SELECT CAST(request_date AS STRING),observation_id,payload_hash,"+IdentityKeySQL+","+NameKeySQL+",request_started_at_ms,event_id FROM "+s.Database()+".events_raw WHERE observation_id IN ("+strings.Join(qs, ",")+")")
		if err != nil {
			logging.Failure("doris_evidence_failed", "doris_evidence", err, "id_count", end-start)
			return nil, logging.WithCause("doris_evidence", err)
		}
		for r.Next() {
			var e consumer.Evidence
			if err := r.Scan(&e.Date, &e.ID, &e.Hash, &e.Identity, &e.Names, &e.Started, &e.EventID); err != nil {
				r.Close()
				return nil, logging.WithCause("doris_evidence", err)
			}
			out = append(out, e)
			if len(out) > 100000 {
				r.Close()
				return nil, logging.WithCause("evidence_limit", nil)
			}
		}
		err = r.Err()
		r.Close()
		if err != nil {
			return nil, logging.WithCause("doris_evidence", err)
		}
	}
	return out, nil
}

// IdentityKeySQL keeps the field order aligned with events.IdentityHash and the request projections.
// This JSON tuple is used only for equality within Doris, not as a payload hash.
const IdentityKeySQL = "CAST(JSON_ARRAY(service_id, provider_id, api_key_id, customer_id, identity_status, config_version, gateway_instance_id, adapter_id, x_request_id, logical_request_id, cluster_name, deployment_environment, mapping_status, mapping_version, customer_mapping_status, customer_mapping_version, user_id) AS STRING)"

const NameKeySQL = "CAST(JSON_ARRAY(service_name, provider_name, customer_name) AS STRING)"

// Database is a validated SQL identifier; New rejects arbitrary SQL fragments.
func (s *Store) Database() string { return "`" + s.Cfg.Database + "`" }

func loadStatus(status string) string {
	switch status {
	case "Success", "Publish Timeout", "Label Already Exists", "Fail", "Cancelled", "uncertain":
		return status
	case "":
		return "missing"
	default:
		return "unrecognized"
	}
}
