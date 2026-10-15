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
	_ "github.com/go-sql-driver/mysql"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type Store struct {
	DB   *sql.DB
	Cfg  config.Doris
	HTTP *http.Client
}

func New(c config.Doris) (*Store, error) {
	dsn := os.Getenv(c.DSNEnv)
	if dsn == "" {
		return nil, errors.New("doris_dsn")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.New("doris_dsn")
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(time.Minute)
	s := &Store{DB: db, Cfg: c}
	s.HTTP = &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(r *http.Request, via []*http.Request) error {
		if len(via) > 3 || !s.allowed(r.URL) || r.Method != "PUT" || len(via) > 0 && via[0].URL.Scheme == "https" && r.URL.Scheme != "https" {
			return errors.New("doris_redirect")
		}
		r.SetBasicAuth(os.Getenv(c.UsernameEnv), os.Getenv(c.PasswordEnv))
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

var keys = map[string][]string{"events_raw": {"request_date", "logical_request_id", "event_id", "payload_hash"}, "ingest_rejections": {"kafka_record_date", "kafka_topic", "kafka_partition", "kafka_offset", "payload_hash"}, "request_conflict_ids": {"logical_request_id"}, "summary_dirty_notifications": {"stat_date", "notification_id"}, "consumer_batch_receipts": {"kafka_topic", "kafka_partition", "first_offset", "last_offset", "batch_id"}, "consumer_partition_origins": {"group_id", "kafka_topic", "kafka_partition", "origin_id"}, "consumer_partition_gaps": {"group_id", "kafka_topic", "kafka_partition", "gap_id"}}

func (s *Store) visible(ctx context.Context, label string) (bool, error) {
	rows, err := s.DB.QueryContext(ctx, "SHOW TRANSACTION FROM metering WHERE LABEL = "+quote(label))
	if err != nil {
		return false, errors.New("transaction_unknown")
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	for rows.Next() {
		vals := make([]sql.RawBytes, len(cols))
		ptr := make([]any, len(cols))
		for i := range vals {
			ptr[i] = &vals[i]
		}
		if rows.Scan(ptr...) != nil {
			return false, errors.New("transaction_unknown")
		}
		for i, c := range cols {
			if c == "TransactionStatus" {
				if string(vals[i]) == "ABORTED" {
					return false, errors.New("transaction_aborted")
				}
				return string(vals[i]) == "VISIBLE", nil
			}
		}
	}
	return false, nil
}
func (s *Store) verify(ctx context.Context, table string, rows []map[string]any) error {
	ks, ok := keys[table]
	if !ok {
		return errors.New("doris_table")
	}
	seen := map[string]bool{}
	wheres := []string{}
	for _, r := range rows {
		parts := []string{}
		for _, k := range ks {
			parts = append(parts, "`"+k+"`="+quote(r[k]))
		}
		if table == "consumer_batch_receipts" {
			for _, k := range []string{"next_offset", "notification_count", "notification_digest"} {
				parts = append(parts, "`"+k+"`="+quote(r[k]))
			}
		}
		where := strings.Join(parts, " AND ")
		if !seen[where] {
			seen[where] = true
			wheres = append(wheres, "("+where+")")
		}
	}
	for i := 0; i < len(wheres); i += 128 {
		end := min(i+128, len(wheres))
		var n int
		if err := s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM metering."+table+" WHERE "+strings.Join(wheres[i:end], " OR ")).Scan(&n); err != nil || n != end-i {
			return errors.New("doris_visibility")
		}
	}
	return nil
}
func (s *Store) Load(ctx context.Context, table, label string, body []byte, rows []map[string]any) error {
	if _, ok := keys[table]; !ok {
		return errors.New("doris_table")
	}
	if v, err := s.visible(ctx, label); err == nil && v {
		return s.verify(ctx, table, rows)
	}
	u, err := url.Parse(strings.TrimRight(s.Cfg.FE, "/") + "/api/metering/" + table + "/_stream_load")
	if err != nil || !s.allowed(u) {
		return errors.New("doris_target")
	}
	r, err := http.NewRequestWithContext(ctx, "PUT", u.String(), bytes.NewReader(body))
	if err != nil {
		return errors.New("doris_request")
	}
	r.SetBasicAuth(os.Getenv(s.Cfg.UsernameEnv), os.Getenv(s.Cfg.PasswordEnv))
	for k, v := range map[string]string{"label": label, "Expect": "100-continue", "format": "json", "read_json_by_line": "true", "max_filter_ratio": "0", "timezone": "+00:00", "strict_mode": "true", "timeout": "10"} {
		r.Header.Set(k, v)
	}
	res, err := s.HTTP.Do(r)
	if err == nil {
		defer res.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(res.Body, 65536))
		var result struct {
			Status                                                                      string
			NumberTotalRows, NumberLoadedRows, NumberFilteredRows, NumberUnselectedRows int
		}
		if res.StatusCode != 200 || json.Unmarshal(b, &result) != nil {
			result.Status = "uncertain"
		}
		if result.Status == "Success" || result.Status == "Publish Timeout" {
			if result.NumberTotalRows != len(rows) || result.NumberLoadedRows != len(rows) || result.NumberFilteredRows != 0 || result.NumberUnselectedRows != 0 {
				return errors.New("doris_row_count")
			}
		} else if result.Status != "Label Already Exists" && result.Status != "uncertain" {
			return errors.New("doris_load")
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
			return s.verify(ctx, table, rows)
		}
		select {
		case <-ctx.Done():
			return errors.New("transaction_unknown")
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
		// Derive the comparison key from typed columns for both old and new rows.
		// Stored legacy identity_hash includes route_name; comparing hash versions
		// directly would turn a harmless diagnostics change into a tenant conflict.
		r, err := s.DB.QueryContext(ctx, "SELECT CAST(request_date AS STRING),logical_request_id,payload_hash,"+IdentityKeySQL+",request_started_at_ms,event_id FROM metering.events_raw WHERE logical_request_id IN ("+strings.Join(qs, ",")+")")
		if err != nil {
			return nil, errors.New("doris_evidence")
		}
		for r.Next() {
			var e consumer.Evidence
			if r.Scan(&e.Date, &e.ID, &e.Hash, &e.Identity, &e.Started, &e.EventID) != nil {
				r.Close()
				return nil, errors.New("doris_evidence")
			}
			out = append(out, e)
			if len(out) > 100000 {
				r.Close()
				return nil, errors.New("evidence_limit")
			}
		}
		err = r.Err()
		r.Close()
		if err != nil {
			return nil, errors.New("doris_evidence")
		}
	}
	return out, nil
}

// IdentityKeySQL keeps the field order aligned with events.IdentityHash and migration 006.
// This JSON tuple is used only for equality within Doris, not as a payload hash.
const IdentityKeySQL = "CAST(JSON_ARRAY(service_id, api_key_id, customer_id, application_id, identity_status, config_version, gateway_instance_id, adapter_id, x_request_id) AS STRING)"
