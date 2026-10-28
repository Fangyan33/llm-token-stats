package summaryworker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/go-sql-driver/mysql"
)

var identifier = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,62}$`)
var bounds = regexp.MustCompile(`keys: \[(\d{4}-\d{2}-\d{2})\]`)

type Store struct {
	DB       *sql.DB
	Database string
}

func Open(c config.Doris) (*Store, error) {
	if !config.DatabaseName(c.Database) {
		return nil, fmt.Errorf("invalid_database")
	}
	d, err := mysql.ParseDSN(os.Getenv(c.DSNEnv))
	if err != nil || d.DBName != c.Database {
		return nil, fmt.Errorf("summary_dsn")
	}
	if d.Params == nil {
		d.Params = map[string]string{}
	}
	d.Params["time_zone"] = "'+00:00'"
	d.Loc = time.UTC
	d.InterpolateParams = true
	d.ReadTimeout = 30 * time.Second
	d.WriteTimeout = 30 * time.Second
	d.Timeout = 5 * time.Second
	db, err := sql.Open("mysql", d.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("summary_connect")
	}
	db.SetMaxOpenConns(2)
	return &Store{db, c.Database}, nil
}
func (s *Store) table(name string) string { return "`" + s.Database + "`.`" + name + "`" }
func (s *Store) rows(ctx context.Context, q string, args ...any) ([]map[string]string, error) {
	rows, err := s.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := []map[string]string{}
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err = rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		m := map[string]string{}
		for i, c := range cols {
			m[c] = vals[i].String
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

type Partition struct {
	Name       string
	Start, End time.Time
}

func (s *Store) partitions(ctx context.Context, table string) (map[string]Partition, error) {
	rows, err := s.rows(ctx, "SHOW PARTITIONS FROM "+s.table(table))
	if err != nil {
		return nil, err
	}
	out := map[string]Partition{}
	for _, r := range rows {
		bs := bounds.FindAllStringSubmatch(r["Range"], -1)
		if len(bs) != 2 {
			return nil, fmt.Errorf("partition_range")
		}
		a, e := ParseDate(bs[0][1])
		if e != nil {
			return nil, e
		}
		b, e := ParseDate(bs[1][1])
		if e != nil {
			return nil, e
		}
		out[a.Format("2006-01-02")] = Partition{r["PartitionName"], a, b}
	}
	return out, nil
}

type Run struct {
	Kind          string            `json:"kind"`
	Date          string            `json:"date"`
	ID            string            `json:"id"`
	Status        string            `json:"status"`
	View          string            `json:"view"`
	Partition     string            `json:"partition"`
	Baseline      []string          `json:"baseline_task_ids"`
	TaskID        string            `json:"task_id"`
	Submitted     bool              `json:"submitted"`
	InputCount    int               `json:"input_count"`
	InputDigest   string            `json:"input_digest"`
	DailyVersions map[string]string `json:"daily_versions,omitempty"`
	CoverageStart string            `json:"coverage_start"`
	CoverageEnd   string            `json:"coverage_end"`
	StartedAt     string            `json:"started_at"`
	ErrorCode     string            `json:"error_code,omitempty"`
}
type State struct {
	Kind          string            `json:"kind"`
	Date          string            `json:"date"`
	Status        string            `json:"status"`
	ActiveID      string            `json:"active_refresh_id,omitempty"`
	SuccessID     string            `json:"success_refresh_id,omitempty"`
	RefreshedAt   string            `json:"refreshed_at,omitempty"`
	CoverageStart string            `json:"coverage_start,omitempty"`
	CoverageEnd   string            `json:"coverage_end,omitempty"`
	DailyVersions map[string]string `json:"daily_versions,omitempty"`
	ErrorCode     string            `json:"error_code,omitempty"`
}

func (s *Store) saveRun(ctx context.Context, r Run) error {
	b, e := json.Marshal(r)
	if e != nil {
		return e
	}
	_, e = s.DB.ExecContext(ctx, "INSERT INTO "+s.table("summary_mv_refresh_runs")+" VALUES (?,?,?,?,?,?)", r.Kind, r.Date, r.ID, r.Status, string(b), stamp())
	return e
}
func (s *Store) saveState(ctx context.Context, r State) error {
	b, e := json.Marshal(r)
	if e != nil {
		return e
	}
	_, e = s.DB.ExecContext(ctx, "INSERT INTO "+s.table("summary_mv_refresh_state")+" VALUES (?,?,?,?,?)", r.Kind, r.Date, r.Status, string(b), stamp())
	return e
}
func (s *Store) state(ctx context.Context, kind, date string) (State, error) {
	r := State{Kind: kind, Date: date, Status: "PENDING"}
	var b string
	e := s.DB.QueryRowContext(ctx, "SELECT record_json FROM "+s.table("summary_mv_refresh_state")+" WHERE period_type=? AND period_start=?", kind, date).Scan(&b)
	if e == sql.ErrNoRows {
		return r, nil
	}
	if e != nil {
		return r, e
	}
	e = json.Unmarshal([]byte(b), &r)
	return r, e
}
func (s *Store) pending(ctx context.Context) ([]Run, error) {
	rows, e := s.rows(ctx, "SELECT record_json FROM "+s.table("summary_mv_refresh_runs")+" WHERE status IN ('PENDING','RUNNING','UNCERTAIN') ORDER BY period_type,period_start")
	if e != nil {
		return nil, e
	}
	var out []Run
	for _, row := range rows {
		var r Run
		if e = json.Unmarshal([]byte(row["record_json"]), &r); e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, nil
}
func (s *Store) notifications(ctx context.Context, date string) ([]string, error) {
	rows, e := s.rows(ctx, "SELECT n.notification_id FROM "+s.table("summary_dirty_notifications")+" n WHERE n.stat_date=? AND NOT EXISTS (SELECT 1 FROM "+s.table("summary_mv_refresh_inputs")+" i JOIN "+s.table("summary_mv_refresh_runs")+" r ON r.period_type='DAY' AND r.period_start=i.stat_date AND r.refresh_id=i.refresh_id AND r.status='SUCCEEDED' WHERE i.stat_date=n.stat_date AND i.notification_id=n.notification_id) ORDER BY n.notification_id", date)
	if e != nil {
		return nil, e
	}
	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r["notification_id"])
	}
	return ids, nil
}
func (s *Store) inputs(ctx context.Context, r Run, ids []string) error {
	for i := 0; i < len(ids); i += 500 {
		end := i + 500
		if end > len(ids) {
			end = len(ids)
		}
		args := []any{}
		qs := []string{}
		for _, id := range ids[i:end] {
			qs = append(qs, "(?,?,?)")
			args = append(args, r.Date, r.ID, id)
		}
		if _, e := s.DB.ExecContext(ctx, "INSERT INTO "+s.table("summary_mv_refresh_inputs")+" VALUES "+strings.Join(qs, ","), args...); e != nil {
			return e
		}
	}
	return nil
}
func stamp() string { return time.Now().UTC().Format("2006-01-02 15:04:05.000") }

func (s *Store) dynamic(ctx context.Context, table string, enabled bool) error {
	_, e := s.DB.ExecContext(ctx, "ALTER TABLE "+s.table(table)+" SET (\"dynamic_partition.enable\"=\""+fmt.Sprint(enabled)+"\")")
	return e
}
