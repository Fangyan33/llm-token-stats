package doris

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/logging"
	"github.com/go-sql-driver/mysql"
)

// An absent transaction lets Load reach the HTTP path without a live database.
type absentTransaction struct{}

func (absentTransaction) Connect(context.Context) (driver.Conn, error) {
	return absentTransaction{}, nil
}
func (absentTransaction) Driver() driver.Driver { return nil }
func (absentTransaction) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected_prepare")
}
func (absentTransaction) Close() error              { return nil }
func (absentTransaction) Begin() (driver.Tx, error) { return nil, errors.New("unexpected_begin") }
func (absentTransaction) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if !strings.HasPrefix(query, "SHOW TRANSACTION FROM ") {
		return nil, errors.New("unexpected_query")
	}
	return absentTransaction{}, nil
}
func (absentTransaction) Columns() []string         { return []string{"TransactionStatus"} }
func (absentTransaction) Next([]driver.Value) error { return io.EOF }

type failedTransaction struct {
	absentTransaction
	err error
}

func (f failedTransaction) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f failedTransaction) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return nil, f.err
}

func TestTransactionAbsenceDoesNotLogAnOutage(t *testing.T) {
	for _, tc := range []struct {
		name, message string
		code          uint16
		absent        bool
	}{
		{"missing", "errCode = 2, detailMessage = transaction with label synthetic_label does not exist", 1105, true},
		{"missing_without_prefix", "transaction with label synthetic_label does not exist", 1105, true},
		{"other_label", "transaction with label another_label does not exist", 1105, false},
		{"real_sql_failure", "unavailable backend", 1105, false},
		{"auth_failure", "synthetic-secret-password", 1045, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			l := logging.New("consumer", &out, slog.LevelInfo)
			defer l.Close()
			restore := logging.SetDefault(l)
			defer restore()
			db := sql.OpenDB(failedTransaction{err: &mysql.MySQLError{Number: tc.code, Message: tc.message}})
			defer db.Close()
			s := &Store{DB: db, Cfg: config.Doris{Database: "metering"}}
			visible, err := s.visible(t.Context(), "synthetic_label")
			if visible || (err == nil) != tc.absent {
				t.Fatal("incorrect transaction classification", visible, err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if !l.Flush(ctx) {
				t.Fatal("flush timeout")
			}
			if strings.Contains(out.String(), "event=doris_sql_failed") == tc.absent || strings.Contains(out.String(), "synthetic-secret-password") {
				t.Fatal("normal absence was logged or SQL outage was hidden", out.String())
			}
		})
	}
}

func testStore(t *testing.T, target string, hosts []string, password string) *Store {
	t.Helper()
	t.Setenv("DORIS_USER", "obsolete-user")
	t.Setenv("DORIS_PASSWORD", "obsolete-password")
	t.Setenv("TEST_DORIS_DSN", "synthetic-user:"+password+"@tcp(127.0.0.1:9030)/metering")
	s, err := New(config.Doris{Database: "metering", DSNEnv: "TEST_DORIS_DSN", FE: target, AllowedHosts: hosts})
	if err != nil {
		t.Fatal(err)
	}
	s.DB.Close()
	s.DB = sql.OpenDB(absentTransaction{})
	t.Cleanup(func() { s.DB.Close(); s.HTTP.CloseIdleConnections() })
	// SQL and HTTP must retain the initialization-time snapshot together.
	t.Setenv("TEST_DORIS_DSN", "changed:changed@tcp(127.0.0.1:9030)/metering")
	return s
}

func host(raw string) string { u, _ := url.Parse(raw); return u.Host }

func TestLoadUsesDSNCredentials(t *testing.T) {
	for _, password := range []string{"synthetic-password", "", "special:@/?#%+中文"} {
		t.Run(password, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				user, pass, ok := r.BasicAuth()
				if !ok || user != "synthetic-user" || pass != password {
					t.Error("HTTP credentials differ from DSN")
				}
				io.Copy(io.Discard, r.Body)
				io.WriteString(w, `{"Status":"Fail"}`)
			}))
			defer server.Close()
			s := testStore(t, server.URL, []string{host(server.URL)}, password)
			if err := s.Load(t.Context(), "consumer_partition_origins", "test", []byte("{}\n"), nil); err == nil || err.Error() != "doris_load" {
				t.Fatalf("expected deliberate HTTP rejection, got %v", err)
			}
			if calls.Load() != 1 {
				t.Fatal("Stream Load was not sent exactly once")
			}
		})
	}
}

func TestLoadRedirectCredentialsAndAllowlist(t *testing.T) {
	for _, allowed := range []bool{true, false} {
		t.Run(map[bool]string{true: "allowed", false: "denied"}[allowed], func(t *testing.T) {
			var redirected atomic.Int32
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				redirected.Add(1)
				user, pass, ok := r.BasicAuth()
				if !ok || user != "synthetic-user" || pass != "redirect-password" {
					t.Error("redirect lost DSN credentials")
				}
				if r.Method != http.MethodPut {
					t.Error("redirect changed method")
				}
				io.Copy(io.Discard, r.Body)
				io.WriteString(w, `{"Status":"Fail"}`)
			}))
			defer backend.Close()
			// A different host forces net/http to strip the original Authorization.
			destination := strings.Replace(backend.URL, "127.0.0.1", "localhost", 1)
			frontend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				user, pass, ok := r.BasicAuth()
				if !ok || user != "synthetic-user" || pass != "redirect-password" {
					t.Error("initial request lost DSN credentials")
				}
				io.Copy(io.Discard, r.Body)
				http.Redirect(w, r, destination+r.URL.Path, http.StatusTemporaryRedirect)
			}))
			defer frontend.Close()
			hosts := []string{host(frontend.URL)}
			if allowed {
				hosts = append(hosts, host(destination))
			}
			s := testStore(t, frontend.URL, hosts, "redirect-password")
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			err := s.Load(ctx, "consumer_partition_origins", "test", []byte("{}\n"), nil)
			if err == nil {
				t.Fatal("deliberately unsuccessful load succeeded")
			}
			want := int32(0)
			if allowed {
				want = 1
			}
			if redirected.Load() != want {
				t.Fatal("redirect allowlist was not respected")
			}
		})
	}
}

func TestRedirectRejectsUnsafeTargets(t *testing.T) {
	s := testStore(t, "https://fe.test:8030", []string{"fe.test:8030", "be.test:8040"}, "password")
	initial, _ := http.NewRequest(http.MethodPut, "https://fe.test:8030/api/load", nil)
	for _, target := range []string{"http://be.test:8040/api/load", "https://other.test:8040/api/load", "https://user:password@be.test:8040/api/load"} {
		r, _ := http.NewRequest(http.MethodPut, target, nil)
		if err := s.HTTP.CheckRedirect(r, []*http.Request{initial}); err == nil {
			t.Fatal("unsafe redirect accepted")
		}
	}
	r, _ := http.NewRequest(http.MethodGet, "https://be.test:8040/api/load", nil)
	if err := s.HTTP.CheckRedirect(r, []*http.Request{initial}); err == nil {
		t.Fatal("non-PUT redirect accepted")
	}
	r.Method = http.MethodPut
	if err := s.HTTP.CheckRedirect(r, []*http.Request{initial, initial, initial, initial}); err == nil {
		t.Fatal("redirect limit ignored")
	}
}

func TestNewRejectsInvalidDSNWithoutCredentialsInErrors(t *testing.T) {
	for _, dsn := range []string{"", "private-user:private-password@tcp(bad/metering", "private-user:private-password@tcp(localhost:9030)/other"} {
		t.Setenv("TEST_DORIS_DSN", dsn)
		_, err := New(config.Doris{Database: "metering", DSNEnv: "TEST_DORIS_DSN"})
		if err == nil || err.Error() != "doris_dsn" {
			t.Fatalf("expected fixed DSN error, got %v", err)
		}
	}
}
