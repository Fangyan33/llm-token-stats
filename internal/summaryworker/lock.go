package summaryworker

import (
	"context"
	"github.com/Fangyan33/llm-token-stats/internal/logging"
	"os"
	"path/filepath"
	"syscall"
)

type localGuard struct{ f *os.File }

func LocalGuard(path string) (Guard, error) {
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return nil, logging.WithFields("summary_lock_file", e, "config_field", "summary_worker.lock_file", "phase", "create_lock_directory")
	}
	f, e := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, logging.WithFields("summary_lock_file", e, "config_field", "summary_worker.lock_file", "phase", "open_lock_file")
	}
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		f.Close()
		return nil, logging.WithFields("summary_already_running", e, "config_field", "summary_worker.lock_file", "phase", "acquire_file_lock")
	}
	return &localGuard{f}, nil
}
func (g *localGuard) Check(ctx context.Context) error { return ctx.Err() }
func (g *localGuard) Close() error                    { return g.f.Close() }
