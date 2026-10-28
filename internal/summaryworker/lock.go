package summaryworker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

type localGuard struct{ f *os.File }

func LocalGuard(path string) (Guard, error) {
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return nil, e
	}
	f, e := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		f.Close()
		return nil, fmt.Errorf("summary_already_running")
	}
	return &localGuard{f}, nil
}
func (g *localGuard) Check(ctx context.Context) error { return ctx.Err() }
func (g *localGuard) Close() error                    { return g.f.Close() }
