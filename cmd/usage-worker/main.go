package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/logging"
	"github.com/Fangyan33/llm-token-stats/internal/summaryworker"
	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
)

func run() error {
	path := flag.String("config", "configs/usage-worker.local.json", "配置文件")
	once := flag.Bool("once", false, "执行一轮后退出")
	from := flag.String("backfill-from", "", "回填起始 UTC 日期，包含")
	to := flag.String("backfill-to", "", "回填结束 UTC 日期，不包含")
	flag.Parse()
	if !*once {
		return configdiag.Source(configdiag.New("once_required", "--once", "required", "--once=true", nil), "command_line")
	}
	c, e := summaryworker.ReadConfig(*path)
	if e != nil {
		return logging.WithCause("configuration_failed", e)
	}
	logging.Info("configuration_loaded", "lock_mode", c.Worker.LockMode, "collection_start_date", c.Worker.CollectionStartDate,
		"backfill_from", *from, "backfill_to", *to, "max_run_seconds", c.Worker.MaxRunSeconds)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	ctx, deadline := context.WithTimeout(ctx, time.Duration(c.Worker.MaxRunSeconds)*time.Second)
	defer deadline()
	store, e := summaryworker.Open(c.Doris)
	if e != nil {
		return e
	}
	defer store.DB.Close()
	logging.Info("doris_client_configured", "database", c.Doris.Database)
	var guard summaryworker.Guard
	if c.Worker.LockMode == "kubernetes" {
		guard, e = summaryworker.KubernetesGuard(ctx, c.Worker.LeaseName, store)
	} else {
		guard, e = summaryworker.LocalGuard(c.Worker.LockFile)
	}
	if e != nil {
		return logging.WithCause("summary_lock_failed", e)
	}
	logging.Info("summary_lock_acquired", "lock_mode", c.Worker.LockMode)
	defer func() {
		if err := guard.Close(); err != nil {
			logging.Error("summary_lock_release_failed", err)
		} else {
			logging.Info("summary_lock_released")
		}
	}()
	worker := summaryworker.Worker{Store: store, Config: c.Worker, Guard: guard}
	if e = worker.Run(ctx, *from, *to); e != nil {
		return e
	}
	return nil
}
func main() {
	logging.Init("usage-worker")
	if e := run(); e != nil {
		if errors.Is(e, context.Canceled) {
			logging.Info("summary_canceled")
		} else {
			logging.Error("summary_failed", e)
		}
		logging.Shutdown()
		os.Exit(1)
	}
	logging.Info("summary_succeeded")
	logging.Shutdown()
}
