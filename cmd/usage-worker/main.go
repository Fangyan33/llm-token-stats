package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/summaryworker"
)

func run() error {
	path := flag.String("config", "configs/usage-worker.local.json", "配置文件")
	once := flag.Bool("once", false, "执行一轮后退出")
	from := flag.String("backfill-from", "", "回填起始 UTC 日期，包含")
	to := flag.String("backfill-to", "", "回填结束 UTC 日期，不包含")
	flag.Parse()
	if !*once {
		return fmt.Errorf("once_required")
	}
	c, e := summaryworker.ReadConfig(*path)
	if e != nil {
		return e
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	ctx, deadline := context.WithTimeout(ctx, time.Duration(c.Worker.MaxRunSeconds)*time.Second)
	defer deadline()
	store, e := summaryworker.Open(c.Doris)
	if e != nil {
		return e
	}
	defer store.DB.Close()
	var guard summaryworker.Guard
	if c.Worker.LockMode == "kubernetes" {
		guard, e = summaryworker.KubernetesGuard(ctx, c.Worker.LeaseName, store)
	} else {
		guard, e = summaryworker.LocalGuard(c.Worker.LockFile)
	}
	if e != nil {
		return e
	}
	defer guard.Close()
	worker := summaryworker.Worker{Store: store, Config: c.Worker, Guard: guard}
	if e = worker.Run(ctx, *from, *to); e != nil {
		return e
	}
	return nil
}
func main() {
	if e := run(); e != nil {
		log.Printf("summary_failed: %s", e)
		os.Exit(1)
	}
	log.Print("summary_succeeded")
}
