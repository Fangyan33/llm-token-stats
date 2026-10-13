package main

import (
	"context"
	"flag"
	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/doris"
	"github.com/Fangyan33/llm-token-stats/internal/health"
	"github.com/Fangyan33/llm-token-stats/internal/ingestworker"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	p := flag.String("config", "configs/consumer.local.json", "配置文件")
	flag.Parse()
	c, err := config.Read(*p)
	if err != nil {
		log.Fatal(err)
	}
	if err = c.ConsumerDefaults(); err != nil {
		log.Fatal(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	h := &health.State{}
	health.Start(ctx, c.Consumer.Health, h)
	s, err := doris.New(c.Doris)
	if err != nil {
		log.Fatal(err)
	}
	defer s.DB.Close()
	w, err := ingestworker.New(c, s, h)
	if err != nil {
		log.Fatal(err)
	}
	if err = w.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatal("consumer_failure")
	}
}
