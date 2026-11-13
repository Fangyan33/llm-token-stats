package main

import (
	"context"
	"flag"
	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/doris"
	"github.com/Fangyan33/llm-token-stats/internal/health"
	"github.com/Fangyan33/llm-token-stats/internal/ingestworker"
	"github.com/Fangyan33/llm-token-stats/internal/logging"
	"github.com/Fangyan33/llm-token-stats/internal/producer"
	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
	"os"
	"os/signal"
	"syscall"
)

func run() error {
	p := flag.String("config", "configs/consumer.local.json", "配置文件")
	flag.Parse()
	c, err := config.ReadConsumer(*p)
	if err != nil {
		return logging.WithCause("configuration_failed", err)
	}
	s, dorisErr := doris.New(c.Doris)
	if s != nil {
		defer s.DB.Close()
	}
	_, kafkaErr := producer.Options(c.Kafka)
	initialization := &configdiag.Report{Source: *p}
	initialization.Merge(dorisErr)
	initialization.Merge(kafkaErr)
	if err := initialization.Err(); err != nil {
		return err
	}
	logging.Info("configuration_loaded", "health_listen", c.Consumer.Health, "kafka_topic", c.Kafka.Topic, "group", c.Consumer.Group,
		"max_batch_rows", c.Consumer.Rows, "max_batch_bytes", c.Consumer.Bytes, "flush_interval_ms", c.Consumer.FlushMS)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	h := &health.State{}
	health.Start(ctx, c.Consumer.Health, h)
	w, err := ingestworker.New(c, s, h)
	if err != nil {
		return logging.WithCause("consumer_initialization_failed", err)
	}
	if err = w.Run(ctx); err != nil && ctx.Err() == nil {
		return logging.WithCause("consumer_failure", err)
	}
	return nil
}
func main() {
	logging.Init("consumer")
	if err := run(); err != nil {
		logging.Error("consumer_failed", err)
		logging.Shutdown()
		os.Exit(1)
	}
	logging.Info("stopped")
	logging.Shutdown()
}
