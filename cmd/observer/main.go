package main

import (
	"context"
	"errors"
	"flag"
	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/health"
	"github.com/Fangyan33/llm-token-stats/internal/identitylookup"
	"github.com/Fangyan33/llm-token-stats/internal/logging"
	"github.com/Fangyan33/llm-token-stats/internal/observation"
	"github.com/Fangyan33/llm-token-stats/internal/producer"
	"github.com/Fangyan33/llm-token-stats/internal/wasmdownload"
	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
	"google.golang.org/grpc"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type limited struct {
	net.Listener
	slots chan struct{}
}
type conn struct {
	net.Conn
	slots chan struct{}
	once  sync.Once
}

func (c *conn) Close() error { err := c.Conn.Close(); c.once.Do(func() { <-c.slots }); return err }
func (l *limited) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		select {
		case l.slots <- struct{}{}:
			return &conn{Conn: c, slots: l.slots}, nil
		default:
			c.Close()
			logging.Warn("connection_rejected", "grpc", "reason", "connection_limit")
		}
	}
}
func run() error {
	path := flag.String("config", "configs/observer.local.json", "配置文件")
	flag.Parse()
	c, err := config.ReadObserver(*path)
	if err != nil {
		return logging.WithCause("configuration_failed", err)
	}
	service, serviceErr := identitylookup.NewService(c.Observer.ServiceLookup)
	customer, customerErr := identitylookup.NewCustomer(c.Observer.CustomerLookup)
	_, kafkaErr := producer.Options(c.Kafka)
	initialization := &configdiag.Report{Source: *path}
	initialization.Merge(serviceErr)
	initialization.Merge(customerErr)
	initialization.Merge(kafkaErr)
	if err := initialization.Err(); err != nil {
		return err
	}
	logging.Info("configuration_loaded", "listen", c.Observer.Listen, "health_listen", c.Observer.Health, "kafka_topic", c.Kafka.Topic,
		"service_lookup_enabled", c.Observer.ServiceLookup.URI != "", "customer_lookup_enabled", c.Observer.CustomerLookup.URI != "")
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	h := &health.State{}
	h.GaugeFunc("lookup_logs_dropped_total", identitylookup.DroppedResponseLogs)
	health.Start(ctx, c.Observer.Health, h)
	var downloads *wasmdownload.Server
	if c.Observer.WasmDownload != nil {
		downloads, err = wasmdownload.New(*c.Observer.WasmDownload, h)
		if err != nil {
			return configdiag.Source(err, *path)
		}
		go func() {
			if err := downloads.Serve(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logging.Error("wasm_http_serve_failure", err)
				cancel()
			}
		}()
		logging.Info("wasm_download_listening", "listen", downloads.Addr().String())
	}
	p, err := producer.New(ctx, c.Kafka, h, func() { logging.Error("producer_shutdown_timeout", nil); logging.Shutdown(); os.Exit(2) })
	if err != nil {
		return logging.WithCause("producer_initialization_failed", err)
	}
	defer p.Close()
	l, err := net.Listen("tcp", c.Observer.Listen)
	if err != nil {
		return logging.WithFields("listen_failure", err, "config_field", "observer.listen", "phase", "bind_listener")
	}
	server := grpc.NewServer(grpc.ForceServerCodec(observation.Codec{}), grpc.MaxRecvMsgSize(32768), grpc.MaxConcurrentStreams(128), grpc.InitialWindowSize(65535), grpc.InitialConnWindowSize(65535))
	observer := &observation.Server{Cfg: c.Observer, Sink: p, Health: h, ServiceResolver: service, CustomerResolver: customer}
	defer observer.CloseRequestTokens()
	pb.RegisterObservationServiceServer(server, observer)
	h.Ready.Store(true)
	logging.Info("grpc_listening", "listen", l.Addr().String(), "max_connections", c.Observer.MaxConnections)
	go func() {
		<-ctx.Done()
		logging.Info("stopping")
		h.Ready.Store(false)
		if downloads != nil {
			shutdown, stop := context.WithTimeout(context.Background(), time.Second)
			if err := downloads.Shutdown(shutdown); err != nil {
				logging.Error("wasm_http_shutdown_failed", err)
			}
			stop()
		}
		done := make(chan struct{})
		go func() { server.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			logging.Warn("grpc_shutdown_forced", "grpc_shutdown", "timeout_ms", 1000)
			server.Stop()
		}
	}()
	if err = server.Serve(&limited{l, make(chan struct{}, c.Observer.MaxConnections)}); err != nil && ctx.Err() == nil {
		return logging.WithCause("serve_failure", err)
	}
	return nil
}
func main() {
	logging.Init("observer")
	if err := run(); err != nil {
		logging.Error("observer_failed", err)
		logging.Shutdown()
		os.Exit(1)
	}
	logging.Info("stopped")
	logging.Shutdown()
}
