package main

import (
	"context"
	"errors"
	"flag"
	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/health"
	"github.com/Fangyan33/llm-token-stats/internal/observation"
	"github.com/Fangyan33/llm-token-stats/internal/producer"
	"github.com/Fangyan33/llm-token-stats/internal/wasmdownload"
	"google.golang.org/grpc"
	"log"
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
		}
	}
}
func main() {
	path := flag.String("config", "configs/observer.local.json", "配置文件")
	flag.Parse()
	c, err := config.Read(*path)
	if err != nil {
		log.Fatal(err)
	}
	if err = c.ObserverDefaults(); err != nil {
		log.Fatal(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	h := &health.State{}
	health.Start(ctx, c.Observer.Health, h)
	var downloads *wasmdownload.Server
	if c.Observer.WasmDownload != nil {
		downloads, err = wasmdownload.New(*c.Observer.WasmDownload, h)
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			if err := downloads.Serve(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Print("wasm_http_serve_failure")
				cancel()
			}
		}()
	}
	p, err := producer.New(ctx, c.Kafka, h, func() { os.Exit(2) })
	if err != nil {
		log.Fatal(err)
	}
	l, err := net.Listen("tcp", c.Observer.Listen)
	if err != nil {
		log.Fatal("listen_failure")
	}
	server := grpc.NewServer(grpc.ForceServerCodec(observation.Codec{}), grpc.MaxRecvMsgSize(32768), grpc.MaxConcurrentStreams(128), grpc.InitialWindowSize(65535), grpc.InitialConnWindowSize(65535))
	pb.RegisterObservationServiceServer(server, &observation.Server{Cfg: c.Observer, Sink: p, Health: h})
	h.Ready.Store(true)
	go func() {
		<-ctx.Done()
		h.Ready.Store(false)
		if downloads != nil {
			shutdown, stop := context.WithTimeout(context.Background(), time.Second)
			downloads.Shutdown(shutdown)
			stop()
		}
		done := make(chan struct{})
		go func() { server.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			server.Stop()
		}
	}()
	if err = server.Serve(&limited{l, make(chan struct{}, c.Observer.MaxConnections)}); err != nil && ctx.Err() == nil {
		log.Fatal("serve_failure")
	}
}
