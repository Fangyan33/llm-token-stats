package health

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/logging"
)

type State struct {
	Ready      atomic.Bool
	mu         sync.Mutex
	counts     map[string]int64
	gauges     map[string]int64
	readGauges map[string]func() int64
}

func (s *State) GaugeFunc(name string, read func() int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.readGauges == nil {
		s.readGauges = map[string]func() int64{}
	}
	s.readGauges[name] = read
}

// Names are fixed by callers; request identifiers never become metric labels.
func (s *State) Gauge(name string, value int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.gauges == nil {
		s.gauges = map[string]int64{}
	}
	s.gauges[name] = value
}

func (s *State) Count(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.counts == nil {
		s.counts = map[string]int64{}
	}
	s.counts[name]++
}
func (s *State) Handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	m.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !s.Ready.Load() {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	})
	m.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		for k, v := range s.counts {
			fmt.Fprintf(w, "metering_%s_total %d\n", k, v)
		}
		for k, v := range s.gauges {
			fmt.Fprintf(w, "metering_%s %d\n", k, v)
		}
		for k, read := range s.readGauges {
			fmt.Fprintf(w, "metering_%s %d\n", k, read())
		}
	})
	return m
}
func Start(ctx context.Context, addr string, s *State) *http.Server {
	s.GaugeFunc("logs_dropped_total", logging.Default().Dropped)
	srv := &http.Server{Addr: addr, Handler: s.Handler(), ReadHeaderTimeout: 3 * time.Second}
	go func() {
		logging.Info("health_listener_starting", "listen", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logging.Error("health_listener_failed", err, "listen", addr)
		}
	}()
	go func() {
		<-ctx.Done()
		c, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := srv.Shutdown(c); err != nil {
			logging.Error("health_shutdown_failed", err)
		}
	}()
	return srv
}
