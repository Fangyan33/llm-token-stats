// Package wasmdownload serves one immutable build artifact, independently of ingestion.
package wasmdownload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/health"
)

const Path = "/wasm/gateway-observer-v1.wasm"
const maxDownloads = 8

type handler struct {
	file     *os.File
	size     int64
	modified time.Time
	etag     string
	slots    chan struct{}
	health   *health.State
}

func open(path string, h *health.State) (*handler, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("wasm_file_unreadable")
	}
	valid := false
	defer func() {
		if !valid {
			f.Close()
		}
	}()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("wasm_file_invalid")
	}
	var header [8]byte
	if _, err := f.ReadAt(header[:], 0); err != nil || !bytes.Equal(header[:], []byte{0, 'a', 's', 'm', 1, 0, 0, 0}) {
		return nil, fmt.Errorf("wasm_file_invalid")
	}
	digest := sha256.New()
	if _, err := io.Copy(digest, io.NewSectionReader(f, 0, info.Size())); err != nil {
		return nil, fmt.Errorf("wasm_file_unreadable")
	}
	valid = true
	return &handler{file: f, size: info.Size(), modified: info.ModTime(), etag: fmt.Sprintf(`"%x"`, digest.Sum(nil)), slots: make(chan struct{}, maxDownloads), health: h}, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != Path {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	select {
	case h.slots <- struct{}{}:
		defer func() { <-h.slots }()
	default:
		h.health.Count("wasm_download_busy")
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	h.health.Count("wasm_download_requests")
	w.Header().Set("Content-Type", "application/wasm")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("ETag", h.etag)
	// Each request has its own offset. The retained descriptor pins this build artifact.
	http.ServeContent(w, r, "gateway-observer-v1.wasm", h.modified, io.NewSectionReader(h.file, 0, h.size))
}

type Server struct {
	http     *http.Server
	listener net.Listener
	handler  *handler
}

// New validates the artifact and binds synchronously, before Observer becomes ready.
func New(c config.WasmDownload, h *health.State) (*Server, error) {
	handler, err := open(c.File, h)
	if err != nil {
		return nil, err
	}
	l, err := net.Listen("tcp", c.Listen)
	if err != nil {
		handler.file.Close()
		return nil, fmt.Errorf("wasm_http_listen_failure")
	}
	return &Server{
		http:     &http.Server{Handler: handler, ReadHeaderTimeout: 3 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192},
		listener: l,
		handler:  handler,
	}, nil
}

func (s *Server) Addr() net.Addr { return s.listener.Addr() }
func (s *Server) Serve() error   { return s.http.Serve(s.listener) }
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.http.Shutdown(ctx)
	if err != nil {
		s.http.Close()
	}
	s.handler.file.Close()
	return err
}
