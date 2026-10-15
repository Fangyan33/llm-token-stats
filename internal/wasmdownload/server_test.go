package wasmdownload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/health"
)

var artifact = append([]byte{0, 'a', 's', 'm', 1, 0, 0, 0}, []byte("synthetic artifact")...)

func artifactPath(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "plugin.wasm")
	if err := os.WriteFile(path, artifact, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDownload(t *testing.T) {
	h, err := open(artifactPath(t), &health.State{})
	if err != nil {
		t.Fatal(err)
	}
	defer h.file.Close()
	for _, method := range []string{"GET", "HEAD"} {
		t.Run(method, func(t *testing.T) {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(method, Path, nil))
			if w.Code != 200 || w.Header().Get("Content-Type") != "application/wasm" || w.Header().Get("Content-Length") != fmt.Sprint(len(artifact)) || w.Header().Get("ETag") != fmt.Sprintf(`"%x"`, sha256.Sum256(artifact)) {
				t.Fatalf("status/headers: %d %v", w.Code, w.Header())
			}
			if method == "GET" && !bytes.Equal(w.Body.Bytes(), artifact) || method == "HEAD" && w.Body.Len() != 0 {
				t.Fatal("unexpected artifact body")
			}
		})
	}
	for _, tc := range []struct {
		method, path string
		status       int
	}{{"POST", Path, 405}, {"PUT", Path, 405}, {"GET", "/wasm/", 404}, {"GET", "/wasm/../../etc/passwd", 404}, {"GET", "/config/observer.json", 404}} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
		if w.Code != tc.status {
			t.Fatalf("%s %s: %d", tc.method, tc.path, w.Code)
		}
	}
	// Concurrent requests must not share a file offset or truncate each other.
	var wg sync.WaitGroup
	for range maxDownloads {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("GET", Path, nil))
			if w.Code != 200 || !bytes.Equal(w.Body.Bytes(), artifact) {
				t.Error("concurrent download mismatch")
			}
		}()
	}
	wg.Wait()
}

type blockedWriter struct {
	http.ResponseWriter
	entered chan<- struct{}
	release <-chan struct{}
}

func (w blockedWriter) Write(b []byte) (int, error) {
	w.entered <- struct{}{}
	<-w.release
	return w.ResponseWriter.Write(b)
}

func TestDownloadConcurrencyLimit(t *testing.T) {
	h, err := open(artifactPath(t), &health.State{})
	if err != nil {
		t.Fatal(err)
	}
	defer h.file.Close()
	entered, release := make(chan struct{}, maxDownloads), make(chan struct{})
	var wg sync.WaitGroup
	for range maxDownloads {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.ServeHTTP(blockedWriter{httptest.NewRecorder(), entered, release}, httptest.NewRequest("GET", Path, nil))
		}()
	}
	defer func() { close(release); wg.Wait() }()
	for range maxDownloads {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("download did not enter writer")
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", Path, nil))
	if w.Code != 503 {
		t.Fatalf("overloaded download status=%d", w.Code)
	}
}

func TestStartupValidation(t *testing.T) {
	for _, content := range [][]byte{nil, []byte("not wasm"), {0, 'a', 's', 'm', 2, 0, 0, 0}} {
		path := filepath.Join(t.TempDir(), "invalid.wasm")
		os.WriteFile(path, content, 0600)
		if h, err := open(path, &health.State{}); err == nil {
			h.file.Close()
			t.Fatal("invalid Wasm accepted")
		}
	}
	for _, path := range []string{t.TempDir(), filepath.Join(t.TempDir(), "missing.wasm")} {
		if _, err := open(path, &health.State{}); err == nil {
			t.Fatal("missing file or directory accepted")
		}
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if _, err := New(config.WasmDownload{File: artifactPath(t), Listen: l.Addr().String()}, &health.State{}); err == nil {
		t.Fatal("occupied port accepted")
	}
}

func TestServerLifecycle(t *testing.T) {
	s, err := New(config.WasmDownload{File: artifactPath(t), Listen: "127.0.0.1:0"}, &health.State{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve() }()
	t.Cleanup(func() { s.Shutdown(context.Background()) })
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: time.Second}
	res, err := client.Get("http://" + s.Addr().String() + Path)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(res.Body)
	res.Body.Close()
	client.CloseIdleConnections()
	if err != nil || !bytes.Equal(b, artifact) {
		t.Fatal("served body mismatch")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("Serve() after shutdown: %v", err)
	}
}
