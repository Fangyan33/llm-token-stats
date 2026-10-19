package identitylookup

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/config"
)

func TestLookupDialTimeoutIncludesDNS(t *testing.T) {
	dialer := newLookupDialer()
	// Stall DNS without opening sockets. The production dialer's timeout must
	// terminate name resolution before the parent's longer safety deadline.
	var calls atomic.Int32
	dialer.Resolver = &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		calls.Add(1)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*lookupDialTimeout)
	defer cancel()
	started := time.Now()
	conn, err := dialer.DialContext(ctx, "tcp", "lookup-timeout.invalid:443")
	if conn != nil {
		conn.Close()
		t.Fatal("stalled DNS unexpectedly connected")
	}
	var timeout net.Error
	if !errors.As(err, &timeout) || !timeout.Timeout() || calls.Load() == 0 {
		t.Fatalf("dial timeout: calls=%d, err=%v", calls.Load(), err)
	}
	if elapsed := time.Since(started); elapsed > lookupDialTimeout+time.Second || ctx.Err() != nil {
		t.Fatalf("dial relied on parent deadline: elapsed=%v, parent=%v", elapsed, ctx.Err())
	}
}

func TestTLSHandshakeTimeoutReleasesConnectionCapacity(t *testing.T) {
	t.Setenv("CONNECTION_TIMEOUT_CREDENTIAL", "synthetic")
	for _, mode := range []string{"request_deadline", "client_timeout", "handshake_timeout"} {
		t.Run(mode, func(t *testing.T) {
			r, err := newRemote(config.Lookup{URI: "https://lookup-timeout.invalid/", CredentialEnv: "CONNECTION_TIMEOUT_CREDENTIAL"})
			if err != nil {
				t.Fatal(err)
			}
			transport := r.client.Transport.(*http.Transport)
			defer transport.CloseIdleConnections()
			if transport.DialContext == nil {
				t.Fatal("production transport has no bounded dialer")
			}
			// Preserve the production handshake timeout and force exhaustion after
			// one connection. The peer reads ClientHello but never answers it.
			transport.MaxConnsPerHost = 1
			clientConn, peer := net.Pipe()
			defer clientConn.Close()
			defer peer.Close()
			closed := make(chan struct{})
			go func() {
				io.Copy(io.Discard, peer)
				close(closed)
			}()
			var calls atomic.Int32
			transport.DialContext = func(context.Context, string, string) (net.Conn, error) {
				if calls.Add(1) == 1 {
					return clientConn, nil
				}
				return nil, errors.New("subsequent dial reached")
			}
			ctx := context.Background()
			r.client.Timeout = 2 * lookupTLSHandshakeTimeout
			if mode == "request_deadline" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
			} else if mode == "client_timeout" {
				r.client.Timeout = 100 * time.Millisecond
			}
			started := time.Now()
			resolver := serviceRemote{r}
			if _, err := resolver.Resolve(ctx, ServiceLookupRequest{}); err == nil || Failure(err) != "lookup_timeout" {
				t.Fatalf("stalled handshake classification: %v", err)
			}
			if mode != "handshake_timeout" && time.Since(started) >= lookupTLSHandshakeTimeout {
				t.Fatal("request waited for handshake instead of its own deadline")
			}
			// The request deadline has expired, but the connection must still be
			// closed by the independent transport timer and free its host slot.
			r.client.Timeout = 2 * lookupTLSHandshakeTimeout
			if _, err := resolver.Resolve(context.Background(), ServiceLookupRequest{}); err == nil || Failure(err) != "lookup_unavailable" || calls.Load() != 2 {
				t.Fatalf("host capacity not released: calls=%d, err=%v", calls.Load(), err)
			}
			if elapsed := time.Since(started); elapsed > lookupTLSHandshakeTimeout+time.Second {
				t.Fatalf("handshake cleanup exceeded bound: %v", elapsed)
			}
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("stalled TLS connection was not closed")
			}
		})
	}
}
