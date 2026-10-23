package identitylookup

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAuthenticationHeadersNotifyBeforeDiagnosticBody(t *testing.T) {
	for _, kind := range []string{"service", "customer"} {
		for _, mode := range []string{"complete", "deadline", "read_failure", "forbidden"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				client, peer := net.Pipe()
				defer client.Close()
				defer peer.Close()
				release := make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				defer unblock()
				message := "synthetic-credential"
				if kind == "customer" {
					message += " synthetic-token"
				}
				body, _ := json.Marshal(map[string]any{"responseCode": "401", "responseMsg": message, "success": false})
				status := http.StatusUnauthorized
				if mode == "forbidden" {
					status = http.StatusForbidden
				}
				peerDone := make(chan struct{})
				go func() {
					defer close(peerDone)
					defer peer.Close()
					req, err := http.ReadRequest(bufio.NewReader(peer))
					if err != nil {
						return
					}
					io.Copy(io.Discard, req.Body)
					req.Body.Close()
					if _, err = fmt.Fprintf(peer, "HTTP/1.1 %d %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n", status, http.StatusText(status), len(body)); err != nil {
						return
					}
					<-release
					if mode == "read_failure" {
						return
					}
					peer.Write(body)
				}()
				transport := &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) { return client, nil }}
				defer transport.CloseIdleConnections()
				var logs bytes.Buffer
				r := &remote{uri: "http://synthetic.invalid/", credential: "synthetic-credential", logger: log.New(&logs, "", 0), client: &http.Client{Transport: transport}}
				flush := testResponseLogQueue(t, r)
				var notifications atomic.Int32
				notified := make(chan error, 1)
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				ctx = WithFailureObserver(ctx, func(err error) {
					notifications.Add(1)
					notified <- err
				})
				done := make(chan error, 1)
				exited := make(chan struct{})
				go func() {
					defer close(exited)
					var err error
					if kind == "service" {
						var value ServiceIdentity
						value, err = (serviceRemote{r}).Resolve(ctx, ServiceLookupRequest{})
						if value != (ServiceIdentity{}) {
							t.Error("authentication returned service identity")
						}
					} else {
						var value CustomerIdentity
						value, err = (customerRemote{r}).Resolve(ctx, []byte("synthetic-token"))
						if value != (CustomerIdentity{}) {
							t.Error("authentication returned customer identity")
						}
					}
					done <- err
				}()
				t.Cleanup(func() {
					cancel()
					unblock()
					client.Close()
					peer.Close()
					select {
					case <-exited:
					case <-time.After(time.Second):
						t.Error("resolver did not exit before log queue cleanup")
					}
				})
				select {
				case err := <-notified:
					if Failure(err) != "lookup_unavailable" || CanUseStale(err) {
						t.Fatal("authentication notification was transient")
					}
				case <-time.After(time.Second):
					t.Fatal("authentication waited for diagnostic body before notification")
				}
				if mode != "forbidden" {
					select {
					case <-done:
						t.Fatal("diagnostic body was abandoned before budget expired")
					default:
					}
				}
				if mode == "deadline" {
					cancel()
				} else {
					unblock()
				}
				select {
				case err := <-done:
					if err == nil || Failure(err) != "lookup_unavailable" || CanUseStale(err) {
						t.Fatalf("diagnostic processing changed confirmed failure: %v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("diagnostics exceeded original query budget")
				}
				unblock()
				select {
				case <-peerDone:
				case <-time.After(time.Second):
					t.Fatal("pipe peer did not exit")
				}
				flush()
				if notifications.Load() != 1 || strings.Count(logs.String(), "\n") != 1 || strings.Contains(logs.String(), "synthetic-credential") || strings.Contains(logs.String(), "synthetic-token") {
					t.Fatal("duplicated notification/log or leaked credential")
				}
				var entry map[string]any
				if json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &entry) != nil || entry["status"] != "lookup_unavailable" || entry["http_status"] != float64(status) {
					t.Fatal("missing authentication diagnostic")
				}
				if mode == "complete" {
					if entry["responseCode"] != "401" || !strings.Contains(fmt.Sprint(entry["responseMsg"]), "[REDACTED]") {
						t.Fatal("complete diagnostic responseMsg was lost")
					}
				} else if _, ok := entry["responseMsg"]; ok {
					t.Fatal("incomplete diagnostic body was logged")
				}
			})
		}
	}
}
