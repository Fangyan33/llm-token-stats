package observation

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/Fangyan33/llm-token-stats/internal/health"
	"github.com/Fangyan33/llm-token-stats/internal/identitylookup"
)

func TestHTTPBusinessErrorsPreserveOtherIdentity(t *testing.T) {
	t.Setenv("HTTP_SNAPSHOT_CREDENTIAL", "synthetic-directory-access")
	for _, failedSide := range []string{"service", "customer"} {
		for _, code := range []string{"650013", "100001", "401"} {
			t.Run(failedSide+"/"+code, func(t *testing.T) {
				directory := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					if r.URL.Path == "/"+failedSide {
						if code == "401" {
							w.WriteHeader(http.StatusUnauthorized)
						}
						json.NewEncoder(w).Encode(map[string]any{"responseCode": code, "responseMsg": "查询失败", "success": false, "data": nil})
						return
					}
					var data any = identitylookup.ServiceIdentity{ServiceID: "s", ServiceName: "服务", ProviderID: "p", ProviderName: "供应商"}
					if r.URL.Path == "/customer" {
						data = identitylookup.CustomerIdentity{APIKeyID: "k", CustomerID: "c", CustomerName: "客户"}
					}
					json.NewEncoder(w).Encode(map[string]any{"responseCode": "000000", "responseMsg": "查询成功", "success": true, "data": data})
				}))
				defer directory.Close()
				s := &Server{Cfg: config.Observer{ClusterName: "cluster-a", DeploymentEnvironment: "stg"}, Health: &health.State{}}
				var err error
				s.ServiceResolver, err = identitylookup.NewService(config.Lookup{URI: directory.URL + "/service", CredentialEnv: "HTTP_SNAPSHOT_CREDENTIAL"})
				if err != nil {
					t.Fatal(err)
				}
				s.CustomerResolver, err = identitylookup.NewCustomer(config.Lookup{URI: directory.URL + "/customer", CredentialEnv: "HTTP_SNAPSHOT_CREDENTIAL"})
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				st := &pb.Start{RequestAuthority: "model.example.com", ApiKey: []byte("synthetic-business-token")}
				e := s.resolve(ctx, st)
				want := "lookup_unavailable"
				if code == "650013" {
					want = "lookup_invalid_response"
				}
				if failedSide == "service" {
					if e.MappingStatus != want || e.ServiceID != nil || e.ServiceName != nil || e.ProviderID != nil || e.ProviderName != nil || e.MappingVersion != nil || e.CustomerMappingStatus != "mapped" || e.CustomerID == nil || *e.CustomerName != "客户" || e.CustomerMappingVersion == nil || e.IdentityStatus != "known" {
						t.Fatal("service failure erased customer or retained partial service")
					}
				} else if e.CustomerMappingStatus != want || e.ApiKeyID != nil || e.CustomerID != nil || e.CustomerName != nil || e.CustomerMappingVersion != nil || e.MappingStatus != "mapped" || e.ServiceID == nil || *e.ServiceName != "服务" || *e.ProviderName != "供应商" || e.MappingVersion == nil || e.IdentityStatus != "invalid" {
					t.Fatal("customer failure erased service or retained partial customer")
				}
				raw, err := events.Canonical(&e)
				if err != nil || bytes.Contains(raw, []byte("responseMsg")) || bytes.Contains(raw, []byte("synthetic-business-token")) || st.ApiKey != nil {
					t.Fatal("snapshot retained response metadata or credential")
				}
			})
		}
	}
}

type blockedLookupLogWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockedLookupLogWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(p), nil
}

func TestBlockedLogsReleaseTokensAndLookupAdmission(t *testing.T) {
	t.Setenv("HTTP_LOG_BACKPRESSURE_CREDENTIAL", "synthetic-directory-access")
	directory := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var data any = identitylookup.ServiceIdentity{ServiceID: "s", ServiceName: "服务", ProviderID: "p", ProviderName: "供应商"}
		if r.URL.Path == "/customer" {
			data = identitylookup.CustomerIdentity{APIKeyID: "k", CustomerID: "c", CustomerName: "客户"}
		}
		json.NewEncoder(w).Encode(map[string]any{"responseCode": "000000", "responseMsg": "查询成功", "success": true, "data": data})
	}))
	defer directory.Close()
	s := &Server{Cfg: config.Observer{ClusterName: "cluster-a", DeploymentEnvironment: "stg", LookupConcurrency: 1, LookupTimeoutMS: 500}, Health: &health.State{}}
	var err error
	s.ServiceResolver, err = identitylookup.NewService(config.Lookup{URI: directory.URL + "/service", CredentialEnv: "HTTP_LOG_BACKPRESSURE_CREDENTIAL"})
	if err != nil {
		t.Fatal(err)
	}
	s.CustomerResolver, err = identitylookup.NewCustomer(config.Lookup{URI: directory.URL + "/customer", CredentialEnv: "HTTP_LOG_BACKPRESSURE_CREDENTIAL"})
	if err != nil {
		t.Fatal(err)
	}
	w := &blockedLookupLogWriter{entered: make(chan struct{}), release: make(chan struct{})}
	previous := log.Writer()
	log.SetOutput(w)
	defer func() {
		close(w.release)
		log.SetOutput(previous)
	}()
	dropped := identitylookup.DroppedResponseLogs()
	// More calls than the shared log queue can retain, with only one available
	// lookup slot. Every response must still map and release the borrowed token.
	for i := 0; i < 300; i++ {
		token := []byte("synthetic-business-token")
		st := &pb.Start{RequestAuthority: "model.example.com", ApiKey: token}
		select {
		case e := <-s.beginResolve(context.Background(), st):
			if e.MappingStatus != "mapped" || e.CustomerMappingStatus != "mapped" || e.ServiceName == nil || e.CustomerName == nil {
				t.Fatalf("log backpressure changed attribution: %s / %s", e.MappingStatus, e.CustomerMappingStatus)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("log backpressure blocked lookup completion")
		}
		awaitLookup(t, func() bool { return len(s.lookupSlots) == 0 })
		if st.ApiKey != nil || !bytes.Equal(token, make([]byte, len(token))) {
			t.Fatal("log backpressure retained a token or lookup slot")
		}
		if i == 0 {
			select {
			case <-w.entered:
			case <-time.After(time.Second):
				t.Fatal("log writer was not blocked")
			}
		}
	}
	if identitylookup.DroppedResponseLogs() <= dropped {
		t.Fatal("saturated log queue did not count dropped entries")
	}
	s.Health.GaugeFunc("lookup_logs_dropped_total", identitylookup.DroppedResponseLogs)
	metrics := httptest.NewRecorder()
	s.Health.Handler().ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(metrics.Body.String(), "metering_lookup_logs_dropped_total ") {
		t.Fatal("log losses were not exposed without writing more logs")
	}
}
