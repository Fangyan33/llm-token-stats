package observation

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/Fangyan33/llm-token-stats/internal/identitylookup"
)

func TestHTTPCacheFailurePolicyAndInvalidation(t *testing.T) {
	t.Setenv("CACHE_TEST_CREDENTIAL", "synthetic-directory-access")
	for _, failedSide := range []string{"service", "customer"} {
		for _, tc := range []struct {
			name, body, status string
			httpStatus         int
			stale              bool
		}{
			{"server", "not JSON", "lookup_unavailable", 503, true},
			{"rate_limit", `{"error":{"code":"rate_limited"}}`, "lookup_rate_limited", 429, true},
			{"unauthorized", `{"responseCode":"401","responseMsg":"Unauthorized","success":false}`, "lookup_unavailable", 401, false},
			{"forbidden", "not JSON", "lookup_unavailable", 403, false},
			{"parameters", `{"responseCode":"100001","responseMsg":"无效的参数！","success":false}`, "lookup_unavailable", 200, false},
			{"ambiguous", `{"responseCode":"650013","responseMsg":"无法唯一确定归属","success":false}`, "lookup_invalid_response", 200, false},
			{"invalid", "not JSON", "lookup_invalid_response", 200, false},
		} {
			t.Run(failedSide+"/"+tc.name, func(t *testing.T) {
				s, clock := cachedLookupServer()
				s.Cfg.LookupTimeoutMS = 1000
				var phase, failedCalls atomic.Int32
				remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					if r.URL.Path == "/"+failedSide {
						failedCalls.Add(1)
						if phase.Load() > 0 {
							if phase.Load() == 2 {
								w.WriteHeader(503)
								return
							}
							w.WriteHeader(tc.httpStatus)
							w.Write([]byte(tc.body))
							return
						}
					}
					var data any = identitylookup.ServiceIdentity{ServiceID: "s", ServiceName: "服务快照", ProviderID: "p", ProviderName: "供应商快照"}
					if r.URL.Path == "/customer" {
						data = identitylookup.CustomerIdentity{APIKeyID: "k", CustomerID: "c", CustomerName: "客户快照"}
					}
					json.NewEncoder(w).Encode(map[string]any{"responseCode": "000000", "responseMsg": "查询成功", "success": true, "data": data})
				}))
				defer remote.Close()
				var err error
				s.ServiceResolver, err = identitylookup.NewService(config.Lookup{URI: remote.URL + "/service", CredentialEnv: "CACHE_TEST_CREDENTIAL"})
				if err != nil {
					t.Fatal(err)
				}
				s.CustomerResolver, err = identitylookup.NewCustomer(config.Lookup{URI: remote.URL + "/customer", CredentialEnv: "CACHE_TEST_CREDENTIAL"})
				if err != nil {
					t.Fatal(err)
				}
				cacheResolve(t, s, "model.example.com", "synthetic-key")
				phase.Store(1)
				clock.add(30 * time.Minute)
				e := cacheResolve(t, s, "model.example.com", "synthetic-key")
				want := tc.status
				if tc.stale {
					want = "mapped"
				}
				if failedSide == "service" && (e.MappingStatus != want || e.CustomerMappingStatus != "mapped") || failedSide == "customer" && (e.CustomerMappingStatus != want || e.MappingStatus != "mapped") {
					t.Fatalf("wrong HTTP cache fallback: %s / %s", e.MappingStatus, e.CustomerMappingStatus)
				}
				phase.Store(2)
				clock.add(5 * time.Second)
				e = cacheResolve(t, s, "model.example.com", "synthetic-key")
				want = "lookup_unavailable"
				if tc.stale {
					want = "mapped"
				}
				if failedSide == "service" && (e.MappingStatus != want || !tc.stale && e.ServiceID != nil) || failedSide == "customer" && (e.CustomerMappingStatus != want || !tc.stale && e.CustomerID != nil) || failedCalls.Load() != 3 {
					t.Fatal("definitive error's revoked identity was resurrected by a later transient failure")
				}
			})
		}
	}
}

func TestSlowUnauthorizedBodyCannotUseStaleIdentity(t *testing.T) {
	t.Setenv("SLOW_AUTH_CREDENTIAL", "synthetic-directory-access")
	for _, side := range []string{"service", "customer"} {
		t.Run(side, func(t *testing.T) {
			s, clock := cachedLookupServer()
			s.Cfg.LookupTimeoutMS = 2000
			if side == "service" {
				s.Cfg.CustomerLookup.Cache.TTLSeconds = cachePointer(3600)
			} else {
				s.Cfg.ServiceLookup.Cache.TTLSeconds = cachePointer(3600)
			}
			var phase, calls atomic.Int32
			entered, headerGate := make(chan struct{}, 1), make(chan struct{})
			stop := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(headerGate) }) }
			defer unblock()
			remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				r.Body.Close()
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/"+side {
					calls.Add(1)
					switch phase.Load() {
					case 1:
						body := `{"responseCode":"401","responseMsg":"Unauthorized","success":false}`
						w.Header().Set("Content-Length", strconv.Itoa(len(body)))
						w.WriteHeader(http.StatusUnauthorized)
						w.(http.Flusher).Flush()
						select { // Headers arrive, diagnostic body never ends.
						case <-r.Context().Done():
						case <-stop:
						}
						return
					case 2:
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
				}
				var data any = identitylookup.ServiceIdentity{ServiceID: "s", ServiceName: "服务", ProviderID: "p", ProviderName: "供应商"}
				if r.URL.Path == "/customer" {
					data = identitylookup.CustomerIdentity{APIKeyID: "k", CustomerID: "c", CustomerName: "客户"}
				}
				json.NewEncoder(w).Encode(map[string]any{"responseCode": "000000", "success": true, "data": data})
			}))
			defer remote.Close()
			defer close(stop) // Cleanup must also finish when an assertion fails.
			gate := func(ctx context.Context, kind string) error {
				if kind == side && phase.Load() == 1 {
					entered <- struct{}{}
					select {
					case <-headerGate:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			}
			service, err := identitylookup.NewService(config.Lookup{URI: remote.URL + "/service", CredentialEnv: "SLOW_AUTH_CREDENTIAL"})
			if err != nil {
				t.Fatal(err)
			}
			s.ServiceResolver = serviceFunc(func(ctx context.Context, request identitylookup.ServiceLookupRequest) (identitylookup.ServiceIdentity, error) {
				if err := gate(ctx, "service"); err != nil {
					return identitylookup.ServiceIdentity{}, err
				}
				return service.Resolve(ctx, request)
			})
			customer, err := identitylookup.NewCustomer(config.Lookup{URI: remote.URL + "/customer", CredentialEnv: "SLOW_AUTH_CREDENTIAL"})
			if err != nil {
				t.Fatal(err)
			}
			s.CustomerResolver = customerFunc(func(ctx context.Context, token []byte) (identitylookup.CustomerIdentity, error) {
				if err := gate(ctx, "customer"); err != nil {
					return identitylookup.CustomerIdentity{}, err
				}
				value, err := customer.Resolve(ctx, token)
				if string(token) != "synthetic-key" {
					t.Error("token cleared before resolver returned")
				}
				return value, err
			})
			if warm := cacheResolve(t, s, "model.example.com", "synthetic-key"); warm.MappingStatus != "mapped" || warm.CustomerMappingStatus != "mapped" {
				t.Fatalf("cannot warm cache within the HTTP budget: %s / %s", warm.MappingStatus, warm.CustomerMappingStatus)
			}
			clock.add(30 * time.Minute)
			phase.Store(1)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			result, finished := make(chan events.Event, 1), make(chan struct{})
			request := cacheRequest("model.example.com", "synthetic-key")
			token := request.ApiKey
			go func() { result <- s.resolveQueries(ctx, request, func() { close(finished) }) }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("refresh was not admitted")
			}
			join := func() <-chan events.Event {
				pending := make(chan events.Event, 1)
				go func() { pending <- s.resolve(ctx, cacheRequest("model.example.com", "synthetic-key")) }()
				return pending
			}
			first, second := join(), join()
			metrics := &s.lookups.services.metrics
			if side == "customer" {
				metrics = &s.lookups.customers.metrics
			}
			awaitLookup(t, func() bool { return metrics.shared.Load() == 2 })
			unblock()
			awaitLookup(t, func() bool { return metrics.failures.Load() == 1 })
			select {
			case <-result:
				t.Fatal("diagnostic worker was abandoned before original budget")
			default:
			}
			if len(s.lookupSlots) != 1 {
				t.Fatal("header notification prematurely released admission")
			}
			cancel() // Query cutoff races the still-unfinished diagnostic body.
			check := func(e events.Event) {
				t.Helper()
				if side == "service" {
					if e.MappingStatus != "lookup_unavailable" || e.ServiceID != nil || e.ProviderID != nil || e.ServiceName != nil || e.ProviderName != nil || e.MappingVersion != nil || e.CustomerMappingStatus != "mapped" || e.CustomerName == nil {
						t.Fatalf("slow 401 changed service/customer: %s / %s", e.MappingStatus, e.CustomerMappingStatus)
					}
				} else if e.CustomerMappingStatus != "lookup_unavailable" || e.ApiKeyID != nil || e.CustomerID != nil || e.CustomerName != nil || e.CustomerMappingVersion != nil || e.IdentityStatus != "invalid" || e.MappingStatus != "mapped" || e.ServiceName == nil {
					t.Fatalf("slow 401 changed service/customer: %s / %s", e.MappingStatus, e.CustomerMappingStatus)
				}
			}
			for _, pending := range []<-chan events.Event{result, first, second} {
				select {
				case e := <-pending:
					check(e)
				case <-time.After(time.Second):
					t.Fatal("authentication result did not freeze within budget")
				}
			}
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Fatal("diagnostic worker did not release admission")
			}
			if metrics.failures.Load() != 1 || metrics.stale.Load() != 0 || len(s.lookupSlots) != 0 || !bytes.Equal(token, make([]byte, len(token))) {
				t.Fatal("duplicated failure, stale attribution, credential or admission leak")
			}
			s.lookups.mu.Lock()
			guards := len(s.lookups.services.guards) + len(s.lookups.customers.guards)
			s.lookups.mu.Unlock()
			if guards != 0 {
				t.Fatal("frozen snapshots retained revocation guards")
			}
			phase.Store(2)
			check(cacheResolve(t, s, "model.example.com", "synthetic-key"))
			phase.Store(0)
			recovered := cacheResolve(t, s, "model.example.com", "synthetic-key")
			if recovered.MappingStatus != "mapped" || recovered.CustomerMappingStatus != "mapped" || calls.Load() != 4 {
				t.Fatal("authentication failure was cached or old value resurrected")
			}
		})
	}
}
