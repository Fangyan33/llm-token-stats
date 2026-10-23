package observation

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/Fangyan33/llm-token-stats/internal/identitylookup"
)

type cacheClock struct{ nanos atomic.Int64 }

func (c *cacheClock) now() time.Time      { return time.Unix(0, c.nanos.Load()) }
func (c *cacheClock) add(d time.Duration) { c.nanos.Add(int64(d)) }
func cachePointer[T any](value T) *T      { return &value }

func cachedLookupServer() (*Server, *cacheClock) {
	s := lookupServer(&sink{})
	s.Cfg.ServiceLookup.Cache = config.LookupCache{}
	s.Cfg.CustomerLookup.Cache = config.LookupCache{}
	clock := &cacheClock{}
	clock.nanos.Store(time.Now().UnixNano())
	s.lookupNow = clock.now
	return s, clock
}

func cacheRequest(host, key string) *pb.Start {
	return &pb.Start{RequestAuthority: host, ApiKey: []byte(key)}
}

func cacheResolve(t *testing.T, s *Server, host, key string) events.Event {
	t.Helper()
	st := cacheRequest(host, key)
	token := st.ApiKey
	var e events.Event
	select {
	case e = <-s.beginResolve(context.Background(), st):
	case <-time.After(3 * time.Second):
		t.Fatal("cache lookup exceeded its bounded deadline")
	}
	awaitLookup(t, func() bool { return len(s.lookupSlots) == 0 })
	if st.ApiKey != nil || st.RequestAuthority != "" || !bytes.Equal(token, make([]byte, len(token))) {
		t.Fatal("completed lookup retained credential")
	}
	return e
}

func TestCacheFreshTTLAndCompleteNameRefresh(t *testing.T) {
	s, clock := cachedLookupServer()
	var serviceCalls, customerCalls atomic.Int32
	s.ServiceResolver = serviceFunc(func(context.Context, identitylookup.ServiceLookupRequest) (identitylookup.ServiceIdentity, error) {
		v, _ := serviceFake{}.Resolve(context.Background(), identitylookup.ServiceLookupRequest{})
		v.ServiceName, v.ProviderName = fmt.Sprintf("服务%d", serviceCalls.Add(1)), "供应商快照"
		return v, nil
	})
	s.CustomerResolver = customerFunc(func(context.Context, []byte) (identitylookup.CustomerIdentity, error) {
		v, _ := customerFake{}.Resolve(context.Background(), nil)
		v.CustomerName = fmt.Sprintf("客户%d", customerCalls.Add(1))
		return v, nil
	})
	first := cacheResolve(t, s, "MODEL.example.com:443", "synthetic-key")
	clock.add(30*time.Minute - time.Nanosecond)
	hit := cacheResolve(t, s, "model.example.com.", "synthetic-key")
	if *hit.ServiceName != *first.ServiceName || *hit.CustomerName != *first.CustomerName || serviceCalls.Load() != 1 || customerCalls.Load() != 1 {
		t.Fatal("fresh cache missed or changed its identity")
	}
	clock.add(time.Nanosecond)
	refreshed := cacheResolve(t, s, "model.example.com", "synthetic-key")
	if serviceCalls.Load() != 2 || customerCalls.Load() != 2 || *refreshed.ServiceName != "服务2" || *refreshed.CustomerName != "客户2" || *refreshed.ProviderName != "供应商快照" {
		t.Fatal("expiry did not refresh complete identities")
	}
	if *first.MappingVersion != *refreshed.MappingVersion || *first.CustomerMappingVersion != *refreshed.CustomerMappingVersion {
		t.Fatal("name refresh changed ID-based mapping version")
	}
	clock.add(29 * time.Minute)
	cacheResolve(t, s, "model.example.com", "synthetic-key")
	if serviceCalls.Load() != 2 || customerCalls.Load() != 2 {
		t.Fatal("successful refresh failed to reset TTL")
	}
}

func TestCacheStaleRetryAndHardExpiration(t *testing.T) {
	s, clock := cachedLookupServer()
	var calls atomic.Int32
	s.ServiceResolver = serviceFunc(func(ctx context.Context, request identitylookup.ServiceLookupRequest) (identitylookup.ServiceIdentity, error) {
		if calls.Add(1) > 1 {
			return identitylookup.ServiceIdentity{}, context.DeadlineExceeded
		}
		return serviceFake{}.Resolve(ctx, request)
	})
	warm := cacheResolve(t, s, "model.example.com", "synthetic-key")
	clock.add(30 * time.Minute)
	stale := cacheResolve(t, s, "model.example.com", "synthetic-key")
	if stale.MappingStatus != "mapped" || *stale.ServiceName != *warm.ServiceName || calls.Load() != 2 {
		t.Fatal("transient refresh failure discarded eligible stale identity")
	}
	clock.add(5*time.Second - time.Nanosecond)
	cacheResolve(t, s, "model.example.com", "synthetic-key")
	if calls.Load() != 2 {
		t.Fatal("failure cooldown did not suppress repeat query")
	}
	clock.add(time.Nanosecond)
	cacheResolve(t, s, "model.example.com", "synthetic-key")
	if calls.Load() != 3 {
		t.Fatal("request did not retry when cooldown expired")
	}
	clock.add(15*time.Minute - 5*time.Second)
	expired := cacheResolve(t, s, "model.example.com", "synthetic-key")
	if expired.MappingStatus != "lookup_timeout" || expired.ServiceID != nil || expired.ServiceName != nil || expired.CustomerMappingStatus != "mapped" || calls.Load() != 4 {
		t.Fatal("failure extended hard expiry or erased independent customer")
	}
}

func TestCacheDisabledZeroGraceAndNoNegativeCache(t *testing.T) {
	for _, mode := range []string{"disabled", "zero_grace", "failed_first", "invalid_first"} {
		t.Run(mode, func(t *testing.T) {
			s, clock := cachedLookupServer()
			if mode == "disabled" {
				s.Cfg.ServiceLookup.Cache.Enabled = cachePointer(false)
			}
			if mode == "zero_grace" {
				s.Cfg.ServiceLookup.Cache.StaleIfErrorSeconds = cachePointer(0)
			}
			var calls atomic.Int32
			s.ServiceResolver = serviceFunc(func(ctx context.Context, request identitylookup.ServiceLookupRequest) (identitylookup.ServiceIdentity, error) {
				n := calls.Add(1)
				if mode == "zero_grace" && n > 1 || mode == "failed_first" && n == 1 {
					return identitylookup.ServiceIdentity{}, context.DeadlineExceeded
				}
				if mode == "invalid_first" && n == 1 {
					return identitylookup.ServiceIdentity{ServiceID: "partial"}, nil
				}
				return serviceFake{}.Resolve(ctx, request)
			})
			cacheResolve(t, s, "model.example.com", "synthetic-key")
			if mode == "zero_grace" {
				clock.add(30 * time.Minute)
			}
			second := cacheResolve(t, s, "model.example.com", "synthetic-key")
			want := "mapped"
			if mode == "zero_grace" {
				want = "lookup_timeout"
			}
			if calls.Load() != 2 || second.MappingStatus != want || s.lookups.customers.metrics.hits.Load() != 1 && mode != "zero_grace" {
				t.Fatal("disabled, failed or invalid lookup was cached, or interface caches were coupled")
			}
		})
	}
}

func TestCacheLRUCapacityAndProcessIsolation(t *testing.T) {
	s, _ := cachedLookupServer()
	s.Cfg.ServiceLookup.Cache.MaxEntries = cachePointer(2)
	s.Cfg.CustomerLookup.Cache.MaxEntries = cachePointer(2)
	var calls atomic.Int32
	s.ServiceResolver = serviceFunc(func(ctx context.Context, request identitylookup.ServiceLookupRequest) (identitylookup.ServiceIdentity, error) {
		calls.Add(1)
		return serviceFake{}.Resolve(ctx, request)
	})
	for _, name := range []string{"a", "b", "a", "c", "b"} {
		cacheResolve(t, s, name+".example.com", "synthetic-"+name)
	}
	if calls.Load() != 4 || len(s.lookups.services.items) != 2 || len(s.lookups.customers.items) != 2 || s.lookups.services.metrics.evictions.Load() != 2 {
		t.Fatal("cache capacity or LRU eviction failed")
	}
	key := s.lookups.customerKey([]byte("synthetic-b"))
	other, _ := cachedLookupServer()
	other.ServiceResolver = s.ServiceResolver
	cacheResolve(t, other, "b.example.com", "synthetic-b")
	if calls.Load() != 5 || key == other.lookups.customerKey([]byte("synthetic-b")) || key == s.lookups.customerKey([]byte("SYNTHETIC-B")) {
		t.Fatal("cache leaked across instances or customer keys were normalized")
	}
	for _, context := range []identitylookup.ServiceLookupRequest{
		{RequestHost: "b.example.com", ClusterName: "different", DeploymentEnvironment: "dev"},
		{RequestHost: "b.example.com", ClusterName: "cluster", DeploymentEnvironment: "stg"},
	} {
		if s.lookups.services.probe(context, s.lookups.now()).ready {
			t.Fatal("service cache ignored deployment context")
		}
	}
	metrics := httptest.NewRecorder()
	s.Health.Handler().ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if strings.Contains(metrics.Body.String(), "synthetic-") || strings.Contains(metrics.Body.String(), "example.com") || strings.Contains(metrics.Body.String(), hex.EncodeToString(key[:])) || !strings.Contains(metrics.Body.String(), "metering_lookup_cache_service_entries 2") {
		t.Fatal("cache metrics missing or contain request identifiers")
	}
}

func TestCacheConcurrentMissSharesWorkersAndClearsJoinCredentials(t *testing.T) {
	s, _ := cachedLookupServer()
	s.Cfg.LookupTimeoutMS = 1500
	entered, release := make(chan struct{}, 2), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var serviceCalls, customerCalls atomic.Int32
	s.ServiceResolver = serviceFunc(func(ctx context.Context, request identitylookup.ServiceLookupRequest) (identitylookup.ServiceIdentity, error) {
		serviceCalls.Add(1)
		entered <- struct{}{}
		<-release
		return serviceFake{}.Resolve(ctx, request)
	})
	s.CustomerResolver = customerFunc(func(ctx context.Context, token []byte) (identitylookup.CustomerIdentity, error) {
		customerCalls.Add(1)
		entered <- struct{}{}
		<-release
		if string(token) != "synthetic-key" {
			t.Error("owner token cleared before actual worker exit")
		}
		return customerFake{}.Resolve(ctx, token)
	})
	owner := s.beginResolve(context.Background(), cacheRequest("model.example.com", "synthetic-key"))
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("owner queries not started")
		}
	}
	const waiters = 20
	results := make([]<-chan events.Event, waiters)
	tokens := make([][]byte, waiters)
	for i := range results {
		st := cacheRequest("MODEL.example.com:443", "synthetic-key")
		tokens[i] = st.ApiKey
		ctx, cancel := context.WithCancel(context.Background())
		results[i] = s.beginResolve(ctx, st)
		cancel() // Observer cancellation must not cancel the shared lookup.
	}
	awaitLookup(t, func() bool { return s.lookups.customers.metrics.shared.Load() == waiters })
	s.lookups.mu.Lock()
	for _, token := range tokens {
		if !bytes.Equal(token, make([]byte, len(token))) {
			t.Error("joining request retained raw credential while waiting")
		}
	}
	s.lookups.mu.Unlock()
	close(release)
	for _, result := range append(results, owner) {
		select {
		case e := <-result:
			if e.MappingStatus != "mapped" || e.CustomerMappingStatus != "mapped" {
				t.Fatal("shared result lost")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("shared wait did not finish")
		}
	}
	awaitLookup(t, func() bool { return len(s.lookupSlots) == 0 })
	if serviceCalls.Load() != 1 || customerCalls.Load() != 1 || s.lookups.services.metrics.shared.Load() != waiters {
		t.Fatal("concurrent misses exceeded one query per key")
	}
}

func TestCacheHitsAndStaleSurviveUnrelatedOverload(t *testing.T) {
	s, clock := cachedLookupServer()
	s.Cfg.LookupTimeoutMS = 1500
	cacheResolve(t, s, "model.example.com", "synthetic-key")
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	s.ServiceResolver = serviceFunc(func(ctx context.Context, request identitylookup.ServiceLookupRequest) (identitylookup.ServiceIdentity, error) {
		close(entered)
		<-release
		return serviceFake{}.Resolve(ctx, request)
	})
	busy := s.beginResolve(context.Background(), cacheRequest("busy.example.com", "synthetic-busy"))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("busy query not admitted")
	}
	hit := <-s.beginResolve(context.Background(), cacheRequest("model.example.com", "synthetic-key"))
	partial := <-s.beginResolve(context.Background(), cacheRequest("model.example.com", "synthetic-new-key"))
	otherSide := <-s.beginResolve(context.Background(), cacheRequest("new.example.com", "synthetic-key"))
	if hit.MappingStatus != "mapped" || hit.CustomerMappingStatus != "mapped" || partial.MappingStatus != "mapped" || partial.CustomerMappingStatus != "lookup_overloaded" || otherSide.MappingStatus != "lookup_overloaded" || otherSide.CustomerMappingStatus != "mapped" {
		t.Fatal("remote admission rejected a cache hit or lost the successful side")
	}
	clock.add(30 * time.Minute)
	stale := <-s.beginResolve(context.Background(), cacheRequest("model.example.com", "synthetic-key"))
	if stale.MappingStatus != "mapped" || stale.CustomerMappingStatus != "mapped" || s.lookups.services.metrics.stale.Load() != 1 {
		t.Fatal("local overload did not fall back to eligible old identities")
	}
	close(release)
	<-busy
	awaitLookup(t, func() bool { return len(s.lookupSlots) == 0 })
}

func TestCacheTimeoutFallbackPreservesOtherSideAndRejectsLateSuccess(t *testing.T) {
	s, clock := cachedLookupServer()
	s.Cfg.LookupTimeoutMS = 20
	warm := cacheResolve(t, s, "model.example.com", "synthetic-key")
	clock.add(30 * time.Minute)
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	s.ServiceResolver = serviceFunc(func(ctx context.Context, request identitylookup.ServiceLookupRequest) (identitylookup.ServiceIdentity, error) {
		<-release // Intentionally ignores cancellation.
		v, _ := serviceFake{}.Resolve(ctx, request)
		v.ServiceName = "迟到名称"
		return v, nil
	})
	snapshot := <-s.beginResolve(context.Background(), cacheRequest("model.example.com", "synthetic-key"))
	if snapshot.MappingStatus != "mapped" || *snapshot.ServiceName != *warm.ServiceName || snapshot.CustomerMappingStatus != "mapped" || len(s.lookupSlots) != 1 {
		t.Fatal("deadline lost stale/independent result or released admission prematurely")
	}
	before, _ := events.Canonical(&snapshot)
	close(release)
	awaitLookup(t, func() bool { return len(s.lookupSlots) == 0 })
	after, _ := events.Canonical(&snapshot)
	if !bytes.Equal(before, after) {
		t.Fatal("late resolver rewrote frozen snapshot")
	}
	s.lookups.mu.Lock()
	entry := s.lookups.services.entry(identitylookup.ServiceLookupRequest{RequestHost: "model.example.com", ClusterName: "cluster", DeploymentEnvironment: "dev"}, clock.now())
	if entry == nil || entry.value.ServiceName == "迟到名称" || clock.now().Before(entry.freshUntil) {
		t.Error("late success refreshed cache")
	}
	s.lookups.mu.Unlock()
}

func TestCacheStaleRecheckedWhenSnapshotFreezes(t *testing.T) {
	s, clock := cachedLookupServer()
	s.Cfg.ServiceLookup.Cache.TTLSeconds = cachePointer(1)
	s.Cfg.ServiceLookup.Cache.StaleIfErrorSeconds = cachePointer(1)
	cacheResolve(t, s, "model.example.com", "synthetic-key")
	clock.add(time.Second)
	s.ServiceResolver = serviceFunc(func(context.Context, identitylookup.ServiceLookupRequest) (identitylookup.ServiceIdentity, error) {
		return identitylookup.ServiceIdentity{}, context.DeadlineExceeded
	})
	cacheResolve(t, s, "model.example.com", "synthetic-key") // Starts cooldown.
	entered, release := make(chan struct{}), make(chan struct{})
	s.CustomerResolver = customerFunc(func(ctx context.Context, token []byte) (identitylookup.CustomerIdentity, error) {
		close(entered)
		<-release
		return customerFake{}.Resolve(ctx, token)
	})
	result := s.beginResolve(context.Background(), cacheRequest("model.example.com", "synthetic-other-key"))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("independent customer did not start")
	}
	clock.add(time.Second)
	close(release)
	e := <-result
	if e.MappingStatus != "lookup_timeout" || e.ServiceID != nil || e.CustomerMappingStatus != "mapped" {
		t.Fatal("cooldown returned a value beyond its hard expiry at snapshot freeze")
	}
}

func TestCacheJoinedWaiterDeadlineDoesNotCancelSharedTask(t *testing.T) {
	s, _ := cachedLookupServer()
	s.Cfg.LookupTimeoutMS = 1500
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	s.ServiceResolver = serviceFunc(func(ctx context.Context, request identitylookup.ServiceLookupRequest) (identitylookup.ServiceIdentity, error) {
		close(entered)
		select {
		case <-release:
			return serviceFake{}.Resolve(ctx, request)
		case <-ctx.Done():
			return identitylookup.ServiceIdentity{}, ctx.Err()
		}
	})
	owner := s.beginResolve(context.Background(), cacheRequest("model.example.com", "synthetic-key"))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("shared task not started")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	joined := s.resolve(ctx, cacheRequest("model.example.com", "synthetic-key"))
	if joined.MappingStatus != "lookup_timeout" || joined.CustomerMappingStatus != "mapped" {
		t.Fatal("joining waiter lost independent result or exceeded deadline")
	}
	close(release)
	if e := <-owner; e.MappingStatus != "mapped" {
		t.Fatal("a waiter cancelled the shared task")
	}
	awaitLookup(t, func() bool { return len(s.lookupSlots) == 0 })
	if s.lookups.services.metrics.shared.Load() != 1 || s.lookups.services.metrics.failures.Load() != 0 {
		t.Fatal("waiter deadline was recorded as shared task failure")
	}
}

func TestCacheLateDefinitiveErrorRevokesOldValue(t *testing.T) {
	s, clock := cachedLookupServer()
	s.Cfg.LookupTimeoutMS = 20
	cacheResolve(t, s, "model.example.com", "synthetic-key")
	clock.add(30 * time.Minute)
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	s.ServiceResolver = serviceFunc(func(context.Context, identitylookup.ServiceLookupRequest) (identitylookup.ServiceIdentity, error) {
		<-release
		return identitylookup.ServiceIdentity{}, errors.New("synthetic definitive failure")
	})
	if e := <-s.beginResolve(context.Background(), cacheRequest("model.example.com", "synthetic-key")); e.MappingStatus != "mapped" {
		t.Fatal("pending query timeout did not use eligible stale value")
	}
	close(release)
	awaitLookup(t, func() bool { return len(s.lookupSlots) == 0 })
	s.ServiceResolver = serviceFunc(func(context.Context, identitylookup.ServiceLookupRequest) (identitylookup.ServiceIdentity, error) {
		return identitylookup.ServiceIdentity{}, context.DeadlineExceeded
	})
	if e := cacheResolve(t, s, "model.example.com", "synthetic-key"); e.MappingStatus != "lookup_timeout" || e.ServiceID != nil {
		t.Fatal("late definitive failure left a reusable stale entry")
	}
}
