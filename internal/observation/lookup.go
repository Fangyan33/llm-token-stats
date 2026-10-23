package observation

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"sync"
	"time"

	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/Fangyan33/llm-token-stats/internal/identitylookup"
	"github.com/Fangyan33/llm-token-stats/pkg/credentials"
)

type lookupState struct {
	mu        sync.Mutex
	now       func() time.Time
	secret    [32]byte
	services  *lookupCache[identitylookup.ServiceLookupRequest, identitylookup.ServiceIdentity]
	customers *lookupCache[[32]byte, identitylookup.CustomerIdentity]
}

func (s *Server) initLookups() {
	s.lookupOnce.Do(func() {
		service, err := s.Cfg.ServiceLookup.Cache.Settings()
		if err != nil {
			s.lookupInitErr = err
			return
		}
		customer, err := s.Cfg.CustomerLookup.Cache.Settings()
		if err != nil {
			s.lookupInitErr = err
			return
		}
		state := &lookupState{now: s.lookupNow,
			services:  newLookupCache[identitylookup.ServiceLookupRequest, identitylookup.ServiceIdentity](service),
			customers: newLookupCache[[32]byte, identitylookup.CustomerIdentity](customer)}
		if state.now == nil {
			state.now = time.Now
		}
		// crypto/rand.Read fills the buffer or terminates the process if the
		// platform's secure random source is unavailable.
		rand.Read(state.secret[:])
		n := s.Cfg.LookupConcurrency
		if n == 0 {
			n = 64
		}
		s.lookupSlots = make(chan struct{}, n)
		state.services.metrics.register(s.Health, "service")
		state.customers.metrics.register(s.Health, "customer")
		s.lookups = state
	})
}

func (state *lookupState) customerKey(token []byte) [32]byte {
	digest := hmac.New(sha256.New, state.secret[:])
	digest.Write(token)
	var key [32]byte
	digest.Sum(key[:0])
	return key
}

func (s *Server) resolve(ctx context.Context, st *pb.Start) events.Event {
	return s.resolveQueries(ctx, st, nil)
}

// Only this coordinator mutates the snapshot. Cache transitions publish bounded,
// immutable outcomes; worker completion owns admission release and token cleanup.
func (s *Server) resolveQueries(ctx context.Context, st *pb.Start, finished func()) events.Event {
	s.initLookups()
	e := s.emptySnapshot(st, "lookup_timeout")
	token := st.ApiKey
	st.ApiKey, st.RequestAuthority = nil, ""
	if s.lookupInitErr != nil {
		clear(token)
		if finished != nil {
			finished()
		}
		if e.RequestHost != nil {
			e.MappingStatus = "lookup_unavailable"
		}
		if e.CustomerMappingStatus == "lookup_timeout" {
			e.CustomerMappingStatus = "lookup_unavailable"
		}
		return e
	}
	state := s.lookups
	var service *lookupTicket[identitylookup.ServiceLookupRequest, identitylookup.ServiceIdentity]
	var customer *lookupTicket[[32]byte, identitylookup.CustomerIdentity]
	state.mu.Lock()
	now := state.now()
	if e.RequestHost != nil {
		request := identitylookup.ServiceLookupRequest{RequestHost: *e.RequestHost, ClusterName: e.ClusterName, DeploymentEnvironment: e.DeploymentEnvironment}
		service = state.services.probe(request, now)
	}
	if credentials.ValidToken(token) {
		customer = state.customers.probe(state.customerKey(token), now)
	}
	newService, newCustomer := service.needsWorker(), customer.needsWorker()
	owned, overloaded := false, false
	if newService || newCustomer {
		if !lookupExpired(ctx) {
			select {
			case s.lookupSlots <- struct{}{}:
				owned = true
			default:
				overloaded = true
			}
		}
		if owned {
			if newService {
				service.start(ctx)
			}
			if newCustomer {
				customer.start(ctx)
			}
		} else {
			if newService {
				result := timedOut[identitylookup.ServiceIdentity]()
				if overloaded {
					result.status = "lookup_overloaded"
				}
				service.reject(result, now)
			}
			if newCustomer {
				result := timedOut[identitylookup.CustomerIdentity]()
				if overloaded {
					result.status = "lookup_overloaded"
				}
				customer.reject(result, now)
			}
			newService, newCustomer = false, false
		}
	}
	if !newCustomer {
		clear(token) // A cache hit/join never retains a second raw credential.
	}
	state.mu.Unlock()
	if overloaded {
		s.Health.Count("lookup_overloaded")
	}
	var workers sync.WaitGroup
	if newService {
		resolver := s.ServiceResolver
		if resolver == nil {
			resolver = identitylookup.UnimplementedServiceResolver{}
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			queryCtx := identitylookup.WithFailureObserver(ctx, func(err error) {
				state.mu.Lock()
				service.confirmFailure(lookupValue[identitylookup.ServiceIdentity]{status: identitylookup.Failure(err)})
				state.mu.Unlock()
			})
			value, err := resolver.Resolve(queryCtx, service.key)
			result := lookupValue[identitylookup.ServiceIdentity]{value: value, status: "mapped"}
			if err != nil {
				result = lookupValue[identitylookup.ServiceIdentity]{status: identitylookup.Failure(err), transient: identitylookup.CanUseStale(err)}
			} else if !validService(value) {
				result = lookupValue[identitylookup.ServiceIdentity]{status: "lookup_invalid_response"}
			}
			state.mu.Lock()
			service.complete(result, state.now())
			service.workerExited()
			state.mu.Unlock()
		}()
	}
	if newCustomer {
		resolver := s.CustomerResolver
		if resolver == nil {
			resolver = identitylookup.UnimplementedCustomerResolver{}
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			queryCtx := identitylookup.WithFailureObserver(ctx, func(err error) {
				state.mu.Lock()
				customer.confirmFailure(lookupValue[identitylookup.CustomerIdentity]{status: identitylookup.Failure(err)})
				state.mu.Unlock()
			})
			value, err := resolver.Resolve(queryCtx, token)
			clear(token)
			result := lookupValue[identitylookup.CustomerIdentity]{value: value, status: "mapped"}
			if err != nil {
				result = lookupValue[identitylookup.CustomerIdentity]{status: identitylookup.Failure(err), transient: identitylookup.CanUseStale(err)}
			} else if !validCustomer(value) {
				result = lookupValue[identitylookup.CustomerIdentity]{status: "lookup_invalid_response"}
			}
			state.mu.Lock()
			customer.complete(result, state.now())
			customer.workerExited()
			state.mu.Unlock()
		}()
	}
	if owned || finished != nil {
		go func() {
			workers.Wait()
			if owned {
				<-s.lookupSlots
			}
			if finished != nil {
				finished()
			}
		}()
	}
	serviceDone, customerDone := service.waitChannel(), customer.waitChannel()
collect:
	for serviceDone != nil || customerDone != nil {
		select {
		case <-serviceDone:
			serviceDone = nil
		case <-customerDone:
			customerDone = nil
		case <-ctx.Done():
			break collect
		}
	}
	// Inspect published outcomes under the same lock as cache invalidation.
	// At a deadline, publish timeout for expired tasks before selecting stale;
	// this does not wait for a resolver that ignores cancellation.
	state.mu.Lock()
	now = state.now()
	if service != nil {
		r := service.freeze(now)
		e.MappingStatus = r.status
		if r.status == "mapped" {
			v := r.value
			e.ServiceID, e.ProviderID = events.Ptr(v.ServiceID), events.Ptr(v.ProviderID)
			e.ServiceName, e.ProviderName = events.Ptr(v.ServiceName), events.Ptr(v.ProviderName)
			e.MappingVersion = events.Ptr(events.Tuple([]any{1, e.ClusterName, e.DeploymentEnvironment, *e.RequestHost, v.ServiceID, v.ProviderID}))
		}
	}
	if customer != nil {
		r := customer.freeze(now)
		e.CustomerMappingStatus = r.status
		if r.status == "mapped" {
			v := r.value
			e.ApiKeyID, e.CustomerID, e.CustomerName = events.Ptr(v.APIKeyID), events.Ptr(v.CustomerID), events.Ptr(v.CustomerName)
			e.IdentityStatus = "known"
			e.CustomerMappingVersion = events.Ptr(events.Tuple([]any{1, e.ClusterName, e.DeploymentEnvironment, v.APIKeyID, v.CustomerID}))
		}
	}
	state.mu.Unlock()
	s.Health.Count("service_mapping_" + e.MappingStatus)
	s.Health.Count("customer_mapping_" + e.CustomerMappingStatus)
	return e
}

func validService(v identitylookup.ServiceIdentity) bool {
	return identitylookup.ValidID(v.ServiceID) && identitylookup.ValidID(v.ProviderID) && identitylookup.ValidName(v.ServiceName) && identitylookup.ValidName(v.ProviderName)
}

func validCustomer(v identitylookup.CustomerIdentity) bool {
	return identitylookup.ValidID(v.APIKeyID) && identitylookup.ValidID(v.CustomerID) && identitylookup.ValidName(v.CustomerName)
}
