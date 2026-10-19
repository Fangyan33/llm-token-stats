package observation

import (
	"context"
	"sync"

	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/Fangyan33/llm-token-stats/internal/identitylookup"
	"github.com/Fangyan33/llm-token-stats/pkg/credentials"
)

type lookupResult struct {
	service  bool
	status   string
	route    identitylookup.ServiceIdentity
	customer identitylookup.CustomerIdentity
}

func (s *Server) resolve(ctx context.Context, st *pb.Start) events.Event {
	return s.resolveQueries(ctx, st, nil)
}

// Only this coordinator mutates the snapshot. Workers publish once into a
// two-entry channel, so a late return cannot block or alter a frozen event.
// finished releases admission only after the actual workers have exited.
func (s *Server) resolveQueries(ctx context.Context, st *pb.Start, finished func()) events.Event {
	e := s.emptySnapshot(st, "lookup_timeout")
	token := st.ApiKey
	st.ApiKey, st.RequestAuthority = nil, ""
	results := make(chan lookupResult, 2)
	var workers sync.WaitGroup
	pending := 0
	if e.RequestHost != nil {
		request := identitylookup.ServiceLookupRequest{RequestHost: *e.RequestHost, ClusterName: e.ClusterName, DeploymentEnvironment: e.DeploymentEnvironment}
		pending++
		workers.Add(1)
		go func() {
			defer workers.Done()
			resolver := s.ServiceResolver
			if resolver == nil {
				resolver = identitylookup.UnimplementedServiceResolver{}
			}
			value, err := resolver.Resolve(ctx, request)
			r := lookupResult{service: true, status: "mapped"}
			if err != nil {
				r.status = identitylookup.Failure(err)
			} else if !identitylookup.ValidID(value.ServiceID) || !identitylookup.ValidID(value.ProviderID) || !identitylookup.ValidName(value.ServiceName) || !identitylookup.ValidName(value.ProviderName) {
				r.status = "lookup_invalid_response"
			} else {
				r.route = value
			}
			results <- r
		}()
	}
	if credentials.ValidToken(token) {
		pending++
		workers.Add(1)
		go func() {
			defer workers.Done()
			resolver := s.CustomerResolver
			if resolver == nil {
				resolver = identitylookup.UnimplementedCustomerResolver{}
			}
			value, err := resolver.Resolve(ctx, token)
			clear(token) // Resolve no longer reads the borrowed bytes.
			r := lookupResult{status: "mapped"}
			if err != nil {
				r.status = identitylookup.Failure(err)
			} else if !identitylookup.ValidID(value.APIKeyID) || !identitylookup.ValidID(value.CustomerID) || !identitylookup.ValidName(value.CustomerName) {
				r.status = "lookup_invalid_response"
			} else {
				r.customer = value
			}
			results <- r
		}()
	} else {
		clear(token)
	}
	if finished != nil {
		go func() { workers.Wait(); finished() }()
	}
	apply := func(r lookupResult) {
		pending--
		if r.service {
			e.MappingStatus = r.status
			if r.status == "mapped" {
				v := r.route
				e.ServiceID, e.ProviderID = events.Ptr(v.ServiceID), events.Ptr(v.ProviderID)
				e.ServiceName, e.ProviderName = events.Ptr(v.ServiceName), events.Ptr(v.ProviderName)
				e.MappingVersion = events.Ptr(events.Tuple([]any{1, e.ClusterName, e.DeploymentEnvironment, *e.RequestHost, v.ServiceID, v.ProviderID}))
			}
		} else {
			e.CustomerMappingStatus = r.status
			if r.status == "mapped" {
				v := r.customer
				e.ApiKeyID, e.CustomerID, e.CustomerName = events.Ptr(v.APIKeyID), events.Ptr(v.CustomerID), events.Ptr(v.CustomerName)
				e.IdentityStatus = "known"
				e.CustomerMappingVersion = events.Ptr(events.Tuple([]any{1, e.ClusterName, e.DeploymentEnvironment, v.APIKeyID, v.CustomerID}))
			}
		}
	}
collect:
	for pending > 0 {
		select {
		case r := <-results:
			apply(r)
		case <-ctx.Done():
			// Preserve results already handed off before freezing. Missing sides
			// retain lookup_timeout; invalid inputs retain their invalid status.
			for pending > 0 {
				select {
				case r := <-results:
					apply(r)
				default:
					break collect
				}
			}
		}
	}
	s.Health.Count("service_mapping_" + e.MappingStatus)
	s.Health.Count("customer_mapping_" + e.CustomerMappingStatus)
	return e
}
