package observation

import (
	"bytes"
	"context"
	"errors"
	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/Fangyan33/llm-token-stats/internal/health"
	"github.com/Fangyan33/llm-token-stats/internal/identitylookup"
	"testing"
	"time"
)

type serviceFunc func(context.Context, identitylookup.ServiceLookupRequest) (identitylookup.ServiceIdentity, error)

func (f serviceFunc) Resolve(c context.Context, r identitylookup.ServiceLookupRequest) (identitylookup.ServiceIdentity, error) {
	return f(c, r)
}

func TestIndependentQueriesShareDeadline(t *testing.T) {
	s := &Server{Cfg: config.Observer{ClusterName: "cluster", DeploymentEnvironment: "stg"}, Health: &health.State{}}
	s.ServiceResolver = serviceFunc(func(ctx context.Context, _ identitylookup.ServiceLookupRequest) (identitylookup.ServiceIdentity, error) {
		<-ctx.Done()
		return identitylookup.ServiceIdentity{}, ctx.Err()
	})
	s.CustomerResolver = customerFake{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	e := s.resolve(ctx, &pb.Start{RequestAuthority: "model.example.com", ApiKey: []byte("synthetic-private")})
	if e.MappingStatus != "lookup_timeout" || e.CustomerMappingStatus != "mapped" || e.CustomerName == nil || e.IdentityStatus != "known" {
		t.Fatal("failed service erased independently resolved customer")
	}
}

type customerFunc func(context.Context, []byte) (identitylookup.CustomerIdentity, error)

func (f customerFunc) Resolve(c context.Context, b []byte) (identitylookup.CustomerIdentity, error) {
	return f(c, b)
}

func TestIdentityLookupSnapshotsAndCredentialLifetime(t *testing.T) {
	for _, mode := range []string{"unimplemented", "mapped", "service_failure", "customer_failure", "invalid_host", "invalid_key", "partial_service", "partial_customer"} {
		t.Run(mode, func(t *testing.T) {
			s := &Server{Cfg: config.Observer{ClusterName: "cluster-a", DeploymentEnvironment: "stg"}, Health: &health.State{}}
			st := &pb.Start{RequestAuthority: "MODEL.Example.invalid:443", ApiKey: []byte("synthetic-sentinel-key")}
			var borrowed []byte
			serviceCalls, customerCalls := 0, 0
			if mode != "unimplemented" {
				s.ServiceResolver = serviceFunc(func(_ context.Context, r identitylookup.ServiceLookupRequest) (identitylookup.ServiceIdentity, error) {
					serviceCalls++
					if r.RequestHost != "model.example.invalid" || r.ClusterName != "cluster-a" || r.DeploymentEnvironment != "stg" {
						t.Fatal("lookup context changed")
					}
					if mode == "service_failure" {
						return identitylookup.ServiceIdentity{}, errors.New("private response that must not be logged")
					}
					if mode == "partial_service" {
						return identitylookup.ServiceIdentity{ServiceID: "service-a"}, nil
					}
					return identitylookup.ServiceIdentity{ServiceID: "service-a", ProviderID: "provider-a", ServiceName: "服务 A", ProviderName: "供应商 A"}, nil
				})
				s.CustomerResolver = customerFunc(func(_ context.Context, key []byte) (identitylookup.CustomerIdentity, error) {
					customerCalls++
					borrowed = key
					if !bytes.Equal(key, []byte("synthetic-sentinel-key")) {
						t.Fatal("wrong credential input")
					}
					if mode == "customer_failure" {
						return identitylookup.CustomerIdentity{}, context.DeadlineExceeded
					}
					if mode == "partial_customer" {
						return identitylookup.CustomerIdentity{APIKeyID: "key-a"}, nil
					}
					return identitylookup.CustomerIdentity{APIKeyID: "key-a", CustomerID: "customer-a", CustomerName: "客户 A"}, nil
				})
			}
			if mode == "invalid_host" {
				st.RequestAuthority = "127.0.0.1"
			}
			if mode == "invalid_key" {
				st.ApiKey = []byte("bad credential")
			}
			e := s.resolve(context.Background(), st)
			if st.ApiKey != nil || st.RequestAuthority != "" || bytes.Count(borrowed, []byte{0}) != len(borrowed) {
				t.Fatal("temporary credential retained")
			}
			b, _ := events.Canonical(&e)
			if bytes.Contains(b, []byte("sentinel")) || bytes.Contains(b, []byte(`"api_key"`)) {
				t.Fatal("credential persisted")
			}
			if mode == "unimplemented" {
				if e.MappingStatus != "lookup_not_implemented" || e.CustomerMappingStatus != "lookup_not_implemented" || e.ServiceID != nil || e.ProviderID != nil || e.ApiKeyID != nil || e.CustomerID != nil {
					t.Fatal("stub manufactured identity")
				}
				return
			}
			if (serviceCalls == 0) != (mode == "invalid_host") || (customerCalls == 0) != (mode == "invalid_key") {
				t.Fatal("invalid input queried")
			}
			if (e.ServiceID != nil) != (mode != "invalid_host" && mode != "service_failure" && mode != "partial_service") {
				t.Fatal("service failure isolation")
			}
			if (e.IdentityStatus == "known") != (mode != "invalid_key" && mode != "customer_failure" && mode != "partial_customer") {
				t.Fatal("customer failure isolation")
			}
			if mode == "mapped" && (e.MappingVersion == nil || e.CustomerMappingVersion == nil || len(*e.MappingVersion) != 64) {
				t.Fatal("missing result version")
			}
		})
	}
}
