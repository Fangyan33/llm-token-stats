// Package identitylookup defines the integration boundary. No network adapter
// is implemented; normal deployments must never manufacture an identity.
package identitylookup

import (
	"context"
	"errors"
	"unicode/utf8"
)

var ErrLookupNotImplemented = errors.New("lookup_not_implemented")

type ServiceLookupRequest struct {
	RequestHost           string
	ClusterName           string
	DeploymentEnvironment string
}

type ServiceIdentity struct {
	ServiceID  string
	ProviderID string
}

type CustomerIdentity struct {
	APIKeyID   string
	CustomerID string
}

type ServiceResolver interface {
	Resolve(context.Context, ServiceLookupRequest) (ServiceIdentity, error)
}

type CustomerResolver interface {
	// Resolve must not retain or log token. The caller clears it on return.
	Resolve(context.Context, []byte) (CustomerIdentity, error)
}

type UnimplementedServiceResolver struct{}
type UnimplementedCustomerResolver struct{}

func (UnimplementedServiceResolver) Resolve(context.Context, ServiceLookupRequest) (ServiceIdentity, error) {
	return ServiceIdentity{}, ErrLookupNotImplemented
}
func (UnimplementedCustomerResolver) Resolve(context.Context, []byte) (CustomerIdentity, error) {
	return CustomerIdentity{}, ErrLookupNotImplemented
}

func ValidID(s string) bool { return s != "" && len(s) <= 128 && utf8.ValidString(s) }

func Failure(err error) string {
	if errors.Is(err, ErrLookupNotImplemented) {
		return "lookup_not_implemented"
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "lookup_timeout"
	}
	return "lookup_unavailable"
}
