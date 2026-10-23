// Package identitylookup resolves immutable request attribution snapshots.
package identitylookup

import (
	"context"
	"errors"
	"unicode/utf8"
)

var ErrLookupNotImplemented = errors.New("lookup_not_implemented")

type ServiceLookupRequest struct {
	RequestHost           string `json:"request_host"`
	ClusterName           string `json:"cluster_name"`
	DeploymentEnvironment string `json:"deployment_environment"`
}

type ServiceIdentity struct {
	ServiceID    string `json:"service_id"`
	ServiceName  string `json:"service_name"`
	ProviderID   string `json:"provider_id"`
	ProviderName string `json:"provider_name"`
}

type CustomerIdentity struct {
	APIKeyID     string `json:"api_key_id"`
	CustomerID   string `json:"customer_id"`
	CustomerName string `json:"customer_name"`
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

func ValidID(s string) bool   { return s != "" && len(s) <= 128 && utf8.ValidString(s) }
func ValidName(s string) bool { return s != "" && len(s) <= 512 && utf8.ValidString(s) }

type lookupError struct {
	code      string
	transient bool
}

func (e lookupError) Error() string { return e.code }
func failure(code string) error {
	return lookupError{code: code, transient: code == "lookup_timeout" || code == "lookup_overloaded" || code == "lookup_rate_limited"}
}
func transientFailure(code string) error { return lookupError{code: code, transient: true} }

// CanUseStale is deliberately narrower than Failure: authentication and input
// errors share lookup_unavailable with transport failures, but revoke old data.
func CanUseStale(err error) bool {
	var e lookupError
	if errors.As(err, &e) {
		return e.transient
	}
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

func Failure(err error) string {
	var e lookupError
	if errors.As(err, &e) {
		return e.code
	}
	if errors.Is(err, ErrLookupNotImplemented) {
		return "lookup_not_implemented"
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "lookup_timeout"
	}
	return "lookup_unavailable"
}
