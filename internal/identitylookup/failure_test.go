package identitylookup

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestStaleEligibilityKeepsPublicStatusButDistinguishesCause(t *testing.T) {
	for _, tc := range []struct {
		name, body, status string
		httpStatus         int
		transportError     error
		stale              bool
	}{
		{"network", "", "lookup_unavailable", 0, errors.New("synthetic network failure"), true},
		{"timeout", "", "lookup_timeout", 0, context.DeadlineExceeded, true},
		{"server", "not JSON", "lookup_unavailable", 503, nil, true},
		{"nonstandard_status", "not JSON", "lookup_unavailable", 600, nil, false},
		{"unauthorized", `{"responseCode":"401","success":false}`, "lookup_unavailable", 401, nil, false},
		{"unauthorized_bad_body", "not JSON", "lookup_unavailable", 401, nil, false},
		{"forbidden", "not JSON", "lookup_unavailable", 403, nil, false},
		{"parameters", `{"responseCode":"100001","success":false}`, "lookup_unavailable", 200, nil, false},
		{"zero_or_multiple", `{"responseCode":"650013","success":false}`, "lookup_invalid_response", 200, nil, false},
		{"legacy_parameters", `{"error":{"code":"invalid_request"}}`, "lookup_unavailable", 400, nil, false},
		{"rate_limit", `{"error":{"code":"rate_limited"}}`, "lookup_rate_limited", 429, nil, true},
		{"invalid_rate_limit", `{"error":{"code":"other"}}`, "lookup_invalid_response", 429, nil, false},
		{"invalid", "not JSON", "lookup_invalid_response", 200, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &remote{uri: "http://synthetic.invalid", client: &http.Client{Transport: lookupTransport(func(*http.Request) (*http.Response, error) {
				if tc.transportError != nil {
					return nil, tc.transportError
				}
				return &http.Response{StatusCode: tc.httpStatus, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})}}
			var result ServiceIdentity
			var record responseRecord
			err := r.post(context.Background(), ServiceLookupRequest{}, &result, &record)
			if err == nil || Failure(err) != tc.status || err.Error() != tc.status || CanUseStale(err) != tc.stale {
				t.Fatalf("unexpected error classification: %v, stale=%v", err, CanUseStale(err))
			}
		})
	}
	for _, status := range []string{"lookup_overloaded", "lookup_rate_limited", "lookup_timeout"} {
		if !CanUseStale(failure(status)) {
			t.Fatalf("transient %s cannot fall back", status)
		}
	}
	if CanUseStale(nil) || CanUseStale(ErrLookupNotImplemented) || CanUseStale(errors.New("unknown resolver failure")) {
		t.Fatal("unclassified failure permits stale identity")
	}
}
