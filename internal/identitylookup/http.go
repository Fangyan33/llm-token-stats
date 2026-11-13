package identitylookup

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/logging"
	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
	"github.com/Fangyan33/llm-token-stats/pkg/strictjson"
)

const (
	maxResponseBytes          = 8192
	lookupDialTimeout         = 2 * time.Second
	lookupTLSHandshakeTimeout = 2 * time.Second
)

func newLookupDialer() *net.Dialer {
	return &net.Dialer{Timeout: lookupDialTimeout, KeepAlive: 30 * time.Second}
}

type remote struct {
	uri, credential string
	client          *http.Client
	mu              sync.Mutex
	tokens          float64
	last            time.Time
	maxRPS          int
	logger          *logging.Logger
}
type serviceRemote struct{ *remote }
type customerRemote struct{ *remote }

func newRemote(c config.Lookup) (*remote, error) { return newRemoteAt(c, "lookup") }
func newRemoteAt(c config.Lookup, prefix string) (*remote, error) {
	u, err := url.Parse(c.URI)
	r := &configdiag.Report{}
	r.Check(err == nil && c.URI != "" && config.LookupURI(c.URI), "lookup_config", prefix+".uri", "invalid_uri", "HTTP(S) URI without credentials, query or fragment")
	r.Number(int64(c.MaxRPS), 0, 100000, "lookup_config", prefix+".max_requests_per_second")
	credential := os.Getenv(c.CredentialEnv)
	reason := ""
	if len(credential) == 0 {
		reason = "environment_variable_empty"
	} else if len(credential) > 4096 {
		reason = "credential_too_long"
	} else {
		for _, b := range []byte(credential) {
			if b <= ' ' || b >= 127 {
				reason = "credential_invalid_character"
				break
			}
		}
	}
	if reason != "" {
		r.Merge(configdiag.Environment("lookup_credential", prefix+".credential_env", c.CredentialEnv, reason, "1..4096 printable ASCII bytes without whitespace"))
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if u != nil && u.Scheme == "https" && c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			r.Merge(configdiag.New("lookup_ca", prefix+".ca_file", "file_read_failed", "readable PEM CA certificate file", err))
		} else {
			pool, err := x509.SystemCertPool()
			if err != nil {
				pool = x509.NewCertPool()
			}
			r.Check(pool.AppendCertsFromPEM(pem), "lookup_ca", prefix+".ca_file", "invalid_ca_pem", "PEM CA certificate file")
			tlsConfig.RootCAs = pool
		}
	}
	if err := r.Err(); err != nil {
		return nil, err
	}
	// No environment proxy, redirects, cookies, tracing or automatic retries.
	// Transport may continue connecting after the request has returned. Bound
	// both phases independently so abandoned connections release host capacity.
	transport := &http.Transport{
		DialContext:     newLookupDialer().DialContext,
		TLSClientConfig: tlsConfig, TLSHandshakeTimeout: lookupTLSHandshakeTimeout,
		MaxIdleConns: 64, MaxIdleConnsPerHost: 64, MaxConnsPerHost: 256,
		IdleConnTimeout: 30 * time.Second, MaxResponseHeaderBytes: 8192, DisableCompression: true,
	}
	rps := c.MaxRPS
	if rps == 0 {
		rps = 1000
	}
	return &remote{uri: c.URI, credential: credential, maxRPS: rps, tokens: float64(rps), last: time.Now(), client: &http.Client{Transport: transport, Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

func NewService(c config.Lookup) (ServiceResolver, error) {
	if c.URI == "" {
		return UnimplementedServiceResolver{}, nil
	}
	r, err := newRemoteAt(c, "observer.service_lookup")
	if err != nil {
		return nil, err
	}
	return serviceRemote{r}, nil
}
func NewCustomer(c config.Lookup) (CustomerResolver, error) {
	if c.URI == "" {
		return UnimplementedCustomerResolver{}, nil
	}
	r, err := newRemoteAt(c, "observer.customer_lookup")
	if err != nil {
		return nil, err
	}
	return customerRemote{r}, nil
}

func (r *remote) post(ctx context.Context, input, output any, record *responseRecord) error {
	if !r.allow() {
		record.Diagnostic = []any{"reason", "endpoint_rate_limit", "limit", r.maxRPS}
		return failure("lookup_overloaded")
	}
	body, err := json.Marshal(input)
	if err != nil {
		return failure("lookup_invalid_response")
	}
	defer clear(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.uri, bytes.NewReader(body))
	if err != nil {
		return failure("lookup_unavailable")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("x-remote-data", r.credential)
	// NewRequest populates GetBody; prohibit body replay on redirects/retries.
	req.GetBody = nil
	res, err := r.client.Do(req)
	if err != nil {
		record.Diagnostic = append([]any{"phase", "request"}, logging.ErrorFields(err)...)
		if ctx.Err() != nil {
			return failure("lookup_timeout")
		}
		var timeout interface{ Timeout() bool }
		if errors.As(err, &timeout) && timeout.Timeout() {
			return failure("lookup_timeout")
		}
		return transientFailure("lookup_unavailable")
	}
	defer res.Body.Close()
	record.HTTPStatus = res.StatusCode
	if res.StatusCode == http.StatusUnauthorized || res.StatusCode == http.StatusForbidden {
		// Authentication is known from headers. Reading a slow diagnostic body
		// must not let the coordinator reinterpret it as a transient timeout.
		reportFailure(ctx, failure("lookup_unavailable"))
	}
	if res.StatusCode >= 500 {
		record.Diagnostic = []any{"reason", "http_server_error"}
		if res.StatusCode < 600 {
			return transientFailure("lookup_unavailable")
		}
		return failure("lookup_unavailable")
	}
	if res.StatusCode == 403 {
		record.Diagnostic = []any{"reason", "http_authentication_rejected"}
		return failure("lookup_unavailable")
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, maxResponseBytes+1))
	defer clear(raw)
	kind, _, mediaErr := mime.ParseMediaType(res.Header.Get("Content-Type"))
	if res.StatusCode == http.StatusUnauthorized {
		// The HTTP status establishes authentication failure even when an
		// intermediary returns a malformed body. Only log valid contract fields.
		if err == nil && mediaErr == nil && kind == "application/json" && len(raw) <= maxResponseBytes {
			candidate := responseRecord{HTTPStatus: res.StatusCode}
			data, decodeErr := decodeResponseEnvelope(raw, &candidate)
			if decodeErr == nil && candidate.ResponseCode == "401" && !*candidate.Success && emptyResponseData(data) {
				*record = candidate
			}
		}
		record.Diagnostic = []any{"reason", "http_authentication_rejected"}
		return failure("lookup_unavailable")
	}
	if err != nil {
		record.Diagnostic = append([]any{"phase", "read_response"}, logging.ErrorFields(err)...)
		if ctx.Err() != nil {
			return failure("lookup_timeout")
		}
		var timeout interface{ Timeout() bool }
		if errors.As(err, &timeout) && timeout.Timeout() {
			return failure("lookup_timeout")
		}
		return transientFailure("lookup_unavailable")
	}
	if mediaErr != nil || kind != "application/json" {
		record.Diagnostic = []any{"reason", "invalid_content_type", "expected", "application/json"}
		return failure("lookup_invalid_response")
	}
	if len(raw) > maxResponseBytes {
		record.Diagnostic = []any{"reason", "response_too_large", "limit", maxResponseBytes}
		return failure("lookup_invalid_response")
	}
	if res.StatusCode == 200 {
		data, err := decodeResponseEnvelope(raw, record)
		if err != nil {
			record.Diagnostic = []any{"reason", "invalid_response_envelope"}
			return failure("lookup_invalid_response")
		}
		if record.ResponseCode == "000000" {
			if !*record.Success || emptyResponseData(data) || strictjson.Decode(data, output) != nil {
				record.Diagnostic = []any{"reason", "invalid_success_payload"}
				return failure("lookup_invalid_response")
			}
			return nil
		}
		if *record.Success || !emptyResponseData(data) {
			record.Diagnostic = []any{"reason", "inconsistent_failure_envelope"}
			return failure("lookup_invalid_response")
		}
		record.Diagnostic = []any{"reason", "business_response_rejected"}
		switch record.ResponseCode {
		case "100001":
			return failure("lookup_unavailable")
		case "650013":
			// This code covers both zero and multiple matches; it cannot prove
			// absence and must never produce unknown_host/unknown_api_key.
			return failure("lookup_invalid_response")
		default:
			return failure("lookup_invalid_response")
		}
	}
	var result struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if strictjson.Decode(raw, &result) != nil {
		record.Diagnostic = []any{"reason", "invalid_error_envelope"}
		return failure("lookup_invalid_response")
	}
	switch {
	case res.StatusCode == 400 && result.Error.Code == "invalid_request":
		return failure("lookup_unavailable")
	case res.StatusCode == 429 && result.Error.Code == "rate_limited":
		return failure("lookup_rate_limited")
	default:
		return failure("lookup_invalid_response")
	}
}

func emptyResponseData(data json.RawMessage) bool {
	return len(data) == 0 || bytes.Equal(bytes.TrimSpace(data), []byte("null"))
}

func decodeResponseEnvelope(raw []byte, record *responseRecord) (json.RawMessage, error) {
	var envelope struct {
		ResponseCode string          `json:"responseCode"`
		ResponseMsg  json.RawMessage `json:"responseMsg"`
		Success      *bool           `json:"success"`
		Data         json.RawMessage `json:"data"`
	}
	if strictjson.Decode(raw, &envelope) != nil || envelope.ResponseCode == "" || envelope.Success == nil {
		return nil, failure("lookup_invalid_response")
	}
	var message string
	if len(envelope.ResponseMsg) != 0 && (envelope.ResponseMsg[0] != '"' || strictjson.Decode(envelope.ResponseMsg, &message) != nil) {
		return nil, failure("lookup_invalid_response")
	}
	record.ResponseCode, record.ResponseMsg, record.Success = envelope.ResponseCode, message, envelope.Success
	return envelope.Data, nil
}

// Independent per-endpoint token buckets; never wait for capacity or retain a token.
func (r *remote) allow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.maxRPS == 0 {
		return true
	} // Injected test transport.
	now := time.Now()
	r.tokens = min(float64(r.maxRPS), r.tokens+now.Sub(r.last).Seconds()*float64(r.maxRPS))
	r.last = now
	if r.tokens < 1 {
		return false
	}
	r.tokens--
	return true
}

func (r serviceRemote) Resolve(ctx context.Context, request ServiceLookupRequest) (result ServiceIdentity, err error) {
	started := time.Now()
	var record responseRecord
	defer func() { record.ElapsedMS = time.Since(started).Milliseconds(); r.logResponse("service", record, err) }()
	err = r.post(ctx, request, &result, &record)
	if err == nil && (!ValidID(result.ServiceID) || !ValidID(result.ProviderID) || !ValidName(result.ServiceName) || !ValidName(result.ProviderName)) {
		err = failure("lookup_invalid_response")
	}
	if err != nil {
		return ServiceIdentity{}, err
	}
	return result, nil
}
func (r customerRemote) Resolve(ctx context.Context, token []byte) (result CustomerIdentity, err error) {
	started := time.Now()
	var record responseRecord
	key := string(token)
	defer func() {
		record.ElapsedMS = time.Since(started).Milliseconds()
		r.logResponse("customer", record, err, key)
	}()
	err = r.post(ctx, struct {
		APIKey string `json:"api_key"`
	}{key}, &result, &record)
	if err == nil && (!ValidID(result.APIKeyID) || !ValidID(result.CustomerID) || !ValidName(result.CustomerName)) {
		err = failure("lookup_invalid_response")
	}
	if err != nil {
		return CustomerIdentity{}, err
	}
	return result, nil
}
