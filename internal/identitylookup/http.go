package identitylookup

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/config"
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
	logger          *log.Logger
	logQueue        *responseLogQueue
}
type serviceRemote struct{ *remote }
type customerRemote struct{ *remote }

func newRemote(c config.Lookup) (*remote, error) {
	u, err := url.Parse(c.URI)
	if err != nil || c.URI == "" || !config.LookupURI(c.URI) || c.MaxRPS < 0 || c.MaxRPS > 100000 {
		return nil, errors.New("lookup_config")
	}
	credential := os.Getenv(c.CredentialEnv)
	if len(credential) == 0 || len(credential) > 4096 {
		return nil, errors.New("lookup_credential")
	}
	for _, b := range []byte(credential) {
		if b <= ' ' || b >= 127 {
			return nil, errors.New("lookup_credential")
		}
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if u.Scheme == "https" && c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, errors.New("lookup_ca")
		}
		pool, err := x509.SystemCertPool()
		if err != nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("lookup_ca")
		}
		tlsConfig.RootCAs = pool
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
	r, err := newRemote(c)
	if err != nil {
		return nil, err
	}
	return serviceRemote{r}, nil
}
func NewCustomer(c config.Lookup) (CustomerResolver, error) {
	if c.URI == "" {
		return UnimplementedCustomerResolver{}, nil
	}
	r, err := newRemote(c)
	if err != nil {
		return nil, err
	}
	return customerRemote{r}, nil
}

func (r *remote) post(ctx context.Context, input, output any, notFound string, record *responseRecord) error {
	if !r.allow() {
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
		if ctx.Err() != nil {
			return failure("lookup_timeout")
		}
		var timeout interface{ Timeout() bool }
		if errors.As(err, &timeout) && timeout.Timeout() {
			return failure("lookup_timeout")
		}
		return failure("lookup_unavailable")
	}
	defer res.Body.Close()
	record.HTTPStatus = res.StatusCode
	if res.StatusCode == 401 || res.StatusCode == 403 || res.StatusCode >= 500 {
		return failure("lookup_unavailable")
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, maxResponseBytes+1))
	defer clear(raw)
	if err != nil {
		if ctx.Err() != nil {
			return failure("lookup_timeout")
		}
		var timeout interface{ Timeout() bool }
		if errors.As(err, &timeout) && timeout.Timeout() {
			return failure("lookup_timeout")
		}
		return failure("lookup_unavailable")
	}
	kind, _, mediaErr := mime.ParseMediaType(res.Header.Get("Content-Type"))
	if mediaErr != nil || kind != "application/json" || len(raw) > maxResponseBytes {
		return failure("lookup_invalid_response")
	}
	if res.StatusCode == 200 {
		var envelope struct {
			ResponseCode string          `json:"responseCode"`
			ResponseMsg  json.RawMessage `json:"responseMsg"`
			Success      *bool           `json:"success"`
			Data         json.RawMessage `json:"data"`
		}
		if strictjson.Decode(raw, &envelope) != nil {
			return failure("lookup_invalid_response")
		}
		var message string
		if len(envelope.ResponseMsg) != 0 && (envelope.ResponseMsg[0] != '"' || strictjson.Decode(envelope.ResponseMsg, &message) != nil) {
			return failure("lookup_invalid_response")
		}
		if envelope.ResponseCode == "" || envelope.Success == nil {
			return failure("lookup_invalid_response")
		}
		record.ResponseCode, record.ResponseMsg, record.Success = envelope.ResponseCode, message, envelope.Success
		if envelope.ResponseCode == "000000" {
			if !*envelope.Success || len(envelope.Data) == 0 || bytes.Equal(envelope.Data, []byte("null")) || strictjson.Decode(envelope.Data, output) != nil {
				return failure("lookup_invalid_response")
			}
			return nil
		}
		if *envelope.Success || (len(envelope.Data) != 0 && !bytes.Equal(envelope.Data, []byte("null"))) {
			return failure("lookup_invalid_response")
		}
		switch envelope.ResponseCode {
		case "unauthenticated", "forbidden":
			return failure("lookup_unavailable")
		case "not_found":
			return failure(notFound)
		default: // Includes ambiguous_mapping and unknown business codes.
			return failure("lookup_invalid_response")
		}
	}
	var result struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if strictjson.Decode(raw, &result) != nil {
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
	var record responseRecord
	defer func() { r.logResponse("service", record, err) }()
	err = r.post(ctx, request, &result, "unknown_host", &record)
	if err == nil && (!ValidID(result.ServiceID) || !ValidID(result.ProviderID) || !ValidName(result.ServiceName) || !ValidName(result.ProviderName)) {
		err = failure("lookup_invalid_response")
	}
	if err != nil {
		return ServiceIdentity{}, err
	}
	return result, nil
}
func (r customerRemote) Resolve(ctx context.Context, token []byte) (result CustomerIdentity, err error) {
	var record responseRecord
	key := string(token)
	defer func() { r.logResponse("customer", record, err, key) }()
	err = r.post(ctx, struct {
		APIKey string `json:"api_key"`
	}{key}, &result, "unknown_api_key", &record)
	if err == nil && (!ValidID(result.APIKeyID) || !ValidID(result.CustomerID) || !ValidName(result.CustomerName)) {
		err = failure("lookup_invalid_response")
	}
	if err != nil {
		return CustomerIdentity{}, err
	}
	return result, nil
}
