// Package requestid validates optional correlation IDs copied from request headers.
package requestid

import (
	"errors"
	"strings"
)

const MaxBytes = 128
const MaxHeaders = 8

// Valid accepts opaque IDs without changing their spelling or truncating them.
// Commas are excluded so joined duplicate header values cannot become an ID.
func Valid(value string) bool {
	if len(value) == 0 || len(value) > MaxBytes {
		return false
	}
	for _, c := range value {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("._:-", c)) {
			return false
		}
	}
	return true
}

// NormalizeHeaders preserves priority while normalizing HTTP field names.
func NormalizeHeaders(headers []string) ([]string, error) {
	if len(headers) > MaxHeaders {
		return nil, errors.New("config_request_id_headers")
	}
	out := make([]string, 0, len(headers))
	seen := map[string]bool{}
	for _, header := range headers {
		if len(header) == 0 || len(header) > MaxBytes {
			return nil, errors.New("config_request_id_headers")
		}
		for _, c := range header {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", c)) {
				return nil, errors.New("config_request_id_headers")
			}
		}
		header = strings.ToLower(header)
		if seen[header] {
			return nil, errors.New("config_request_id_headers")
		}
		seen[header] = true
		out = append(out, header)
	}
	return out, nil
}
