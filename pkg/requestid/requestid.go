// Package requestid validates optional correlation IDs copied from request headers.
package requestid

import (
	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
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
	r := &configdiag.Report{}
	r.Number(int64(len(headers)), 0, MaxHeaders, "config_request_id_headers", "$")
	out := make([]string, 0, len(headers))
	seen := map[string]bool{}
	for i, header := range headers {
		valid := len(header) > 0 && len(header) <= MaxBytes
		for _, c := range header {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", c)) {
				valid = false
			}
		}
		field := configdiag.Index("", i)
		r.Check(valid, "config_request_id_headers", field, "invalid_header_name", "HTTP token of 1..128 bytes")
		header = strings.ToLower(header)
		r.Check(!seen[header], "config_request_id_headers", field, "duplicate_header", "case-insensitively unique header")
		seen[header] = true
		out = append(out, header)
	}
	if err := r.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
