// Package requestmeta bounds optional, untrusted request-header snapshots.
package requestmeta

import (
	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
	"strings"
	"unicode/utf8"

	"github.com/Fangyan33/llm-token-stats/pkg/requestid"
)

const MaxUserID = 512
const MaxUserAgent = 2048
const MaxHeaders = 16
const MaxValue = 1024
const MaxJSONBytes = 4096

func Allowed(name string) bool {
	switch strings.ToLower(name) {
	case "authorization", "proxy-authorization", "cookie", "set-cookie", "x-api-key", "api-key", "x-remote-data":
		return false
	}
	return true
}

func Normalize(names []string, max int) ([]string, error) {
	r := &configdiag.Report{}
	r.Number(int64(len(names)), 0, int64(max), "config_request_headers", "$")
	out := make([]string, 0, len(names))
	seen := map[string]bool{}
	for i, name := range names {
		field := configdiag.Index("", i)
		n, err := requestid.NormalizeHeaders([]string{name})
		r.Check(err == nil, "config_request_headers", field, "invalid_header_name", "HTTP token of 1..128 bytes")
		r.Check(Allowed(name), "config_request_headers", field, "sensitive_header", "header outside the sensitive-header denylist")
		if err != nil {
			continue
		}
		r.Check(!seen[n[0]], "config_request_headers", field, "duplicate_header", "case-insensitively unique header")
		seen[n[0]] = true
		out = append(out, n[0])
	}
	if err := r.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func Value(s string, max int) bool {
	if len(s) == 0 || len(s) > max || !utf8.ValidString(s) || strings.TrimSpace(s) == "" {
		return false
	}
	for _, c := range s {
		if c < 32 && c != '\t' || c == 127 {
			return false
		}
	}
	return true
}

// quotedSize matches encoding/json, including HTML and U+2028/U+2029 escapes,
// without reflection or allocating a serialization inside the Wasm callback.
func quotedSize(s string) int {
	n := 2
	for _, c := range s {
		switch {
		case c == '"' || c == '\\' || c == '\n' || c == '\r' || c == '\t' || c == '\b' || c == '\f':
			n += 2
		case c < 32 || c == '<' || c == '>' || c == '&' || c == '\u2028' || c == '\u2029':
			n += 6
		default:
			n += utf8.RuneLen(c)
		}
	}
	return n
}

func JSONSize(userID, agent *string, headers map[string]string) int {
	n := len(`{"user_id":,"user_agent":,"request_headers":{}}`)
	for _, s := range []*string{userID, agent} {
		if s == nil {
			n += 4
		} else {
			n += quotedSize(*s)
		}
	}
	for k, v := range headers {
		n += quotedSize(k) + 1 + quotedSize(v) + 1
	}
	if len(headers) > 0 {
		n--
	}
	return n
}

func Valid(userID, agent *string, headers map[string]string) bool {
	if userID != nil && !Value(*userID, MaxUserID) || agent != nil && !Value(*agent, MaxUserAgent) || len(headers) > MaxHeaders {
		return false
	}
	for k, v := range headers {
		n, err := Normalize([]string{k}, 1)
		if err != nil || n[0] != k || !Value(v, MaxValue) {
			return false
		}
	}
	return JSONSize(userID, agent, headers) <= MaxJSONBytes
}
