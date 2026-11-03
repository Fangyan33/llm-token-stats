// Package requestmeta bounds optional, untrusted request-header snapshots.
package requestmeta

import (
	"errors"
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
	if len(names) > max {
		return nil, errors.New("config_request_headers")
	}
	// Reuse the HTTP token grammar and case-insensitive duplicate detection.
	out := make([]string, 0, len(names))
	seen := map[string]bool{}
	for _, name := range names {
		n, err := requestid.NormalizeHeaders([]string{name})
		if err != nil || !Allowed(name) {
			return nil, errors.New("config_request_headers")
		}
		if seen[n[0]] {
			return nil, errors.New("config_request_headers")
		}
		seen[n[0]] = true
		out = append(out, n[0])
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
