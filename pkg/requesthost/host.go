package requesthost

import (
	"errors"
	"net"
	"strconv"
	"strings"
	"unicode/utf8"
)

// Normalize returns only a canonical DNS name, never an IP or raw authority.
func Normalize(authority string) string {
	if len(authority) == 0 || len(authority) > 512 || !utf8.ValidString(authority) {
		return ""
	}
	// Reject Unicode before case conversion; Kelvin sign lowercases to ASCII k.
	for _, c := range authority {
		if c > 127 {
			return ""
		}
	}
	host := authority
	if strings.Contains(host, ":") {
		if strings.Count(host, ":") != 1 {
			return ""
		}
		var port string
		host, port, _ = strings.Cut(host, ":")
		for _, c := range port {
			if c < '0' || c > '9' {
				return ""
			}
		}
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return ""
		}
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if len(host) == 0 || len(host) > 253 || net.ParseIP(host) != nil {
		return ""
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return ""
		}
		for _, c := range label {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
				return ""
			}
		}
	}
	return host
}

// NormalizePattern accepts a DNS name, a leading *. suffix, or * for every
// valid DNS host. Patterns never contain ports, URLs or other glob syntax.
func NormalizePattern(pattern string) string {
	for _, c := range pattern {
		if c > 127 {
			return ""
		}
	}
	if pattern == "*" {
		return pattern
	}
	if strings.Contains(pattern, ":") {
		return ""
	}
	suffix, wildcard := strings.CutPrefix(pattern, "*.")
	host := Normalize(suffix)
	if host == "" {
		return ""
	}
	if wildcard {
		return "*." + host
	}
	return host
}

// NormalizePatterns validates a bounded allowlist at configuration load time.
func NormalizePatterns(patterns []string) ([]string, error) {
	if len(patterns) > 1024 {
		return nil, errors.New("config_request_hosts")
	}
	result := make([]string, 0, len(patterns))
	seen := map[string]bool{}
	for _, pattern := range patterns {
		host := NormalizePattern(pattern)
		if host == "" || seen[host] {
			return nil, errors.New("config_request_hosts")
		}
		seen[host] = true
		result = append(result, host)
	}
	return result, nil
}

// Match checks canonical hosts against normalized patterns. A suffix wildcard
// includes every subdomain depth, but excludes the suffix's root domain.
func Match(patterns []string, host string) bool {
	if host == "" || Normalize(host) != host {
		return false
	}
	for _, pattern := range patterns {
		if pattern == "*" || pattern == host {
			return true
		}
		if suffix, wildcard := strings.CutPrefix(pattern, "*."); wildcard && strings.HasSuffix(host, "."+suffix) {
			return true
		}
	}
	return false
}
