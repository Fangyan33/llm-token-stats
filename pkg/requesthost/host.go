package requesthost

import (
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
