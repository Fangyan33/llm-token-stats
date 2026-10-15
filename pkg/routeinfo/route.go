// Package routeinfo normalizes optional gateway diagnostics, never tenant identity.
package routeinfo

import "unicode/utf8"

// Name keeps a bounded actual Envoy route name, or an explicit unknown marker.
// A wildcard is a selection rule and cannot identify an actual route.
func Name(actual string) string {
	if actual == "" || actual == "*" || len(actual) > 128 || !utf8.ValidString(actual) {
		return "unknown"
	}
	return actual
}
