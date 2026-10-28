package gatewayconfig

import (
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestRequestHostConfiguration(t *testing.T) {
	data, err := os.ReadFile("../../configs/gateway-observer.example.json")
	if err != nil {
		t.Fatal(err)
	}
	var maximum, oversized []string
	for i := 0; i < 1024; i++ {
		maximum = append(maximum, "h"+strconv.Itoa(i)+".example.com")
		if i < 512 {
			oversized = append(oversized, "h"+strconv.Itoa(i)+"."+strings.Repeat("a", 63)+"."+strings.Repeat("b", 63)+".example.com")
		}
	}
	for _, tc := range []struct {
		name          string
		patterns      any
		omit, invalid bool
		want          []string
	}{
		{name: "missing", omit: true, want: []string{}},
		{name: "empty", patterns: []string{}, want: []string{}},
		{name: "all", patterns: []string{"*"}, want: []string{"*"}},
		{name: "normalized", patterns: []string{"MODEL.Example.com.", "*.Example.INVALID."}, want: []string{"model.example.com", "*.example.invalid"}},
		{name: "duplicate", patterns: []string{"example.com", "EXAMPLE.COM."}, invalid: true},
		{name: "duplicate_wildcard", patterns: []string{"*.example.com", "*.EXAMPLE.COM."}, invalid: true},
		{name: "null", invalid: true},
		{name: "not_array", patterns: "example.com", invalid: true},
		{name: "port", patterns: []string{"example.com:443"}, invalid: true},
		{name: "ip", patterns: []string{"127.0.0.1"}, invalid: true},
		{name: "glob", patterns: []string{"model-*.example.com"}, invalid: true},
		{name: "unicode", patterns: []string{"K.example.com"}, invalid: true},
		{name: "long_label", patterns: []string{strings.Repeat("a", 64) + ".com"}, invalid: true},
		{name: "too_many", patterns: make([]string, 1025), invalid: true},
		{name: "maximum", patterns: maximum, want: maximum},
		{name: "document_bytes", patterns: oversized, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var c map[string]any
			if err := json.Unmarshal(data, &c); err != nil {
				t.Fatal(err)
			}
			if tc.omit {
				delete(c, "accepted_request_hosts")
			} else {
				c["accepted_request_hosts"] = tc.patterns
			}
			b, _ := json.Marshal(c)
			got, err := Decode(b)
			if (err != nil) != tc.invalid {
				t.Fatalf("Decode error=%v, want invalid=%v", err, tc.invalid)
			}
			if !tc.invalid && !reflect.DeepEqual(got.AcceptedRequestHosts, tc.want) {
				t.Fatalf("patterns=%v, want %v", got.AcceptedRequestHosts, tc.want)
			}
		})
	}
}

func TestLogicalRequestIDHeaderConfiguration(t *testing.T) {
	data, err := os.ReadFile("../../configs/gateway-observer.example.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name          string
		headers       any
		omit, invalid bool
		want          []string
	}{
		{name: "missing", omit: true, want: []string{}},
		{name: "empty", headers: []string{}, want: []string{}},
		{name: "priority_and_case", headers: []string{"X-Client-Request-Id", "request_id"}, want: []string{"x-client-request-id", "request_id"}},
		{name: "maximum_name", headers: []string{strings.Repeat("a", 128)}, want: []string{strings.Repeat("a", 128)}},
		{name: "maximum_list", headers: []string{"a", "b", "c", "d", "e", "f", "g", "h"}, want: []string{"a", "b", "c", "d", "e", "f", "g", "h"}},
		{name: "null", invalid: true},
		{name: "not_list", headers: "x-client-id", invalid: true},
		{name: "duplicate_case", headers: []string{"X-Client-Id", "x-client-id"}, invalid: true},
		{name: "empty_name", headers: []string{""}, invalid: true},
		{name: "whitespace", headers: []string{" x-client-id"}, invalid: true},
		{name: "pseudo_header", headers: []string{":path"}, invalid: true},
		{name: "non_ascii", headers: []string{"x-K-id"}, invalid: true},
		{name: "long_name", headers: []string{strings.Repeat("a", 129)}, invalid: true},
		{name: "too_many", headers: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"}, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var c map[string]any
			if err := json.Unmarshal(data, &c); err != nil {
				t.Fatal(err)
			}
			if tc.omit {
				delete(c, "logical_request_id_headers")
			} else {
				c["logical_request_id_headers"] = tc.headers
			}
			b, _ := json.Marshal(c)
			got, err := Decode(b)
			if (err != nil) != tc.invalid {
				t.Fatalf("Decode=%v, want invalid=%t", err, tc.invalid)
			}
			if !tc.invalid && !reflect.DeepEqual(got.LogicalRequestIDHeaders, tc.want) {
				t.Fatalf("headers=%v, want %v", got.LogicalRequestIDHeaders, tc.want)
			}
		})
	}
}

func TestWildcardRouteFiltersOnlyPostAndExactPath(t *testing.T) {
	c := Config{Enabled: true, Routes: []Route{{RouteName: "*", Enabled: true, Methods: []string{"POST"}, Paths: []string{"/v1/chat/completions"}}}}
	for _, tc := range []struct {
		name, route, method, path string
		want                      bool
	}{
		{"missing_route", "", "POST", "/v1/chat/completions", true},
		{"other_route", "other.named-match", "POST", "/v1/chat/completions", true},
		{"old_route", "llm-chat", "POST", "/v1/chat/completions", true},
		{"query_ignored", "", "POST", "/v1/chat/completions?model=test", true},
		{"get", "llm-chat", "GET", "/v1/chat/completions", false},
		{"head", "", "HEAD", "/v1/chat/completions", false},
		{"options", "", "OPTIONS", "/v1/chat/completions", false},
		{"put", "", "PUT", "/v1/chat/completions", false},
		{"method_case", "", "post", "/v1/chat/completions", false},
		{"other_path", "llm-chat", "POST", "/v1/embeddings", false},
		{"health", "", "POST", "/healthz", false},
		{"path_suffix", "", "POST", "/v1/chat/completions/extra", false},
		{"trailing_slash", "", "POST", "/v1/chat/completions/", false},
		{"path_case", "", "POST", "/V1/chat/completions", false},
		{"encoded_path", "", "POST", "/v1/chat/%63ompletions", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rule, _, ok := c.Match(tc.route, tc.method, tc.path)
			if ok != tc.want || ok && (rule.RouteName != "*") {
				t.Fatalf("Match = (%+v, %t), want selected=%t", rule, ok, tc.want)
			}
		})
	}
	c.Enabled = false
	if _, _, ok := c.Match("", "POST", "/v1/chat/completions"); ok {
		t.Fatal("disabled plugin selected request")
	}
	c.Enabled, c.Routes[0].Enabled = true, false
	if _, _, ok := c.Match("", "POST", "/v1/chat/completions"); ok {
		t.Fatal("disabled wildcard selected request")
	}
}

func TestExactRouteAndOrderedRuleCompatibility(t *testing.T) {
	c := Config{Enabled: true, Routes: []Route{{RouteName: "llm-chat", Enabled: true, Methods: []string{"POST"}, Paths: []string{"/v1/chat/completions"}}}}
	for _, route := range []string{"", "other", "llm-chat"} {
		if _, _, ok := c.Match(route, "POST", "/v1/chat/completions"); ok != (route == "llm-chat") {
			t.Fatalf("exact route changed: %q", route)
		}
	}
	c.Routes[0].Enabled = false
	c.Routes = append(c.Routes, Route{RouteName: "*", Enabled: true, Paths: []string{"/v1/chat/completions"}})
	if _, _, ok := c.Match("llm-chat", "POST", "/v1/chat/completions"); ok {
		t.Fatal("later wildcard bypassed an earlier disabled exact rule")
	}
	if _, _, ok := c.Match("other", "POST", "/v1/chat/completions"); !ok {
		t.Fatal("wildcard did not accept other route")
	}
}
