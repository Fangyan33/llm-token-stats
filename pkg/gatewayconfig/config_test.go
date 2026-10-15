package gatewayconfig

import "testing"

func TestWildcardRouteFiltersOnlyPostAndExactPath(t *testing.T) {
	c := Config{Enabled: true, Routes: []Route{{RouteName: "*", Enabled: true, Methods: []string{"POST"}, Paths: []string{"/v1/chat/completions"}, ServiceID: "model-service-a"}}}
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
			if ok != tc.want || ok && (rule.RouteName != "*" || rule.ServiceID != "model-service-a") {
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
