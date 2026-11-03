package requestmeta

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestJSONBudgetMatchesCanonicalEscaping(t *testing.T) {
	for _, value := range []string{"plain", "用户\t<&>\u2028\u2029\"\\", strings.Repeat("<", 512)} {
		var agent *string
		headers := map[string]string{"x-team": value, "x-other": "v"}
		b, err := json.Marshal(struct {
			UserID    *string           `json:"user_id"`
			UserAgent *string           `json:"user_agent"`
			Headers   map[string]string `json:"request_headers"`
		}{&value, agent, headers})
		if err != nil || JSONSize(&value, agent, headers) != len(b) {
			t.Fatal("JSON size mismatch")
		}
	}
}

func TestValuesAndConfiguration(t *testing.T) {
	for _, names := range [][]string{{"authorization"}, {"Cookie"}, {"x-api-key"}, {"x-remote-data"}, {"x-user", "X-User"}, {":path"}, {""}} {
		if _, err := Normalize(names, 16); err == nil {
			t.Fatal("invalid or credential header accepted")
		}
	}
	for _, s := range []string{"", " \t", "bad\rvalue", "bad\nvalue", "\x00", "\xff", strings.Repeat("a", 513)} {
		if Value(s, MaxUserID) {
			t.Fatal("invalid user ID accepted")
		}
	}
	uid := "用户@example.org"
	if !Valid(&uid, nil, map[string]string{"x-team": "团队"}) {
		t.Fatal("opaque Unicode metadata rejected")
	}
	if Valid(nil, nil, map[string]string{"X-Team": "x"}) || Valid(nil, nil, map[string]string{"authorization": "x"}) {
		t.Fatal("unsafe key accepted")
	}
	large := strings.Repeat("<", MaxUserAgent)
	if Valid(nil, &large, nil) {
		t.Fatal("escaped JSON budget ignored")
	}
}
