package logsafe

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestLevelEnvironmentValues(t *testing.T) {
	for _, tc := range []struct {
		value string
		level Level
		valid bool
	}{{"", Info, true}, {"  DEBUG ", Debug, true}, {"Info", Info, true}, {"WARN", Warn, true}, {"error", Error, true}, {"warning", Info, false}, {"nonsense", Info, false}} {
		if level, valid := ParseLevel(tc.value); level != tc.level || valid != tc.valid {
			t.Fatal(tc.value, level, valid)
		}
	}
}
func TestRedactBeforeTruncatingAndEscapeControls(t *testing.T) {
	const secret = "overlapping-business-secret"
	got := Clean(strings.Repeat("中", 338)+secret+"\n\x1b\u0085", "overlapping", secret)
	if !utf8.ValidString(got) || strings.Contains(got, "overlapping") || !strings.Contains(got, "[REDACTED]") {
		t.Fatal("secret prefix leaked at truncation", got)
	}
	got = Clean("message\n\r\t\x1b\u0085")
	if strings.ContainsAny(got, "\n\r\t\x1b\u0085") {
		t.Fatal("control characters were not escaped")
	}
}
