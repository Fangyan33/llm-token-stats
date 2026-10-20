package requesthost

import (
	"strings"
	"testing"
)

func TestNormalize(t *testing.T) {
	for input, want := range map[string]string{
		"MODEL.Example.INVALID.:443": "model.example.invalid", "xn--fsqu00a.example": "xn--fsqu00a.example",
		"localhost": "localhost", "a:1": "a", "a:65535": "a",
	} {
		if Normalize(input) != want {
			t.Fatal("valid authority normalization")
		}
	}
	for _, input := range []string{"", "127.0.0.1", "127.0.0.1:80", "[::1]:443", "::1", "a:0", "a:65536", "a:", "a:+80", "a:abc", "a..", "-a.test", "a-.test", "a_b.test", "a/path", "a?query", "a@b", "例子.example", string([]byte{255}), strings.Repeat("a", 64) + ".test", strings.Repeat("x", 513)} {
		if Normalize(input) != "" {
			t.Fatal("invalid authority accepted")
		}
	}
	for _, input := range []string{"K.example.invalid", "K.example.invalid.:443", "k.K.example.invalid", "K.example.invalid:４４３", "é.example.invalid", "k。example.invalid"} {
		if Normalize(input) != "" {
			t.Fatalf("non-ASCII authority accepted: %q", input)
		}
	}
	host := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 61)
	if len(host) != 253 || Normalize(host) != host || Normalize(host+"x") != "" {
		t.Fatal("host byte limit")
	}
}

func TestPatterns(t *testing.T) {
	for input, want := range map[string]string{
		"*": "*", "MODEL.Example.invalid.": "model.example.invalid",
		"*.Example.INVALID.": "*.example.invalid", "localhost": "localhost",
		"": "", "*.": "", "*.127.0.0.1": "", "127.0.0.1": "",
		"model-*.example.com": "", "**.example.com": "", "example.*": "",
		"?.example.com": "", "[ab].example.com": "", "example.com:443": "",
		"https://example.com": "", "*.example.com:443": "", "*.example..com": "",
		"*.example.com..": "", " example.com": "", "例子.example": "",
	} {
		if got := NormalizePattern(input); got != want {
			t.Errorf("NormalizePattern(%q) = %q, want %q", input, got, want)
		}
	}
	for _, tc := range []struct {
		pattern, host string
		want          bool
	}{
		{"model.example.com", "model.example.com", true},
		{"model.example.com", "other.example.com", false},
		{"*.example.com", "a.example.com", true},
		{"*.example.com", "a.b.example.com", true},
		{"*.example.com", "example.com", false},
		{"*.example.com", "badexample.com", false},
		{"*.example.com", "a.example.com.evil", false},
		{"*", "localhost", true}, {"*", "xn--fsqu00a.example", true},
		{"*", "", false}, {"*", "127.0.0.1", false}, {"*", "bad_host", false},
		{"*", "UPPER.example.com", false}, {"*", "example.com:443", false},
	} {
		if got := Match([]string{tc.pattern}, tc.host); got != tc.want {
			t.Errorf("Match(%q, %q) = %v, want %v", tc.pattern, tc.host, got, tc.want)
		}
	}
	if Match(nil, "example.com") || Match([]string{}, "example.com") || !Match([]string{"other.com", "*.example.com"}, "a.example.com") {
		t.Fatal("empty list or multiple patterns")
	}
}
