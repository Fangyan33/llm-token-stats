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
	host := strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 61)
	if len(host) != 253 || Normalize(host) != host || Normalize(host+"x") != "" {
		t.Fatal("host byte limit")
	}
}
