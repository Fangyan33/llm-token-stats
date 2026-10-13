package strictjson

import (
	"bytes"
	"testing"
)

func TestStrictJSON(t *testing.T) {
	for _, b := range []string{`{"a":1,"a":2}`, `{"a":"\ud800"}`, `{"a":"\udc00"}`, `[1,]`, `{} {}`, `NaN`, "{\"x\":\"\xff\"}"} {
		if Validate([]byte(b)) == nil {
			t.Fatal("accepted invalid")
		}
	}
	for _, b := range []string{`{"a":"\ud83d\ude00","b":9223372036854775807}`, `[true,false,null]`} {
		if Validate([]byte(b)) != nil {
			t.Fatal("rejected valid")
		}
	}
}
func TestRewriteTokenPreservation(t *testing.T) {
	for _, opts := range []string{"", `,"stream_options":null`, `,"stream_options":{}`, `,"stream_options":{"include_usage":false,"x":1}`, `,"stream_options":{"include_usage":true}`} {
		b := []byte(`{ "messages": [{"content":"你好"}], "n":9007199254740993, "stream":true` + opts + ` }`)
		out, err := RewriteUsage(b, 262144)
		if err != nil {
			t.Fatal(err)
		}
		for _, fragment := range [][]byte{[]byte(`"n":9007199254740993`), []byte(`"messages": [{"content":"你好"}]`)} {
			if !bytes.Contains(out, fragment) {
				t.Fatal("changed business tokens")
			}
		}
		again, err := RewriteUsage(out, 262144)
		if err != nil || !bytes.Equal(out, again) {
			t.Fatal("not idempotent")
		}
	}
}
