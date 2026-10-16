package credentials

import (
	"bytes"
	"strings"
	"testing"
)

func TestBearerSingleBoundedCredential(t *testing.T) {
	for _, value := range []string{"Bearer AbC-_.~+/=", "bEaReR  AbC-_.~+/="} {
		if string(Bearer([]byte(value))) != "AbC-_.~+/=" {
			t.Fatal("token changed")
		}
	}
	for _, value := range []string{"", "Bearer", "Bearer ", "Basic abc", " Bearer abc", "Bearer abc def", "Bearer abc\t", "Bearer abc,Bearer def", "Bearer abc,abc", "Bearer 密钥", "Bearer " + strings.Repeat("a", 4097)} {
		if Bearer([]byte(value)) != nil {
			t.Fatal("invalid credential accepted")
		}
	}
	token := bytes.Repeat([]byte{'a'}, MaxAPIKeyBytes)
	if !bytes.Equal(Bearer(append([]byte("Bearer "), token...)), token) {
		t.Fatal("boundary rejected")
	}
}
