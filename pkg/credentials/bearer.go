// Package credentials handles transient credentials; never format its input in errors.
package credentials

import "strings"

const MaxAPIKeyBytes = 4096

func ValidToken(token []byte) bool {
	if len(token) == 0 || len(token) > MaxAPIKeyBytes {
		return false
	}
	for _, b := range token {
		if b <= ' ' || b >= 127 || b == ',' {
			return false
		}
	}
	return true
}

// Bearer accepts a single credential. The caller must use the joined header
// property, so duplicate Authorization fields cannot silently select the first.
func Bearer(value []byte) []byte {
	if len(value) > MaxAPIKeyBytes+32 {
		return nil
	}
	i := strings.IndexByte(string(value), ' ')
	if i < 0 || !strings.EqualFold(string(value[:i]), "Bearer") {
		return nil
	}
	for i < len(value) && value[i] == ' ' {
		i++
	}
	if !ValidToken(value[i:]) {
		return nil
	}
	return value[i:]
}
