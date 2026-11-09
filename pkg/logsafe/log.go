// Package logsafe contains the small logging primitives also used in TinyGo.
package logsafe

import (
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Level int

const (
	Debug Level = -4
	Info  Level = 0
	Warn  Level = 4
	Error Level = 8
)

func ParseLevel(value string) (Level, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug":
		return Debug, true
	case "", "info":
		return Info, true
	case "warn":
		return Warn, true
	case "error":
		return Error, true
	default:
		return Info, false
	}
}

func (l Level) String() string {
	switch l {
	case Debug:
		return "DEBUG"
	case Warn:
		return "WARN"
	case Error:
		return "ERROR"
	default:
		return "INFO"
	}
}

// Clean redacts before truncation, including secrets crossing the cutoff. It
// also escapes controls that Go's text handler would otherwise leave literal.
func Clean(value string, secrets ...string) string {
	secrets = append([]string(nil), secrets...)
	sort.Slice(secrets, func(i, j int) bool { return len(secrets[i]) > len(secrets[j]) })
	var replacements []string
	for _, secret := range secrets {
		if secret != "" {
			replacements = append(replacements, secret, "[REDACTED]")
		}
	}
	if len(replacements) > 0 {
		value = strings.NewReplacer(replacements...).Replace(value)
	}
	if len(value) > 1024 {
		end := 1024
		for !utf8.RuneStart(value[end]) {
			end--
		}
		value = value[:end]
	}
	var out strings.Builder
	for _, c := range value {
		if unicode.IsControl(c) {
			q := strconv.QuoteRuneToASCII(c)
			out.WriteString(q[1 : len(q)-1])
		} else {
			out.WriteRune(c)
		}
	}
	return out.String()
}
