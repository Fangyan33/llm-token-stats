package strictjson

import (
	"bytes"
	"encoding/json"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
)

// DecodeConfig opts into field/position diagnostics. Decode and Fields continue
// returning only ErrInvalid for business payloads; no raw JSON or decoder error
// strings are retained in the diagnostic.
func DecodeConfig(b []byte, v any) error {
	p := parser{b: b, max: 100000, detailed: true}
	if !utf8.Valid(b) {
		for p.i < len(b) {
			c, n := utf8.DecodeRune(b[p.i:])
			if c == utf8.RuneError && n == 1 {
				break
			}
			p.i += n
		}
		p.invalid("invalid_utf8")
	} else {
		_, err := p.value(64, true)
		p.space()
		if err != nil || p.i != len(b) {
			p.invalid("invalid_json")
		}
	}
	if p.problem != nil {
		locateLine(b, &p.problem.Issues[0])
		return p.problem
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	d.UseNumber()
	if err := d.Decode(v); err != nil {
		field, reason, expected, offset := "$", "invalid_json", "configuration object", int64(1)
		switch e := err.(type) {
		case *json.UnmarshalTypeError:
			reason, expected, offset = "type_mismatch", e.Type.String(), e.Offset
			field, _ = configLocation(b, reflect.TypeOf(v), "", 0, "", offset)
		default:
			const unknown = "json: unknown field "
			if strings.HasPrefix(err.Error(), unknown) {
				key, unquoteErr := strconv.Unquote(strings.TrimPrefix(err.Error(), unknown))
				if unquoteErr == nil {
					reason, expected = "unknown_field", "declared configuration field"
					field, offset = configLocation(b, reflect.TypeOf(v), "", 0, key, 0)
				}
			}
		}
		if field == "" {
			field = "$"
		}
		r := configdiag.New("config_invalid", field, reason, expected, nil)
		r.Issues[0].Offset = offset
		locateLine(b, &r.Issues[0])
		return r
	}
	return nil
}

func locateLine(b []byte, i *configdiag.Issue) {
	i.Line, i.Column = 1, 1
	end := min(max(i.Offset-1, 0), int64(len(b)))
	for _, c := range b[:end] {
		if c == '\n' {
			i.Line++
			i.Column = 1
		} else {
			i.Column++
		}
	}
}

// Walk only on decode failure. Struct tags distinguish a genuinely unknown key
// from an identically named valid field elsewhere; offsets preserve array indices.
func configLocation(b []byte, t reflect.Type, path string, base int64, unknown string, offset int64) (string, int64) {
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	trimmed := bytes.TrimSpace(b)
	if t == nil || len(trimmed) == 0 {
		return path, base + 1
	}
	if t.Kind() == reflect.Struct && trimmed[0] == '{' {
		fields, _ := Fields(b, 64, 100000)
		keys := make([]string, 0, len(fields))
		for key := range fields {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool { return fields[keys[i]].Start < fields[keys[j]].Start })
		for _, key := range keys {
			span := fields[key]
			var member reflect.Type
			for n := 0; n < t.NumField(); n++ {
				f := t.Field(n)
				name := strings.Split(f.Tag.Get("json"), ",")[0]
				if name == "" {
					name = f.Name
				}
				if name != "-" && strings.EqualFold(key, name) {
					member = f.Type
					break
				}
			}
			child := configdiag.Join(path, configdiag.PathSegment(key))
			if unknown != "" {
				if member == nil && key == unknown {
					return child, base + int64(span.Start) + 1
				}
				if member != nil {
					if found, at := configLocation(b[span.Start:span.End], member, child, base+int64(span.Start), unknown, 0); at > 0 {
						return found, at
					}
				}
			} else if offset > base+int64(span.Start) && offset <= base+int64(span.End) {
				return configLocation(b[span.Start:span.End], member, child, base+int64(span.Start), "", offset)
			}
		}
	} else if (t.Kind() == reflect.Slice || t.Kind() == reflect.Array) && trimmed[0] == '[' {
		d := json.NewDecoder(bytes.NewReader(b))
		_, _ = d.Token()
		for n := 0; d.More(); n++ {
			var raw json.RawMessage
			if d.Decode(&raw) != nil {
				break
			}
			end := base + d.InputOffset()
			start := end - int64(len(raw))
			child := configdiag.Index(path, n)
			if unknown != "" {
				if found, at := configLocation(raw, t.Elem(), child, start, unknown, 0); at > 0 {
					return found, at
				}
			} else if offset > start && offset <= end {
				return configLocation(raw, t.Elem(), child, start, "", offset)
			}
		}
	}
	if unknown != "" {
		return "", 0
	}
	return path, offset
}
