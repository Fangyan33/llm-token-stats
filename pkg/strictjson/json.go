// Package strictjson validates bounded JSON without silently accepting duplicate
// keys, malformed UTF-8, or unpaired UTF-16 escapes. Errors never contain input.
package strictjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"

	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
)

var ErrInvalid = errors.New("invalid_json")

type Span struct{ Start, End int }
type parser struct {
	b         []byte
	i, n, max int
	detailed  bool
	path      string
	problem   *configdiag.Report
}

func (p *parser) invalid(reason string) error {
	if p.detailed && p.problem == nil {
		field := p.path
		if field == "" {
			field = "$"
		}
		p.problem = configdiag.New("config_invalid", field, reason, "valid strict JSON", nil)
		p.problem.Issues[0].Offset = int64(p.i + 1)
	}
	return ErrInvalid
}

func Validate(b []byte) error { _, err := Fields(b, 64, 100000); return err }
func Fields(b []byte, depth, elements int) (map[string]Span, error) {
	if !utf8.Valid(b) || depth < 1 || elements < 1 {
		return nil, ErrInvalid
	}
	p := parser{b: b, max: elements}
	fields, err := p.value(depth, true)
	p.space()
	if err != nil || p.i != len(b) {
		return nil, ErrInvalid
	}
	return fields, nil
}
func (p *parser) space() {
	for p.i < len(p.b) && (p.b[p.i] == ' ' || p.b[p.i] == '\n' || p.b[p.i] == '\r' || p.b[p.i] == '\t') {
		p.i++
	}
}
func (p *parser) str(decode bool) (string, error) {
	start := p.i
	p.i++
	for p.i < len(p.b) {
		c := p.b[p.i]
		p.i++
		if c == '"' {
			if !decode {
				return "", nil
			}
			var s string
			if json.Unmarshal(p.b[start:p.i], &s) != nil {
				return "", p.invalid("invalid_json_string")
			}
			return s, nil
		}
		if c < 32 {
			return "", p.invalid("invalid_json_string")
		}
		if c == '\\' {
			if p.i >= len(p.b) {
				return "", p.invalid("invalid_json_string")
			}
			e := p.b[p.i]
			p.i++
			if e == 'u' {
				v, ok := p.hex()
				if !ok {
					return "", p.invalid("invalid_json_string")
				}
				if v >= 0xd800 && v <= 0xdbff {
					if p.i+2 > len(p.b) || p.b[p.i] != '\\' || p.b[p.i+1] != 'u' {
						return "", p.invalid("invalid_json_string")
					}
					p.i += 2
					w, ok := p.hex()
					if !ok || w < 0xdc00 || w > 0xdfff {
						return "", p.invalid("invalid_json_string")
					}
				} else if v >= 0xdc00 && v <= 0xdfff {
					return "", p.invalid("invalid_json_string")
				}
			} else if !bytes.ContainsRune([]byte(`"\/bfnrt`), rune(e)) {
				return "", p.invalid("invalid_json_string")
			}
		}
	}
	return "", p.invalid("invalid_json_string")
}
func (p *parser) hex() (uint16, bool) {
	if p.i+4 > len(p.b) {
		return 0, false
	}
	var v uint16
	for j := 0; j < 4; j++ {
		c := p.b[p.i]
		p.i++
		v <<= 4
		switch {
		case c >= '0' && c <= '9':
			v += uint16(c - '0')
		case c >= 'a' && c <= 'f':
			v += uint16(c - 'a' + 10)
		case c >= 'A' && c <= 'F':
			v += uint16(c - 'A' + 10)
		default:
			return 0, false
		}
	}
	return v, true
}
func (p *parser) value(depth int, capture bool) (map[string]Span, error) {
	p.space()
	p.n++
	if depth < 0 || p.n > p.max || p.i >= len(p.b) {
		return nil, p.invalid("invalid_json")
	}
	c := p.b[p.i]
	if c == '"' {
		_, err := p.str(false)
		return nil, err
	}
	if c == '{' || c == '[' {
		p.i++
		p.space()
		end := byte('}')
		if c == '[' {
			end = ']'
		}
		seen := map[string]bool{}
		var out map[string]Span
		if capture && c == '{' {
			out = map[string]Span{}
		}
		if p.i < len(p.b) && p.b[p.i] == end {
			p.i++
			return out, nil
		}
		index := 0
		for {
			p.space()
			key := ""
			if c == '{' {
				if p.i >= len(p.b) || p.b[p.i] != '"' {
					return nil, p.invalid("invalid_json")
				}
				var err error
				key, err = p.str(true)
				if err != nil {
					return nil, err
				}
				if seen[key] || len(seen) >= 8192 {
					if p.detailed {
						p.path = configdiag.Join(p.path, configdiag.PathSegment(key))
					}
					reason := "duplicate_key"
					if len(seen) >= 8192 {
						reason = "object_field_limit"
					}
					return nil, p.invalid(reason)
				}
				seen[key] = true
				p.space()
				if p.i >= len(p.b) || p.b[p.i] != ':' {
					return nil, p.invalid("invalid_json")
				}
				p.i++
				p.space()
			}
			start := p.i
			parent := p.path
			if p.detailed {
				if c == '{' {
					p.path = configdiag.Join(parent, configdiag.PathSegment(key))
				} else {
					p.path = configdiag.Index(parent, index)
				}
			}
			if _, err := p.value(depth-1, false); err != nil {
				return nil, err
			}
			p.path = parent
			index++
			if out != nil {
				out[key] = Span{start, p.i}
			}
			p.space()
			if p.i >= len(p.b) {
				return nil, p.invalid("invalid_json")
			}
			x := p.b[p.i]
			p.i++
			if x == end {
				return out, nil
			}
			if x != ',' {
				return nil, p.invalid("invalid_json")
			}
		}
	}
	start := p.i
	for p.i < len(p.b) && !bytes.ContainsRune([]byte(" \r\n\t,]}"), rune(p.b[p.i])) {
		p.i++
	}
	if p.i == start || !json.Valid(p.b[start:p.i]) {
		return nil, p.invalid("invalid_json")
	}
	return nil, nil
}
func Decode(b []byte, v any) error {
	if Validate(b) != nil {
		return ErrInvalid
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	d.UseNumber()
	if d.Decode(v) != nil {
		return ErrInvalid
	}
	if d.Decode(new(any)) != io.EOF {
		return ErrInvalid
	}
	return nil
}
