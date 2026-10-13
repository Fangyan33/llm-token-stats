package strictjson

import (
	"bytes"
	"sort"
)

type edit struct {
	a, b int
	v    []byte
}

// RewriteUsage changes only the token spans needed for include_usage.
func RewriteUsage(b []byte, max int) ([]byte, error) {
	fields, err := Fields(b, 64, 100000)
	if err != nil || fields == nil {
		return nil, ErrInvalid
	}
	s, ok := fields["stream"]
	if !ok {
		return b, nil
	}
	v := bytes.TrimSpace(b[s.Start:s.End])
	if bytes.Equal(v, []byte("false")) {
		return b, nil
	}
	if !bytes.Equal(v, []byte("true")) {
		return nil, ErrInvalid
	}
	es := []edit{}
	opts, ok := fields["stream_options"]
	if !ok {
		end := len(b) - 1
		for end >= 0 && b[end] != '}' {
			end--
		}
		comma := []byte(",")
		if len(fields) == 0 {
			comma = nil
		}
		es = append(es, edit{end, end, append(comma, []byte(`"stream_options":{"include_usage":true}`)...)})
	} else if bytes.Equal(b[opts.Start:opts.End], []byte("null")) {
		es = append(es, edit{opts.Start, opts.End, []byte(`{"include_usage":true}`)})
	} else {
		part := b[opts.Start:opts.End]
		inner, e := Fields(part, 64, 100000)
		if e != nil || inner == nil {
			return nil, ErrInvalid
		}
		if u, ok := inner["include_usage"]; ok {
			es = append(es, edit{opts.Start + u.Start, opts.Start + u.End, []byte("true")})
		} else {
			comma := []byte(",")
			if len(inner) == 0 {
				comma = nil
			}
			es = append(es, edit{opts.End - 1, opts.End - 1, append(comma, []byte(`"include_usage":true`)...)})
		}
	}
	sort.Slice(es, func(i, j int) bool { return es[i].a < es[j].a })
	size := len(b)
	for _, e := range es {
		size += len(e.v) - (e.b - e.a)
	}
	if size > max {
		return nil, ErrInvalid
	}
	out := make([]byte, 0, size)
	pos := 0
	for _, e := range es {
		out = append(out, b[pos:e.a]...)
		out = append(out, e.v...)
		pos = e.b
	}
	out = append(out, b[pos:]...)
	return out, nil
}
