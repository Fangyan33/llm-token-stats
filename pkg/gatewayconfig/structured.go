package gatewayconfig

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
	"math"
	"unicode/utf8"

	w "github.com/Fangyan33/llm-token-stats/pkg/observationwire"
)

var errConfiguration = configdiag.New("config_invalid", "$", "invalid_struct", "valid bounded protobuf Struct", nil)

// configurationJSON accepts legacy strict JSON or Envoy's binary protobuf Struct.
// The wire decoder is bounded and reflection-free; malformed input never panics.
func configurationJSON(b []byte) ([]byte, error) {
	if len(b) > 65536 {
		r := &configdiag.Report{}
		r.Number(int64(len(b)), 0, 65536, "config_invalid", "$")
		return nil, r
	}
	trimmed := bytes.TrimSpace(b)
	if len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[') {
		return b, nil
	}
	d := structDecoder{}
	value, err := d.object(b, 0)
	if err != nil {
		return nil, err
	}
	result, err := json.Marshal(value)
	if err != nil || len(result) > 65536 {
		return nil, errConfiguration
	}
	return result, nil
}

type structDecoder struct {
	nodes int
	path  string
}

func (d *structDecoder) invalid(reason string) error {
	field := d.path
	if field == "" {
		field = "$"
	}
	return configdiag.New("config_invalid", field, reason, "valid bounded protobuf Struct", nil)
}

func (d *structDecoder) bounded(depth int) bool { d.nodes++; return depth <= 64 && d.nodes <= 100000 }

// Wire types: varint=0, fixed64=1, bytes=2. Keep VM initialization free of
// protobuf helper packages that may probe the process executable or filesystem.
func field(b []byte) (int, int, []byte, []byte, error) {
	tag, size, err := w.Var(b)
	if err != nil || tag>>3 == 0 || tag>>3 > 536870911 {
		return 0, 0, nil, nil, errConfiguration
	}
	n, t := int(tag>>3), int(tag&7)
	b = b[size:]
	if t != 2 {
		return n, t, b, nil, nil
	}
	length, size, err := w.Var(b)
	if err != nil || length > uint64(len(b)-size) {
		return 0, 0, nil, nil, errConfiguration
	}
	b = b[size:]
	return n, t, b[:int(length)], b[int(length):], nil

}
func (d *structDecoder) object(b []byte, depth int) (map[string]any, error) {
	if !d.bounded(depth) {
		return nil, d.invalid("invalid_struct")
	}
	result := map[string]any{}
	for len(b) > 0 {
		n, t, entry, rest, err := field(b)
		if err != nil || n != 1 || t != 2 {
			return nil, d.invalid("invalid_struct")
		}
		b = rest
		key := ""
		var value any
		hasKey, hasValue := false, false
		for len(entry) > 0 {
			n, t, data, next, err := field(entry)
			if err != nil || t != 2 {
				return nil, d.invalid("invalid_struct")
			}
			entry = next
			switch n {
			case 1:
				if hasKey || !utf8.Valid(data) {
					return nil, d.invalid("invalid_struct")
				}
				key = string(data)
				hasKey = true
			case 2:
				if hasValue {
					return nil, d.invalid("invalid_struct")
				}
				parent := d.path
				if hasKey {
					d.path = configdiag.Join(parent, configdiag.PathSegment(key))
				}
				value, err = d.value(data, depth+1)
				if err != nil {
					return nil, err
				}
				d.path = parent
				hasValue = true
			default:
				return nil, d.invalid("invalid_struct")
			}
		}
		if !hasKey || !hasValue {
			return nil, d.invalid("invalid_struct")
		}
		if _, exists := result[key]; exists {
			d.path = configdiag.Join(d.path, configdiag.PathSegment(key))
			return nil, d.invalid("duplicate_key")
		}
		result[key] = value
	}
	return result, nil
}
func (d *structDecoder) value(b []byte, depth int) (any, error) {
	if !d.bounded(depth) || len(b) == 0 {
		return nil, d.invalid("invalid_struct")
	}
	n, t, data, rest, err := field(b)
	if err != nil {
		return nil, d.invalid("invalid_struct")
	}
	switch n {
	case 1, 4:
		if t != 0 {
			return nil, d.invalid("invalid_struct")
		}
		v, size, consumeErr := w.Var(data)
		if consumeErr != nil || size != len(data) || v > 1 || n == 1 && v != 0 {
			return nil, d.invalid("invalid_struct")
		}
		if n == 1 {
			return nil, nil
		}
		return v == 1, nil
	case 2:
		if t != 1 {
			return nil, d.invalid("invalid_struct")
		}
		if len(data) != 8 {
			return nil, d.invalid("invalid_struct")
		}
		number := math.Float64frombits(binary.LittleEndian.Uint64(data))
		if math.IsNaN(number) || math.IsInf(number, 0) || math.Trunc(number) != number || number < -9007199254740991 || number > 9007199254740991 {
			return nil, d.invalid("invalid_integer")
		}
		return int64(number), nil
	case 3:
		if t != 2 || len(rest) != 0 || !utf8.Valid(data) {
			return nil, d.invalid("invalid_struct")
		}
		return string(data), nil
	case 5:
		if t != 2 || len(rest) != 0 {
			return nil, d.invalid("invalid_struct")
		}
		return d.object(data, depth+1)
	case 6:
		if t != 2 || len(rest) != 0 {
			return nil, d.invalid("invalid_struct")
		}
		result := make([]any, 0)
		for len(data) > 0 {
			n, t, child, next, err := field(data)
			if err != nil || n != 1 || t != 2 {
				return nil, d.invalid("invalid_struct")
			}
			data = next
			parent := d.path
			d.path = configdiag.Index(parent, len(result))
			v, err := d.value(child, depth+1)
			if err != nil {
				return nil, err
			}
			d.path = parent
			result = append(result, v)
		}
		return result, nil
	}
	return nil, d.invalid("invalid_struct")
}
