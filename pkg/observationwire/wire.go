// Package observationwire is the bounded reflection-free v1 wire codec.
package observationwire

import (
	"errors"
	"unicode/utf8"
)

var Err = errors.New("protocol_error")

type Field struct {
	N    int
	V    uint64
	B    []byte
	Wire int
}

func Var(b []byte) (uint64, int, error) {
	var v uint64
	for i, c := range b {
		if i >= 10 || i == 9 && c > 1 {
			return 0, 0, Err
		}
		v |= uint64(c&127) << uint(i*7)
		if c < 128 {
			if i > 0 && c == 0 {
				return 0, 0, Err
			}
			return v, i + 1, nil
		}
	}
	return 0, 0, Err
}
func Read(b []byte) ([]Field, error) {
	if len(b) > 32768 {
		return nil, Err
	}
	out := []Field{}
	for len(b) > 0 {
		tag, n, err := Var(b)
		if err != nil || tag>>3 == 0 || tag>>3 > 536870911 {
			return nil, Err
		}
		b = b[n:]
		f := Field{N: int(tag >> 3), Wire: int(tag & 7)}
		switch f.Wire {
		case 0:
			f.V, n, err = Var(b)
			if err != nil {
				return nil, Err
			}
			b = b[n:]
		case 2:
			var l uint64
			l, n, err = Var(b)
			if err != nil || l > uint64(len(b)-n) {
				return nil, Err
			}
			b = b[n:]
			f.B = b[:int(l)]
			b = b[int(l):]
		case 1:
			if len(b) < 8 {
				return nil, Err
			}
			b = b[8:]
		case 5:
			if len(b) < 4 {
				return nil, Err
			}
			b = b[4:]
		default:
			return nil, Err
		}
		out = append(out, f)
		if len(out) > 256 {
			return nil, Err
		}
	}
	return out, nil
}
func AppendVar(b []byte, v uint64) []byte {
	for v >= 128 {
		b = append(b, byte(v)|128)
		v >>= 7
	}
	return append(b, byte(v))
}
func UInt(b []byte, n int, v uint64) []byte { b = AppendVar(b, uint64(n<<3)); return AppendVar(b, v) }
func Bytes(b []byte, n int, v []byte) []byte {
	b = AppendVar(b, uint64(n<<3|2))
	b = AppendVar(b, uint64(len(v)))
	return append(b, v...)
}
func String(b []byte, n int, v string) []byte { return Bytes(b, n, []byte(v)) }
func Frame(seq uint64, kind int, payload []byte) []byte {
	b := UInt(nil, 1, 1)
	b = UInt(b, 3, seq)
	return Bytes(b, kind, payload)
}
func Validate(b []byte, kind string) error {
	fs, err := Read(b)
	if err != nil {
		return err
	}
	seen := map[int]bool{}
	one := false
	for _, f := range fs {
		wire, known, child, isString := descriptor(kind, f.N)
		if !known {
			continue
		}
		if seen[f.N] || wire != f.Wire {
			return Err
		}
		seen[f.N] = true
		if f.Wire == 0 {
			uint32Field := kind == "frame" && f.N == 1 || kind == "headers" && f.N == 1 || kind == "chunk" && f.N == 1 || kind == "end" && (f.N == 8 || f.N == 9)
			boolField := kind == "start" && f.N == 14 || kind == "headers" && f.N == 4 || kind == "chunk" && f.N == 5 || kind == "end" && (f.N == 6 || f.N == 7) || kind == "ack" && f.N == 3
			if uint32Field && f.V > 4294967295 || boolField && f.V > 1 {
				return Err
			}
		}
		if kind == "frame" && f.N >= 10 {
			if one {
				return Err
			}
			one = true
		}
		if child != "" {
			if Validate(f.B, child) != nil {
				return Err
			}
		}
		if isString && !utf8.Valid(f.B) {
			return Err
		}
		if kind == "start" && (f.N == 17 && len(f.B) > 4096 || f.N == 16 && len(f.B) > 512 || f.N == 18 && len(f.B) > 128) {
			return Err
		}
		if kind == "chunk" && f.N == 4 && len(f.B) > 16384 {
			return Err
		}
	}
	return nil
}
func descriptor(kind string, n int) (int, bool, string, bool) {
	switch kind {
	case "frame":
		switch n {
		case 1, 3:
			return 0, true, "", false
		case 2:
			return -1, true, "", false
		case 10:
			return 2, true, "start", false
		case 11:
			return 2, true, "headers", false
		case 12:
			return 2, true, "chunk", false
		case 13:
			return 2, true, "end", false
		}
	case "start":
		if n == 1 || n == 14 {
			return 0, true, "", false
		}
		if n == 5 || n == 7 || n == 8 || n == 9 || n == 10 {
			return -1, true, "", false
		}
		if n == 17 {
			return 2, true, "", false
		}
		if n >= 2 && n <= 16 || n == 18 {
			return 2, true, "", true
		}
	case "headers":
		if n == 1 || n == 4 {
			return 0, true, "", false
		}
		if n == 2 || n == 3 {
			return 2, true, "", true
		}
	case "chunk":
		if n >= 1 && n <= 5 {
			if n == 4 {
				return 2, true, "", false
			}
			return 0, true, "", false
		}
	case "end":
		if n >= 1 && n <= 9 {
			return 0, true, "", false
		}
		if n == 10 || n == 11 {
			return 2, true, "", true
		}
	case "ack":
		if n == 1 || n == 4 {
			return 2, true, "", true
		}
		if n == 2 || n == 3 {
			return 0, true, "", false
		}
	}
	return 0, false, "", false
}

type Ack struct {
	ID     string
	Seq    uint64
	Close  bool
	Reason string
}

func DecodeAck(b []byte) (Ack, error) {
	var a Ack
	if len(b) > 1024 || Validate(b, "ack") != nil {
		return a, Err
	}
	fs, _ := Read(b)
	for _, f := range fs {
		switch f.N {
		case 1:
			a.ID = string(f.B)
		case 2:
			a.Seq = f.V
		case 3:
			if f.V > 1 {
				return a, Err
			}
			a.Close = f.V == 1
		case 4:
			a.Reason = string(f.B)
		}
	}
	if !UUID(a.ID) || a.Seq < 1 {
		return a, Err
	}
	switch a.Reason {
	case "accepted", "finalized", "protocol_error", "parse_limit", "resource_limit", "observation_timeout":
	default:
		return a, Err
	}
	return a, nil
}
func UUID(s string) bool {
	if len(s) != 36 || s[14] != '4' || !(s[19] == '8' || s[19] == '9' || s[19] == 'a' || s[19] == 'b') {
		return false
	}
	for i, c := range []byte(s) {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
		} else if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
