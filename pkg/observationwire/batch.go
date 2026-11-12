package observationwire

// Batch framing remains reflection-free in Wasm. Nested frames retain their
// strict singular/oneof validation; only batch item fields are repeatable.
func ValidateBatch(b []byte, ack bool) error {
	fs, err := Read(b)
	if err != nil {
		return Err
	}
	seenSeq := false
	count := 0
	for _, f := range fs {
		switch f.N {
		case 1:
			if seenSeq || f.Wire != 0 || f.V == 0 {
				return Err
			}
			seenSeq = true
		case 2:
			count++
			if count > 128 || f.Wire != 2 {
				return Err
			}
			items, err := Read(f.B)
			if err != nil {
				return Err
			}
			var id, payload bool
			for _, x := range items {
				switch x.N {
				case 1:
					if id || x.Wire != 0 || x.V == 0 {
						return Err
					}
					id = true
				case 2:
					if payload || x.Wire != 2 {
						return Err
					}
					payload = true
					kind := "frame"
					if ack {
						kind = "ack"
					}
					if Validate(x.B, kind) != nil {
						return Err
					}
				}
			}
			if !id || !payload {
				return Err
			}
		}
	}
	if !seenSeq || (!ack && count == 0) {
		return Err
	}
	return nil
}

type CaptureAck struct {
	Capture uint64
	Ack
}
type BatchAck struct {
	Seq  uint64
	Acks []CaptureAck
}

func DecodeBatchAck(b []byte) (BatchAck, error) {
	return DecodeBatchAckInto(b, nil)
}

// DecodeBatchAckInto reuses caller-owned storage. Strings own their bytes;
// callers may clear the receive buffer immediately after a successful decode.
func DecodeBatchAckInto(b []byte, storage []CaptureAck) (BatchAck, error) {
	out := BatchAck{Acks: storage[:0]}
	if len(b) > 32768 {
		return BatchAck{}, Err
	}
	c := cursor{b: b}
	for len(c.b) > 0 {
		f, err := c.next()
		if err != nil {
			return BatchAck{}, Err
		}
		switch f.N {
		case 1:
			if out.Seq != 0 || f.Wire != 0 || f.V == 0 {
				return BatchAck{}, Err
			}
			out.Seq = f.V
		case 2:
			if f.Wire != 2 || len(out.Acks) >= 128 {
				return BatchAck{}, Err
			}
			item := cursor{b: f.B}
			var a CaptureAck
			var seen uint8
			for len(item.b) > 0 {
				x, err := item.next()
				if err != nil {
					return BatchAck{}, Err
				}
				switch x.N {
				case 1:
					if seen&1 != 0 || x.Wire != 0 || x.V == 0 {
						return BatchAck{}, Err
					}
					seen |= 1
					a.Capture = x.V
				case 2:
					if seen&2 != 0 || x.Wire != 2 {
						return BatchAck{}, Err
					}
					seen |= 2
					a.Ack, err = DecodeAck(x.B)
					if err != nil {
						return BatchAck{}, err
					}
				}
			}
			if seen != 3 {
				return BatchAck{}, Err
			}
			out.Acks = append(out.Acks, a)
		}
	}
	if out.Seq == 0 {
		return BatchAck{}, Err
	}
	return out, nil
}
