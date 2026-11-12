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
	var out BatchAck
	if ValidateBatch(b, true) != nil {
		return out, Err
	}
	fs, _ := Read(b)
	for _, f := range fs {
		if f.N == 1 {
			out.Seq = f.V
		}
		if f.N == 2 {
			items, _ := Read(f.B)
			var a CaptureAck
			for _, x := range items {
				if x.N == 1 {
					a.Capture = x.V
				}
				if x.N == 2 {
					var err error
					a.Ack, err = DecodeAck(x.B)
					if err != nil {
						return out, err
					}
				}
			}
			out.Acks = append(out.Acks, a)
		}
	}
	return out, nil
}
