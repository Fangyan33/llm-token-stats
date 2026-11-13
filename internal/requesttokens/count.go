// Package requesttokens implements a reproducible text/tools estimate, not
// model-reported usage. No request text may leave this package in diagnostics.
package requesttokens

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/Fangyan33/llm-token-stats/pkg/strictjson"
	"github.com/tiktoken-go/tokenizer"
	"strings"
	"unicode"
	"unicode/utf8"
)

const Encoding = "o200k_base"
const Basis = "text_tools_v1"

var ErrBody = errors.New("invalid_body")

type Counter struct{ codec tokenizer.Codec }

func NewCounter() (*Counter, error) {
	c, err := tokenizer.Get(tokenizer.O200kBase)
	return &Counter{codec: c}, err
}

// Parts validates the complete JSON, decodes text strings and canonicalizes only
// selected tool values. Transport fragments are never tokenization boundaries.
func Parts(body []byte) ([]string, error) {
	parts, _, err := partsAndModel(body)
	return parts, err
}

func partsAndModel(body []byte) ([]string, *string, error) {
	fields, err := strictjson.Fields(body, 64, 100000)
	if err != nil {
		return nil, nil, ErrBody
	}
	var model *string
	if span, ok := fields["model"]; ok {
		model = requestModel(body[span.Start:span.End])
	}
	parts, err := textParts(body, fields)
	return parts, model, err
}

func requestModel(raw []byte) *string {
	// A 1024-byte decoded name needs at most six JSON bytes per byte plus quotes.
	// Reject before decoding, so an oversized model never allocates a large copy.
	if len(raw) < 2 || len(raw) > 6*1024+2 {
		return nil
	}
	var model string
	if json.Unmarshal(raw, &model) != nil || len(model) > 1024 || !utf8.ValidString(model) || strings.TrimSpace(model) == "" || strings.IndexFunc(model, unicode.IsControl) >= 0 {
		return nil
	}
	return &model
}

func textParts(body []byte, fields map[string]strictjson.Span) ([]string, error) {
	span, ok := fields["messages"]
	if !ok {
		return nil, ErrBody
	}
	var messages []map[string]json.RawMessage
	if json.Unmarshal(body[span.Start:span.End], &messages) != nil || len(messages) == 0 {
		return nil, ErrBody
	}
	var parts []string
	tool := func(raw json.RawMessage, array bool) error {
		if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
			return nil
		}
		if array && raw[0] != '[' || !array && raw[0] != '{' {
			return ErrBody
		}
		var value any
		d := json.NewDecoder(bytes.NewReader(raw))
		d.UseNumber()
		if d.Decode(&value) != nil {
			return ErrBody
		}
		var b bytes.Buffer
		e := json.NewEncoder(&b)
		e.SetEscapeHTML(false)
		if e.Encode(value) != nil {
			return ErrBody
		}
		parts = append(parts, string(bytes.TrimSuffix(b.Bytes(), []byte{'\n'})))
		return nil
	}
	for _, message := range messages {
		if message == nil {
			return nil, ErrBody
		}
		raw := message["content"]
		if len(raw) > 0 && !bytes.Equal(raw, []byte("null")) {
			switch raw[0] {
			case '"':
				var text string
				if json.Unmarshal(raw, &text) != nil {
					return nil, ErrBody
				}
				parts = append(parts, text)
			case '[':
				var blocks []map[string]json.RawMessage
				if json.Unmarshal(raw, &blocks) != nil {
					return nil, ErrBody
				}
				for _, block := range blocks {
					if block == nil {
						return nil, ErrBody
					}
					var kind string
					if json.Unmarshal(block["type"], &kind) != nil {
						return nil, ErrBody
					}
					if kind == "text" {
						var text string
						if json.Unmarshal(block["text"], &text) != nil {
							return nil, ErrBody
						}
						parts = append(parts, text)
					}
				}
			default:
				return nil, ErrBody
			}
		}
		if tool(message["tool_calls"], true) != nil || tool(message["function_call"], false) != nil {
			return nil, ErrBody
		}
	}
	for _, name := range []string{"tools", "functions"} {
		if span, ok := fields[name]; ok {
			if tool(body[span.Start:span.End], true) != nil {
				return nil, ErrBody
			}
		}
	}
	if len(parts) == 0 {
		return nil, ErrBody
	}
	return parts, nil
}

func (c *Counter) Count(body []byte) (int64, error) {
	n, _, err := c.Analyze(body)
	return n, err
}

// Analyze reuses one complete JSON validation for model extraction and counting.
// A valid model survives unsupported message content or a tokenizer error.
func (c *Counter) Analyze(body []byte) (int64, *string, error) {
	parts, model, err := partsAndModel(body)
	if err != nil {
		return 0, model, err
	}
	var total int64
	for _, text := range parts {
		n, err := c.codec.Count(text)
		if err != nil {
			return 0, model, errors.New("tokenizer_error")
		}
		total += int64(n)
	}
	return total, model, nil
}
