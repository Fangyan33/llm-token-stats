package chatcompletions

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/Fangyan33/llm-token-stats/pkg/strictjson"
	"mime"
	"strings"
	"unicode/utf8"
)

var ErrLimit = errors.New("parse_limit")

type Parser struct {
	Request, Response, line, data              []byte
	SSE, Done, ReqSupported, RespSupported     bool
	RequestModel, ResponseModel                *string
	Usage                                      *events.Usage
	Raw                                        string
	Truncated, Conflict, hasCandidate          bool
	Anomalies                                  []string
	ResponseBytes                              int
	MaxRequest, MaxJSON, MaxEvent, MaxResponse int
}

func New() *Parser {
	return &Parser{Usage: events.Unknown(), Raw: "{}", MaxRequest: 262144, MaxJSON: 1048576, MaxEvent: 262144, MaxResponse: 8388608}
}
func (p *Parser) Anomaly(a string) { p.Anomalies = events.Add(p.Anomalies, a) }
func (p *Parser) Content(request bool, ct, enc string) {
	supported := true
	if enc != "" && !strings.EqualFold(enc, "identity") {
		p.Anomaly("unsupported_encoding")
		supported = false
	}
	mt, _, err := mime.ParseMediaType(strings.ToLower(ct))
	if err != nil || (mt != "application/json" && (!request && mt != "text/event-stream" || request)) {
		p.Anomaly("unsupported_content_type")
		supported = false
	}
	if request {
		p.ReqSupported = supported
	} else {
		p.RespSupported = supported
		p.SSE = mt == "text/event-stream"
	}
}
func (p *Parser) Feed(request bool, b []byte, eos bool) error {
	if request {
		if p.ReqSupported {
			if len(p.Request)+len(b) > p.MaxRequest {
				return ErrLimit
			}
			p.Request = append(p.Request, b...)
			if eos {
				return p.request()
			}
		}
		return nil
	}
	p.ResponseBytes += len(b)
	if p.ResponseBytes > p.MaxResponse {
		return ErrLimit
	}
	if !p.RespSupported {
		return nil
	}
	if !p.SSE {
		if len(p.Response)+len(b) > p.MaxJSON {
			return ErrLimit
		}
		p.Response = append(p.Response, b...)
		if eos {
			return p.document(p.Response, false)
		}
		return nil
	}
	for _, c := range b {
		if c == '\n' {
			if err := p.sseLine(); err != nil {
				return err
			}
			p.line = p.line[:0]
		} else {
			if len(p.line)+len(p.data)+1 > p.MaxEvent {
				return ErrLimit
			}
			p.line = append(p.line, c)
		}
	}
	if eos {
		if len(p.line) > 0 {
			if err := p.sseLine(); err != nil {
				return err
			}
			p.line = nil
		}
		if len(p.data) > 0 {
			if err := p.event(); err != nil {
				return err
			}
		}
		if !p.Done {
			p.Anomaly("protocol_incomplete")
		}
	}
	return nil
}
func (p *Parser) request() error {
	var m map[string]json.RawMessage
	if strictjson.Decode(p.Request, &m) != nil || m == nil {
		p.Anomaly("invalid_model")
		p.Anomaly("protocol_error")
		return errors.New("protocol_error")
	}
	p.RequestModel = p.model(m["model"])
	if v, ok := m["stream"]; ok {
		var flag bool
		if json.Unmarshal(v, &flag) != nil || bytes.Equal(v, []byte("null")) {
			p.Anomaly("protocol_error")
			return errors.New("protocol_error")
		}
	}
	p.Request = nil
	return nil
}
func (p *Parser) model(b []byte) *string {
	if len(b) == 0 {
		return nil
	}
	var s string
	if json.Unmarshal(b, &s) != nil || s == "" || len(s) > 1024 || !utf8.ValidString(s) {
		p.Anomaly("invalid_model")
		return nil
	}
	return &s
}
func (p *Parser) sseLine() error {
	line := bytes.TrimSuffix(p.line, []byte("\r"))
	if len(line) == 0 {
		return p.event()
	}
	if line[0] == ':' {
		return nil
	}
	if bytes.HasPrefix(line, []byte("data:")) {
		v := line[5:]
		v = bytes.TrimPrefix(v, []byte(" "))
		if len(p.data)+len(v)+1 > p.MaxEvent {
			return ErrLimit
		}
		p.data = append(p.data, v...)
		p.data = append(p.data, '\n')
	}
	return nil
}
func (p *Parser) event() error {
	if len(p.data) == 0 {
		return nil
	}
	d := bytes.TrimSuffix(p.data, []byte("\n"))
	defer func() { p.data = p.data[:0] }()
	if !utf8.Valid(d) {
		return errors.New("protocol_error")
	}
	if p.Done {
		if len(bytes.TrimSpace(d)) > 0 {
			p.Anomaly("protocol_incomplete")
		}
		return nil
	}
	if bytes.Equal(d, []byte("[DONE]")) {
		p.Done = true
		return nil
	}
	return p.document(d, true)
}
func (p *Parser) document(b []byte, sse bool) error {
	var m map[string]json.RawMessage
	if strictjson.Decode(b, &m) != nil || m == nil {
		return errors.New("protocol_error")
	}
	if v, ok := m["model"]; ok {
		p.ResponseModel = p.model(v)
	}
	v, ok := m["usage"]
	if !ok || bytes.Equal(v, []byte("null")) {
		return nil
	}
	if sse {
		var choices []json.RawMessage
		if json.Unmarshal(m["choices"], &choices) != nil || choices == nil || len(choices) != 0 {
			return nil
		}
	}
	var raw map[string]json.RawMessage
	if json.Unmarshal(v, &raw) != nil || raw == nil {
		p.Anomaly("invalid_usage")
		return nil
	}
	u := events.Unknown()
	safe := map[string]any{}
	truncated := false
	paths := [][2]string{{"prompt_tokens", ""}, {"completion_tokens", ""}, {"prompt_tokens_details", "cached_tokens"}, {"prompt_tokens_details", "cache_write_tokens"}, {"completion_tokens_details", "reasoning_tokens"}, {"total_tokens", ""}}
	for i, path := range paths {
		val := raw[path[0]]
		if path[1] != "" {
			var d map[string]json.RawMessage
			if len(val) > 0 && (json.Unmarshal(val, &d) != nil || d == nil) {
				p.Anomaly("invalid_usage")
				truncated = true
			}
			val = d[path[1]]
		}
		if len(val) == 0 {
			continue
		}
		var n int64
		if json.Unmarshal(val, &n) != nil || bytes.Equal(val, []byte("null")) || n < 0 {
			p.Anomaly("invalid_usage")
			truncated = true
			continue
		}
		events.Tokens(u)[i].Value = &n
		events.Tokens(u)[i].Source = "reported"
		if path[1] == "" {
			safe[path[0]] = n
		} else {
			d, _ := safe[path[0]].(map[string]any)
			if d == nil {
				d = map[string]any{}
				safe[path[0]] = d
			}
			d[path[1]] = n
		}
	}
	for key, v := range raw {
		if key != "prompt_tokens" && key != "completion_tokens" && key != "total_tokens" && key != "prompt_tokens_details" && key != "completion_tokens_details" {
			truncated = true
		}
		if strings.HasSuffix(key, "_details") {
			var d map[string]json.RawMessage
			if json.Unmarshal(v, &d) == nil {
				for k := range d {
					if !(key == "prompt_tokens_details" && (k == "cached_tokens" || k == "cache_write_tokens") || key == "completion_tokens_details" && k == "reasoning_tokens") {
						truncated = true
					}
				}
			}
		}
	}
	encoded, _ := json.Marshal(safe)
	if p.Raw != "{}" || p.hasCandidate {
		a, _ := json.Marshal(p.Usage)
		c, _ := json.Marshal(u)
		if !bytes.Equal(a, c) || p.Raw != string(encoded) {
			p.Conflict = true
			p.Anomaly("usage_conflict")
		}
	} else {
		p.Usage = u
		p.Raw = string(encoded)
		p.Truncated = truncated
	}
	p.hasCandidate = true
	return nil
}
func (p *Parser) Finalize() {
	if p.SSE && p.RespSupported && !p.Done {
		p.Anomaly("protocol_incomplete")
	}
	if p.Conflict {
		p.Usage = events.Unknown()
	}
	s, bad := events.Status(p.Usage)
	if bad {
		p.Anomaly("usage_conflict")
	}
	if s == "unknown" {
		p.Anomaly("missing_usage")
	}
	p.Request = nil
	p.Response = nil
	p.line = nil
	p.data = nil
}
