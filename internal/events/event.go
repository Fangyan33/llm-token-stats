package events

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/Fangyan33/llm-token-stats/pkg/strictjson"
	"math"
	"sort"
	"time"
)

const MaxBytes = 32768

func Ptr[T any](v T) *T    { return &v }
func Hash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func Tuple(v any) string   { b, _ := json.Marshal(v); return Hash(b) }
func Canonical(e *Event) ([]byte, error) {
	b, err := json.Marshal(e)
	if err != nil || len(b) > MaxBytes {
		return nil, errors.New("event_size")
	}
	return b, nil
}
func IdentityHash(e *Event) string {
	return Tuple([]any{e.ServiceID, e.ApiKeyID, e.CustomerID, e.ApplicationID, e.IdentityStatus, e.ConfigVersion, e.GatewayInstanceID, e.RouteName, e.AdapterID, e.XRequestID})
}
func Decode(b, key []byte, now time.Time) (*Event, error) {
	if len(b) > MaxBytes {
		return nil, errors.New("event_size")
	}
	var obj map[string]any
	if strictjson.Decode(b, &obj) != nil {
		return nil, errors.New("event_json")
	}
	if v, ok := obj["schema_version"].(json.Number); ok {
		if n, err := v.Int64(); err == nil && n > 0 && n != 1 {
			return nil, errors.New("unknown_schema")
		}
	}
	if err := check(obj, schema); err != nil {
		return nil, err
	}
	var e Event
	if strictjson.Decode(b, &e) != nil {
		return nil, errors.New("event_json")
	}
	if string(key) != e.LogicalRequestID || e.ObservationID != e.LogicalRequestID {
		return nil, errors.New("event_id")
	}
	suffix := ":usage"
	if e.EventType == "request_started" {
		suffix = ":start"
	}
	if e.EventID != e.LogicalRequestID+suffix {
		return nil, errors.New("event_id")
	}
	if !now.IsZero() {
		date := time.UnixMilli(e.RequestStartedAtMs).UTC().Format("2006-01-02")
		if e.RequestStartedAtMs > now.UnixMilli()+300000 || date < now.UTC().AddDate(0, 0, -364).Format("2006-01-02") {
			return nil, errors.New("event_time")
		}
	}
	known := e.ApiKeyID != nil && e.CustomerID != nil && e.ApplicationID != nil
	if (e.IdentityStatus == "known") != known || !known && !Has(e.AnomalyCodes, "identity_invalid") {
		return nil, errors.New("event_identity")
	}
	if !sort.StringsAreSorted(e.AnomalyCodes) {
		return nil, errors.New("event_anomaly_order")
	}
	if err := RawValid(e.RawUsageJSON); err != nil {
		return nil, err
	}
	if _, err := Canonical(&e); err != nil {
		return nil, err
	}
	if e.EventType == "request_started" {
		return &e, nil
	}
	if !e.TerminalObserved && (!Has(e.AnomalyCodes, "missing_end") || *e.ObservationStatus == "complete" || *e.BusinessOutcome == "response_complete") {
		return nil, errors.New("event_terminal")
	}
	if e.RequestFinishedAtMs != nil && *e.RequestFinishedAtMs < e.RequestStartedAtMs && !Has(e.AnomalyCodes, "clock_anomaly") {
		return nil, errors.New("event_clock")
	}
	expected, conflict := Status(e.Usage)
	if e.UsageStatus == "conflict" {
		if !Has(e.AnomalyCodes, "usage_conflict") || e.UsageEligible {
			return nil, errors.New("event_usage_conflict")
		}
	} else if conflict || e.UsageStatus != expected || Has(e.AnomalyCodes, "usage_conflict") {
		return nil, errors.New("event_usage_state")
	}
	if e.UsageStatus == "unknown" && !Has(e.AnomalyCodes, "missing_usage") {
		return nil, errors.New("event_missing_usage")
	}
	for _, a := range e.AnomalyCodes {
		if e.UsageEligible && a != "invalid_model" {
			return nil, errors.New("event_eligible")
		}
		if *e.ObservationStatus == "complete" && Has([]string{"missing_end", "protocol_error", "protocol_incomplete", "parse_limit", "transport_lost", "observation_timeout"}, a) {
			return nil, errors.New("event_complete")
		}
	}
	return &e, nil
}
func Has(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
func Add(xs []string, x string) []string {
	if !Has(xs, x) {
		xs = append(xs, x)
		sort.Strings(xs)
	}
	return xs
}
func Unknown() *Usage {
	u := &Usage{}
	for _, t := range Tokens(u) {
		t.Source = "unknown"
	}
	return u
}
func Tokens(u *Usage) []*Token {
	return []*Token{&u.InputTokens, &u.OutputTokens, &u.CachedReadTokens, &u.CachedWriteTokens, &u.ReasoningTokens, &u.ReportedTotalTokens}
}
func Status(u *Usage) (string, bool) {
	s := "unknown"
	for _, t := range Tokens(u) {
		if t.Value != nil {
			s = "partial"
		}
	}
	i, o := u.InputTokens.Value, u.OutputTokens.Value
	if i != nil && o != nil {
		s = "known"
	}
	bad := false
	for _, p := range [][2]*int64{{u.CachedReadTokens.Value, i}, {u.CachedWriteTokens.Value, i}, {u.ReasoningTokens.Value, o}} {
		bad = bad || p[0] != nil && p[1] != nil && *p[0] > *p[1]
	}
	a, b := u.CachedReadTokens.Value, u.CachedWriteTokens.Value
	if a != nil && b != nil && i != nil {
		bad = bad || *a > math.MaxInt64-*b || *a+*b > *i
	}
	if i != nil && o != nil {
		if *i > math.MaxInt64-*o {
			bad = true
		} else if u.ReportedTotalTokens.Value != nil {
			bad = bad || *i+*o != *u.ReportedTotalTokens.Value
		}
	}
	return s, bad
}
func RawValid(s string) error {
	if len(s) > 4096 {
		return errors.New("raw_usage")
	}
	var m map[string]json.RawMessage
	if strictjson.Decode([]byte(s), &m) != nil || m == nil {
		return errors.New("raw_usage")
	}
	for k, b := range m {
		switch k {
		case "prompt_tokens", "completion_tokens", "total_tokens":
			var n int64
			if json.Unmarshal(b, &n) != nil || bytes.Equal(bytes.TrimSpace(b), []byte("null")) || n < 0 {
				return errors.New("raw_usage")
			}
		case "prompt_tokens_details", "completion_tokens_details":
			var d map[string]json.RawMessage
			if json.Unmarshal(b, &d) != nil || d == nil {
				return errors.New("raw_usage")
			}
			for key, v := range d {
				if !(k == "prompt_tokens_details" && (key == "cached_tokens" || key == "cache_write_tokens") || k == "completion_tokens_details" && key == "reasoning_tokens") {
					return errors.New("raw_usage")
				}
				var n int64
				if json.Unmarshal(v, &n) != nil || bytes.Equal(bytes.TrimSpace(v), []byte("null")) || n < 0 {
					return errors.New("raw_usage")
				}
			}
		default:
			return errors.New("raw_usage")
		}
	}
	return nil
}
