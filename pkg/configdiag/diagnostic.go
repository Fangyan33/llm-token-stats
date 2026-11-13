// Package configdiag carries bounded, input-free configuration diagnostics in
// both native services and TinyGo. Callers supply schema paths and fixed reasons,
// never configuration strings, credentials, URLs, or parser error messages.
package configdiag

import (
	"strconv"
	"strings"
)

const MaxIssues = 32

type Issue struct {
	Code, Field, Reason, Expected string
	Actual, RelatedField, EnvName string
	Offset                        int64
	Line, Column                  int
	Cause                         error
}

type Report struct {
	Issues []Issue
	Total  int
	Source string
	Cause  error
}

func (r *Report) Error() string {
	if len(r.Issues) == 0 {
		return "config_invalid"
	}
	// Keep the field-bearing lookup diagnostic used by existing CLI callers.
	if i := r.Issues[0]; i.Code == "config_lookup" {
		return i.Code + ": " + i.Field + " " + i.Expected
	}
	return r.Issues[0].Code
}
func (r *Report) Unwrap() error { return r.Cause }
func (r *Report) Err() error {
	if r.Total == 0 {
		return nil
	}
	return r
}
func (r *Report) Add(i Issue) {
	r.Total++
	if len(r.Issues) < MaxIssues {
		r.Issues = append(r.Issues, i)
	}
}
func (r *Report) Check(ok bool, code, field, reason, expected string) {
	if !ok {
		r.Add(Issue{Code: code, Field: field, Reason: reason, Expected: expected})
	}
}
func (r *Report) Number(n, low, high int64, code, field string) {
	if n < low || n > high {
		r.Add(Issue{Code: code, Field: field, Reason: "out_of_range", Expected: "[" + strconv.FormatInt(low, 10) + ", " + strconv.FormatInt(high, 10) + "]", Actual: strconv.FormatInt(n, 10)})
	}
}
func (r *Report) Relation(ok bool, code, field, related, expected string, actual int64) {
	if !ok {
		r.Add(Issue{Code: code, Field: field, RelatedField: related, Reason: "constraint_violation", Expected: expected, Actual: strconv.FormatInt(actual, 10)})
	}
}
func (r *Report) Merge(err error) {
	if err == nil {
		return
	}
	if other := Find(err); other != nil {
		for _, i := range other.Issues {
			r.Add(i)
		}
		r.Total += other.Total - len(other.Issues)
		if r.Cause == nil {
			r.Cause = other.Cause
		}
		if r.Source == "" {
			r.Source = other.Source
		}
	} else {
		// A new unclassified validation error must never turn into success.
		r.Add(Issue{Code: "config_invalid", Field: "$", Reason: "validation_failed", Expected: "valid configuration", Cause: err})
		if r.Cause == nil {
			r.Cause = err
		}
	}
}
func New(code, field, reason, expected string, cause error) *Report {
	r := &Report{Cause: cause}
	r.Check(false, code, field, reason, expected)
	r.Issues[0].Cause = cause
	return r
}
func Find(err error) *Report {
	for depth := 0; err != nil && depth < 12; depth++ {
		if r, ok := err.(*Report); ok {
			return r
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			break
		}
		err = u.Unwrap()
	}
	return nil
}
func Source(err error, source string) error {
	if r := Find(err); r != nil {
		r.Source = source
	}
	return err
}
func Prefix(err error, prefix string) error {
	if r := Find(err); r != nil {
		copy := &Report{Total: r.Total, Source: r.Source, Cause: r.Cause}
		for _, i := range r.Issues {
			i.Field = Join(prefix, strings.TrimPrefix(i.Field, "$"))
			copy.Issues = append(copy.Issues, i)
		}
		return copy
	}
	return err
}
func Index(field string, i int) string { return field + "[" + strconv.Itoa(i) + "]" }

// EnvironmentName permits a variable's name, never its value, in diagnostics.
func EnvironmentName(name string) string {
	if len(name) == 0 || len(name) > 128 {
		return ""
	}
	for i, c := range name {
		if c != '_' && !(c >= 'A' && c <= 'Z') && !(c >= 'a' && c <= 'z') && !(i > 0 && c >= '0' && c <= '9') {
			return ""
		}
	}
	return name
}
func Environment(code, field, name, reason, expected string) *Report {
	r := New(code, field, reason, expected, nil)
	r.Issues[0].EnvName = EnvironmentName(name)
	return r
}

// Fields contains sanitized-by-construction strings, also usable by the Wasm
// logger. Numeric values must be constructed with Number/Relation, not raw input.
func (i Issue) Fields() []string {
	fields := []string{"error_code", i.Code, "config_field", i.Field, "reason", i.Reason, "expected", i.Expected}
	for _, pair := range [][2]string{{"actual", i.Actual}, {"related_field", i.RelatedField}, {"env_name", i.EnvName}} {
		if pair[1] != "" {
			fields = append(fields, pair[0], pair[1])
		}
	}
	if i.Offset > 0 {
		fields = append(fields, "byte_offset", strconv.FormatInt(i.Offset, 10), "line", strconv.Itoa(i.Line), "column", strconv.Itoa(i.Column))
	}
	return fields
}

// PathSegment bounds and escapes identifiers used for unknown configuration
// keys. Non-identifiers cannot smuggle a URL, body fragment, or controls to logs.
func PathSegment(s string) string {
	if EnvironmentName(s) == "" {
		return "<unknown>"
	}
	return s
}
func Join(prefix, field string) string {
	if field == "" || field == "$" {
		return prefix
	}
	if prefix == "" || prefix == "$" {
		return field
	}
	if strings.HasPrefix(field, "[") {
		return prefix + field
	}
	return prefix + "." + field
}
