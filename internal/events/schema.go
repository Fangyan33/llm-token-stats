package events

import (
	"encoding/json"
	"fmt"
	eventsv1 "github.com/Fangyan33/llm-token-stats/api/events/v1"
	"reflect"
	"regexp"
	"strconv"
)

var schema map[string]any

func init() {
	if err := json.Unmarshal(eventsv1.Schema, &schema); err != nil {
		panic(err)
	}
}
func matches(v any, s map[string]any) bool { return check(v, s) == nil }
func check(v any, s map[string]any) error {
	bad := func() error { return fmt.Errorf("event_schema") }
	if ref, ok := s["$ref"].(string); ok {
		if err := check(v, schema["$defs"].(map[string]any)[ref[len("#/$defs/"):]].(map[string]any)); err != nil {
			return err
		}
	}
	if t, ok := s["type"]; ok {
		types, ok := t.([]any)
		if !ok {
			types = []any{t}
		}
		valid := false
		for _, t := range types {
			switch t {
			case "null":
				valid = valid || v == nil
			case "string":
				_, yes := v.(string)
				valid = valid || yes
			case "object":
				_, yes := v.(map[string]any)
				valid = valid || yes
			case "array":
				_, yes := v.([]any)
				valid = valid || yes
			case "boolean":
				_, yes := v.(bool)
				valid = valid || yes
			case "integer":
				n, yes := v.(json.Number)
				if yes {
					_, err := strconv.ParseInt(string(n), 10, 64)
					valid = valid || err == nil
				}
			}
		}
		if !valid {
			return bad()
		}
	}
	if c, ok := s["const"]; ok {
		if !reflect.DeepEqual(v, c) {
			if n, ok := v.(json.Number); !ok || string(n) != fmt.Sprint(c) {
				return bad()
			}
		}
	}
	if es, ok := s["enum"].([]any); ok {
		yes := false
		for _, e := range es {
			yes = yes || reflect.DeepEqual(v, e)
		}
		if !yes {
			return bad()
		}
	}
	if ss, ok := s["allOf"].([]any); ok {
		for _, x := range ss {
			if err := check(v, x.(map[string]any)); err != nil {
				return err
			}
		}
	}
	if ss, ok := s["anyOf"].([]any); ok {
		yes := false
		for _, x := range ss {
			yes = yes || matches(v, x.(map[string]any))
		}
		if !yes {
			return bad()
		}
	}
	if x, ok := s["if"].(map[string]any); ok {
		k := "else"
		if matches(v, x) {
			k = "then"
		}
		if x, ok := s[k].(map[string]any); ok {
			if err := check(v, x); err != nil {
				return err
			}
		}
	}
	switch x := v.(type) {
	case map[string]any:
		props, _ := s["properties"].(map[string]any)
		if req, ok := s["required"].([]any); ok {
			for _, k := range req {
				if _, ok := x[k.(string)]; !ok {
					return bad()
				}
			}
		}
		for k, val := range x {
			if p, ok := props[k].(map[string]any); ok {
				if err := check(val, p); err != nil {
					return err
				}
			} else if s["additionalProperties"] == false {
				return bad()
			}
		}
	case []any:
		if n, ok := s["maxItems"].(float64); ok && len(x) > int(n) {
			return bad()
		}
		seen := map[string]bool{}
		for _, v := range x {
			b, _ := json.Marshal(v)
			if s["uniqueItems"] == true && seen[string(b)] {
				return bad()
			}
			seen[string(b)] = true
			if it, ok := s["items"].(map[string]any); ok {
				if err := check(v, it); err != nil {
					return err
				}
			}
		}
	case string:
		if n, ok := s["maxLength"].(float64); ok && len(x) > int(n) {
			return bad()
		}
		if n, ok := s["minLength"].(float64); ok && len(x) < int(n) {
			return bad()
		}
		if p, ok := s["pattern"].(string); ok && !regexp.MustCompile(p).MatchString(x) {
			return bad()
		}
	case json.Number:
		n, err := strconv.ParseInt(string(x), 10, 64)
		if err != nil {
			return bad()
		}
		if lo, ok := s["minimum"].(float64); ok && float64(n) < lo {
			return bad()
		}
		if hi, ok := s["maximum"].(float64); ok && float64(n) > hi {
			return bad()
		}
	}
	return nil
}
