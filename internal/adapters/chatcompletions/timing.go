package chatcompletions

import "encoding/json"

// SetElapsed records gateway callback time, never Observer receipt/parse time.
// Missing or regressing metadata poisons subsequent timings, but preserves an
// already measured TTFT and never changes usage or observation eligibility.
func (p *Parser) SetElapsed(us *int64) {
	p.ElapsedUS = us
	if us == nil || *us < 0 || (p.previousUS != nil && *us < *p.previousUS) {
		p.timingBroken = true
		p.LastOutputUS = nil
	}
	if us != nil {
		v := *us
		p.previousUS = &v
	}
}
func generated(raw json.RawMessage) bool {
	var s string
	return json.Unmarshal(raw, &s) == nil && s != ""
}
func (p *Parser) output(m map[string]json.RawMessage) {
	var choices []struct {
		Delta map[string]json.RawMessage `json:"delta"`
	}
	if json.Unmarshal(m["choices"], &choices) != nil {
		return
	}
	found := false
	for _, c := range choices {
		for _, k := range []string{"content", "reasoning_content", "reasoning", "refusal"} {
			found = found || generated(c.Delta[k])
		}
		var tools []struct {
			Function map[string]json.RawMessage `json:"function"`
		}
		json.Unmarshal(c.Delta["tool_calls"], &tools)
		var legacy map[string]json.RawMessage
		json.Unmarshal(c.Delta["function_call"], &legacy)
		for _, t := range tools {
			found = found || generated(t.Function["name"]) || generated(t.Function["arguments"])
		}
		found = found || generated(legacy["name"]) || generated(legacy["arguments"])
	}
	if found && !p.timingBroken && p.ElapsedUS != nil {
		v := *p.ElapsedUS
		if p.FirstOutputUS == nil {
			p.FirstOutputUS = &v
		}
		p.LastOutputUS = &v
	}
}
