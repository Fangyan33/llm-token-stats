package events

import (
	"os"
	"testing"
	"time"
)

func TestOnlyRequestInputMayBeEstimated(t *testing.T) {
	load := func(name string) *Event {
		t.Helper()
		b, err := os.ReadFile("../../testdata/collection/" + name + ".json")
		if err != nil {
			t.Fatal(err)
		}
		e, err := Decode(b, []byte("7f6c2a90-4b13-4b68-9d21-67283e0c5f44"), time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	valid := func(e *Event) bool {
		b, err := Canonical(e)
		if err != nil {
			return false
		}
		_, err = Decode(b, []byte(e.ObservationID), time.Time{})
		return err == nil
	}
	for _, n := range []int64{0, 1000000} {
		e := load("request-estimated")
		e.Usage.InputTokens.Value = Ptr(n)
		if !valid(e) {
			t.Fatal("valid request estimate rejected", n)
		}
	}
	for i := range Tokens(Unknown()) {
		e := load("usage-known")
		*Tokens(e.Usage)[i] = Token{Value: Ptr(int64(1)), Source: "estimated"}
		if valid(e) {
			t.Fatal("response estimate accepted", i)
		}
		if i > 0 {
			e = load("request-estimated")
			*Tokens(e.Usage)[i] = Token{Value: Ptr(int64(1)), Source: "estimated"}
			if valid(e) {
				t.Fatal("non-input request estimate accepted", i)
			}
		}
	}
	for _, change := range []func(*Event){
		func(e *Event) { e.Usage.InputTokens.Source = "reported" },
		func(e *Event) { e.Usage.InputTokens.Value = nil },
		func(e *Event) { e.UsageStatus = "known" },
		func(e *Event) { e.UsageStatus = "unknown" },
		func(e *Event) { e.UsageEligible = true },
		func(e *Event) { e.Usage.OutputTokens = Token{Value: Ptr(int64(1)), Source: "reported"} },
		func(e *Event) { e.RawUsageJSON = `{"prompt_tokens":1}` },
	} {
		e := load("request-estimated")
		change(e)
		if valid(e) {
			t.Fatal("invalid request estimate accepted")
		}
	}
	e := load("request-unknown")
	e.UsageStatus = "partial"
	if valid(e) {
		t.Fatal("unknown request marked partial")
	}
}
