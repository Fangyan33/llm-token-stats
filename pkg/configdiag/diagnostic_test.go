package configdiag

import (
	"errors"
	"strings"
	"testing"
)

func TestUnknownValidationFailureIsNotSilentlyAccepted(t *testing.T) {
	cause := errors.New("private-config-value")
	r := &Report{}
	r.Merge(cause)
	if r.Err() == nil || !errors.Is(r.Err(), cause) {
		t.Fatal("validation failure lost")
	}
	if strings.Contains(strings.Join(r.Issues[0].Fields(), " "), "private-config-value") {
		t.Fatal("raw error exposed")
	}
}

func TestMergedReportsBoundDetailsAndKeepCountsAndArrayPaths(t *testing.T) {
	r := &Report{}
	for n := 0; n < 80; n++ {
		r.Number(-1, 0, 100, "config_invalid", Index("offsets", n))
	}
	merged := &Report{}
	merged.Merge(Prefix(r, "consumer"))
	if merged.Total != 80 || len(merged.Issues) != MaxIssues || merged.Issues[0].Field != "consumer.offsets[0]" {
		t.Fatal("lost count or indexed path")
	}
	if got := Prefix(New("config_invalid", "$", "required", "list", nil), "headers").(*Report).Issues[0].Field; got != "headers" {
		t.Fatal(got)
	}
}
