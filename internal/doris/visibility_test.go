package doris

import (
	"strings"
	"testing"
)

func TestVisibilityPredicateKeepsCorrelatedKeys(t *testing.T) {
	got := visibilityPredicate([]string{"date", "observation", "hash"}, [][]string{{"'today'", "'a'", "'x'"}, {"'today'", "'b'", "'y'"}})
	want := "`date`='today' AND ((`observation`='a' AND `hash`='x') OR (`observation`='b' AND `hash`='y'))"
	if got != want {
		t.Fatalf("key pairs lost correlation: %s", got)
	}
}

func TestVisibilityPredicateFactorsPartitionAndEscapedLiterals(t *testing.T) {
	topic := quote("synthetic' OR 1=1 --\\topic")
	got := visibilityPredicate([]string{"topic", "partition", "offset", "hash"}, [][]string{{topic, "'0'", "'1'", "'hash'"}, {topic, "'0'", "'2'", "'hash'"}})
	if strings.Count(got, topic) != 1 || !strings.Contains(got, "`offset` IN ('1','2')") || !strings.Contains(got, "`hash`='hash'") {
		t.Fatalf("partition key factoring: %s", got)
	}
}

func TestVisibilityPredicateChecksReceiptFields(t *testing.T) {
	ks := []string{"batch_id", "next_offset", "notification_count", "notification_digest"}
	got := visibilityPredicate(ks, [][]string{{"'batch'", "'20000'", "'1'", "'digest'"}})
	for _, part := range []string{"`batch_id`='batch'", "`next_offset`='20000'", "`notification_count`='1'", "`notification_digest`='digest'"} {
		if !strings.Contains(got, part) {
			t.Fatalf("receipt confirmation omitted %s: %s", part, got)
		}
	}
}
