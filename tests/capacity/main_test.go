package main

import "testing"

func TestMixedContextsMatchAcceptance(t *testing.T) {
	counts := map[int64]int{}
	var bytes int64
	for i := 0; i < 100; i++ {
		tokens := mixedInputTokens[mixedBodyIndexes[i%len(mixedBodyIndexes)]]
		counts[tokens]++
		bytes += tokens * 5
	}
	for tokens, want := range map[int64]int{1000000: 20, 200000: 50, 500000: 20, 100000: 10} {
		if counts[tokens] != want {
			t.Fatalf("tokens=%d count=%d want=%d", tokens, counts[tokens], want)
		}
	}
	if bytes != 205000000 {
		t.Fatalf("prompt bytes=%d want=205000000", bytes)
	}
}
