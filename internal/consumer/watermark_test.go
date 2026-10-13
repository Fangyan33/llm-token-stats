package consumer

import "testing"

func TestContinuousDoesNotUseMax(t *testing.T) {
	if n := Continuous(10, []Range{{20, 30}, {10, 15}}); n != 15 {
		t.Fatal(n)
	}
	if n := Continuous(10, []Range{{14, 20}, {10, 15}, {12, 18}, {20, 21}}); n != 21 {
		t.Fatal(n)
	}
}
