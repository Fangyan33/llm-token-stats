package consumer

import "sort"

type Range struct{ First, Next int64 }

// Continuous returns the proven prefix. It cannot cross an uncovered interval.
func Continuous(origin int64, rs []Range) int64 {
	copy := append([]Range{}, rs...)
	sort.Slice(copy, func(i, j int) bool { return copy[i].First < copy[j].First })
	next := origin
	for _, r := range copy {
		if r.First < 0 || r.Next <= r.First || r.First > next {
			break
		}
		if r.Next > next {
			next = r.Next
		}
	}
	return next
}
