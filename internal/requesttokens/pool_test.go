package requesttokens

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolBoundsAndRunningOwnership(t *testing.T) {
	var reserved atomic.Int64
	entered, unblock := make(chan struct{}), make(chan struct{})
	results := make(chan Result, 8)
	p := NewPool(1, 1, 8, func(n int64) bool { reserved.Add(n); return true }, func(n int64) { reserved.Add(-n) }, func(r Result) { results <- r })
	p.count = func([]byte) (int64, *string, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-unblock
		return 1, nil, nil
	}
	job := func(n int64) Job {
		return Job{Chunks: [][]byte{[]byte("x")}, Reserved: n, BodyBytes: 1}
	}
	reserved.Add(4)
	if !p.Submit(job(4)) {
		t.Fatal("first rejected")
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start")
	}
	reserved.Add(4)
	if !p.Submit(job(4)) {
		t.Fatal("waiting rejected")
	}
	if p.Submit(job(1)) {
		t.Fatal("count bound ignored")
	}
	if n, b := p.Stats(); n != 1 || b != 4 {
		t.Fatal(n, b)
	}
	if reserved.Load() <= 8 {
		t.Fatal("running scratch was released at dequeue")
	}
	close(unblock)
	p.Close()
	if reserved.Load() != 0 {
		t.Fatal("credits leaked", reserved.Load())
	}
	if p.Submit(job(1)) {
		t.Fatal("closed accepted")
	}
}
func TestPoolByteBound(t *testing.T) {
	// No workers is useful to test the independent byte limit deterministically.
	p := NewPool(0, 4, 8, func(int64) bool { return true }, func(int64) {}, func(Result) {})
	if !p.Submit(Job{Reserved: 8}) || p.Submit(Job{Reserved: 1}) {
		t.Fatal("byte bound ignored")
	}
	// Drain the synthetic zero-worker queue before closing it.
	<-p.jobs
	p.Close()
}
