package requesttokens

import (
	"sync"
)

// Result is an input estimate, independent of model-reported response usage.
type Result struct {
	Value        *int64
	Reason       string
	RequestModel *string
}
type Job struct {
	Chunks    [][]byte
	BodyBytes int64
	Reserved  int64
	Complete  func(Result)
}
type Pool struct {
	mu          sync.Mutex
	jobs        chan Job
	queuedBytes int64
	maxBytes    int64
	closed      bool
	done        sync.WaitGroup
	reserve     func(int64) bool
	release     func(int64)
	report      func(Result)
	count       func([]byte) (int64, *string, error)
}

func NewPool(workers, requests int, maxBytes int64, reserve func(int64) bool, release func(int64), report func(Result)) *Pool {
	p := &Pool{jobs: make(chan Job, requests), maxBytes: maxBytes, reserve: reserve, release: release, report: report}
	p.done.Add(workers)
	for i := 0; i < workers; i++ {
		go p.worker()
	}
	return p
}
func (p *Pool) Submit(j Job) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || j.Reserved > p.maxBytes-p.queuedBytes {
		return false
	}
	select {
	case p.jobs <- j:
		p.queuedBytes += j.Reserved
		return true
	default:
		return false
	}
}
func (p *Pool) worker() {
	defer p.done.Done()
	c, initErr := NewCounter()
	for j := range p.jobs {
		p.mu.Lock()
		p.queuedBytes -= j.Reserved
		closed := p.closed
		p.mu.Unlock()
		// Cover joined input, JSON copies, UTF-32 regex scratch and BPE working
		// arrays before allocating. This is a logical bound, not an RSS guarantee.
		scratch := 32*j.BodyBytes + 65536
		var result Result
		reserved := false
		reason := "resource_limit"
		if closed {
			reason = "shutdown"
		} else if initErr != nil {
			reason = "tokenizer_error"
		} else if p.reserve(scratch) {
			reserved = true
		}
		if reserved {
			body := make([]byte, 0, int(j.BodyBytes))
			for _, b := range j.Chunks {
				body = append(body, b...)
			}
			var n int64
			var err error
			if p.count != nil {
				n, result.RequestModel, err = p.count(body)
			} else {
				n, result.RequestModel, err = c.Analyze(body)
			}
			clear(body)
			if err == nil {
				result.Value = &n
				reason = "known"
			} else {
				reason = err.Error()
			}
			p.release(scratch)
		}
		for _, b := range j.Chunks {
			clear(b)
		}
		p.release(j.Reserved)
		result.Reason = reason
		p.report(result)
		if j.Complete != nil {
			j.Complete(result)
		}
	}
}
func (p *Pool) Stats() (int, int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.jobs), p.queuedBytes
}

// Running work retains its slot and credits; Close never spawns replacements.
func (p *Pool) Close() {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.jobs)
	}
	p.mu.Unlock()
	p.done.Wait()
}
