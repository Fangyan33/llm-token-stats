package config

import (
	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
	"runtime"
)

// The pool is per Observer process, independent of live business/SSE streams.
func (c *Observer) RequestTokenDefaults() error {
	if c.RequestBodyMaxBytes == 0 {
		c.RequestBodyMaxBytes = 32 << 20
	}
	if c.RequestTokenizerWorkers == 0 {
		c.RequestTokenizerWorkers = max(1, runtime.GOMAXPROCS(0)-1)
	}
	if c.RequestTokenizerQueueRequests == 0 {
		c.RequestTokenizerQueueRequests = 200
	}
	if c.RequestTokenizerQueueBytes == 0 {
		c.RequestTokenizerQueueBytes = 512 << 20
	}
	r := &configdiag.Report{}
	r.Number(c.RequestBodyMaxBytes, 1, 128<<20, "config_observer_request_tokens", "observer.request_body_max_bytes")
	r.Number(int64(c.RequestTokenizerWorkers), 1, 1024, "config_observer_request_tokens", "observer.request_tokenizer_workers")
	r.Number(int64(c.RequestTokenizerQueueRequests), 1, 65536, "config_observer_request_tokens", "observer.request_tokenizer_queue_requests")
	r.Number(c.RequestTokenizerQueueBytes, 1, 32<<30, "config_observer_request_tokens", "observer.request_tokenizer_queue_bytes")
	return r.Err()
}
