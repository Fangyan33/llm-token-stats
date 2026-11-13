package config

import (
	"github.com/Fangyan33/llm-token-stats/internal/logging"
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
	if c.RequestBodyMaxBytes < 1 || c.RequestBodyMaxBytes > 128<<20 || c.RequestTokenizerWorkers < 1 || c.RequestTokenizerWorkers > 1024 || c.RequestTokenizerQueueRequests < 1 || c.RequestTokenizerQueueRequests > 65536 || c.RequestTokenizerQueueBytes < 1 || c.RequestTokenizerQueueBytes > 32<<30 {
		return logging.WithCause("config_observer_request_tokens", nil)
	}
	return nil
}
