package config

import (
	"runtime"
	"testing"
)

func TestRequestTokenSettings(t *testing.T) {
	c := Observer{}
	if c.RequestTokenDefaults() != nil || c.RequestBodyMaxBytes != 32<<20 || c.RequestTokenizerWorkers != max(1, runtime.GOMAXPROCS(0)-1) || c.RequestTokenizerQueueRequests != 200 || c.RequestTokenizerQueueBytes != 512<<20 {
		t.Fatal(c)
	}
	for _, c := range []Observer{{RequestBodyMaxBytes: -1}, {RequestBodyMaxBytes: (128 << 20) + 1}, {RequestTokenizerWorkers: -1}, {RequestTokenizerWorkers: 1025}, {RequestTokenizerQueueRequests: -1}, {RequestTokenizerQueueRequests: 65537}, {RequestTokenizerQueueBytes: -1}, {RequestTokenizerQueueBytes: (32 << 30) + 1}} {
		if c.RequestTokenDefaults() == nil {
			t.Fatal("invalid config accepted", c)
		}
	}
	c = Observer{RequestBodyMaxBytes: 128 << 20, RequestTokenizerWorkers: 64, RequestTokenizerQueueRequests: 400, RequestTokenizerQueueBytes: 1 << 30}
	if c.RequestTokenDefaults() != nil {
		t.Fatal("explicit capacity rejected")
	}
}
