package observation

import (
	"context"
	"testing"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/config"
)

func TestHeaderFailureRevokesAllPendingCandidates(t *testing.T) {
	for _, candidateKind := range []string{"fresh", "cooldown", "evicted", "expired"} {
		t.Run(candidateKind, func(t *testing.T) {
			settings, _ := (config.LookupCache{}).Settings()
			settings.MaxEntries = 1
			c := newLookupCache[string, string](settings)
			now := time.Now()
			warm := func(key string) {
				ticket := c.probe(key, now)
				ticket.start(context.Background())
				ticket.complete(lookupValue[string]{status: "mapped", value: "old identity"}, now)
				ticket.workerExited()
				if ticket.freeze(now).status != "mapped" {
					t.Fatal("warm cache failed")
				}
			}
			warm("key")
			if candidateKind == "cooldown" {
				now = now.Add(settings.TTL)
				refresh := c.probe("key", now)
				refresh.start(context.Background())
				refresh.complete(timedOut[string](), now)
				refresh.workerExited()
				refresh.freeze(now)
			}
			candidate := c.probe("key", now)
			if !candidate.ready {
				t.Fatal("candidate was not cached")
			}
			switch candidateKind {
			case "evicted":
				warm("other")
			case "expired":
				now = now.Add(settings.TTL + settings.StaleIfError)
			case "cooldown":
				now = now.Add(settings.RetryInterval)
			default:
				now = now.Add(settings.TTL)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			owner := c.probe("key", now)
			owner.start(ctx)
			shared := c.probe("key", now)
			owner.confirmFailure(lookupValue[string]{status: "lookup_unavailable"})
			owner.confirmFailure(lookupValue[string]{status: "lookup_unavailable"})
			cancel()
			if owner.freeze(now).status != "lookup_unavailable" || candidate.freeze(now).status != "lookup_unavailable" || shared.freeze(now).status != "lookup_unavailable" {
				t.Fatal("confirmed authentication failure became timeout or a cached identity")
			}
			owner.complete(lookupValue[string]{status: "lookup_unavailable"}, now)
			owner.workerExited()
			wantFailures := int64(1)
			wantStale := int64(0)
			if candidateKind == "cooldown" {
				wantFailures++
				wantStale++
			}
			if c.metrics.failures.Load() != wantFailures || c.metrics.stale.Load() != wantStale || len(c.guards) != 0 {
				t.Fatal("duplicated failure/stale metric or leaked revocation guard")
			}
			fresh := c.probe("key", now)
			if !fresh.needsWorker() {
				t.Fatal("authentication error became persistent negative cache")
			}
			fresh.start(context.Background())
			fresh.complete(lookupValue[string]{status: "mapped", value: "new identity"}, now)
			fresh.workerExited()
			if result := fresh.freeze(now); result.status != "mapped" || result.value != "new identity" || len(c.guards) != 0 {
				t.Fatal("old revocation contaminated a new successful query")
			}
		})
	}
}
