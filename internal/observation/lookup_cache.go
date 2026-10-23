package observation

import (
	"container/list"
	"context"
	"sync/atomic"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/health"
)

// All cache, flight and admission transitions share lookupState.mu. Remote
// calls and health callbacks never run while holding it. Metrics are atomic so
// the health handler can read them without acquiring the cache lock.
type cacheMetrics struct {
	hits, misses, shared, stale, evictions, failures, entries atomic.Int64
}

func (m *cacheMetrics) register(h *health.State, kind string) {
	if h == nil {
		return
	}
	for name, counter := range map[string]*atomic.Int64{
		"hits_total": &m.hits, "misses_total": &m.misses,
		"shared_waits_total": &m.shared, "stale_used_total": &m.stale,
		"evictions_total": &m.evictions, "refresh_failures_total": &m.failures,
		"entries": &m.entries,
	} {
		h.GaugeFunc("lookup_cache_"+kind+"_"+name, counter.Load)
	}
}

// Outcomes retain only validated identities and fixed status codes, never the
// resolver's error text, raw response, request or credential.
type lookupValue[V any] struct {
	value     V
	status    string
	transient bool
}

func timedOut[V any]() lookupValue[V] {
	return lookupValue[V]{status: "lookup_timeout", transient: true}
}

type cacheEntry[K comparable, V any] struct {
	key                               K
	value                             V
	freshUntil, hardUntil, retryAfter time.Time
	lastFailure                       lookupValue[V]
}

type lookupFlight[V any] struct {
	ctx       context.Context
	done      chan struct{}
	completed bool
	result    lookupValue[V]
	confirmed lookupValue[V]
}

// Pending snapshots share a revocation guard for their key, even after a cache
// entry expires or is evicted. Revocation detaches this generation so later
// requests can query again; it is not a persistent negative cache.
type lookupGuard struct {
	readers int
	status  string
}

type lookupTicket[K comparable, V any] struct {
	cache  *lookupCache[K, V]
	key    K
	flight *lookupFlight[V]
	ready  bool
	result lookupValue[V]
	guard  *lookupGuard
}

type lookupCache[K comparable, V any] struct {
	settings config.LookupCacheSettings
	items    map[K]*list.Element
	lru      list.List
	flights  map[K]*lookupFlight[V]
	guards   map[K]*lookupGuard
	metrics  cacheMetrics
}

func newLookupCache[K comparable, V any](settings config.LookupCacheSettings) *lookupCache[K, V] {
	return &lookupCache[K, V]{settings: settings, items: make(map[K]*list.Element), flights: make(map[K]*lookupFlight[V]), guards: make(map[K]*lookupGuard)}
}

func (c *lookupCache[K, V]) remove(element *list.Element) {
	delete(c.items, element.Value.(*cacheEntry[K, V]).key)
	c.lru.Remove(element)
	c.metrics.entries.Add(-1)
}

func (c *lookupCache[K, V]) entry(key K, now time.Time) *cacheEntry[K, V] {
	if element := c.items[key]; element != nil {
		entry := element.Value.(*cacheEntry[K, V])
		if !now.Before(entry.hardUntil) {
			c.remove(element)
			return nil
		}
		c.lru.MoveToFront(element)
		return entry
	}
	return nil
}

func (c *lookupCache[K, V]) probe(key K, now time.Time) *lookupTicket[K, V] {
	t := &lookupTicket[K, V]{cache: c, key: key}
	if !c.settings.Enabled {
		return t
	}
	t.guard = c.guards[key]
	if t.guard == nil {
		t.guard = &lookupGuard{}
		c.guards[key] = t.guard
	}
	t.guard.readers++
	entry := c.entry(key, now)
	if entry != nil && now.Before(entry.freshUntil) {
		c.metrics.hits.Add(1)
		t.ready, t.result = true, lookupValue[V]{value: entry.value, status: "mapped"}
		return t
	}
	c.metrics.misses.Add(1)
	if entry != nil && now.Before(entry.retryAfter) {
		// Recheck the entry at snapshot freeze: another side may still be
		// waiting, and a hard expiry or definitive error can revoke this value.
		t.ready, t.result = true, entry.lastFailure
		return t
	}
	if flight := c.flights[key]; flight != nil {
		c.metrics.shared.Add(1)
		t.flight = flight
	}
	return t
}

func (t *lookupTicket[K, V]) needsWorker() bool {
	return t != nil && !t.ready && t.flight == nil
}

func (t *lookupTicket[K, V]) start(ctx context.Context) {
	t.flight = &lookupFlight[V]{ctx: ctx, done: make(chan struct{})}
	if t.cache.settings.Enabled {
		t.cache.flights[t.key] = t.flight
	}
}

func (c *lookupCache[K, V]) recordFailure(key K, result lookupValue[V], now time.Time) {
	if !c.settings.Enabled {
		return
	}
	c.metrics.failures.Add(1)
	if result.transient {
		if entry := c.entry(key, now); entry != nil {
			entry.retryAfter = now.Add(c.settings.RetryInterval)
			entry.lastFailure = result
		}
	} else {
		c.revoke(key, result.status)
	}
}

func (c *lookupCache[K, V]) revoke(key K, status string) {
	if guard := c.guards[key]; guard != nil {
		guard.status = status
		delete(c.guards, key)
	}
	if element := c.items[key]; element != nil {
		c.remove(element)
	}
}

// Header confirmation only changes attribution eligibility. The original
// worker still reads diagnostics within its original deadline, preserving logs,
// admission ownership and token cleanup without another reader goroutine.
func (t *lookupTicket[K, V]) confirmFailure(result lookupValue[V]) {
	if t.flight.confirmed.status == "" {
		if !t.flight.completed && t.cache.settings.Enabled {
			t.cache.metrics.failures.Add(1)
		}
		t.flight.confirmed = result
	}
	t.cache.revoke(t.key, t.flight.confirmed.status)
}

func (t *lookupTicket[K, V]) reject(result lookupValue[V], now time.Time) {
	t.ready, t.result = true, result
	t.cache.recordFailure(t.key, result, now)
}

// Closing done publishes the result once. A timed-out flight stays registered
// until its actual worker exits, bounding uncooperative resolvers as well.
func (t *lookupTicket[K, V]) complete(result lookupValue[V], now time.Time) {
	// Definitive late errors revoke cache data and pending candidates. An
	// already published outcome is not sent again; frozen events never change.
	if result.status != "mapped" && !result.transient {
		t.cache.revoke(t.key, result.status)
	}
	if t.flight.completed {
		return
	}
	if t.flight.confirmed.status != "" {
		result = t.flight.confirmed
	} else if lookupExpired(t.flight.ctx) {
		result = timedOut[V]()
	}
	c := t.cache
	if result.status == "mapped" && c.settings.Enabled {
		if element := c.items[t.key]; element != nil {
			c.remove(element)
		}
		entry := &cacheEntry[K, V]{key: t.key, value: result.value, freshUntil: now.Add(c.settings.TTL)}
		entry.hardUntil = entry.freshUntil.Add(c.settings.StaleIfError)
		c.items[t.key] = c.lru.PushFront(entry)
		c.metrics.entries.Add(1)
		if c.lru.Len() > c.settings.MaxEntries {
			c.remove(c.lru.Back())
			c.metrics.evictions.Add(1)
		}
	} else if result.status != "mapped" && t.flight.confirmed.status == "" {
		c.recordFailure(t.key, result, now)
	}
	t.flight.result, t.flight.completed = result, true
	close(t.flight.done)
}

func (t *lookupTicket[K, V]) workerExited() {
	if t.cache.flights[t.key] == t.flight {
		delete(t.cache.flights, t.key)
	}
}

func (t *lookupTicket[K, V]) waitChannel() <-chan struct{} {
	if t == nil || t.ready {
		return nil
	}
	return t.flight.done
}

func (t *lookupTicket[K, V]) freeze(now time.Time) lookupValue[V] {
	defer func() {
		if t.guard != nil {
			t.guard.readers--
			if t.guard.readers == 0 && t.cache.guards[t.key] == t.guard {
				delete(t.cache.guards, t.key)
			}
		}
	}()
	result := t.result
	if !t.ready {
		if !t.flight.completed && lookupExpired(t.flight.ctx) {
			t.complete(timedOut[V](), now)
		}
		result = timedOut[V]() // A joined task can have a later deadline.
		if t.flight.completed {
			result = t.flight.result
		}
	}
	if t.flight != nil && t.flight.confirmed.status != "" {
		return t.flight.confirmed
	}
	if t.guard != nil && t.guard.status != "" {
		return lookupValue[V]{status: t.guard.status}
	}
	if result.transient && t.cache.settings.Enabled {
		if entry := t.cache.entry(t.key, now); entry != nil {
			t.cache.metrics.stale.Add(1)
			return lookupValue[V]{value: entry.value, status: "mapped"}
		}
	}
	return result
}

func lookupExpired(ctx context.Context) bool {
	if ctx.Err() != nil {
		return true
	}
	deadline, ok := ctx.Deadline()
	return ok && !time.Now().Before(deadline)
}
