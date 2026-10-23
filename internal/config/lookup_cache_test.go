package config

import (
	"encoding/json"
	"testing"
	"time"
)

func TestLookupCacheDefaultsAndExplicitValues(t *testing.T) {
	for _, input := range []string{`{}`, `{"enabled":false,"stale_if_error_seconds":0}`, `{"ttl_seconds":60,"max_entries":2,"stale_retry_interval_seconds":1}`} {
		var cache LookupCache
		if err := json.Unmarshal([]byte(input), &cache); err != nil {
			t.Fatal(err)
		}
		got, err := cache.Settings()
		if err != nil {
			t.Fatal(err)
		}
		switch input {
		case `{}`:
			if !got.Enabled || got.MaxEntries != 10000 || got.TTL != 30*time.Minute || got.StaleIfError != 15*time.Minute || got.RetryInterval != 5*time.Second {
				t.Fatalf("wrong cache defaults: %+v", got)
			}
		case `{"enabled":false,"stale_if_error_seconds":0}`:
			if got.Enabled || got.StaleIfError != 0 {
				t.Fatal("explicit false or zero replaced by default")
			}
		default:
			if got.TTL != time.Minute || got.MaxEntries != 2 || got.RetryInterval != time.Second {
				t.Fatalf("independent options ignored: %+v", got)
			}
		}
	}
}

func TestLookupCacheBoundsRejectStartup(t *testing.T) {
	for _, input := range []string{
		`{"max_entries":0}`, `{"max_entries":100001}`, `{"ttl_seconds":0}`, `{"ttl_seconds":86401}`,
		`{"stale_if_error_seconds":-1}`, `{"stale_if_error_seconds":86401}`,
		`{"stale_retry_interval_seconds":0}`, `{"stale_retry_interval_seconds":301}`,
		`{"enabled":false,"ttl_seconds":-1}`,
	} {
		for _, customer := range []bool{false, true} {
			c, err := Read("../../configs/observer.local.json")
			if err != nil {
				t.Fatal(err)
			}
			var cache LookupCache
			if err := json.Unmarshal([]byte(input), &cache); err != nil {
				t.Fatal(err)
			}
			c.Observer.ServiceLookup.Cache = cache
			if customer {
				c.Observer.ServiceLookup.Cache = LookupCache{}
				c.Observer.CustomerLookup.Cache = cache
			}
			if err := c.ObserverDefaults(); err == nil || err.Error() != "config_lookup_cache" {
				t.Fatalf("bad cache configuration accepted: %s", input)
			}
		}
	}
}
