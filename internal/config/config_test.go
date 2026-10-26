package config

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestReadKafkaTopicIsolationAndInvalidNames(t *testing.T) {
	for _, topic := range []string{"llm-metering-events", "llm-metering-k8s-0123456789ab", "", ".", "..", "bad/topic", "bad topic", "bad\nname", strings.Repeat("a", 250)} {
		t.Run(topic, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			// Escape untrusted topic strings as JSON, including control characters.
			data := []byte(`{"kafka":{"bootstrap_servers":["kafka:9092"],"topic":` + strconv.Quote(topic) + `}}`)
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			_, err := Read(path)
			valid := topic == "llm-metering-events" || topic == "llm-metering-k8s-0123456789ab"
			if (err == nil) != valid {
				t.Fatalf("Read topic %q error = %v", topic, err)
			}
		})
	}
}

func TestObserverWasmDownloadConfiguration(t *testing.T) {
	for _, path := range []string{"../../configs/observer.local.json", "../../deploy/local/observer.json", "../../configs/observer.production.example.json"} {
		t.Run(path, func(t *testing.T) {
			t.Setenv("POD_UID", "observer-test")
			c, err := Read(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.ObserverDefaults(); err != nil || c.Observer.WasmDownload == nil || c.Observer.WasmDownload.File == "" {
				t.Fatalf("example config: %v", err)
			}
			c.Observer.WasmDownload.Listen = ""
			if err := c.ObserverDefaults(); err != nil || c.Observer.WasmDownload.Listen != ":9093" {
				t.Fatalf("default listener: %v", err)
			}
			c.Observer.WasmDownload.File = ""
			if err := c.ObserverDefaults(); err == nil {
				t.Fatal("missing file accepted")
			}
			c.Observer.WasmDownload = nil
			if err := c.ObserverDefaults(); err != nil {
				t.Fatalf("legacy config: %v", err)
			}
		})
	}
}

func TestConsumerFlushConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name, path string
		override   int
		want       int
		wantErr    bool
	}{
		{name: "local_default", path: "../../configs/consumer.local.json", want: 15000},
		{name: "deployment_default", path: "../../deploy/local/consumer.json", want: 15000},
		{name: "production_example", path: "../../configs/consumer.production.example.json", want: 15000},
		{name: "explicit_shorter_interval", path: "../../configs/consumer.local.json", override: 1000, want: 1000},
		{name: "minimum_interval", path: "../../configs/consumer.local.json", override: 1, want: 1},
		{name: "above_maximum", path: "../../configs/consumer.local.json", override: 15001, wantErr: true},
		{name: "negative_interval", path: "../../configs/consumer.local.json", override: -1, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := Read(tc.path)
			if err != nil {
				t.Fatal(err)
			}
			if tc.override != 0 {
				c.Consumer.FlushMS = tc.override
			}
			if err = c.ConsumerDefaults(); (err != nil) != tc.wantErr {
				t.Fatalf("ConsumerDefaults() error = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && c.Consumer.FlushMS != tc.want {
				t.Fatalf("flush interval = %d, want %d", c.Consumer.FlushMS, tc.want)
			}
		})
	}
}

func TestRejectRemovedAdmissionFields(t *testing.T) {
	for _, component := range []string{"observer", "consumer"} {
		for _, field := range []string{"accepted_service_ids", "accepted_request_hosts"} {
			data, err := os.ReadFile("../../configs/" + component + ".local.json")
			if err != nil {
				t.Fatal(err)
			}
			data = []byte(strings.Replace(string(data), `"accepted_config_versions":`, strconv.Quote(field)+`: ["example.com"], "accepted_config_versions":`, 1))
			path := filepath.Join(t.TempDir(), "config.json")
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := Read(path); err == nil {
				t.Fatalf("%s accepted removed field %s", component, field)
			}
		}
	}
}

func TestLookupContextAndURI(t *testing.T) {
	for _, change := range []func(*Observer){
		func(o *Observer) { o.ClusterName = "" }, func(o *Observer) { o.DeploymentEnvironment = "staging" },
		func(o *Observer) { o.ServiceLookup.URI = "https://user:password@example.invalid/" },
		func(o *Observer) { o.CustomerLookup.URI = "https://example.invalid/?api_key=secret" },
		func(o *Observer) { o.ServiceLookup.URI = "relative/path" },
	} {
		c, err := Read("../../configs/observer.local.json")
		if err != nil {
			t.Fatal(err)
		}
		change(&c.Observer)
		if c.ObserverDefaults() == nil {
			t.Fatal("invalid lookup configuration accepted")
		}
	}
	for _, name := range []string{"metering", "metering_isolated_123", "x"} {
		if !DatabaseName(name) {
			t.Fatal("valid database rejected")
		}
	}
	for _, name := range []string{"", "metering.bad", "metering`", "x;DROP DATABASE metering", "a-b"} {
		if DatabaseName(name) {
			t.Fatal("unsafe database accepted")
		}
	}
}

func TestLookupEnvironmentAndBounds(t *testing.T) {
	for _, environment := range []string{"prd", "stg", "dev", "production", "test", "development", "PRD", ""} {
		want := environment == "prd" || environment == "stg" || environment == "dev"
		if Environment(environment) != want {
			t.Fatal("environment alias accepted")
		}
	}
	for _, change := range []func(*Observer){
		func(o *Observer) { o.LookupConcurrency = 257 }, func(o *Observer) { o.LookupTimeoutMS = 2001 },
		func(o *Observer) { o.ServiceLookup = Lookup{URI: "ftp://example.com/", CredentialEnv: "TOKEN"} },
		func(o *Observer) { o.ServiceLookup = Lookup{URI: "https://example.com/"} },
		func(o *Observer) { o.ServiceLookup.MaxRPS = -1 },
	} {
		c, err := Read("../../configs/observer.local.json")
		if err != nil {
			t.Fatal(err)
		}
		change(&c.Observer)
		if c.ObserverDefaults() == nil {
			t.Fatal("lookup configuration accepted")
		}
	}
	for _, scheme := range []string{"http", "https"} {
		c, err := Read("../../configs/observer.local.json")
		if err != nil {
			t.Fatal(err)
		}
		c.Observer.ServiceLookup = Lookup{URI: scheme + "://example.com/service", CredentialEnv: "TOKEN"}
		c.Observer.CustomerLookup = Lookup{URI: scheme + "://example.com/customer", CredentialEnv: "TOKEN"}
		if err := c.ObserverDefaults(); err != nil {
			t.Fatalf("supported lookup scheme: %v", err)
		}
	}
}

func TestLookupStartupErrorsIdentifyFieldWithoutInput(t *testing.T) {
	const sensitive = "private-credential/never-log-this"
	for _, tc := range []struct {
		name, field string
		change      func(*Observer)
	}{
		{"timeout", "lookup_timeout_ms", func(o *Observer) { o.LookupTimeoutMS = 2001 }},
		{"concurrency", "lookup_max_concurrency", func(o *Observer) { o.LookupConcurrency = 257 }},
		{"service_rate", "service_lookup.max_requests_per_second", func(o *Observer) { o.ServiceLookup.MaxRPS = -1 }},
		{"customer_rate", "customer_lookup.max_requests_per_second", func(o *Observer) { o.CustomerLookup.MaxRPS = 100001 }},
		{"service_credential", "service_lookup.credential_env", func(o *Observer) { o.ServiceLookup.CredentialEnv = sensitive }},
		{"customer_credential", "customer_lookup.credential_env", func(o *Observer) {
			o.CustomerLookup.URI = "https://example.invalid/customer"
			o.CustomerLookup.CredentialEnv = sensitive
		}},
		{"service_ca", "service_lookup.ca_file", func(o *Observer) { o.ServiceLookup.CAFile = sensitive + strings.Repeat("x", 4096) }},
		{"customer_ca", "customer_lookup.ca_file", func(o *Observer) {
			o.CustomerLookup.URI = "https://example.invalid/customer"
			o.CustomerLookup.CAFile = sensitive + strings.Repeat("x", 4096)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("POD_UID", "observer-test")
			c, err := Read("../../configs/observer.local.json")
			if err != nil {
				t.Fatal(err)
			}
			c.Observer.ServiceLookup.URI = "https://example.invalid/service"
			tc.change(&c.Observer)
			err = c.ObserverDefaults()
			if err == nil || !strings.HasPrefix(err.Error(), "config_lookup:") || !strings.Contains(err.Error(), "observer."+tc.field) {
				t.Fatalf("missing field diagnostic: %v", err)
			}
			if strings.Contains(err.Error(), sensitive) || strings.Contains(err.Error(), "example.invalid") {
				t.Fatal("configuration error contains input")
			}
		})
	}
}

func TestServiceLookupEnabledWithCustomerLookupEmpty(t *testing.T) {
	t.Setenv("POD_UID", "observer-test")
	c, err := Read("../../configs/observer.production.example.json")
	if err != nil {
		t.Fatal(err)
	}
	c.Observer.ClusterName = "test-cluster"
	c.Observer.DeploymentEnvironment = "stg"
	c.Observer.ServiceLookup.URI = "http://service.example.invalid/api/service/subsystem-by-host"
	c.Observer.CustomerLookup.URI = ""
	if err := c.ObserverDefaults(); err != nil {
		t.Fatalf("service lookup with customer placeholder rejected: %v", err)
	}
}
