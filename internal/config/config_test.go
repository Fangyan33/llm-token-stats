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

func TestServiceAdmissionAndLegacyConfiguration(t *testing.T) {
	for _, component := range []string{"observer", "consumer"} {
		for _, mode := range []string{"services", "legacy_routes", "legacy_without_route", "missing", "duplicate", "mixed"} {
			t.Run(component+"/"+mode, func(t *testing.T) {
				t.Setenv("POD_UID", "observer-test")
				c, err := Read("../../configs/" + component + ".local.json")
				if err != nil {
					t.Fatal(err)
				}
				services := []string{"model-service-a"}
				var routes []Route
				switch mode {
				case "legacy_routes":
					services = nil
					routes = []Route{{RouteName: "old-route", ServiceID: "model-service-a"}, {RouteName: "*", ServiceID: "model-service-a"}}
				case "legacy_without_route":
					services = nil
					routes = []Route{{ServiceID: "model-service-a"}}
				case "missing":
					services = nil
				case "duplicate":
					services = append(services, services[0])
				case "mixed":
					routes = []Route{{ServiceID: "other"}}
				}
				if component == "observer" {
					c.Observer.AcceptedServices, c.Observer.Routes = services, routes
					err = c.ObserverDefaults()
				} else {
					c.Consumer.AcceptedServices, c.Consumer.Routes = services, routes
					err = c.ConsumerDefaults()
				}
				valid := mode == "services" || mode == "legacy_routes" || mode == "legacy_without_route"
				if (err == nil) != valid {
					t.Fatalf("configuration error: %v", err)
				}
				if valid && (!AcceptsService(services, routes, "model-service-a") || AcceptsService(services, routes, "unregistered")) {
					t.Fatal("service allowlist bypassed")
				}
			})
		}
	}
}
