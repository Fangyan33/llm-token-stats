package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
	"github.com/Fangyan33/llm-token-stats/pkg/strictjson"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

type Kafka struct {
	Brokers            []string `json:"bootstrap_servers"`
	Topic              string   `json:"topic"`
	CAFile             string   `json:"ca_file"`
	CertFile           string   `json:"cert_file"`
	KeyFile            string   `json:"key_file"`
	UsernameEnv        string   `json:"username_env"`
	PasswordEnv        string   `json:"password_env"`
	ProducerMaxRecords int      `json:"producer_max_records"`
	ProducerMaxBytes   int      `json:"producer_max_bytes"`
	ProducerBatchBytes int      `json:"producer_batch_bytes"`
	ProducerLingerMS   int      `json:"producer_linger_ms"`
	ProducerDeliveryMS int      `json:"producer_delivery_timeout_ms"`
	ProducerWatchdogMS int      `json:"producer_watchdog_timeout_ms"`
}

func (k *Kafka) ProducerDefaults() error {
	if k.ProducerMaxRecords == 0 {
		k.ProducerMaxRecords = 16384
	}
	if k.ProducerMaxBytes == 0 {
		k.ProducerMaxBytes = 64 << 20
	}
	if k.ProducerBatchBytes == 0 {
		k.ProducerBatchBytes = min(1<<20, k.ProducerMaxBytes)
	}
	if k.ProducerLingerMS == 0 {
		k.ProducerLingerMS = 5
	}
	if k.ProducerDeliveryMS == 0 {
		k.ProducerDeliveryMS = 10000
	}
	if k.ProducerWatchdogMS == 0 {
		k.ProducerWatchdogMS = k.ProducerDeliveryMS + 2000
	}
	r := &configdiag.Report{}
	r.Number(int64(k.ProducerMaxRecords), 1, 1048576, "config_producer", "kafka.producer_max_records")
	r.Number(int64(k.ProducerMaxBytes), 32768, 1<<30, "config_producer", "kafka.producer_max_bytes")
	r.Number(int64(k.ProducerBatchBytes), 1024, 4<<20, "config_producer", "kafka.producer_batch_bytes")
	r.Relation(k.ProducerBatchBytes <= k.ProducerMaxBytes, "config_producer", "kafka.producer_batch_bytes", "kafka.producer_max_bytes", "producer_batch_bytes <= producer_max_bytes", int64(k.ProducerBatchBytes))
	r.Number(int64(k.ProducerLingerMS), 0, 1000, "config_producer", "kafka.producer_linger_ms")
	r.Number(int64(k.ProducerDeliveryMS), 1000, 60000, "config_producer", "kafka.producer_delivery_timeout_ms")
	r.Number(int64(k.ProducerWatchdogMS), 1, 120000, "config_producer", "kafka.producer_watchdog_timeout_ms")
	r.Relation(k.ProducerWatchdogMS > k.ProducerDeliveryMS, "config_producer", "kafka.producer_watchdog_timeout_ms", "kafka.producer_delivery_timeout_ms", "watchdog timeout > delivery timeout", int64(k.ProducerWatchdogMS))
	return r.Err()
}

type Lookup struct {
	URI           string      `json:"uri"`
	CredentialEnv string      `json:"credential_env"`
	CAFile        string      `json:"ca_file"`
	MaxRPS        int         `json:"max_requests_per_second"`
	Cache         LookupCache `json:"cache"`
}

// Pointers distinguish omitted defaults from explicit false and zero values.
type LookupCache struct {
	Enabled                   *bool `json:"enabled,omitempty"`
	MaxEntries                *int  `json:"max_entries,omitempty"`
	TTLSeconds                *int  `json:"ttl_seconds,omitempty"`
	StaleIfErrorSeconds       *int  `json:"stale_if_error_seconds,omitempty"`
	StaleRetryIntervalSeconds *int  `json:"stale_retry_interval_seconds,omitempty"`
}

type LookupCacheSettings struct {
	Enabled       bool
	MaxEntries    int
	TTL           time.Duration
	StaleIfError  time.Duration
	RetryInterval time.Duration
}

func (c LookupCache) Settings() (LookupCacheSettings, error) { return c.settings("cache") }
func (c LookupCache) settings(prefix string) (LookupCacheSettings, error) {
	value := func(p *int, fallback int) int {
		if p != nil {
			return *p
		}
		return fallback
	}
	entries, ttl := value(c.MaxEntries, 10000), value(c.TTLSeconds, 1800)
	stale, retry := value(c.StaleIfErrorSeconds, 900), value(c.StaleRetryIntervalSeconds, 5)
	r := &configdiag.Report{}
	r.Number(int64(entries), 1, 100000, "config_lookup_cache", prefix+".max_entries")
	r.Number(int64(ttl), 1, 86400, "config_lookup_cache", prefix+".ttl_seconds")
	r.Number(int64(stale), 0, 86400, "config_lookup_cache", prefix+".stale_if_error_seconds")
	r.Number(int64(retry), 1, 300, "config_lookup_cache", prefix+".stale_retry_interval_seconds")
	if err := r.Err(); err != nil {
		return LookupCacheSettings{}, err
	}
	return LookupCacheSettings{
		Enabled:    c.Enabled == nil || *c.Enabled,
		MaxEntries: entries, TTL: time.Duration(ttl) * time.Second,
		StaleIfError: time.Duration(stale) * time.Second, RetryInterval: time.Duration(retry) * time.Second,
	}, nil
}

type Observer struct {
	ClusterName                   string        `json:"cluster_name"`
	DeploymentEnvironment         string        `json:"deployment_environment"`
	ServiceLookup                 Lookup        `json:"service_lookup"`
	CustomerLookup                Lookup        `json:"customer_lookup"`
	LookupTimeoutMS               int           `json:"lookup_timeout_ms"`
	LookupConcurrency             int           `json:"lookup_max_concurrency"`
	WasmDownload                  *WasmDownload `json:"wasm_download"`
	Listen                        string        `json:"listen"`
	Health                        string        `json:"health_listen"`
	ProducerID                    string        `json:"producer_id"`
	MaxActive                     int           `json:"max_active_observations"`
	ParserBudget                  int64         `json:"parser_budget_bytes"`
	AcceptedConfigs               []string      `json:"accepted_config_versions"`
	AcceptedAdapters              []string      `json:"accepted_adapter_ids"`
	Flags                         []string      `json:"response_flags_allowlist"`
	Details                       []string      `json:"response_code_details_allowlist"`
	MaxConnections                int           `json:"max_connections"`
	ReceiveQueueBatches           int           `json:"receive_queue_batches"`
	ReceiveQueueBytes             int64         `json:"receive_queue_bytes"`
	RequestBodyMaxBytes           int64         `json:"request_body_max_bytes"`
	RequestTokenizerWorkers       int           `json:"request_tokenizer_workers"`
	RequestTokenizerQueueRequests int           `json:"request_tokenizer_queue_requests"`
	RequestTokenizerQueueBytes    int64         `json:"request_tokenizer_queue_bytes"`
}
type WasmDownload struct {
	Listen string `json:"listen"`
	File   string `json:"file"`
}
type Doris struct {
	FE           string   `json:"fe_url"`
	AllowedHosts []string `json:"allowed_load_hosts"`
	DSNEnv       string   `json:"dsn_env"`
	Database     string   `json:"database"`
}
type Position struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}
type Consumer struct {
	Health          string     `json:"health_listen"`
	Group           string     `json:"group_id"`
	Rows            int        `json:"max_batch_rows"`
	Bytes           int        `json:"max_batch_bytes"`
	FlushMS         int        `json:"flush_interval_ms"`
	Initial         []Position `json:"initial_offsets"`
	Resume          []Position `json:"gap_resume_offsets"`
	AcceptedConfigs []string   `json:"accepted_config_versions"`
	Flags           []string   `json:"response_flags_allowlist"`
	Details         []string   `json:"response_code_details_allowlist"`
}
type Config struct {
	Kafka    Kafka    `json:"kafka"`
	Observer Observer `json:"observer"`
	Consumer Consumer `json:"consumer"`
	Doris    Doris    `json:"doris"`
}

func Read(path string) (Config, error) {
	var c Config
	b, err := os.ReadFile(path)
	if err != nil {
		return c, configdiag.Source(configdiag.New("config_file", "$", "file_read_failed", "readable configuration file", err), path)
	}
	if len(b) > 65536 {
		r := &configdiag.Report{}
		r.Number(int64(len(b)), 0, 65536, "config_invalid", "$")
		return c, configdiag.Source(r, path)
	}
	if err := strictjson.DecodeConfig(b, &c); err != nil {
		return c, configdiag.Source(err, path)
	}
	r := &configdiag.Report{Source: path}
	r.Check(kafkaTopicName.MatchString(c.Kafka.Topic) && c.Kafka.Topic != "." && c.Kafka.Topic != "..", "config_kafka", "kafka.topic", "invalid_topic", "1..249 ASCII letters, digits, '.', '_' or '-'; not '.' or '..'")
	r.Check(len(c.Kafka.Brokers) > 0, "config_kafka", "kafka.bootstrap_servers", "empty_list", "at least one broker")
	if err := r.Err(); err != nil {
		return c, err
	}
	return c, nil
}

// Kafka topic names are configurable so isolated deployments do not share data.
var kafkaTopicName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,249}$`)

func (c *Config) ObserverDefaults() error {
	r := &configdiag.Report{}
	r.Merge(c.Kafka.ProducerDefaults())
	r.Merge(c.Observer.RequestTokenDefaults())
	if c.Observer.ReceiveQueueBatches == 0 {
		c.Observer.ReceiveQueueBatches = 4096
	}
	if c.Observer.ReceiveQueueBytes == 0 {
		c.Observer.ReceiveQueueBytes = 128 << 20
	}
	r.Number(int64(c.Observer.ReceiveQueueBatches), 1, 65536, "config_observer_queue", "observer.receive_queue_batches")
	r.Number(c.Observer.ReceiveQueueBytes, 32768, 4<<30, "config_observer_queue", "observer.receive_queue_bytes")
	if c.Observer.LookupTimeoutMS == 0 {
		c.Observer.LookupTimeoutMS = 200
	}
	if c.Observer.LookupConcurrency == 0 {
		c.Observer.LookupConcurrency = 64
	}
	r.Number(int64(c.Observer.LookupTimeoutMS), 1, 2000, "config_lookup", "observer.lookup_timeout_ms")
	r.Number(int64(c.Observer.LookupConcurrency), 1, 256, "config_lookup", "observer.lookup_max_concurrency")
	for _, named := range []struct {
		name   string
		lookup Lookup
	}{{"service_lookup", c.Observer.ServiceLookup}, {"customer_lookup", c.Observer.CustomerLookup}} {
		lookup := named.lookup
		prefix := "observer." + named.name
		_, cacheErr := lookup.Cache.settings(prefix + ".cache")
		r.Merge(cacheErr)
		r.Number(int64(lookup.MaxRPS), 0, 100000, "config_lookup", prefix+".max_requests_per_second")
		if lookup.URI != "" {
			r.Check(regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`).MatchString(lookup.CredentialEnv), "config_lookup", prefix+".credential_env", "invalid_environment_name", "environment variable name")
			r.Check(lookup.CAFile == "" || text(lookup.CAFile, 4096), "config_lookup", prefix+".ca_file", "invalid_text", "UTF-8 path of at most 4096 bytes")
		}
		r.Check(LookupURI(lookup.URI), "config_observer", prefix+".uri", "invalid_uri", "empty or HTTP(S) URI without credentials, query or fragment")
	}
	if w := c.Observer.WasmDownload; w != nil {
		if w.Listen == "" {
			w.Listen = ":9093"
		}
		r.Check(text(w.File, 4096), "config_wasm_download", "observer.wasm_download.file", "invalid_text", "nonempty UTF-8 path of at most 4096 bytes")
		r.Check(text(w.Listen, 256), "config_wasm_download", "observer.wasm_download.listen", "invalid_text", "nonempty UTF-8 address of at most 256 bytes")
	}
	if c.Observer.Listen == "" {
		c.Observer.Listen = ":9090"
	}
	if c.Observer.Health == "" {
		c.Observer.Health = ":9091"
	}
	if c.Observer.MaxActive == 0 {
		c.Observer.MaxActive = 1048576
	}
	if c.Observer.ParserBudget == 0 {
		c.Observer.ParserBudget = 10 << 30
	}
	if c.Observer.MaxConnections == 0 {
		c.Observer.MaxConnections = 1024
	}
	if c.Observer.ProducerID == "" {
		c.Observer.ProducerID = os.Getenv("POD_UID")
	}
	r.Check(text(c.Observer.ClusterName, 128), "config_observer", "observer.cluster_name", "invalid_text", "nonempty UTF-8 string of at most 128 bytes")
	r.Check(Environment(c.Observer.DeploymentEnvironment), "config_observer", "observer.deployment_environment", "invalid_enum", "prd, stg or dev")
	r.Check(text(c.Observer.ProducerID, 128), "config_observer", "observer.producer_id", "invalid_text", "nonempty UTF-8 string of at most 128 bytes; defaults to POD_UID")
	r.Number(int64(c.Observer.MaxActive), 1, 1048576, "config_observer", "observer.max_active_observations")
	r.Number(c.Observer.ParserBudget, 3*32768, 64<<30, "config_observer", "observer.parser_budget_bytes")
	r.Number(int64(c.Observer.MaxConnections), 1, 4096, "config_observer", "observer.max_connections")
	validateStrings(r, c.Observer.AcceptedConfigs, 1, 1024, "config_observer", "observer.accepted_config_versions")
	r.Check(len(c.Observer.AcceptedAdapters) == 1 && c.Observer.AcceptedAdapters[0] == "chat_completions_v1", "config_observer", "observer.accepted_adapter_ids", "invalid_adapter", "exactly [chat_completions_v1]")
	validateStrings(r, c.Observer.Flags, 0, 128, "config_observer", "observer.response_flags_allowlist")
	validateStrings(r, c.Observer.Details, 0, 128, "config_observer", "observer.response_code_details_allowlist")
	return r.Err()
}
func (c *Config) ConsumerDefaults() error {
	if c.Consumer.Health == "" {
		c.Consumer.Health = ":9092"
	}
	if c.Consumer.Group == "" {
		c.Consumer.Group = "llm-metering-doris-v1"
	}
	if c.Consumer.Rows == 0 {
		c.Consumer.Rows = 5000
	}
	if c.Consumer.Bytes == 0 {
		c.Consumer.Bytes = 4 << 20
	}
	if c.Consumer.FlushMS == 0 {
		c.Consumer.FlushMS = 15000
	}
	if c.Doris.Database == "" {
		c.Doris.Database = "metering"
	}
	r := &configdiag.Report{}
	r.Check(text(c.Consumer.Group, 128), "config_consumer", "consumer.group_id", "invalid_text", "nonempty UTF-8 string of at most 128 bytes")
	r.Number(int64(c.Consumer.Rows), 1, 20000, "config_consumer", "consumer.max_batch_rows")
	r.Number(int64(c.Consumer.Bytes), 32768, 16<<20, "config_consumer", "consumer.max_batch_bytes")
	r.Number(int64(c.Consumer.FlushMS), 1, 600000, "config_consumer", "consumer.flush_interval_ms")
	r.Check(DatabaseName(c.Doris.Database), "config_consumer", "doris.database", "invalid_identifier", "ASCII letter followed by up to 63 letters, digits or underscores")
	validateStrings(r, c.Consumer.AcceptedConfigs, 1, 1024, "config_consumer", "consumer.accepted_config_versions")
	r.Check(len(c.Doris.AllowedHosts) > 0, "config_consumer", "doris.allowed_load_hosts", "empty_list", "at least one allowed load host")
	validateStrings(r, c.Consumer.Flags, 0, 128, "config_consumer", "consumer.response_flags_allowlist")
	validateStrings(r, c.Consumer.Details, 0, 128, "config_consumer", "consumer.response_code_details_allowlist")
	for j, ps := range [][]Position{c.Consumer.Initial, c.Consumer.Resume} {
		field := []string{"consumer.initial_offsets", "consumer.gap_resume_offsets"}[j]
		seen := map[int32]bool{}
		for i, p := range ps {
			prefix := configdiag.Index(field, i)
			r.Number(int64(p.Partition), 0, 2147483647, "config_offsets", prefix+".partition")
			r.Number(p.Offset, 0, 9223372036854775807, "config_offsets", prefix+".offset")
			r.Check(!seen[p.Partition], "config_offsets", prefix+".partition", "duplicate_partition", "unique partition within this offset list")
			seen[p.Partition] = true
		}
	}
	return r.Err()
}

func validateStrings(r *configdiag.Report, xs []string, low, high int64, code, field string) {
	r.Number(int64(len(xs)), low, high, code, field)
	seen := map[string]bool{}
	for i, s := range xs {
		name := configdiag.Index(field, i)
		r.Check(text(s, 128), code, name, "invalid_text", "nonempty UTF-8 string of at most 128 bytes")
		r.Check(!seen[s], code, name, "duplicate_value", "unique list entry")
		seen[s] = true
	}
}

// ReadObserver and ReadConsumer aggregate common and component-specific checks
// after decoding succeeds. Syntax/type errors never trigger validation of a
// partially decoded configuration.
func ReadObserver(path string) (Config, error) { return readComponent(path, true) }
func ReadConsumer(path string) (Config, error) { return readComponent(path, false) }
func readComponent(path string, observer bool) (Config, error) {
	c, err := Read(path)
	if err != nil {
		d := configdiag.Find(err)
		if d == nil || len(d.Issues) == 0 || d.Issues[0].Code != "config_kafka" {
			return c, err
		}
	}
	r := &configdiag.Report{Source: path}
	r.Merge(err)
	if observer {
		r.Merge(c.ObserverDefaults())
	} else {
		r.Merge(c.ConsumerDefaults())
	}
	return c, r.Err()
}

func text(s string, max int) bool { return len(s) > 0 && len(s) <= max && utf8.ValidString(s) }
func (k Kafka) TLS() (*tls.Config, error) {
	if k.CAFile == "" && k.CertFile == "" {
		return nil, nil
	}
	t := &tls.Config{MinVersion: tls.VersionTLS12}
	r := &configdiag.Report{}
	if k.CAFile != "" {
		b, err := os.ReadFile(k.CAFile)
		if err != nil {
			r.Merge(configdiag.New("kafka_tls", "kafka.ca_file", "file_read_failed", "readable PEM CA certificate file", err))
		} else {
			t.RootCAs = x509.NewCertPool()
			r.Check(t.RootCAs.AppendCertsFromPEM(b), "kafka_tls", "kafka.ca_file", "invalid_ca_pem", "PEM CA certificate file")
		}
	}
	if k.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(k.CertFile, k.KeyFile)
		if err != nil {
			field, related, reason := "kafka.cert_file", "kafka.key_file", "invalid_certificate"
			var pe *os.PathError
			if errors.As(err, &pe) {
				reason = "file_read_failed"
				if pe.Path == k.KeyFile {
					field, related = related, field
				}
			} else {
				// Classify only local TLS diagnostics. PEM labels and parser error
				// text themselves must never become log fields.
				message := err.Error()
				if strings.Contains(message, "certificate input") {
					reason = "invalid_certificate_pem"
				} else if strings.Contains(message, "does not match") {
					field, related, reason = "kafka.key_file", "kafka.cert_file", "certificate_key_mismatch"
				} else if strings.Contains(message, "key input") || strings.Contains(message, "private key") {
					field, related, reason = "kafka.key_file", "kafka.cert_file", "invalid_private_key"
				}
			}
			d := configdiag.New("kafka_tls", field, reason, "readable matching PEM certificate and private key", err)
			d.Issues[0].RelatedField = related
			r.Merge(d)
		} else {
			t.Certificates = []tls.Certificate{cert}
		}
	}
	if err := r.Err(); err != nil {
		return nil, err
	}
	return t, nil
}

func Duration(ms int) time.Duration { return time.Duration(ms) * time.Millisecond }

// DatabaseName is the only accepted unquoted SQL identifier grammar.
var databaseName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,63}$`)

func DatabaseName(name string) bool { return databaseName.MatchString(name) }
func Environment(s string) bool     { return s == "prd" || s == "stg" || s == "dev" }

// LookupURI accepts an empty placeholder or an HTTP(S) endpoint without credentials or query data.
func LookupURI(s string) bool {
	if s == "" {
		return true
	}
	u, err := url.Parse(s)
	return err == nil && len(s) <= 2048 && (u.Scheme == "http" || u.Scheme == "https") && u.Hostname() != "" && u.User == nil && u.RawQuery == "" && !u.ForceQuery && u.Fragment == "" && u.Opaque == ""
}
