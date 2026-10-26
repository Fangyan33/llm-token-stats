package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/Fangyan33/llm-token-stats/pkg/strictjson"
	"net/url"
	"os"
	"regexp"
	"time"
	"unicode/utf8"
)

type Kafka struct {
	Brokers     []string `json:"bootstrap_servers"`
	Topic       string   `json:"topic"`
	CAFile      string   `json:"ca_file"`
	CertFile    string   `json:"cert_file"`
	KeyFile     string   `json:"key_file"`
	UsernameEnv string   `json:"username_env"`
	PasswordEnv string   `json:"password_env"`
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

func (c LookupCache) Settings() (LookupCacheSettings, error) {
	value := func(p *int, fallback int) int {
		if p != nil {
			return *p
		}
		return fallback
	}
	entries, ttl := value(c.MaxEntries, 10000), value(c.TTLSeconds, 1800)
	stale, retry := value(c.StaleIfErrorSeconds, 900), value(c.StaleRetryIntervalSeconds, 5)
	if entries < 1 || entries > 100000 || ttl < 1 || ttl > 86400 || stale < 0 || stale > 86400 || retry < 1 || retry > 300 {
		return LookupCacheSettings{}, fmt.Errorf("config_lookup_cache")
	}
	return LookupCacheSettings{
		Enabled:    c.Enabled == nil || *c.Enabled,
		MaxEntries: entries, TTL: time.Duration(ttl) * time.Second,
		StaleIfError: time.Duration(stale) * time.Second, RetryInterval: time.Duration(retry) * time.Second,
	}, nil
}

type Observer struct {
	ClusterName           string        `json:"cluster_name"`
	DeploymentEnvironment string        `json:"deployment_environment"`
	ServiceLookup         Lookup        `json:"service_lookup"`
	CustomerLookup        Lookup        `json:"customer_lookup"`
	LookupTimeoutMS       int           `json:"lookup_timeout_ms"`
	LookupConcurrency     int           `json:"lookup_max_concurrency"`
	WasmDownload          *WasmDownload `json:"wasm_download"`
	Listen                string        `json:"listen"`
	Health                string        `json:"health_listen"`
	ProducerID            string        `json:"producer_id"`
	MaxActive             int           `json:"max_active_streams"`
	ParserBudget          int64         `json:"parser_budget_bytes"`
	MaxObservationMS      int           `json:"max_observation_ms"`
	AcceptedConfigs       []string      `json:"accepted_config_versions"`
	AcceptedAdapters      []string      `json:"accepted_adapter_ids"`
	Flags                 []string      `json:"response_flags_allowlist"`
	Details               []string      `json:"response_code_details_allowlist"`
	MaxConnections        int           `json:"max_connections"`
}
type WasmDownload struct {
	Listen string `json:"listen"`
	File   string `json:"file"`
}
type Doris struct {
	FE           string   `json:"fe_url"`
	AllowedHosts []string `json:"allowed_load_hosts"`
	DSNEnv       string   `json:"dsn_env"`
	UsernameEnv  string   `json:"username_env"`
	PasswordEnv  string   `json:"password_env"`
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
		return c, fmt.Errorf("config_file")
	}
	if len(b) > 65536 || strictjson.Decode(b, &c) != nil {
		return c, fmt.Errorf("config_invalid")
	}
	if !kafkaTopicName.MatchString(c.Kafka.Topic) || c.Kafka.Topic == "." || c.Kafka.Topic == ".." || len(c.Kafka.Brokers) == 0 {
		return c, fmt.Errorf("config_kafka")
	}
	return c, nil
}

// Kafka topic names are configurable so isolated deployments do not share data.
var kafkaTopicName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,249}$`)

func (c *Config) ObserverDefaults() error {
	if c.Observer.LookupTimeoutMS == 0 {
		c.Observer.LookupTimeoutMS = 200
	}
	if c.Observer.LookupConcurrency == 0 {
		c.Observer.LookupConcurrency = 64
	}
	if c.Observer.LookupTimeoutMS < 1 || c.Observer.LookupTimeoutMS > 2000 {
		return fmt.Errorf("config_lookup: observer.lookup_timeout_ms must be in [1, 2000]")
	}
	if c.Observer.LookupConcurrency < 1 || c.Observer.LookupConcurrency > 256 {
		return fmt.Errorf("config_lookup: observer.lookup_max_concurrency must be in [1, 256]")
	}
	for _, named := range []struct {
		name   string
		lookup Lookup
	}{{"service_lookup", c.Observer.ServiceLookup}, {"customer_lookup", c.Observer.CustomerLookup}} {
		lookup := named.lookup
		if _, err := lookup.Cache.Settings(); err != nil {
			return err
		}
		if lookup.MaxRPS < 0 || lookup.MaxRPS > 100000 {
			return fmt.Errorf("config_lookup: observer.%s.max_requests_per_second must be in [0, 100000]", named.name)
		}
		if lookup.URI != "" {
			if !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`).MatchString(lookup.CredentialEnv) {
				return fmt.Errorf("config_lookup: observer.%s.credential_env must be an environment variable name", named.name)
			}
			if lookup.CAFile != "" && !text(lookup.CAFile, 4096) {
				return fmt.Errorf("config_lookup: observer.%s.ca_file must be valid UTF-8 and at most 4096 bytes", named.name)
			}
		}
	}
	if w := c.Observer.WasmDownload; w != nil {
		if w.Listen == "" {
			w.Listen = ":9093"
		}
		if !text(w.File, 4096) || !text(w.Listen, 256) {
			return fmt.Errorf("config_wasm_download")
		}
	}
	if c.Observer.Listen == "" {
		c.Observer.Listen = ":9090"
	}
	if c.Observer.Health == "" {
		c.Observer.Health = ":9091"
	}
	if c.Observer.MaxActive == 0 {
		c.Observer.MaxActive = 4096
	}
	if c.Observer.ParserBudget == 0 {
		c.Observer.ParserBudget = 256 << 20
	}
	if c.Observer.MaxObservationMS == 0 {
		c.Observer.MaxObservationMS = 605000
	}
	if c.Observer.MaxConnections == 0 {
		c.Observer.MaxConnections = 128
	}
	if c.Observer.ProducerID == "" {
		c.Observer.ProducerID = os.Getenv("POD_UID")
	}
	if !text(c.Observer.ClusterName, 128) || !Environment(c.Observer.DeploymentEnvironment) || !LookupURI(c.Observer.ServiceLookup.URI) || !LookupURI(c.Observer.CustomerLookup.URI) || !text(c.Observer.ProducerID, 128) || c.Observer.MaxActive < 1 || c.Observer.MaxActive > 4096 || c.Observer.ParserBudget < 3*32768 || c.Observer.ParserBudget > 256<<20 || c.Observer.MaxObservationMS < 1 || c.Observer.MaxObservationMS > 605000 || c.Observer.MaxConnections < 1 || c.Observer.MaxConnections > 128 || !registration(c.Observer.AcceptedConfigs) || len(c.Observer.AcceptedAdapters) != 1 || c.Observer.AcceptedAdapters[0] != "chat_completions_v1" || !codes(c.Observer.Flags) || !codes(c.Observer.Details) {
		return fmt.Errorf("config_observer")
	}
	return nil
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
	if !text(c.Consumer.Group, 128) || c.Consumer.Rows < 1 || c.Consumer.Rows > 5000 || c.Consumer.Bytes < 32768 || c.Consumer.Bytes > 4<<20 || c.Consumer.FlushMS < 1 || c.Consumer.FlushMS > 15000 || !DatabaseName(c.Doris.Database) || !registration(c.Consumer.AcceptedConfigs) || len(c.Doris.AllowedHosts) == 0 || !codes(c.Consumer.Flags) || !codes(c.Consumer.Details) {
		return fmt.Errorf("config_consumer")
	}
	for _, ps := range [][]Position{c.Consumer.Initial, c.Consumer.Resume} {
		seen := map[int32]bool{}
		for _, p := range ps {
			if p.Partition < 0 || p.Offset < 0 || seen[p.Partition] {
				return fmt.Errorf("config_offsets")
			}
			seen[p.Partition] = true
		}
	}
	return nil
}

func text(s string, max int) bool { return len(s) > 0 && len(s) <= max && utf8.ValidString(s) }
func codes(xs []string) bool {
	if len(xs) > 128 {
		return false
	}
	seen := map[string]bool{}
	for _, s := range xs {
		if !text(s, 128) || seen[s] {
			return false
		}
		seen[s] = true
	}
	return true
}
func registration(configs []string) bool {
	return len(configs) > 0 && len(configs) <= 1024 && codesConfig(configs)
}
func codesConfig(xs []string) bool {
	seen := map[string]bool{}
	for _, s := range xs {
		if !text(s, 128) || seen[s] {
			return false
		}
		seen[s] = true
	}
	return true
}
func (k Kafka) TLS() (*tls.Config, error) {
	if k.CAFile == "" && k.CertFile == "" {
		return nil, nil
	}
	t := &tls.Config{MinVersion: tls.VersionTLS12}
	if k.CAFile != "" {
		b, err := os.ReadFile(k.CAFile)
		if err != nil {
			return nil, fmt.Errorf("kafka_tls")
		}
		t.RootCAs = x509.NewCertPool()
		if !t.RootCAs.AppendCertsFromPEM(b) {
			return nil, fmt.Errorf("kafka_tls")
		}
	}
	if k.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(k.CertFile, k.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("kafka_tls")
		}
		t.Certificates = []tls.Certificate{cert}
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
