package gatewayconfig

import (
	"github.com/Fangyan33/llm-token-stats/pkg/configdiag"
	"github.com/Fangyan33/llm-token-stats/pkg/requesthost"
	"github.com/Fangyan33/llm-token-stats/pkg/requestid"
	"github.com/Fangyan33/llm-token-stats/pkg/requestmeta"
	"github.com/Fangyan33/llm-token-stats/pkg/strictjson"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Flush shared transport batches within 10ms without pausing business traffic.
const MaxTickMS int64 = 10

func Decode(b []byte) (Config, error) {
	c := Config{UserIDHeader: []string{"user-id"}, ObservationMaxMS: 1800000}
	var err error
	b, err = configurationJSON(b)
	if err != nil {
		return c, err
	}
	if err = strictjson.DecodeConfig(b, &c); err != nil {
		return c, err
	}
	r := &configdiag.Report{Source: "plugin_configuration"}
	fields, _ := strictjson.Fields(b, 64, 100000)
	for _, named := range []struct{ name, code string }{
		{"accepted_request_hosts", "config_request_hosts"}, {"logical_request_id_headers", "config_request_id_headers"},
		{"observation_max_ms", "config_limit"}, {"metrics_enabled", "config_metrics"}, {"request_body_capture_enabled", "config_metrics"},
		{"user_id_header", "config_request_headers"}, {"extra_request_headers", "config_request_headers"},
	} {
		if span, present := fields[named.name]; present {
			r.Check(strings.TrimSpace(string(b[span.Start:span.End])) != "null", named.code, named.name, "null_not_allowed", "non-null configuration value")
		}
	}
	if value, e := requestmeta.Normalize(c.UserIDHeader, 8); e != nil {
		r.Merge(configdiag.Prefix(e, "user_id_header"))
	} else {
		c.UserIDHeader = value
	}
	if value, e := requestmeta.Normalize(c.ExtraRequestHeaders, requestmeta.MaxHeaders); e != nil {
		r.Merge(configdiag.Prefix(e, "extra_request_headers"))
	} else {
		c.ExtraRequestHeaders = value
	}
	r.Number(c.SchemaVersion, 1, 1, "config_invalid", "schema_version")
	r.Check(str(c.ConfigVersion, 128), "config_invalid", "config_version", "invalid_text", "nonempty UTF-8 string of at most 128 bytes")
	r.Check(str(c.ObserverCluster, 256), "config_invalid", "observer_cluster", "invalid_text", "nonempty UTF-8 string of at most 256 bytes")
	r.Number(int64(len(c.Routes)), 0, 128, "config_invalid", "routes")
	if value, e := requestid.NormalizeHeaders(c.LogicalRequestIDHeaders); e != nil {
		r.Merge(configdiag.Prefix(e, "logical_request_id_headers"))
	} else {
		c.LogicalRequestIDHeaders = value
	}
	if value, e := requesthost.NormalizePatterns(c.AcceptedRequestHosts); e != nil {
		r.Merge(configdiag.Prefix(e, "accepted_request_hosts"))
	} else {
		c.AcceptedRequestHosts = value
	}
	seen := map[string]bool{}
	for i, route := range c.Routes {
		field := configdiag.Index("routes", i)
		r.Check(str(route.RouteName, 128), "config_route", field+".route_name", "invalid_text", "nonempty UTF-8 string of at most 128 bytes")
		r.Check(!seen[route.RouteName], "config_route", field+".route_name", "duplicate_route", "unique route name")
		seen[route.RouteName] = true
		r.Check(route.AdapterID == "chat_completions_v1", "config_route", field+".adapter_id", "invalid_adapter", "chat_completions_v1")
		r.Check(len(route.Methods) == 1 && route.Methods[0] == "POST", "config_route", field+".methods", "invalid_methods", "exactly [POST]")
		r.Number(int64(len(route.Paths)), 1, 16, "config_route", field+".paths")
		paths := map[string]bool{}
		for j, path := range route.Paths {
			name := configdiag.Index(field+".paths", j)
			valid := str(path, 1024) && strings.HasPrefix(path, "/") && !strings.ContainsAny(path, "?#*[]{}()|^$\\")
			for _, c := range path {
				if unicode.IsSpace(c) {
					valid = false
				}
			}
			r.Check(valid, "config_path", name, "invalid_path", "exact absolute UTF-8 path of 1..1024 bytes without whitespace, query, fragment or pattern characters")
			r.Check(!paths[path], "config_path", name, "duplicate_path", "unique path within this route")
			paths[path] = true
		}
	}
	for _, limit := range []struct {
		name             string
		value, low, high int64
	}{
		{"chunk_bytes", c.ChunkBytes, 1, 16384}, {"response_body_max_bytes", c.ResponseBodyMaxBytes, 1, 8388608},
		{"pod_max_observations", c.PodMaxObservations, 1, 2147483647}, {"pod_inflight_bytes", c.PodInflightBytes, 65536, 2147483647},
		{"pod_inflight_messages", c.PodInflightMessages, 128, 2147483647}, {"transport_streams_per_worker", c.TransportStreamsPerWorker, 1, 16},
		{"transport_batch_max_bytes", c.TransportBatchMaxBytes, 32768, 32768}, {"transport_batch_max_frames", c.TransportBatchMaxFrames, 1, 128},
		{"ack_timeout_ms", c.AckTimeoutMS, 1, 2000}, {"end_ack_timeout_ms", c.EndAckTimeoutMS, 1, 2000},
		{"observation_max_ms", c.ObservationMaxMS, 1, 3600000}, {"tick_ms", c.TickMS, 1, MaxTickMS},
		{"metadata_max_bytes", c.MetadataMaxBytes, 6144, 6144}, {"ack_max_bytes", c.AckMaxBytes, 1, 32768},
	} {
		code := "config_budget"
		if limit.value < 1 || limit.value > 2147483647 {
			code = "config_limit"
		}
		r.Number(limit.value, limit.low, limit.high, code, limit.name)
	}
	r.Relation(c.TickMS <= c.AckTimeoutMS, "config_budget", "tick_ms", "ack_timeout_ms", "tick_ms <= ack_timeout_ms", c.TickMS)
	r.Relation(c.TickMS <= c.EndAckTimeoutMS, "config_budget", "tick_ms", "end_ack_timeout_ms", "tick_ms <= end_ack_timeout_ms", c.TickMS)
	return c, r.Err()
}
func str(s string, max int) bool { return len(s) > 0 && len(s) <= max && utf8.ValidString(s) }
func (c Config) Match(route, method, path string) (Route, string, bool) {
	if !c.Enabled {
		return Route{}, "disabled", false
	}
	if method != "POST" {
		return Route{}, "method", false
	}
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	for _, r := range c.Routes {
		if r.RouteName == "*" || r.RouteName == route {
			if !r.Enabled {
				return Route{}, "route_disabled", false
			}
			for _, p := range r.Paths {
				if p == path {
					return r, "", true
				}
			}
			return Route{}, "path", false
		}
	}
	return Route{}, "route", false
}
