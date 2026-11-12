package gatewayconfig

import (
	"errors"
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
	c := Config{UserIDHeader: []string{"user-id"}}
	var err error
	b, err = configurationJSON(b)
	if err != nil || strictjson.Decode(b, &c) != nil {
		return c, errors.New("config_invalid")
	}
	if c.AcceptedRequestHosts == nil {
		fields, _ := strictjson.Fields(b, 64, 100000)
		if _, present := fields["accepted_request_hosts"]; present {
			return c, errors.New("config_request_hosts") // Explicit null is not an array.
		}
	}
	if c.LogicalRequestIDHeaders == nil {
		fields, _ := strictjson.Fields(b, 64, 100000)
		if _, present := fields["logical_request_id_headers"]; present {
			return c, errors.New("config_request_id_headers")
		}
	}
	fields, _ := strictjson.Fields(b, 64, 100000)
	if span, present := fields["metrics_enabled"]; present && strings.TrimSpace(string(b[span.Start:span.End])) == "null" {
		return c, errors.New("config_metrics")
	}
	for _, name := range []string{"user_id_header", "extra_request_headers"} {
		if span, present := fields[name]; present && strings.TrimSpace(string(b[span.Start:span.End])) == "null" {
			return c, errors.New("config_request_headers")
		}
	}
	var headerErr error
	c.UserIDHeader, headerErr = requestmeta.Normalize(c.UserIDHeader, 8)
	if headerErr != nil {
		return c, headerErr
	}
	c.ExtraRequestHeaders, headerErr = requestmeta.Normalize(c.ExtraRequestHeaders, requestmeta.MaxHeaders)
	if headerErr != nil {
		return c, headerErr
	}
	if c.SchemaVersion != 1 || !str(c.ConfigVersion, 128) || !str(c.ObserverCluster, 256) || len(c.Routes) > 128 {
		return c, errors.New("config_invalid")
	}
	c.LogicalRequestIDHeaders, err = requestid.NormalizeHeaders(c.LogicalRequestIDHeaders)
	if err != nil {
		return c, err
	}
	c.AcceptedRequestHosts, err = requesthost.NormalizePatterns(c.AcceptedRequestHosts)
	if err != nil {
		return c, err
	}
	seen := map[string]bool{}
	for _, r := range c.Routes {
		if !str(r.RouteName, 128) || seen[r.RouteName] || r.AdapterID != "chat_completions_v1" || len(r.Methods) != 1 || r.Methods[0] != "POST" || len(r.Paths) < 1 || len(r.Paths) > 16 {
			return c, errors.New("config_route")
		}
		seen[r.RouteName] = true
		paths := map[string]bool{}
		for _, p := range r.Paths {
			if !str(p, 1024) || p[0] != '/' || strings.ContainsAny(p, "?#*[]{}()|^$\\") || paths[p] {
				return c, errors.New("config_path")
			}
			for _, c := range p {
				if unicode.IsSpace(c) {
					return Config{}, errors.New("config_path")
				}
			}
			paths[p] = true
		}
	}
	for _, n := range []int64{c.ChunkBytes, c.ResponseBodyMaxBytes, c.PodMaxObservations, c.PodInflightBytes, c.PodInflightMessages, c.TransportStreamsPerWorker, c.TransportBatchMaxBytes, c.TransportBatchMaxFrames, c.AckTimeoutMS, c.EndAckTimeoutMS, c.ObservationMaxMS, c.TickMS, c.MetadataMaxBytes, c.AckMaxBytes} {
		if n < 1 || n > 2147483647 {
			return c, errors.New("config_limit")
		}
	}
	if c.ChunkBytes > 16384 || c.ResponseBodyMaxBytes > 8388608 || c.ObservationMaxMS > 600000 || c.TransportStreamsPerWorker > 16 || c.TransportBatchMaxBytes != 32768 || c.TransportBatchMaxFrames > 128 || c.PodInflightBytes < 65536 || c.PodInflightMessages < 128 || c.MetadataMaxBytes != 6144 || c.AckMaxBytes > 32768 || c.AckTimeoutMS > 2000 || c.EndAckTimeoutMS > 2000 || c.TickMS > MaxTickMS || c.TickMS > c.AckTimeoutMS || c.TickMS > c.EndAckTimeoutMS {
		return c, errors.New("config_budget")
	}
	return c, nil
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
