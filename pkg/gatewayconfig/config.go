package gatewayconfig

import (
	"errors"
	"github.com/Fangyan33/llm-token-stats/pkg/strictjson"
	"strings"
	"unicode"
	"unicode/utf8"
)

func Decode(b []byte) (Config, error) {
	var c Config
	if len(b) > 65536 || strictjson.Decode(b, &c) != nil {
		return c, errors.New("config_invalid")
	}
	if c.SchemaVersion != 1 || !str(c.ConfigVersion, 128) || !str(c.GatewayInstanceID, 128) || !str(c.ObserverCluster, 256) || len(c.Routes) > 128 {
		return c, errors.New("config_invalid")
	}
	if c.Enabled && strings.Contains(c.GatewayInstanceID, "RENDER_") {
		return c, errors.New("config_placeholder")
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
	if c.TickMS > c.RewriteTimeoutMS || c.TickMS > c.EndAckTimeoutMS {
		return c, errors.New("config_budget")
	}
	for _, n := range []int64{c.ChunkBytes, c.CallbackMaxBytes, c.CallbackMaxChunks, c.RequestBodyMaxBytes, c.RewriteTimeoutMS, c.RewriteReservationBytes, c.RootRewriteBudgetBytes, c.RootMaxRewrites, c.ResponseBodyMaxBytes, c.StreamInflightBytes, c.StreamInflightMessages, c.RootMaxStreams, c.RootInflightBytes, c.RootInflightMessages, c.AckTimeoutMS, c.EndAckTimeoutMS, c.ObservationMaxMS, c.TickMS, c.MetadataMaxBytes, c.AckMaxBytes} {
		if n < 1 || n > 2147483647 {
			return c, errors.New("config_limit")
		}
	}
	chunks := (c.RequestBodyMaxBytes + c.ChunkBytes - 1) / c.ChunkBytes
	if c.ChunkBytes > 16384 || c.RequestBodyMaxBytes > 262144 || c.ResponseBodyMaxBytes > 8388608 || c.RewriteTimeoutMS > 2000 || c.ObservationMaxMS > 600000 || c.AckMaxBytes > 1024 || c.MetadataMaxBytes < 6144 || c.MetadataMaxBytes > 6144 || c.CallbackMaxBytes > c.ChunkBytes*c.CallbackMaxChunks || c.StreamInflightBytes < c.RequestBodyMaxBytes+chunks*256+8192 || c.StreamInflightMessages < chunks+4 || c.RootInflightBytes < c.StreamInflightBytes || c.RootInflightMessages < c.StreamInflightMessages || c.RewriteReservationBytes < c.RequestBodyMaxBytes*4 || c.RootRewriteBudgetBytes < c.RewriteReservationBytes*c.RootMaxRewrites || c.TickMS > c.AckTimeoutMS || c.ObserverCluster == "" {
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
