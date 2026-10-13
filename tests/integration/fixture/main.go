// Synthetic fixture only: no real authentication or model inference.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	w "github.com/Fangyan33/llm-token-stats/pkg/observationwire"
	"google.golang.org/grpc"
	"io"
	"net"
	"net/http"
	"time"
)

type raw []byte
type codec struct{}

func (codec) Name() string                    { return "proto" }
func (codec) Marshal(v any) ([]byte, error)   { return []byte(*v.(*raw)), nil }
func (codec) Unmarshal(b []byte, v any) error { *v.(*raw) = append([]byte{}, b...); return nil }

type auth interface{}

func check(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	var req raw
	if err := dec(&req); err != nil {
		return nil, err
	}
	meta := []byte{}
	for _, p := range [][2]string{{"api_key_id", "key-test"}, {"customer_id", "customer-test"}, {"application_id", "app-test"}} {
		entry := w.String(nil, 1, p[0])
		entry = w.Bytes(entry, 2, w.String(nil, 3, p[1]))
		meta = w.Bytes(meta, 1, entry)
	}
	res := raw(w.Bytes(w.Bytes(w.Bytes(nil, 1, nil), 3, nil), 4, meta))
	return &res, nil
}
func main() {
	l, _ := net.Listen("tcp", ":9002")
	s := grpc.NewServer(grpc.ForceServerCodec(codec{}))
	s.RegisterService(&grpc.ServiceDesc{ServiceName: "envoy.service.auth.v3.Authorization", HandlerType: (*auth)(nil), Methods: []grpc.MethodDesc{{MethodName: "Check", Handler: check}}}, struct{}{})
	go s.Serve(l)
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "ok") })
	http.HandleFunc("/v1/chat/completions", func(out http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
		if err != nil {
			out.WriteHeader(400)
			return
		}
		var req struct {
			Stream  bool `json:"stream"`
			Options struct {
				Include bool `json:"include_usage"`
			} `json:"stream_options"`
			TestCase string `json:"test_case"`
		}
		if json.Unmarshal(b, &req) != nil {
			out.WriteHeader(400)
			return
		}
		out.Header().Set("x-test-body-sha256", fmt.Sprintf("%x", sha256.Sum256(b)))
		if req.TestCase == "slow_response" {
			time.Sleep(200 * time.Millisecond)
		}
		out.Header().Add("x-metering-request-id", "forged")
		out.Header().Add("x-metering-request-id", "forged-again")
		if req.Stream {
			if !req.Options.Include {
				out.WriteHeader(422)
				return
			}
			out.Header().Set("Content-Type", "text/event-stream")
			out.Header().Set("Trailer", "x-test-end")
			fmt.Fprint(out, "data: {\"model\":\"model-test\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"你好\"}}]}\n\n")
			out.(http.Flusher).Flush()
			time.Sleep(20 * time.Millisecond)
			if req.TestCase != "missing_usage" {
				fmt.Fprint(out, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":4,\"total_tokens\":14}}\n\n")
			}
			fmt.Fprint(out, "data: [DONE]\n\n")
			out.Header().Set("x-test-end", "done")
		} else {
			out.Header().Set("Content-Type", "application/json")
			fmt.Fprint(out, `{"model":"model-test","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":4,"total_tokens":14}}`)
		}
	})
	http.ListenAndServe(":8081", nil)
}
