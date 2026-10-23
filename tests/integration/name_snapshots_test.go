//go:build integration

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/Fangyan33/llm-token-stats/gen/observation/v1"
	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/Fangyan33/llm-token-stats/internal/events"
	"github.com/Fangyan33/llm-token-stats/internal/health"
	"github.com/Fangyan33/llm-token-stats/internal/identitylookup"
	"github.com/Fangyan33/llm-token-stats/internal/observation"
	"github.com/Fangyan33/llm-token-stats/internal/producer"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// Real HTTP/HTTPS adapter -> Observer gRPC -> Kafka -> running Consumer -> Doris.
// Only the remote attribution directory is synthetic; no production credentials.
func TestHTTPNamesThroughKafkaToDoris(t *testing.T) {
	for _, protocol := range []string{"http", "https"} {
		t.Run(protocol, func(t *testing.T) { testNamesThroughKafkaToDoris(t, protocol == "https", false) })
	}
}

func TestCachedHTTPNamesThroughKafkaToDoris(t *testing.T) {
	for _, protocol := range []string{"http", "https"} {
		t.Run(protocol, func(t *testing.T) { testNamesThroughKafkaToDoris(t, protocol == "https", true) })
	}
}

func testNamesThroughKafkaToDoris(t *testing.T, secure, cached bool) {
	db := database(t)
	var version atomic.Int32
	var unavailable atomic.Bool
	var unauthorized atomic.Bool
	stop := make(chan struct{})
	var serviceCalls, customerCalls atomic.Int32
	token := "synthetic-private-" + uuid(t)
	credential := "synthetic-directory-" + uuid(t)
	remote := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.Header.Get("x-remote-data") != credential {
			t.Error("remote authentication")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]any{"responseCode": "401", "responseMsg": "Unauthorized", "success": false})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/service" {
			serviceCalls.Add(1)
		} else {
			customerCalls.Add(1)
		}
		if unauthorized.Load() && r.URL.Path == "/customer" {
			io.Copy(io.Discard, r.Body)
			r.Body.Close()
			body := `{"responseCode":"401","responseMsg":"Unauthorized","success":false}`
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.WriteHeader(http.StatusUnauthorized)
			w.(http.Flusher).Flush()
			select {
			case <-r.Context().Done():
			case <-stop:
			}
			return
		}
		if unavailable.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		suffix := "旧名称"
		if version.Load() == 1 {
			suffix = "新名称"
		}
		if r.URL.Path == "/service" {
			var request identitylookup.ServiceLookupRequest
			if json.NewDecoder(r.Body).Decode(&request) != nil || request.RequestHost != "names.example.com" || request.DeploymentEnvironment != "dev" {
				t.Error("service request")
			}
			json.NewEncoder(w).Encode(map[string]any{"responseCode": "000000", "responseMsg": "服务归属查询成功", "success": true, "data": identitylookup.ServiceIdentity{ServiceID: "model-service-a", ProviderID: "provider-names", ServiceName: "服务" + suffix, ProviderName: "供应商" + suffix}})
		} else {
			var request map[string]string
			if json.NewDecoder(r.Body).Decode(&request) != nil || request["api_key"] != token {
				t.Error("customer request")
			}
			json.NewEncoder(w).Encode(map[string]any{"responseCode": "000000", "responseMsg": "客户归属查询成功", "success": true, "data": identitylookup.CustomerIdentity{APIKeyID: "key-names", CustomerID: "customer-names", CustomerName: "客户" + suffix}})
		}
	}))
	if secure {
		remote.StartTLS()
	} else {
		remote.Start()
	}
	defer remote.Close()
	defer close(stop)
	ca := ""
	if secure {
		ca = filepath.Join(t.TempDir(), "ca.pem")
		if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: remote.Certificate().Raw}), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("NAME_TEST_CREDENTIAL", credential)
	service, err := identitylookup.NewService(config.Lookup{URI: remote.URL + "/service", CredentialEnv: "NAME_TEST_CREDENTIAL", CAFile: ca})
	if err != nil {
		t.Fatal(err)
	}
	customer, err := identitylookup.NewCustomer(config.Lookup{URI: remote.URL + "/customer", CredentialEnv: "NAME_TEST_CREDENTIAL", CAFile: ca})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	h := &health.State{}
	sink, err := producer.New(ctx, config.Kafka{Brokers: []string{"127.0.0.1:29092"}, Topic: "llm-metering-development"}, h, func() { t.Error("producer watchdog") })
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.ForceServerCodec(observation.Codec{}))
	cache := config.LookupCache{Enabled: &cached}
	if cached {
		ttl, stale, retry := 2, 10, 1
		cache.TTLSeconds, cache.StaleIfErrorSeconds, cache.StaleRetryIntervalSeconds = &ttl, &stale, &retry
	}
	pb.RegisterObservationServiceServer(server, &observation.Server{Cfg: config.Observer{ClusterName: "name-tests", DeploymentEnvironment: "dev", ProducerID: "name-tests", MaxActive: 4, ParserBudget: 64 << 20, MaxObservationMS: 5000, LookupTimeoutMS: 1000, LookupConcurrency: 2, ServiceLookup: config.Lookup{Cache: cache}, CustomerLookup: config.Lookup{Cache: cache}, AcceptedConfigs: []string{"collection-v1"}, AcceptedAdapters: []string{"chat_completions_v1"}}, Sink: sink, Health: h, ServiceResolver: service, CustomerResolver: customer})
	go server.Serve(listener)
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///names", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	collect := func() string {
		t.Helper()
		stream, err := pb.NewObservationServiceClient(conn).Observe(ctx)
		if err != nil {
			t.Fatal(err)
		}
		start := &pb.Start{RequestStartedAtMs: time.Now().UnixMilli(), GatewayInstanceId: "name-gateway", ConfigVersion: "collection-v1", AdapterId: "chat_completions_v1", Method: "POST", RequestAuthority: "NAMES.example.com:443", ApiKey: []byte(token), RequestEndStream: true, RequestContentType: "application/json"}
		if err := stream.Send(&pb.ObservationFrame{SchemaVersion: 1, FrameSeq: 1, Payload: &pb.ObservationFrame_Start{Start: start}}); err != nil {
			t.Fatal(err)
		}
		ack, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		id := ack.LogicalRequestId
		body := []byte(`{"model":"names","usage":{"prompt_tokens":10,"completion_tokens":4,"total_tokens":14}}`)
		frames := []*pb.ObservationFrame{
			{Payload: &pb.ObservationFrame_ResponseHeaders{ResponseHeaders: &pb.ResponseHeaders{HttpStatus: 200, ContentType: "application/json"}}},
			{Payload: &pb.ObservationFrame_BodyChunk{BodyChunk: &pb.BodyChunk{Direction: pb.Direction_RESPONSE, BodySeq: 1, Data: body, EndStream: true}}},
			{Payload: &pb.ObservationFrame_End{End: &pb.End{RequestFinishedAtMs: time.Now().UnixMilli(), LastResponseBodySeq: 1, ResponseBytesSeen: uint64(len(body)), RequestEndStreamSeen: true, ResponseEndStreamSeen: true}}},
		}
		for i, f := range frames {
			f.SchemaVersion = 1
			f.FrameSeq = uint64(i + 2)
			if err := stream.Send(f); err != nil {
				t.Fatal(err)
			}
		}
		stream.CloseSend()
		for {
			if _, err := stream.Recv(); err != nil {
				break
			}
		}
		return id
	}
	first := collect()
	version.Store(1)
	second := collect()
	ids, names := []string{first, second}, []string{"旧名称", "新名称"}
	if cached {
		names[1] = "旧名称"
		time.Sleep(2100 * time.Millisecond)
		ids, names = append(ids, collect()), append(names, "新名称")
		unavailable.Store(true)
		time.Sleep(2100 * time.Millisecond)
		ids, names = append(ids, collect()), append(names, "新名称")
		ids, names = append(ids, collect()), append(names, "新名称")
		unauthorized.Store(true)
		time.Sleep(1100 * time.Millisecond)
		ids, names = append(ids, collect()), append(names, "新名称")
	}
	wantCalls := int32(2)
	if cached {
		wantCalls = 4 // Initial, refreshed, unavailable, then unauthorized; two requests use cache.
	}
	if serviceCalls.Load() != wantCalls || customerCalls.Load() != wantCalls {
		t.Fatalf("unexpected remote query count: %d / %d, want %d", serviceCalls.Load(), customerCalls.Load(), wantCalls)
	}
	for i, id := range ids {
		want := names[i]
		eventually(t, func() bool {
			var n int
			return db.QueryRow("SELECT COUNT(*) FROM metering_development.events_raw WHERE logical_request_id=?", id).Scan(&n) == nil && n == 2
		})
		rows, err := db.Query("SELECT service_name,provider_name,customer_name,event_json,kafka_partition,kafka_offset FROM metering_development.events_raw WHERE logical_request_id=?", id)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var a, b, raw string
			var c sql.NullString
			var part int32
			var offset int64
			if err := rows.Scan(&a, &b, &c, &raw, &part, &offset); err != nil {
				t.Fatal(err)
			}
			var event events.Event
			if json.Unmarshal([]byte(raw), &event) != nil || a != "服务"+want || b != "供应商"+want || event.ServiceName == nil || *event.ServiceName != a || event.ProviderName == nil || *event.ProviderName != b || strings.Contains(raw, token) || strings.Contains(raw, credential) {
				t.Fatal("name snapshot persistence")
			}
			if cached && i == len(ids)-1 {
				if c.Valid || event.CustomerName != nil || event.CustomerID != nil || event.ApiKeyID != nil || event.CustomerMappingVersion != nil || event.CustomerMappingStatus != "lookup_unavailable" || event.IdentityStatus != "invalid" || event.MappingStatus != "mapped" || event.UsageEligible {
					t.Fatal("slow 401 preserved old customer attribution in Kafka/Doris")
				}
			} else if !c.Valid || c.String != "客户"+want || event.CustomerName == nil || *event.CustomerName != c.String {
				t.Fatal("customer name snapshot persistence")
			}
			for _, field := range []string{"responseCode", "responseMsg", "服务归属查询成功", "客户归属查询成功"} {
				if strings.Contains(raw, field) {
					t.Fatal("remote response envelope persisted in event")
				}
			}
			reader, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:29092"), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{"llm-metering-development": {part: kgo.NewOffset().At(offset)}}))
			if err != nil {
				t.Fatal(err)
			}
			fetch := reader.PollRecords(ctx, 1)
			reader.Close()
			records := fetch.Records()
			if len(records) != 1 || string(records[0].Value) != raw {
				t.Fatal("Kafka snapshot differs from Doris")
			}
		}
		rows.Close()
		for _, view := range []string{"requests", "request_statistics", "event_details", "usage_effective"} {
			if cached && i == len(ids)-1 && view == "usage_effective" {
				var count int
				if err := db.QueryRow("SELECT COUNT(*) FROM metering_development.usage_effective WHERE logical_request_id=?", id).Scan(&count); err != nil || count != 0 {
					t.Fatal("unauthorized attribution became eligible usage")
				}
				continue
			}
			var name string
			if err := db.QueryRow("SELECT service_name FROM metering_development."+view+" WHERE logical_request_id=? LIMIT 1", id).Scan(&name); err != nil || name != "服务"+want {
				t.Fatalf("%s name: %q %v", view, name, err)
			}
		}
	}
}
