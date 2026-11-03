//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Fangyan33/llm-token-stats/internal/config"
	"github.com/go-sql-driver/mysql"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Run the actual Consumer entrypoint in a Pod with only a DSN for Doris auth.
func startDSNConsumer(t *testing.T, topic, group, connection string) (string, func()) {
	t.Helper()
	image := os.Getenv("METERING_CONSUMER_IMAGE")
	if image == "" {
		t.Fatal("missing Consumer test image")
	}
	name := "llm-token-stats-dsn-" + uuid(t)
	c := config.Config{
		Kafka:    config.Kafka{Brokers: []string{"llm-token-stats-kafka:9092"}, Topic: topic},
		Consumer: config.Consumer{Health: ":9092", Group: group, FlushMS: 100, AcceptedConfigs: []string{"collection-v1"}},
		Doris:    config.Doris{Database: "metering_development", DSNEnv: "DORIS_DSN", FE: "http://llm-token-stats-doris:8040", AllowedHosts: []string{"llm-token-stats-doris:8040"}},
	}
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	metadata := map[string]any{"name": name, "annotations": map[string]string{"sidecar.istio.io/inject": "false"}}
	kubeRequest(t, "POST", "configmaps", map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": metadata, "data": map[string]string{"consumer.json": string(data)}})
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		kubeRequest(t, "DELETE", "pods/"+name, map[string]any{"gracePeriodSeconds": 5})
		// Wait for ownership release before starting the replacement instance.
		eventually(t, func() bool {
			var pods struct {
				Items []json.RawMessage `json:"items"`
			}
			if err := json.Unmarshal(kubeRequest(t, "GET", "pods?fieldSelector=metadata.name%3D"+name, nil), &pods); err != nil {
				t.Fatal(err)
			}
			return len(pods.Items) == 0
		})
		kubeRequest(t, "DELETE", "configmaps/"+name, nil)
	}
	t.Cleanup(stop)
	container := map[string]any{"name": "consumer", "image": image, "imagePullPolicy": "IfNotPresent", "args": []string{"--config", "/config/consumer.json"},
		"env":            []map[string]string{{"name": "DORIS_DSN", "value": connection}},
		"readinessProbe": map[string]any{"httpGet": map[string]any{"path": "/readyz", "port": 9092}, "periodSeconds": 1},
		"volumeMounts":   []any{map[string]any{"name": "config", "mountPath": "/config", "readOnly": true}},
		"resources":      map[string]any{"requests": map[string]string{"cpu": "50m", "memory": "64Mi"}, "limits": map[string]string{"cpu": "1", "memory": "256Mi"}}}
	spec := map[string]any{"restartPolicy": "Never", "automountServiceAccountToken": false, "terminationGracePeriodSeconds": 5,
		"containers": []any{container}, "volumes": []any{map[string]any{"name": "config", "configMap": map[string]string{"name": name}}}}
	kubeRequest(t, "POST", "pods", map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": metadata, "spec": spec})
	base := ""
	eventually(t, func() bool {
		var pod struct{ Status struct{ PodIP, Phase string } }
		if err := json.Unmarshal(kubeRequest(t, "GET", "pods/"+name, nil), &pod); err != nil {
			t.Fatal(err)
		}
		if pod.Status.Phase == "Failed" {
			t.Fatal("DSN Consumer exited unexpectedly")
		}
		if pod.Status.PodIP == "" {
			return false
		}
		base = "http://" + pod.Status.PodIP + ":9092"
		status, _ := dsnConsumerHTTP(base, "/livez")
		return status == 200
	})
	return base, stop
}

func dsnConsumerHTTP(base, path string) (int, string) {
	res, err := (&http.Client{Timeout: time.Second}).Get(base + path)
	if err != nil {
		return 0, ""
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(res.Body, 65536))
	return res.StatusCode, string(b)
}

func TestDSNCredentialsEndToEnd(t *testing.T) {
	db, cl := database(t), kafka(t)
	user := "dsn_" + strings.ReplaceAll(uuid(t), "-", "")[:24]
	// Synthetic credentials for this disposable database, never a deployment secret.
	password := "Synthetic:@/pass%?"
	if _, err := db.Exec("CREATE USER '" + user + "' IDENTIFIED BY '" + password + "'"); err != nil {
		t.Fatal("create synthetic Doris account failed")
	}
	t.Cleanup(func() {
		if _, err := db.Exec("DROP USER '" + user + "'"); err != nil {
			t.Error("drop synthetic Doris account failed")
		}
	})
	// Include SHOW TRANSACTION and audit privileges without changing root auth.
	if _, err := db.Exec("GRANT ADMIN_PRIV ON *.*.* TO '" + user + "'"); err != nil {
		t.Fatalf("grant synthetic Doris account failed: %v", err)
	}
	parsed, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User, parsed.Passwd = user, password
	good := parsed.FormatDSN()
	parsed.Passwd = "synthetic-wrong-password"
	wrong := parsed.FormatDSN()
	connector, err := mysql.NewConnector(parsed)
	if err != nil {
		t.Fatal("invalid synthetic DSN")
	}
	conn, err := connector.Connect(t.Context())
	if conn != nil {
		conn.Close()
	}
	var authErr *mysql.MySQLError
	if !errors.As(err, &authErr) || authErr.Number != 1045 {
		t.Fatal("wrong password must be rejected with 1045 over Service address")
	}
	topic, group := batchingTopic(t, cl)
	publish(t, cl, topic, 0, "synthetic", []byte(`{}`))
	base, stop := startDSNConsumer(t, topic, group, wrong)
	eventually(t, func() bool {
		_, metrics := dsnConsumerHTTP(base, "/metrics")
		for _, line := range strings.Split(metrics, "\n") {
			fields := strings.Fields(line)
			if len(fields) == 2 && fields[0] == "metering_partition_paused_total" {
				n, _ := strconv.Atoi(fields[1])
				return n >= 2
			}
		}
		return false
	})
	if status, _ := dsnConsumerHTTP(base, "/readyz"); status != 503 {
		t.Fatalf("wrong-password readiness = %d", status)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = group
	req.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: topic, Partitions: []int32{0}}}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil || resp.ErrorCode != 0 || len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 || resp.Topics[0].Partitions[0].ErrorCode != 0 || resp.Topics[0].Partitions[0].Offset >= 0 {
		t.Fatal("wrong-password Consumer must not commit an offset")
	}
	stop()
	base, _ = startDSNConsumer(t, topic, group, good)
	eventually(t, func() bool { status, _ := dsnConsumerHTTP(base, "/readyz"); return status == 200 })
	waitBatchCommit(t, cl, topic, group, 0, 1)
	for _, table := range []string{"consumer_partition_origins", "ingest_rejections", "consumer_batch_receipts"} {
		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM metering_development."+table+" WHERE kafka_topic=? AND kafka_partition=0", topic).Scan(&count); err != nil || count != 1 {
			t.Fatal(fmt.Sprintf("DSN-only Consumer did not persist expected row in %s", table))
		}
	}
}
