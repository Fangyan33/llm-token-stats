//go:build integration

package integration

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if os.Getenv("METERING_TEST_NAMESPACE") == "" || os.Getenv("POD_IP") == "" {
		fmt.Fprintln(os.Stderr, "集成测试必须在隔离 Kubernetes Job 内运行，请执行 make k8s-test")
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// Only the isolated test namespace is accessible to this service account.
func kubeRequest(t *testing.T, method, resource string, value any) []byte {
	t.Helper()
	const account = "/var/run/secrets/kubernetes.io/serviceaccount/"
	token, err := os.ReadFile(account + "token")
	if err != nil {
		t.Fatal(err)
	}
	ca, err := os.ReadFile(account + "ca.crt")
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		t.Fatal("invalid Kubernetes CA")
	}
	var body []byte
	if value != nil {
		body, err = json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
	}
	endpoint := "https://kubernetes.default.svc/api/v1/namespaces/" + os.Getenv("METERING_TEST_NAMESPACE") + "/" + resource
	req, err := http.NewRequest(method, endpoint, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	req.Header.Set("Content-Type", "application/json")
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}
	defer transport.CloseIdleConnections()
	res, err := (&http.Client{Transport: transport, Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	out, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode >= 300 && !(method == "DELETE" && res.StatusCode == 404) {
		t.Fatalf("Kubernetes %s %s: %d %s", method, resource, res.StatusCode, out)
	}
	return out
}

func reachableServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	l, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	s := httptest.NewUnstartedServer(handler)
	s.Listener = l
	s.Start()
	s.URL = "http://" + net.JoinHostPort(os.Getenv("POD_IP"), fmt.Sprint(l.Addr().(*net.TCPAddr).Port))
	t.Cleanup(s.Close)
	return s
}

// Every fault probe uses the pinned real Envoy in its own Pod. Neither Docker
// access nor a host mount is available to the Job.
func startEnvoy(t *testing.T, config map[string]any, localWasm bool) (base, admin string, stop func()) {
	t.Helper()
	name := "e2e-envoy-" + uuid(t)
	config["node"] = map[string]any{"id": name, "cluster": "integration", "metadata": map[string]string{
		"NAMESPACE": os.Getenv("METERING_TEST_NAMESPACE"), "NAME": name}}
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	meta := map[string]any{"name": name, "labels": map[string]string{"app": "e2e-envoy", "app.kubernetes.io/part-of": "llm-token-stats-verification"}}
	kubeRequest(t, "POST", "configmaps", map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": meta, "data": map[string]string{"envoy.json": string(data)}})
	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		if t.Failed() {
			t.Logf("Envoy %s: %s", name, kubeRequest(t, "GET", "pods/"+name+"/log?container=envoy", nil))
		}
		kubeRequest(t, "DELETE", "pods/"+name, map[string]any{"gracePeriodSeconds": 0})
		kubeRequest(t, "DELETE", "configmaps/"+name, nil)
	}
	t.Cleanup(stop)
	meta["annotations"] = map[string]string{"sidecar.istio.io/inject": "false"}
	container := map[string]any{"name": "envoy", "image": "istio/proxyv2:1.13.5", "imagePullPolicy": "IfNotPresent",
		"command": []string{"/usr/local/bin/envoy"}, "args": []string{"-c", "/config/envoy.json", "--concurrency", "1", "--log-level", "warning"},
		"volumeMounts": []any{map[string]any{"name": "config", "mountPath": "/config", "readOnly": true}},
		"resources":    map[string]any{"requests": map[string]string{"cpu": "50m", "memory": "64Mi"}, "limits": map[string]string{"cpu": "1", "memory": "512Mi"}}}
	spec := map[string]any{"restartPolicy": "Never", "automountServiceAccountToken": false, "containers": []any{container},
		"volumes": []any{map[string]any{"name": "config", "configMap": map[string]string{"name": name}}}}
	if localWasm {
		spec["volumes"] = append(spec["volumes"].([]any), map[string]any{"name": "wasm", "emptyDir": map[string]any{}})
		container["volumeMounts"] = append(container["volumeMounts"].([]any), map[string]any{"name": "wasm", "mountPath": "/wasm", "readOnly": true})
		spec["initContainers"] = []any{map[string]any{"name": "wasm", "image": os.Getenv("METERING_TEST_IMAGE"), "imagePullPolicy": "IfNotPresent",
			"command":      []string{"cp", "/suite/dist/gateway-observer-v1.wasm", "/wasm/observer.wasm"},
			"volumeMounts": []any{map[string]any{"name": "wasm", "mountPath": "/wasm"}}}}
	}
	kubeRequest(t, "POST", "pods", map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": meta, "spec": spec})
	eventually(t, func() bool {
		var pod struct {
			Status struct {
				PodIP string `json:"podIP"`
				Phase string `json:"phase"`
			} `json:"status"`
		}
		if err := json.Unmarshal(kubeRequest(t, "GET", "pods/"+name, nil), &pod); err != nil {
			t.Fatal(err)
		}
		if pod.Status.Phase == "Failed" {
			t.Fatalf("Envoy Pod failed: %s", kubeRequest(t, "GET", "pods/"+name+"/log?container=envoy", nil))
		}
		if pod.Status.PodIP == "" {
			return false
		}
		base, admin = "http://"+pod.Status.PodIP+":8080", "http://"+pod.Status.PodIP+":9901"
		client := &http.Client{Timeout: time.Second}
		res, err := client.Get(admin + "/ready")
		if err != nil {
			return false
		}
		defer res.Body.Close()
		return res.StatusCode == 200
	})
	return
}

func componentLogs(t *testing.T) []byte {
	t.Helper()
	var pods struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Spec struct {
				Containers []struct {
					Name string `json:"name"`
				} `json:"containers"`
			} `json:"spec"`
		} `json:"items"`
	}
	if err := json.Unmarshal(kubeRequest(t, "GET", "pods?labelSelector=e2e-component%3Dtrue", nil), &pods); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) < 4 {
		t.Fatal("privacy check requires all four Kubernetes components")
	}
	var logs []byte
	for _, pod := range pods.Items {
		for _, c := range pod.Spec.Containers {
			logs = append(logs, kubeRequest(t, "GET", "pods/"+pod.Metadata.Name+"/log?container="+c.Name, nil)...)
		}
	}
	return logs
}
