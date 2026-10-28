package summaryworker

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// Lease ownership is checked before mutations. Expiry alone never authorizes
// takeover: the old Pod must be gone/terminated and Doris jobs must be idle.
type kubeGuard struct {
	mu                           sync.Mutex
	client                       *http.Client
	base, path, namespace, owner string
	last                         time.Time
	lost                         bool
	stop                         chan struct{}
	done                         chan struct{}
	store                        *Store
}
type lease struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name            string `json:"name"`
		ResourceVersion string `json:"resourceVersion"`
	} `json:"metadata"`
	Spec struct {
		Holder      string `json:"holderIdentity"`
		Duration    int    `json:"leaseDurationSeconds"`
		Renew       string `json:"renewTime"`
		Acquire     string `json:"acquireTime"`
		Transitions int    `json:"leaseTransitions"`
	} `json:"spec"`
}

func KubernetesGuard(ctx context.Context, name string, store *Store) (Guard, error) {
	ns, pod, uid := os.Getenv("POD_NAMESPACE"), os.Getenv("POD_NAME"), os.Getenv("POD_UID")
	if ns == "" || pod == "" || uid == "" {
		return nil, fmt.Errorf("pod_identity_required")
	}
	ca, e := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if e != nil {
		return nil, fmt.Errorf("kubernetes_ca")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("kubernetes_ca")
	}
	g := &kubeGuard{client: &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}}}, base: "https://kubernetes.default.svc", path: "/apis/coordination.k8s.io/v1/namespaces/" + ns + "/leases/" + name, namespace: ns, owner: pod + "/" + uid + "/" + newID(), stop: make(chan struct{}), done: make(chan struct{}), store: store}
	var l lease
	if _, e = g.api(ctx, "GET", g.path, nil, &l); e != nil {
		return nil, e
	}
	if l.Spec.Holder != "" && l.Spec.Holder != g.owner {
		renew, e := time.Parse(time.RFC3339Nano, l.Spec.Renew)
		if e != nil || l.Spec.Duration <= 0 || time.Now().Before(renew.Add(time.Duration(l.Spec.Duration)*time.Second)) {
			return nil, fmt.Errorf("summary_already_running")
		}
		old := strings.Split(l.Spec.Holder, "/")
		if len(old) != 3 {
			return nil, fmt.Errorf("lease_owner_unverifiable")
		}
		var p struct {
			Metadata struct {
				UID string `json:"uid"`
			} `json:"metadata"`
			Status struct {
				Phase string `json:"phase"`
			} `json:"status"`
		}
		code, e := g.api(ctx, "GET", "/api/v1/namespaces/"+ns+"/pods/"+old[0], nil, &p)
		if code != http.StatusNotFound && (e != nil || p.Metadata.UID == old[1] && (p.Status.Phase != "Succeeded" && p.Status.Phase != "Failed")) {
			return nil, fmt.Errorf("old_pod_not_terminated")
		}
		if e = g.idle(ctx); e != nil {
			return nil, e
		}
		frontends, e := store.rows(ctx, "SHOW FRONTENDS")
		if e != nil {
			return nil, e
		}
		if len(frontends) != 1 {
			return nil, fmt.Errorf("multi_fe_takeover_requires_verification")
		}
		processes, e := store.rows(ctx, "SHOW PROCESSLIST")
		if e != nil {
			return nil, e
		}
		if e = sqlIdle(processes, store.Database); e != nil {
			return nil, e
		}
	}
	l.Spec.Holder = g.owner
	l.Spec.Duration = 90
	l.Spec.Renew = time.Now().UTC().Format(time.RFC3339Nano)
	l.Spec.Acquire = l.Spec.Renew
	l.Spec.Transitions++
	if _, e = g.api(ctx, "PUT", g.path, l, nil); e != nil {
		return nil, e
	}
	g.last = time.Now()
	go func() {
		defer close(g.done)
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-g.stop:
				return
			case <-ctx.Done():
				g.mu.Lock()
				g.lost = true
				g.mu.Unlock()
				return
			case <-ticker.C:
				g.mu.Lock()
				var next lease
				_, e := g.api(ctx, "GET", g.path, nil, &next)
				if e == nil && next.Spec.Holder == g.owner {
					next.Spec.Renew = time.Now().UTC().Format(time.RFC3339Nano)
					_, e = g.api(ctx, "PUT", g.path, next, nil)
				} else {
					e = fmt.Errorf("lease_lost")
				}
				if e != nil {
					g.lost = true
					g.mu.Unlock()
					return
				}
				g.last = time.Now()
				g.mu.Unlock()
			}
		}
	}()
	return g, nil
}
func (g *kubeGuard) api(ctx context.Context, method, path string, in, out any) (int, error) {
	var b []byte
	if in != nil {
		var e error
		b, e = json.Marshal(in)
		if e != nil {
			return 0, e
		}
	}
	token, e := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if e != nil {
		return 0, fmt.Errorf("kubernetes_token")
	}
	req, e := http.NewRequestWithContext(ctx, method, g.base+path, bytes.NewReader(b))
	if e != nil {
		return 0, e
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	req.Header.Set("Content-Type", "application/json")
	res, e := g.client.Do(req)
	if e != nil {
		return 0, fmt.Errorf("kubernetes_request")
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return res.StatusCode, fmt.Errorf("kubernetes_status_%d", res.StatusCode)
	}
	if out != nil {
		if e = json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(out); e != nil {
			return res.StatusCode, fmt.Errorf("kubernetes_response")
		}
	}
	return res.StatusCode, nil
}
func (g *kubeGuard) Check(ctx context.Context) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.lost || time.Since(g.last) > 45*time.Second {
		return fmt.Errorf("summary_lease_lost")
	}
	return nil
}
func (g *kubeGuard) idle(ctx context.Context) error {
	for _, v := range []string{"usage_daily_mv", "usage_monthly_mv"} {
		rows, e := g.store.tasks(ctx, v)
		if e != nil {
			return e
		}
		for _, r := range rows {
			if r["Status"] == "RUNNING" || r["Status"] == "PENDING" {
				return fmt.Errorf("old_refresh_not_finished")
			}
		}
	}
	return nil
}
func (g *kubeGuard) Close() error {
	close(g.stop)
	<-g.done
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.lost {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if e := g.idle(ctx); e != nil {
		return e
	}
	var l lease
	if _, e := g.api(ctx, "GET", g.path, nil, &l); e != nil {
		return e
	}
	if l.Spec.Holder != g.owner {
		return fmt.Errorf("summary_lease_lost")
	}
	l.Spec.Holder = ""
	_, e := g.api(ctx, "PUT", g.path, l, nil)
	return e
}

// A dead Pod can leave a server-side INSERT running after its TCP connection
// closes. Do not take over while any statement in this database is still active.
func sqlIdle(rows []map[string]string, database string) error {
	for _, r := range rows {
		if _, ok := r["Command"]; !ok {
			return fmt.Errorf("process_list_unverifiable")
		}
		if r["CurrentConnected"] == "Yes" || r["Db"] != database {
			continue
		}
		if !strings.EqualFold(r["Command"], "Sleep") {
			return fmt.Errorf("old_sql_not_finished")
		}
	}
	return nil
}
