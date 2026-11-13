// collection-load supplies a synthetic streaming server, an open-loop client,
// and an exact client-to-Doris reconciliation. Usage is fixture-reported; the
// prompt content can be checked offline with verify_token_profiles.py. Bodies
// are not logged.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type result struct {
	ID            string `json:"id"`
	Input         int64  `json:"input"`
	Bytes         int    `json:"body_bytes"`
	Status        int    `json:"status"`
	Done          bool   `json:"done"`
	Usage         bool   `json:"usage"`
	BodyUnchanged bool   `json:"body_unchanged"`
	Elapsed       int64  `json:"elapsed_us"`
	DispatchLag   int64  `json:"dispatch_lag_us"`
	Error         string `json:"error,omitempty"`
}

var mixedInputTokens = [...]int64{100000, 200000, 500000, 1000000}
var mixedBodyIndexes = [...]int{3, 1, 1, 2, 1, 3, 1, 2, 1, 0}

func writeJSON(path string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	// A killed load generator must leave either the previous checkpoint or the
	// new one, rather than a truncated JSON report.
	temporary := path + ".tmp"
	if e := os.WriteFile(temporary, append(b, '\n'), 0644); e != nil {
		return e
	}
	return os.Rename(temporary, path)
}
func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
func fixture(args []string) error {
	f := flag.NewFlagSet("fixture", flag.ContinueOnError)
	addr := f.String("listen", "127.0.0.1:28081", "listen address")
	duration := f.Duration("duration", 60*time.Second, "response stream duration")
	omit := f.Bool("omit-usage", false, "omit upstream usage")
	readBufferKiB := f.Int("read-buffer-kib", 64, "bounded upload discard buffer per active upload")
	if e := f.Parse(args); e != nil {
		return e
	}
	if *duration < 0 || *readBufferKiB < 1 || *readBufferKiB > 64 {
		return errors.New("invalid fixture parameters")
	}
	var active, peak, total atomic.Int64
	var uploading, uploadedBytes atomic.Int64
	uploadBuffers := sync.Pool{New: func() any { return make([]byte, *readBufferKiB<<10) }}
	var mu sync.Mutex
	bySecond := map[int64]int64{}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") })
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		json.NewEncoder(w).Encode(map[string]any{"total": total.Load(), "active": active.Load(), "peak": peak.Load(), "uploading": uploading.Load(), "uploaded_bytes": uploadedBytes.Load(), "by_unix_second": bySecond})
	})
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			w.WriteHeader(405)
			return
		}
		// Discard the upload incrementally. No full request buffer or JSON parser.
		uploading.Add(1)
		buffer := uploadBuffers.Get().([]byte)
		var readErr error
		digest := sha256.New()
		for {
			n, e := r.Body.Read(buffer)
			uploadedBytes.Add(int64(n))
			digest.Write(buffer[:n])
			if e != nil {
				readErr = e
				break
			}
		}
		uploadBuffers.Put(buffer)
		uploading.Add(-1)
		if readErr != io.EOF {
			return
		}
		input, e := strconv.ParseInt(r.Header.Get("X-Synthetic-Input-Tokens"), 10, 64)
		if e != nil || input < 1 {
			w.WriteHeader(400)
			return
		}
		total.Add(1)
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		mu.Lock()
		bySecond[time.Now().Unix()]++
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Test-Body-SHA256", hex.EncodeToString(digest.Sum(nil)))
		flush := w.(http.Flusher)
		started := time.Now()
		for i := 0; i < 128; i++ {
			if wait := time.Until(started.Add(time.Duration(i+1) * *duration / 128)); wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-r.Context().Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
			if _, e := io.WriteString(w, "data: {\"model\":\"capacity-fixture\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"xxxxxxxx\"}}]}\n\n"); e != nil {
				return
			}
			flush.Flush()
		}
		if !*omit {
			fmt.Fprintf(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":%d,\"completion_tokens\":1024,\"total_tokens\":%d}}\n\n", input, input+1024)
		}
		io.WriteString(w, "data: [DONE]\n\n")
		flush.Flush()
	})
	server := &http.Server{Addr: *addr, Handler: h2c.NewHandler(mux, &http2.Server{MaxConcurrentStreams: 100000, MaxReadFrameSize: 1 << 20, MaxUploadBufferPerConnection: 32 << 20, MaxUploadBufferPerStream: 8 << 20}), ReadHeaderTimeout: 10 * time.Second}
	ctx, cancel := signalContext()
	defer cancel()
	go func() { <-ctx.Done(); server.Close() }()
	e := server.ListenAndServe()
	if errors.Is(e, http.ErrServerClosed) {
		return nil
	}
	return e
}
func load(args []string) error {
	f := flag.NewFlagSet("load", flag.ContinueOnError)
	endpoint := f.String("url", "http://127.0.0.1:28080/v1/chat/completions", "gateway URL")
	output := f.String("output", "load.json", "summary path")
	results := f.String("results", "requests.jsonl", "per-request outcome path")
	prefix := f.String("prefix", "capacity", "request ID prefix")
	qps := f.Int("qps", 200, "open-loop scheduled requests/second")
	seconds := f.Int("seconds", 360, "arrival period including warmup")
	count := f.Int("count", 0, "closed-loop request count; 0 uses open-loop")
	concurrent := f.Int("concurrency", 2, "closed-loop concurrency only")
	connections := f.Int("connections", 8, "independent HTTP/2 connections")
	profile := f.String("profile", "mixed", "mixed or 16k or 32m")
	omit := f.Bool("omit-usage", false, "expect missing upstream usage")
	timeout := f.Duration("timeout", 180*time.Second, "per-request timeout")
	if e := f.Parse(args); e != nil {
		return e
	}
	if *qps < 1 || *seconds < 1 || *count < 0 || *concurrent < 1 || *connections < 1 || len(*prefix) > 80 || *timeout < time.Second {
		return errors.New("invalid load parameters")
	}
	type body struct {
		data   []byte
		tokens int64
	}
	bodies := []body{}
	for _, n := range mixedInputTokens {
		if *profile == "16k" {
			n = 16384
		} else if *profile == "32m" {
			n = (32 << 20) / 5
		}
		payload, e := json.Marshal(map[string]any{"model": "capacity-fixture", "stream": true, "stream_options": map[string]bool{"include_usage": false}, "messages": []any{map[string]string{"role": "user", "content": strings.Repeat(" word", int(n))}}})
		if e != nil {
			return e
		}
		bodies = append(bodies, body{payload, n})
		if *profile != "mixed" {
			break
		}
	}
	if *profile != "mixed" && *profile != "16k" && *profile != "32m" {
		return errors.New("invalid profile")
	}
	out, e := os.Create(*results)
	if e != nil {
		return e
	}
	defer out.Close()
	enc := json.NewEncoder(out)
	ctx, cancel := signalContext()
	defer cancel()
	clients := make([]*http.Client, *connections)
	for i := range clients {
		tr := &http2.Transport{AllowHTTP: true, DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		}}
		defer tr.CloseIdleConnections()
		clients[i] = &http.Client{Transport: tr, Timeout: *timeout}
	}
	var active, peak, issued, complete, businessErrors, maxLag atomic.Int64
	var writeMu sync.Mutex
	var writeErr error
	started := time.Now()
	target := *qps * *seconds
	if *count > 0 {
		target = *count
	}
	summary := func(finished bool) map[string]any {
		return map[string]any{"target_requests": target, "issued": issued.Load(), "complete": complete.Load(), "business_errors": businessErrors.Load(), "active": active.Load(), "peak_active": peak.Load(), "max_dispatch_lag_us": maxLag.Load(), "elapsed_seconds": time.Since(started).Seconds(), "qps": *qps, "arrival_seconds": *seconds, "closed_loop_concurrency": *concurrent, "connections": *connections, "profile": *profile, "prefix": *prefix, "omit_usage": *omit, "cancelled": ctx.Err() != nil, "finished": finished, "token_measurement": "synthetic reported usage; repeated space-word content; see offline tokenizer verification"}
	}
	if e := writeJSON(*output, summary(false)); e != nil {
		return e
	}
	checkpointStop, checkpointDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(checkpointDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-checkpointStop:
				return
			case <-ticker.C:
				if e := writeJSON(*output, summary(false)); e != nil {
					fmt.Fprintln(os.Stderr, "load checkpoint:", e)
					return
				}
			}
		}
	}()
	var wg sync.WaitGroup
	request := func(i int, at time.Time) {
		issued.Add(1)
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		b := bodies[0]
		if len(bodies) > 1 {
			b = bodies[mixedBodyIndexes[i%len(mixedBodyIndexes)]]
		}
		cr := result{ID: fmt.Sprintf("%s-%09d", *prefix, i), Input: b.tokens, Bytes: len(b.data), DispatchLag: time.Since(at).Microseconds()}
		for old := maxLag.Load(); cr.DispatchLag > old; old = maxLag.Load() {
			if maxLag.CompareAndSwap(old, cr.DispatchLag) {
				break
			}
		}
		begin := time.Now()
		req, e := http.NewRequestWithContext(ctx, "POST", *endpoint, bytes.NewReader(b.data))
		if e == nil {
			req.Host = "model.example.invalid"
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Request-Id", cr.ID)
			req.Header.Set("X-Client-Request-Id", cr.ID)
			req.Header.Set("X-Synthetic-Input-Tokens", strconv.FormatInt(b.tokens, 10))
			req.Header.Set("Authorization", "Bearer synthetic-capacity")
			var res *http.Response
			res, e = clients[i%len(clients)].Do(req)
			if e == nil {
				cr.Status = res.StatusCode
				digest := sha256.Sum256(b.data)
				cr.BodyUnchanged = res.Header.Get("X-Test-Body-SHA256") == hex.EncodeToString(digest[:])
				scanner := bufio.NewScanner(res.Body)
				scanner.Buffer(make([]byte, 4096), 32768)
				for scanner.Scan() {
					line := scanner.Text()
					if line == "data: [DONE]" {
						cr.Done = true
					}
					if strings.HasPrefix(line, "data: ") && strings.Contains(line, "\"usage\"") {
						var doc struct {
							Usage *struct {
								Input  int64 `json:"prompt_tokens"`
								Output int64 `json:"completion_tokens"`
							} `json:"usage"`
						}
						if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &doc) == nil && doc.Usage != nil {
							cr.Usage = doc.Usage.Input == b.tokens && doc.Usage.Output == 1024
						}
					}
				}
				e = scanner.Err()
				res.Body.Close()
			}
		}
		cr.Elapsed = time.Since(begin).Microseconds()
		if e != nil {
			cr.Error = e.Error()
		}
		if cr.Status != 200 || !cr.Done || cr.Error != "" || cr.Usage == *omit || !cr.BodyUnchanged {
			businessErrors.Add(1)
		} else {
			complete.Add(1)
		}
		writeMu.Lock()
		if e := enc.Encode(cr); e != nil && writeErr == nil {
			writeErr = e
		}
		writeMu.Unlock()
	}
	if *count > 0 {
		var next atomic.Int64
		for w := 0; w < *concurrent; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					if ctx.Err() != nil {
						return
					}
					i := int(next.Add(1)) - 1
					if i >= target {
						return
					}
					request(i, time.Now())
				}
			}()
		}
	} else {
	arrivals:
		for i := 0; i < target; i++ {
			at := started.Add(time.Duration(i) * time.Second / time.Duration(*qps))
			if wait := time.Until(at); wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-ctx.Done():
					timer.Stop()
					break arrivals
				case <-timer.C:
				}
			}
			if ctx.Err() != nil {
				break
			}
			wg.Add(1)
			go func(i int, at time.Time) { defer wg.Done(); request(i, at) }(i, at)
		}
	}
	wg.Wait()
	close(checkpointStop)
	<-checkpointDone
	if e := writeJSON(*output, summary(true)); e != nil {
		return e
	}
	if writeErr != nil {
		return writeErr
	}
	if issued.Load() != int64(target) || businessErrors.Load() != 0 {
		return errors.New("load did not complete all expected business responses")
	}
	return nil
}
func verify(args []string) error {
	f := flag.NewFlagSet("verify", flag.ContinueOnError)
	file := f.String("results", "requests.jsonl", "client outcomes")
	database := f.String("database", "metering_capacity", "isolated database")
	out := f.String("output", "verification.json", "result path")
	omit := f.Bool("omit-usage", false, "upstream omitted usage")
	if e := f.Parse(args); e != nil {
		return e
	}
	for _, c := range *database {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_') {
			return errors.New("invalid database")
		}
	}
	db, e := sql.Open("mysql", os.Getenv("DORIS_DSN"))
	if e != nil {
		return e
	}
	defer db.Close()
	input, e := os.Open(*file)
	if e != nil {
		return e
	}
	defer input.Close()
	scanner := bufio.NewScanner(input)
	var total, bad, missing int
	var examples []string
	for {
		batch := map[string]result{}
		args := []any{}
		marks := []string{}
		for len(batch) < 512 && scanner.Scan() {
			var r result
			if e := json.Unmarshal(scanner.Bytes(), &r); e != nil {
				return e
			}
			batch[r.ID] = r
			args = append(args, r.ID)
			marks = append(marks, "?")
		}
		if len(batch) == 0 {
			break
		}
		rows, e := db.Query("SELECT logical_request_id,event_type,usage_status,input_tokens,output_tokens,request_model,response_model,terminal_observed,observation_status,usage_eligible,anomaly_codes FROM "+*database+".events_raw WHERE logical_request_id IN ("+strings.Join(marks, ",")+")", args...)
		if e != nil {
			return e
		}
		seen := map[string][2]int{}
		invalid := map[string]bool{}
		for rows.Next() {
			var id, typ, usage, anomalies string
			var in, out, terminal, eligible sql.NullInt64
			var reqModel, respModel, status sql.NullString
			if e := rows.Scan(&id, &typ, &usage, &in, &out, &reqModel, &respModel, &terminal, &status, &eligible, &anomalies); e != nil {
				rows.Close()
				return e
			}
			counts := seen[id]
			if typ == "request_started" {
				counts[0]++
			} else if typ == "usage_observed" {
				counts[1]++
				r := batch[id]
				good := r.Status == 200 && r.Done && r.Error == "" && r.Usage != *omit && !reqModel.Valid && respModel.Valid && respModel.String == "capacity-fixture" && terminal.Valid && terminal.Int64 == 1 && status.String == "complete"
				if *omit {
					good = good && usage == "unknown" && !in.Valid && !out.Valid && eligible.Int64 == 0 && strings.Contains(anomalies, "missing_usage")
				} else {
					good = good && usage == "known" && in.Valid && in.Int64 == r.Input && out.Valid && out.Int64 == 1024 && eligible.Int64 == 1
				}
				for _, a := range []string{"transport_lost", "missing_end", "parse_limit", "protocol_error", "observation_timeout", "protocol_incomplete"} {
					if strings.Contains(anomalies, a) {
						good = false
					}
				}
				if !good {
					invalid[id] = true
				}
			}
			seen[id] = counts
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return e
		}
		for id := range batch {
			total++
			if seen[id] != [2]int{1, 1} {
				missing++
				invalid[id] = true
			}
			if invalid[id] {
				bad++
				if len(examples) < 20 {
					examples = append(examples, id)
				}
			}
		}
	}
	if e := scanner.Err(); e != nil {
		return e
	}
	if e := writeJSON(*out, map[string]any{"client_requests": total, "invalid": bad, "missing_or_duplicate_events": missing, "examples": examples, "passed": total > 0 && bad == 0}); e != nil {
		return e
	}
	if total == 0 || bad != 0 {
		return errors.New("client-to-Doris reconciliation failed")
	}
	return nil
}
func main() {
	var e error
	if len(os.Args) < 2 {
		e = errors.New("expected fixture, load or verify")
	} else {
		switch os.Args[1] {
		case "fixture":
			e = fixture(os.Args[2:])
		case "load":
			e = load(os.Args[2:])
		case "verify":
			e = verify(os.Args[2:])
		default:
			e = errors.New("unknown command")
		}
	}
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
