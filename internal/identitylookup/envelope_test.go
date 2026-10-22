package identitylookup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"
)

func lookupSuccessJSON(data string) string {
	return `{"responseCode":"000000","responseMsg":"请求处理成功","success":true,"data":` + data + `}`
}

type lookupTransport func(*http.Request) (*http.Response, error)

func (f lookupTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestResponseEnvelopes(t *testing.T) {
	for _, kind := range []string{"service", "customer"} {
		t.Run(kind, func(t *testing.T) {
			data := `{"service_id":"s","service_name":"服务","provider_id":"p","provider_name":"供应商"}`
			id, name := "s", "服务"
			if kind == "customer" {
				data = `{"api_key_id":"k","customer_id":"c","customer_name":"客户"}`
				id, name = "k", "客户"
			}
			good := lookupSuccessJSON(data)
			cases := []struct{ name, body, want string }{
				{"success", good, ""},
				{"no_message", strings.Replace(good, `"responseMsg":"请求处理成功",`, "", 1), ""},
				{"empty_message", strings.Replace(good, "请求处理成功", "", 1), ""},
				{"null_message", strings.Replace(good, `"请求处理成功"`, "null", 1), "lookup_invalid_response"},
				{"number_message", strings.Replace(good, `"请求处理成功"`, "123", 1), "lookup_invalid_response"},
				{"no_code", `{"success":true,"data":` + data + `}`, "lookup_invalid_response"},
				{"null_code", strings.Replace(good, `"000000"`, "null", 1), "lookup_invalid_response"},
				{"number_code", strings.Replace(good, `"000000"`, "0", 1), "lookup_invalid_response"},
				{"no_success", strings.Replace(good, `"success":true,`, "", 1), "lookup_invalid_response"},
				{"null_success", strings.Replace(good, `"success":true`, `"success":null`, 1), "lookup_invalid_response"},
				{"string_success", strings.Replace(good, `"success":true`, `"success":"true"`, 1), "lookup_invalid_response"},
				{"false_success", strings.Replace(good, `"success":true`, `"success":false`, 1), "lookup_invalid_response"},
				{"no_data", `{"responseCode":"000000","success":true}`, "lookup_invalid_response"},
				{"null_data", lookupSuccessJSON("null"), "lookup_invalid_response"},
				{"empty_data", lookupSuccessJSON(`{}`), "lookup_invalid_response"},
				{"array_data", lookupSuccessJSON(`[]`), "lookup_invalid_response"},
				{"string_data", lookupSuccessJSON(`"data"`), "lookup_invalid_response"},
				{"partial_data", lookupSuccessJSON(`{"service_id":"s"}`), "lookup_invalid_response"},
				{"null_name", lookupSuccessJSON(strings.Replace(data, `"`+name+`"`, "null", 1)), "lookup_invalid_response"},
				{"long_id", lookupSuccessJSON(strings.Replace(data, `"`+id+`"`, `"`+strings.Repeat(id, 129)+`"`, 1)), "lookup_invalid_response"},
				{"long_name", lookupSuccessJSON(strings.Replace(data, `"`+name+`"`, `"`+strings.Repeat("中", 171)+`"`, 1)), "lookup_invalid_response"},
				{"id_boundary", lookupSuccessJSON(strings.Replace(data, `"`+id+`"`, `"`+strings.Repeat(id, 128)+`"`, 1)), ""},
				{"name_boundary", lookupSuccessJSON(strings.Replace(data, `"`+name+`"`, `"`+strings.Repeat("中", 170)+`ab"`, 1)), ""},
				{"unknown_code", `{"responseCode":"unexpected","success":false}`, "lookup_invalid_response"},
				{"zero_matches", `{"responseCode":"650013","responseMsg":"未查询到数据","success":false}`, "lookup_invalid_response"},
				{"multiple_matches", `{"responseCode":"650013","responseMsg":"查询数据多条","success":false}`, "lookup_invalid_response"},
				{"number_parameter_code", `{"responseCode":100001,"success":false}`, "lookup_invalid_response"},
				{"number_mapping_code", `{"responseCode":650013,"success":false}`, "lookup_invalid_response"},
				{"unknown_field", strings.Replace(good, `"success":true`, `"extra":1,"success":true`, 1), "lookup_invalid_response"},
				{"duplicate_code", strings.Replace(good, `"success":true`, `"responseCode":"000000","success":true`, 1), "lookup_invalid_response"},
				{"duplicate_data_field", lookupSuccessJSON(`{"service_id":"s","service_id":"x"}`), "lookup_invalid_response"},
				{"invalid_utf8", strings.Replace(good, "请求处理成功", "\xff", 1), "lookup_invalid_response"},
				{"unpaired_surrogate", strings.Replace(good, "请求处理成功", `\ud800`, 1), "lookup_invalid_response"},
				{"multiple_objects", good + good, "lookup_invalid_response"},
				{"null_root", `null`, "lookup_invalid_response"},
				{"flat_response", data, "lookup_invalid_response"},
				{"oversize_envelope", strings.Replace(good, "请求处理成功", strings.Repeat("x", maxResponseBytes), 1), "lookup_invalid_response"},
			}
			for _, code := range []string{"650013", "100001", "unauthenticated", "forbidden", "not_found", "ambiguous_mapping", "401"} {
				want := "lookup_invalid_response"
				if code == "100001" {
					want = "lookup_unavailable"
				}
				for _, tail := range []string{"", `,"data":null`} {
					cases = append(cases, struct{ name, body, want string }{code + tail,
						`{"responseCode":"` + code + `","responseMsg":"业务失败","success":false` + tail + `}`, want})
				}
				cases = append(cases,
					struct{ name, body, want string }{code + "_true", `{"responseCode":"` + code + `","success":true}`, "lookup_invalid_response"},
					struct{ name, body, want string }{code + "_with_data", `{"responseCode":"` + code + `","success":false,"data":` + data + `}`, "lookup_invalid_response"})
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					var logs bytes.Buffer
					calls := 0
					r := &remote{credential: "synthetic-credential", uri: "http://directory.invalid/", logger: log.New(&logs, "", 0), client: &http.Client{Transport: lookupTransport(func(*http.Request) (*http.Response, error) {
						calls++
						return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
					})}}
					flushLogs := testResponseLogQueue(t, r)
					var err error
					if kind == "service" {
						var got ServiceIdentity
						got, err = (serviceRemote{r}).Resolve(context.Background(), ServiceLookupRequest{})
						if tc.want != "" && got != (ServiceIdentity{}) {
							t.Fatal("partial service identity returned")
						}
					} else {
						var got CustomerIdentity
						got, err = (customerRemote{r}).Resolve(context.Background(), []byte("synthetic-token"))
						if tc.want != "" && got != (CustomerIdentity{}) {
							t.Fatal("partial customer identity returned")
						}
					}
					if (tc.want == "" && err != nil) || (tc.want != "" && (err == nil || Failure(err) != tc.want)) {
						t.Fatalf("classification = %v, want %s", err, tc.want)
					}
					flushLogs()
					if calls != 1 || strings.Count(logs.String(), "\n") != 1 {
						t.Fatal("query retried or logged more than once")
					}
					var entry map[string]any
					if json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &entry) != nil || entry["lookup_type"] != kind || entry["http_status"] != float64(200) {
						t.Fatal("invalid response log")
					}
					wantLevel, wantStatus := "INFO", "mapped"
					if tc.want != "" {
						wantLevel, wantStatus = "WARN", tc.want
					}
					if entry["level"] != wantLevel || entry["status"] != wantStatus {
						t.Fatal("invalid log outcome")
					}
				})
			}
		})
	}
}

func TestResponseMessageLogPrivacy(t *testing.T) {
	credential, token := "synthetic-access", "synthetic-access-business"
	for _, success := range []bool{true, false} {
		t.Run(map[bool]string{true: "success", false: "failure"}[success], func(t *testing.T) {
			message := "凭据=" + credential + "; token=" + token + "\n\r\t\x1b\u0085"
			body := map[string]any{"responseCode": "000000", "success": success, "responseMsg": message,
				"data": map[string]string{"api_key_id": "private-id", "customer_id": "private-customer", "customer_name": "private-name"}}
			if !success {
				body["responseCode"], body["data"] = "650013", nil
			}
			encoded, _ := json.Marshal(body)
			var logs bytes.Buffer
			r := &remote{credential: credential, uri: "http://private-directory.invalid/", logger: log.New(&logs, "", 0), client: &http.Client{Transport: lookupTransport(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(encoded))}, nil
			})}}
			flushLogs := testResponseLogQueue(t, r)
			key := []byte(token)
			_, err := (customerRemote{r}).Resolve(context.Background(), key)
			flushLogs()
			if success && err != nil {
				t.Fatal(err)
			}
			if string(key) != token {
				t.Fatal("logging changed token")
			}
			for _, secret := range []string{credential, token, "private-directory", "private-id", "private-customer", "private-name"} {
				if strings.Contains(logs.String(), secret) {
					t.Fatal("log leaked credential, endpoint or identity")
				}
			}
			if strings.Count(logs.String(), "\n") != 1 || !strings.Contains(logs.String(), `[REDACTED]`) || strings.ContainsAny(strings.TrimSuffix(logs.String(), "\n"), "\r\t\x1b\u0085") {
				t.Fatal("log message was not safely escaped")
			}
			var entry map[string]any
			if json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &entry) != nil || !strings.HasPrefix(entry["responseMsg"].(string), "凭据=[REDACTED]; token=[REDACTED]") {
				t.Fatal("responseMsg missing or overlap redaction failed")
			}
		})
	}
}

func TestAuthenticationResponseEnvelopes(t *testing.T) {
	for _, kind := range []string{"service", "customer"} {
		t.Run(kind, func(t *testing.T) {
			privateMessage := "synthetic-credential"
			if kind == "customer" {
				privateMessage += " synthetic-token"
			}
			privateBody, _ := json.Marshal(map[string]any{"responseCode": "401", "success": false, "responseMsg": privateMessage})
			for _, tc := range []struct {
				name, body, media string
				valid             bool
			}{
				{"unauthorized", `{"success":false,"responseCode":"401","responseMsg":"Unauthorized"}`, "application/json", true},
				{"null_data", `{"success":false,"responseCode":"401","responseMsg":"Unauthorized","data":null}`, "application/json", true},
				{"no_message", `{"success":false,"responseCode":"401"}`, "application/json", true},
				{"redaction", string(privateBody), "application/json", true},
				{"read_failure", "", "application/json", false},
				{"html", `private gateway error`, "text/html", false},
				{"malformed", `{"responseCode":"401","responseMsg":"private"`, "application/json", false},
				{"duplicate", `{"responseCode":"401","responseCode":"401","responseMsg":"private","success":false}`, "application/json", false},
				{"number_code", `{"responseCode":401,"responseMsg":"private","success":false}`, "application/json", false},
				{"unknown_field", `{"responseCode":"401","responseMsg":"private","success":false,"extra":1}`, "application/json", false},
				{"wrong_code", `{"responseCode":"650013","responseMsg":"private","success":false}`, "application/json", false},
				{"wrong_success", `{"responseCode":"401","responseMsg":"private","success":true}`, "application/json", false},
				{"no_success", `{"responseCode":"401","responseMsg":"private"}`, "application/json", false},
				{"non_null_data", `{"responseCode":"401","responseMsg":"private","success":false,"data":{}}`, "application/json", false},
				{"oversize", `{"responseCode":"401","success":false,"responseMsg":"` + strings.Repeat("private", maxResponseBytes/7+1) + `"}`, "application/json", false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var logs bytes.Buffer
					r := &remote{uri: "http://directory.invalid/", credential: "synthetic-credential", logger: log.New(&logs, "", 0), client: &http.Client{Transport: lookupTransport(func(*http.Request) (*http.Response, error) {
						var body io.Reader = strings.NewReader(tc.body)
						if tc.name == "read_failure" {
							body = failedLookupResponseReader{}
						}
						return &http.Response{StatusCode: http.StatusUnauthorized, Header: http.Header{"Content-Type": []string{tc.media}}, Body: io.NopCloser(body)}, nil
					})}}
					flushLogs := testResponseLogQueue(t, r)
					var err error
					if kind == "service" {
						var got ServiceIdentity
						got, err = (serviceRemote{r}).Resolve(context.Background(), ServiceLookupRequest{})
						if got != (ServiceIdentity{}) {
							t.Fatal("authentication failure returned service identity")
						}
					} else {
						var got CustomerIdentity
						got, err = (customerRemote{r}).Resolve(context.Background(), []byte("synthetic-token"))
						if got != (CustomerIdentity{}) {
							t.Fatal("authentication failure returned customer identity")
						}
					}
					flushLogs()
					if err == nil || Failure(err) != "lookup_unavailable" || strings.Count(logs.String(), "\n") != 1 || strings.Contains(logs.String(), "private") || strings.Contains(logs.String(), "synthetic-credential") || strings.Contains(logs.String(), "synthetic-token") {
						t.Fatal("authentication failure classification or log privacy", err)
					}
					var entry map[string]any
					if json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &entry) != nil || entry["http_status"] != float64(401) || entry["status"] != "lookup_unavailable" || entry["level"] != "WARN" {
						t.Fatal("authentication log metadata")
					}
					if tc.valid {
						expectedMessage := "Unauthorized"
						if tc.name == "redaction" {
							expectedMessage = "[REDACTED]"
							if kind == "customer" {
								expectedMessage += " [REDACTED]"
							}
						}
						if entry["responseCode"] != "401" || entry["success"] != false || (tc.name != "no_message" && entry["responseMsg"] != expectedMessage) {
							t.Fatal("valid authentication response was not logged")
						}
					} else if _, ok := entry["responseMsg"]; ok {
						t.Fatal("invalid authentication response fields were logged")
					}
				})
			}
		})
	}
}

type failedLookupResponseReader struct{}

func (failedLookupResponseReader) Read([]byte) (int, error) {
	return 0, errors.New("private-response-read-error")
}

func TestLogMessageLimitAndFailurePrivacy(t *testing.T) {
	secret := "synthetic-secret-crossing-cutoff"
	for _, tc := range []struct{ input, want string }{
		{strings.Repeat("中", 342), strings.Repeat("中", 341)},
		{strings.Repeat("a", 1020) + secret, strings.Repeat("a", 1020) + "[RED"},
		{"plain message", "plain message"},
	} {
		got := logValue(tc.input, secret)
		if got != tc.want || len(got) > maxLogMessageBytes || !utf8.ValidString(got) {
			t.Fatal("unsafe log truncation")
		}
	}
	for _, body := range []string{
		`{"responseCode":"000000","success":true,"responseMsg":"` + secret + `",broken}`,
		lookupSuccessJSON(`{}`) + strings.Repeat(secret, maxResponseBytes/len(secret)+1),
	} {
		var logs bytes.Buffer
		r := &remote{uri: "http://directory.invalid", logger: log.New(&logs, "", 0), client: &http.Client{Transport: lookupTransport(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
		})}}
		flushLogs := testResponseLogQueue(t, r)
		(serviceRemote{r}).Resolve(context.Background(), ServiceLookupRequest{})
		flushLogs()
		if strings.Contains(logs.String(), "responseMsg") || strings.Contains(logs.String(), secret) {
			t.Fatal("malformed response logged raw content")
		}
	}
	var logs bytes.Buffer
	r := &remote{uri: "http://directory.invalid", logger: log.New(&logs, "", 0), client: &http.Client{Transport: lookupTransport(func(*http.Request) (*http.Response, error) {
		return nil, errors.New(secret)
	})}}
	flushLogs := testResponseLogQueue(t, r)
	(serviceRemote{r}).Resolve(context.Background(), ServiceLookupRequest{})
	flushLogs()
	if strings.Contains(logs.String(), secret) || !strings.Contains(logs.String(), "lookup_unavailable") || strings.Count(logs.String(), "\n") != 1 {
		t.Fatal("transport failure leaked input or logged multiple times")
	}
}
