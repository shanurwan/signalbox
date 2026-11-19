package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shanurwan/signalbox/internal/config"
)

func newTest(t *testing.T, extra string) (*Server, *httptest.Server) {
	t.Helper()
	cfg, err := config.Parse([]byte(`
ingest_token: in-secret
api_token: api-secret
state_file: ` + filepath.ToSlash(filepath.Join(t.TempDir(), "state.json")) + `
rules:
  - name: api-5xx
    when: error_ratio > 0.10
    min_requests: 10
    window: 1m
` + extra))
	if err != nil {
		t.Fatal(err)
	}
	s := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return s, ts
}

func post(t *testing.T, url, token, ctype, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest("POST", url, strings.NewReader(body))
	req.Header.Set("Content-Type", ctype)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func otlpRequests(n, errors int) string {
	var recs []string
	now := time.Now().UnixNano()
	for i := 0; i < n; i++ {
		code := 200
		if i < errors {
			code = 503
		}
		recs = append(recs, fmt.Sprintf(`{"timeUnixNano":"%d","body":{"stringValue":"/pay"},"attributes":[
{"key":"http.response.status_code","value":{"intValue":"%d"}},{"key":"totalDuration","value":{"stringValue":"40"}},
{"key":"requestId","value":{"stringValue":"r%d"}}]}`, now, code, i))
	}
	return `{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"api"}}]},
"scopeLogs":[{"logRecords":[` + strings.Join(recs, ",") + `]}]}]}`
}

func TestOTLPToAlertEndToEnd(t *testing.T) {
	s, ts := newTest(t, "")
	if resp := post(t, ts.URL+"/v1/logs", "", "application/json", otlpRequests(20, 5)); resp.StatusCode != 401 {
		t.Fatalf("no token: %d", resp.StatusCode)
	}
	resp := post(t, ts.URL+"/v1/logs", "in-secret", "application/json", otlpRequests(20, 5))
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("%d %s", resp.StatusCode, b)
	}
	// The same batch again, as a forwarder replaying after a reconnect: deduplicated by request ID.
	post(t, ts.URL+"/v1/logs", "in-secret", "application/json", otlpRequests(20, 5))
	if d := s.Pipe.Duplicates.Load(); d != 20 {
		t.Fatalf("duplicates = %d, want 20", d)
	}
	s.Engine.Evaluate(time.Now())

	req, _ := http.NewRequest("GET", ts.URL+"/api/alerts", nil)
	req.Header.Set("Authorization", "Bearer api-secret")
	r2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Alerts []struct {
			Rule, State string
			Value       float64
			Labels      map[string]string
		}
	}
	json.NewDecoder(r2.Body).Decode(&out)
	if len(out.Alerts) != 1 || out.Alerts[0].State != "firing" || out.Alerts[0].Value != 0.25 || out.Alerts[0].Labels["service"] != "api" {
		t.Fatalf("alerts: %+v", out)
	}

	if err := s.SaveState(); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(s.cfg.StateFile)
	if !strings.Contains(string(b), `"api-5xx"`) {
		t.Fatalf("state file missing the firing series: %s", b)
	}
}

func TestNDJSONAndTail(t *testing.T) {
	_, ts := newTest(t, "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", ts.URL+"/tail?service=worker&level=error&token=api-secret", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	br.ReadString('\n') // ": connected"

	nd := `{"service":"worker","level":"info","msg":"tick"}
{"service":"web","level":"error","msg":"other service"}
{"service":"worker","level":"error","msg":"job 42 failed","job":42}
`
	if r := post(t, ts.URL+"/ingest", "in-secret", "application/x-ndjson", nd); r.StatusCode != 200 {
		t.Fatalf("ingest %d", r.StatusCode)
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(line, "data: ") {
			if !strings.Contains(line, "job 42 failed") || !strings.Contains(line, `"job":"42"`) {
				t.Fatalf("tail delivered the wrong record: %s", line)
			}
			return
		}
	}
}

func TestMetricsExposeREDAndSelf(t *testing.T) {
	_, ts := newTest(t, "")
	post(t, ts.URL+"/v1/logs", "in-secret", "application/json", otlpRequests(10, 2))
	req, _ := http.NewRequest("GET", ts.URL+"/metrics", nil)
	req.Header.Set("Authorization", "Bearer api-secret")
	resp, _ := http.DefaultClient.Do(req)
	b, _ := io.ReadAll(resp.Body)
	for _, want := range []string{
		`signalbox_http_requests_total{code="5xx",environment="",service="api"} 2`,
		`signalbox_http_requests_total{code="2xx",environment="",service="api"} 8`,
		`signalbox_ingested_records_total 10`,
		`signalbox_http_request_duration_seconds_count{environment="",service="api"} 10`,
	} {
		if !strings.Contains(string(b), want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}
