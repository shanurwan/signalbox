package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// fakeClickHouse records the form of each query and answers with one row.
func fakeClickHouse(t *testing.T) (*sync.Mutex, *[]url.Values, string) {
	var mu sync.Mutex
	var got []url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Like ClickHouse: the body is the SQL, settings and parameters
		// are in the URL.
		b, _ := io.ReadAll(r.Body)
		v := r.URL.Query()
		v.Set("query", string(b))
		if !strings.HasPrefix(string(b), "SELECT") { // table creation from Init, not used here
			w.WriteHeader(200)
			return
		}
		mu.Lock()
		got = append(got, v)
		mu.Unlock()
		io.WriteString(w, `{"meta":[],"data":[{"time":"2026-11-20 00:00:00","requests":10,"q":[1,2,3]}],"rows":1}`)
	}))
	t.Cleanup(srv.Close)
	return &mu, &got, srv.URL
}

func TestQueryEndpointsBindTheCallersTenant(t *testing.T) {
	mu, got, chURL := fakeClickHouse(t)
	_, ts := newTest(t, "clickhouse: {url: \""+chURL+"\"}\n")
	tk := createTenant(t, ts.URL, "acme", "")

	code, out := call(t, "GET", ts.URL+"/api/requests?service=api&since=1h&step=1m&tenant=globex", tk.APIToken, "")
	if code != 200 || !strings.Contains(out, `"requests": 10`) || strings.Contains(out, `"q"`) {
		t.Fatalf("requests: %d %s", code, out)
	}
	evil := "x' OR 1=1 --"
	if code, _ := call(t, "GET", ts.URL+"/api/logs?q="+url.QueryEscape(evil)+"&level=error&limit=5", tk.APIToken, ""); code != 200 {
		t.Fatalf("logs: %d", code)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(*got) != 2 {
		t.Fatalf("%d queries", len(*got))
	}
	for _, v := range *got {
		// ?tenant=globex from a tenant token is ignored: the token decides.
		if v.Get("param_tenant") != "acme" {
			t.Fatalf("tenant param %q", v.Get("param_tenant"))
		}
		if strings.Contains(v.Get("query"), "acme") || strings.Contains(v.Get("query"), "OR 1=1") {
			t.Fatalf("a value was pasted into SQL: %s", v.Get("query"))
		}
		if v.Get("readonly") != "2" || v.Get("max_execution_time") == "" {
			t.Fatalf("query not limited: %v", v)
		}
	}
	r, l := (*got)[0], (*got)[1]
	if r.Get("param_step") != "60" || r.Get("param_since") != "3600" || r.Get("param_service") != "api" {
		t.Fatalf("requests params %v", r)
	}
	if l.Get("param_text") != evil || l.Get("param_severity") != "17" || l.Get("param_limit") != "5" {
		t.Fatalf("logs params %v", l)
	}
}

func TestQueryLimits(t *testing.T) {
	_, _, chURL := fakeClickHouse(t)
	_, ts := newTest(t, "clickhouse: {url: \""+chURL+"\"}\n")
	for _, q := range []string{
		"/api/requests?since=10000h",
		"/api/requests?since=24h&step=1s",
		"/api/requests?since=banana",
		"/api/logs?limit=5000",
		"/api/logs?level=loud",
	} {
		if code, out := call(t, "GET", ts.URL+q, "api-secret", ""); code != 400 {
			t.Errorf("%s: %d %s", q, code, out)
		}
	}
	_, ts2 := newTest(t, "")
	if code, _ := call(t, "GET", ts2.URL+"/api/logs", "api-secret", ""); code != 501 {
		t.Errorf("without storage: %d", code)
	}
}
