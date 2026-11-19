package server

import (
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
	"sync"
	"testing"
	"time"

	"github.com/shanurwan/signalbox/internal/config"
)

func call(t *testing.T, method, url, token, body string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

type tokens struct {
	IngestToken string `json:"ingest_token"`
	APIToken    string `json:"api_token"`
}

func createTenant(t *testing.T, base, id, body string) tokens {
	t.Helper()
	code, out := call(t, "PUT", base+"/api/tenants/"+id, "api-secret", body)
	if code != 201 {
		t.Fatalf("create %s: %d %s", id, code, out)
	}
	var tk tokens
	json.Unmarshal([]byte(out), &tk)
	if !strings.HasPrefix(tk.IngestToken, "sb_in_") || !strings.HasPrefix(tk.APIToken, "sb_api_") {
		t.Fatalf("tokens: %s", out)
	}
	return tk
}

// hook collects webhook deliveries.
type hook struct {
	mu     sync.Mutex
	events []map[string]any
}

func newHook(t *testing.T) (*hook, string) {
	h := &hook{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var ev map[string]any
		json.NewDecoder(r.Body).Decode(&ev)
		h.mu.Lock()
		h.events = append(h.events, ev)
		h.mu.Unlock()
	}))
	t.Cleanup(srv.Close)
	return h, srv.URL
}

func (h *hook) wait(t *testing.T, n int) []map[string]any {
	t.Helper()
	for i := 0; i < 100; i++ {
		h.mu.Lock()
		got := append([]map[string]any(nil), h.events...)
		h.mu.Unlock()
		if len(got) >= n {
			return got
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("webhook got fewer than %d events", n)
	return nil
}

// otlpWithTenantAttr is a request log batch whose resource claims to be
// another tenant. The claim must be ignored: the token decides.
func otlpWithTenantAttr(n, errs int, claim string) string {
	return strings.Replace(otlpRequests(n, errs), `"attributes":[{"key":"service.name"`,
		`"attributes":[{"key":"tenant","value":{"stringValue":"`+claim+`"}},{"key":"service.name"`, 1)
}

func TestTenantLifecycleAndIsolation(t *testing.T) {
	s, ts := newTest(t, "tenant_receivers_allow_private: true\n")
	runOutbox(t, s)
	acme := createTenant(t, ts.URL, "acme", "")
	globex := createTenant(t, ts.URL, "globex", "")

	// acme sets up its own receiver and rule through its API token.
	h, hookURL := newHook(t)
	if code, out := call(t, "PUT", ts.URL+"/api/receivers/team", acme.APIToken, `{"type":"webhook","url":"`+hookURL+`"}`); code != 200 {
		t.Fatalf("receiver: %d %s", code, out)
	}
	rule := "when: error_ratio > 0.2\nmin_requests: 10\nwindow: 1m\nnotify: [team]\n"
	if code, out := call(t, "PUT", ts.URL+"/api/rules/checkout-5xx", acme.APIToken, rule); code != 200 {
		t.Fatalf("rule: %d %s", code, out)
	}
	// A rule naming a receiver the tenant does not have is refused.
	if code, _ := call(t, "PUT", ts.URL+"/api/rules/x", globex.APIToken, rule); code != 400 {
		t.Fatalf("globex used acme's receiver: %d", code)
	}

	// acme sends 50% errors; globex sends the same service name, healthy,
	// and claims in an attribute to be acme.
	if resp := post(t, ts.URL+"/v1/logs", acme.IngestToken, "application/json", otlpRequests(20, 10)); resp.StatusCode != 200 {
		t.Fatalf("acme ingest %d", resp.StatusCode)
	}
	if resp := post(t, ts.URL+"/v1/logs", globex.IngestToken, "application/json", otlpWithTenantAttr(20, 0, "acme")); resp.StatusCode != 200 {
		t.Fatalf("globex ingest %d", resp.StatusCode)
	}
	s.Engine.Evaluate(time.Now())

	evs := h.wait(t, 1)
	if evs[0]["tenant"] != "acme" || evs[0]["rule"] != "checkout-5xx" || evs[0]["value"] != 0.5 {
		t.Fatalf("acme's webhook got %+v", evs[0])
	}

	alerts := func(token, query string) string {
		code, out := call(t, "GET", ts.URL+"/api/alerts"+query, token, "")
		if code != 200 {
			t.Fatalf("alerts: %d %s", code, out)
		}
		return out
	}
	if out := alerts(globex.APIToken, ""); strings.Contains(out, "acme") || strings.Contains(out, "firing") {
		t.Fatalf("globex sees: %s", out)
	}
	// The platform rule api-5xx fired for acme only: globex's records did
	// not land in acme's series despite the attribute.
	if out := alerts(acme.APIToken, ""); strings.Count(out, `"state": "firing"`) != 2 {
		t.Fatalf("acme: %s", out)
	}
	// A tenant cannot widen its view with ?tenant=.
	if out := alerts(globex.APIToken, "?tenant=acme"); strings.Contains(out, "firing") {
		t.Fatalf("globex read acme via ?tenant=: %s", out)
	}
	if out := alerts("api-secret", ""); strings.Count(out, `"state": "firing"`) != 2 || !strings.Contains(out, "notifications_pending") {
		t.Fatalf("admin: %s", out)
	}

	// Operator endpoints stay closed to tenants.
	for _, path := range []string{"/api/tenants", "/metrics"} {
		if code, _ := call(t, "GET", ts.URL+path, acme.APIToken, ""); code != 403 {
			t.Fatalf("%s with a tenant token: %d", path, code)
		}
	}
	if code, _ := call(t, "GET", ts.URL+"/api/alerts", "sb_api_guess", ""); code != 401 {
		t.Fatalf("unknown token: %d", code)
	}
	// An ingest token is not an API token, and the reverse.
	if code, _ := call(t, "GET", ts.URL+"/api/alerts", acme.IngestToken, ""); code != 401 {
		t.Fatalf("ingest token on the API: %d", code)
	}
	if resp := post(t, ts.URL+"/v1/logs", acme.APIToken, "application/json", otlpRequests(1, 0)); resp.StatusCode != 401 {
		t.Fatalf("API token on ingest: %d", resp.StatusCode)
	}

	// A receiver in use cannot be deleted from under its rule.
	if code, _ := call(t, "DELETE", ts.URL+"/api/receivers/team", acme.APIToken, ""); code != 409 {
		t.Fatalf("delete receiver in use: %d", code)
	}
	// Deleting the firing rule resolves its alert at the next evaluation.
	if code, _ := call(t, "DELETE", ts.URL+"/api/rules/checkout-5xx", acme.APIToken, ""); code != 200 {
		t.Fatal("delete rule")
	}
	s.Engine.Evaluate(time.Now())
	evs = h.wait(t, 2)
	if evs[1]["status"] != "resolved" {
		t.Fatalf("after delete: %+v", evs[1])
	}

	// Deleting the tenant revokes its tokens at once.
	if code, _ := call(t, "DELETE", ts.URL+"/api/tenants/acme", "api-secret", ""); code != 200 {
		t.Fatal("delete tenant")
	}
	if resp := post(t, ts.URL+"/v1/logs", acme.IngestToken, "application/json", otlpRequests(1, 0)); resp.StatusCode != 401 {
		t.Fatalf("deleted tenant's token still works: %d", resp.StatusCode)
	}
}

func runOutbox(t *testing.T, s *Server) {
	ctx, cancel := contextWithCancel()
	t.Cleanup(cancel)
	go s.Outbox.Run(ctx)
}

func TestTenantRateLimitIsBackPressure(t *testing.T) {
	s, ts := newTest(t, "")
	tk := createTenant(t, ts.URL, "small", `{"limits":{"records_per_second":10,"burst":10}}`)
	// The first batch is larger than the burst and still passes: the balance
	// was positive. The next one finds it in debt.
	if resp := post(t, ts.URL+"/v1/logs", tk.IngestToken, "application/json", otlpRequests(30, 0)); resp.StatusCode != 200 {
		t.Fatalf("first: %d", resp.StatusCode)
	}
	resp := post(t, ts.URL+"/v1/logs", tk.IngestToken, "application/json", otlpRequests(5, 0))
	if resp.StatusCode != 429 || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("second: %d %q", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	// Other tenants are unaffected.
	if resp := post(t, ts.URL+"/v1/logs", "in-secret", "application/json", otlpRequests(30, 0)); resp.StatusCode != 200 {
		t.Fatalf("default tenant: %d", resp.StatusCode)
	}
	_, out := call(t, "GET", ts.URL+"/api/usage", tk.APIToken, "")
	if !strings.Contains(out, `"records_accepted": 30`) || !strings.Contains(out, `"records_rejected": 5`) {
		t.Fatalf("usage: %s", out)
	}
	if s.Pipe.Records.Load() != 60 {
		t.Fatalf("pipeline saw %d records", s.Pipe.Records.Load())
	}
}

// Request IDs are only unique within a tenant. Two tenants that happen to
// use the same IDs must both be counted.
func TestDedupeIsPerTenant(t *testing.T) {
	s, ts := newTest(t, "")
	a := createTenant(t, ts.URL, "a", "")
	b := createTenant(t, ts.URL, "b", "")
	post(t, ts.URL+"/v1/logs", a.IngestToken, "application/json", otlpRequests(10, 0))
	post(t, ts.URL+"/v1/logs", b.IngestToken, "application/json", otlpRequests(10, 0)) // same IDs r0..r9
	post(t, ts.URL+"/v1/logs", b.IngestToken, "application/json", otlpRequests(10, 0)) // a real replay
	if got, dup := s.Pipe.Records.Load(), s.Pipe.Duplicates.Load(); got != 20 || dup != 10 {
		t.Fatalf("records %d duplicates %d, want 20 and 10", got, dup)
	}
}

func TestTenantStateSurvivesRestartWithoutTokensOnDisk(t *testing.T) {
	dir := t.TempDir()
	cfgYAML := "ingest_token: in-secret\napi_token: api-secret\ntenant_receivers_allow_private: true\nstate_file: " +
		filepath.ToSlash(filepath.Join(dir, "state.json")) + "\nrules: []\n"
	start := func() (*Server, *httptest.Server) {
		cfg, err := config.Parse([]byte(cfgYAML))
		if err != nil {
			t.Fatal(err)
		}
		s := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err := s.LoadState(); err != nil {
			t.Fatal(err)
		}
		ts := httptest.NewServer(s.Handler())
		t.Cleanup(ts.Close)
		return s, ts
	}
	s1, ts1 := start()
	tk := createTenant(t, ts1.URL, "acme", `{"limits":{"max_rules":3}}`)
	call(t, "PUT", ts1.URL+"/api/receivers/team", tk.APIToken, "type: webhook\nurl: https://hooks.example.com/T0/abc\n")
	call(t, "PUT", ts1.URL+"/api/rules/errs", tk.APIToken, "match: {min_severity: error}\nwhen: count > 0\nwindow: 1m\nnotify: [team]\n")
	if err := s1.SaveState(); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, "state.json"))
	if strings.Contains(string(raw), tk.IngestToken) || strings.Contains(string(raw), tk.APIToken) {
		t.Fatal("a token was written to the state file")
	}

	_, ts2 := start()
	code, out := call(t, "GET", ts2.URL+"/api/rules", tk.APIToken, "")
	if code != 200 || !strings.Contains(out, `"name": "errs"`) || !strings.Contains(out, `"scope": "tenant"`) {
		t.Fatalf("rules after restart: %d %s", code, out)
	}
	_, out = call(t, "GET", ts2.URL+"/api/receivers", tk.APIToken, "")
	if !strings.Contains(out, `"host": "hooks.example.com"`) || strings.Contains(out, "/T0/abc") {
		t.Fatalf("receivers after restart (URL path must not be shown): %s", out)
	}
	if resp := post(t, ts2.URL+"/v1/logs", tk.IngestToken, "application/json", otlpRequests(1, 0)); resp.StatusCode != 200 {
		t.Fatalf("ingest token after restart: %d", resp.StatusCode)
	}
	// Limits survive too.
	for i := 0; i < 3; i++ {
		call(t, "PUT", fmt.Sprintf("%s/api/rules/r%d", ts2.URL, i), tk.APIToken, "when: count > 0\nwindow: 1m\n")
	}
	if code, _ := call(t, "PUT", ts2.URL+"/api/rules/r9", tk.APIToken, "when: count > 0\nwindow: 1m\n"); code != 403 {
		t.Fatalf("rule limit after restart: %d", code)
	}
}

func TestTokenRotationRevokesOldTokens(t *testing.T) {
	_, ts := newTest(t, "")
	old := createTenant(t, ts.URL, "acme", "")
	code, out := call(t, "PUT", ts.URL+"/api/tenants/acme?rotate=1", "api-secret", "")
	var nw tokens
	json.Unmarshal([]byte(out), &nw)
	if code != 200 || nw.IngestToken == "" || nw.IngestToken == old.IngestToken {
		t.Fatalf("rotate: %d %s", code, out)
	}
	if resp := post(t, ts.URL+"/v1/logs", old.IngestToken, "application/json", otlpRequests(1, 0)); resp.StatusCode != 401 {
		t.Fatalf("old token: %d", resp.StatusCode)
	}
	if resp := post(t, ts.URL+"/v1/logs", nw.IngestToken, "application/json", otlpRequests(1, 0)); resp.StatusCode != 200 {
		t.Fatalf("new token: %d", resp.StatusCode)
	}
	// Updating limits without rotate keeps the tokens and returns none.
	code, out = call(t, "PUT", ts.URL+"/api/tenants/acme", "api-secret", `{"limits":{"max_series":5}}`)
	if code != 200 || strings.Contains(out, "sb_in_") {
		t.Fatalf("update: %d %s", code, out)
	}
	if resp := post(t, ts.URL+"/v1/logs", nw.IngestToken, "application/json", otlpRequests(1, 0)); resp.StatusCode != 200 {
		t.Fatalf("token after a limits update: %d", resp.StatusCode)
	}
}

// By default a tenant's webhook cannot reach the network signalbox runs in.
func TestTenantReceiverCannotReachPrivateAddresses(t *testing.T) {
	s, ts := newTest(t, "")
	runOutbox(t, s)
	tk := createTenant(t, ts.URL, "acme", "")
	h, hookURL := newHook(t) // listens on 127.0.0.1
	call(t, "PUT", ts.URL+"/api/receivers/team", tk.APIToken, `{"type":"webhook","url":"`+hookURL+`"}`)
	if code, _ := call(t, "POST", ts.URL+"/api/test-notify?receiver=team", tk.APIToken, ""); code != 200 {
		t.Fatal("test-notify")
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && s.Outbox.Pending() > 0 {
		time.Sleep(20 * time.Millisecond)
	}
	h.mu.Lock()
	n := len(h.events)
	h.mu.Unlock()
	if n != 0 {
		t.Fatal("a tenant receiver reached a loopback address")
	}
	if st := s.Outbox.Stats()["acme/team"]; st.Dropped != 1 || st.Failed != 0 {
		t.Fatalf("want dropped once without retries, got %+v", st)
	}
}

func TestAdminCanActForATenant(t *testing.T) {
	_, ts := newTest(t, "")
	createTenant(t, ts.URL, "acme", "")
	if code, out := call(t, "PUT", ts.URL+"/api/rules/errs?tenant=acme", "api-secret", "when: count > 0\nwindow: 1m\n"); code != 200 {
		t.Fatalf("%d %s", code, out)
	}
	_, out := call(t, "GET", ts.URL+"/api/rules?tenant=acme", "api-secret", "")
	if !strings.Contains(out, `"tenant": "acme"`) || !strings.Contains(out, `"name": "errs"`) {
		t.Fatal(out)
	}
	if code, _ := call(t, "GET", ts.URL+"/api/rules?tenant=nobody", "api-secret", ""); code != 404 {
		t.Fatalf("unknown tenant: %d", code)
	}
	// A tenant rule cannot take a platform rule's name.
	if code, _ := call(t, "PUT", ts.URL+"/api/rules/api-5xx?tenant=acme", "api-secret", "when: count > 0\nwindow: 1m\n"); code != 409 {
		t.Fatalf("shadowing a platform rule: %d", code)
	}
}

func contextWithCancel() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}

// Limits updates, token rotation, rule changes, ingest and saves at once:
// run under -race, this checks every shared map has one owner lock.
func TestConcurrentTenantWrites(t *testing.T) {
	s, ts := newTest(t, "")
	tk := createTenant(t, ts.URL, "acme", "")
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				switch w {
				case 0:
					call(t, "PUT", ts.URL+"/api/tenants/acme", "api-secret", fmt.Sprintf(`{"limits":{"max_rules":%d}}`, 100+i))
				case 1:
					call(t, "PUT", fmt.Sprintf("%s/api/rules/r%d?tenant=acme", ts.URL, i), "api-secret", "when: count > 0\nwindow: 1m\n")
				case 2:
					post(t, ts.URL+"/v1/logs", tk.IngestToken, "application/json", otlpRequests(5, 1)).Body.Close()
				case 3:
					s.SaveState()
					call(t, "PUT", ts.URL+"/api/tenants/acme?rotate=1", "api-secret", "")
				}
			}
		}(w)
	}
	wg.Wait()
	if _, out := call(t, "GET", ts.URL+"/api/rules?tenant=acme", "api-secret", ""); strings.Count(out, `"scope": "tenant"`) != 20 {
		t.Fatalf("rules: %s", out)
	}
}
