package server

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/shanurwan/signalbox/internal/config"
	"github.com/shanurwan/signalbox/internal/engine"
	"github.com/shanurwan/signalbox/internal/model"
)

// A tenant is the unit of isolation: on a platform, one project or team.
// Every record belongs to the tenant whose ingest token delivered it. A
// tenant sees and changes only its own alerts, rules, receivers, live tail
// and stored logs, and its limits keep it from using up what others share.
//
// With no tenants created, everything belongs to the default tenant and
// signalbox behaves as a single-team service configured by its file.
type tenant struct {
	ID      string
	Created time.Time

	mu     sync.Mutex
	limits config.TenantLimits
	// SHA-256 of the tokens; the tokens themselves are never stored.
	// Guarded by the registry's lock, which also guards the token indexes.
	ingestHash string
	apiHash    string
	tokens     float64 // rate-limit balance, in records
	last       time.Time
	rules      map[string]string // name -> spec as the tenant sent it
	receivers  map[string]string

	accepted, rejected atomic.Uint64
}

func newTenant(id string, limits config.TenantLimits, now time.Time) *tenant {
	return &tenant{ID: id, Created: now, limits: limits, tokens: limits.Burst, last: now,
		rules: map[string]string{}, receivers: map[string]string{}}
}

// allow charges n records to the tenant's rate limit. A batch passes while
// the balance is positive and may take it below zero, so a batch larger
// than the burst still gets through and the debt is repaid before the next
// one: the average rate holds without splitting exports.
func (t *tenant) allow(n int, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	rate := t.limits.RecordsPerSecond
	if rate <= 0 {
		t.accepted.Add(uint64(n))
		return true
	}
	t.tokens += now.Sub(t.last).Seconds() * rate
	t.last = now
	if t.tokens > t.limits.Burst {
		t.tokens = t.limits.Burst
	}
	if t.tokens <= 0 {
		t.rejected.Add(uint64(n))
		return false
	}
	t.tokens -= float64(n)
	t.accepted.Add(uint64(n))
	return true
}

func hashToken(tok string) string {
	sum := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(sum[:])
}

func newToken(prefix string) string {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand does not fail on supported platforms
	}
	return prefix + base64.RawURLEncoding.EncodeToString(b)
}

type registry struct {
	mu       sync.RWMutex
	byID     map[string]*tenant
	byIngest map[string]*tenant
	byAPI    map[string]*tenant
}

func newRegistry() *registry {
	return &registry{byID: map[string]*tenant{}, byIngest: map[string]*tenant{}, byAPI: map[string]*tenant{}}
}

func (g *registry) get(id string) *tenant {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.byID[id]
}

// add registers a new tenant; false if one with that id already exists.
func (g *registry) add(t *tenant) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.byID[t.ID] != nil {
		return false
	}
	g.index(t)
	return true
}

// setTokens replaces a tenant's token hashes; the old tokens stop working at
// once. The hashes are guarded by the registry's lock, not the tenant's.
func (g *registry) setTokens(t *tenant, ingestHash, apiHash string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.byIngest, t.ingestHash)
	delete(g.byAPI, t.apiHash)
	t.ingestHash, t.apiHash = ingestHash, apiHash
	g.index(t)
}

func (g *registry) hashes(t *tenant) (string, string) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return t.ingestHash, t.apiHash
}

// put registers a tenant during startup, replacing one with the same id.
func (g *registry) put(t *tenant) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if old := g.byID[t.ID]; old != nil {
		delete(g.byIngest, old.ingestHash)
		delete(g.byAPI, old.apiHash)
	}
	g.index(t)
}

func (g *registry) index(t *tenant) {
	g.byID[t.ID] = t
	if t.ingestHash != "" {
		g.byIngest[t.ingestHash] = t
	}
	if t.apiHash != "" {
		g.byAPI[t.apiHash] = t
	}
}

func (g *registry) remove(id string) *tenant {
	g.mu.Lock()
	defer g.mu.Unlock()
	t := g.byID[id]
	if t != nil {
		delete(g.byID, id)
		delete(g.byIngest, t.ingestHash)
		delete(g.byAPI, t.apiHash)
	}
	return t
}

func (g *registry) all() []*tenant {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]*tenant, 0, len(g.byID))
	for _, t := range g.byID {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// ---- authentication ------------------------------------------------------

func bearer(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		tok, ok := strings.CutPrefix(h, "Bearer ")
		if !ok {
			return "\x00invalid" // present but not a bearer token: never matches, never open
		}
		return tok
	}
	return r.URL.Query().Get("token")
}

// ingestTenant maps an ingest token to its tenant. A lookup by the token's
// SHA-256 does not leak, through timing, how much of a guess was right.
func (s *Server) ingestTenant(tok string) *tenant {
	if tok == "" {
		if s.cfg.IngestToken == "" {
			return s.tenants.get(model.DefaultTenant)
		}
		return nil
	}
	s.tenants.mu.RLock()
	defer s.tenants.mu.RUnlock()
	return s.tenants.byIngest[hashToken(tok)]
}

// caller is who an API request acts as.
type caller struct {
	t     *tenant
	admin bool // the config file's api_token: every tenant, and tenant management
}

// scope is the tenant whose data a read covers: the caller's own, or for
// the admin token, the one named by ?tenant= or every tenant ("").
func (c caller) scope(r *http.Request) string {
	if c.admin && r.URL.Query().Get("tenant") == "" {
		return ""
	}
	return c.t.ID
}

func (s *Server) apiCaller(w http.ResponseWriter, r *http.Request) (caller, bool) {
	tok := bearer(r)
	admin := false
	switch {
	case s.cfg.APIToken == "" && tok == "":
		admin = true
	case tok != "" && s.cfg.APIToken != "":
		want := hashToken(s.cfg.APIToken)
		admin = subtle.ConstantTimeCompare([]byte(hashToken(tok)), []byte(want)) == 1
	}
	if admin {
		id := r.URL.Query().Get("tenant")
		if id == "" {
			id = model.DefaultTenant
		}
		t := s.tenants.get(id)
		if t == nil {
			http.Error(w, "unknown tenant", http.StatusNotFound)
			return caller{}, false
		}
		return caller{t: t, admin: true}, true
	}
	if tok != "" {
		s.tenants.mu.RLock()
		t := s.tenants.byAPI[hashToken(tok)]
		s.tenants.mu.RUnlock()
		if t != nil {
			return caller{t: t}, true
		}
	}
	http.Error(w, "unauthorized", http.StatusUnauthorized)
	return caller{}, false
}

func (s *Server) api(h func(http.ResponseWriter, *http.Request, caller)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if c, ok := s.apiCaller(w, r); ok {
			h(w, r, c)
		}
	}
}

func (s *Server) adminOnly(h http.HandlerFunc) http.HandlerFunc {
	return s.api(func(w http.ResponseWriter, r *http.Request, c caller) {
		if !c.admin {
			http.Error(w, "this needs the admin token", http.StatusForbidden)
			return
		}
		h(w, r)
	})
}

// ---- tenant management (admin) ------------------------------------------

type tenantView struct {
	ID          string              `json:"id"`
	Created     time.Time           `json:"created"`
	Limits      config.TenantLimits `json:"limits"`
	IngestToken string              `json:"ingest_token,omitempty"` // only when just created or rotated
	APIToken    string              `json:"api_token,omitempty"`
	Usage       *usageView          `json:"usage,omitempty"`
}

type usageView struct {
	RecordsAccepted uint64 `json:"records_accepted"`
	RecordsRejected uint64 `json:"records_rejected"`
	engine.TenantUsage
	Receivers int `json:"receivers"`
}

func (s *Server) usageOf(t *tenant) *usageView {
	t.mu.Lock()
	nrecv := len(t.receivers)
	t.mu.Unlock()
	return &usageView{RecordsAccepted: t.accepted.Load(), RecordsRejected: t.rejected.Load(),
		TenantUsage: s.Engine.Usage(t.ID), Receivers: nrecv}
}

func readBody(r *http.Request, limit int64) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r.Body, limit))
}

// handlePutTenant creates a tenant and returns its tokens, which are shown
// once. On an existing tenant it updates the limits; ?rotate=1 issues new
// tokens and the old ones stop working at once.
func (s *Server) handlePutTenant(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !config.ValidName(id) || id == model.DefaultTenant {
		http.Error(w, "tenant id: lower case letters, digits, '_', '.' or '-', and not \"default\"", http.StatusBadRequest)
		return
	}
	var req struct {
		Limits config.TenantLimits `json:"limits"`
	}
	if raw, err := readBody(r, 64<<10); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	} else if len(strings.TrimSpace(string(raw))) > 0 {
		dec := json.NewDecoder(strings.NewReader(string(raw)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			http.Error(w, "bad tenant: "+err.Error(), http.StatusBadRequest)
			return
		}
	}
	limits := req.Limits.Merge(s.cfg.TenantDefaults)
	now := time.Now()
	v := tenantView{ID: id, Limits: limits}
	status := http.StatusOK
	rotate := r.URL.Query().Get("rotate") == "1"
	t := s.tenants.get(id)
	if t == nil {
		t = newTenant(id, limits, now)
		v.IngestToken, v.APIToken = newToken("sb_in_"), newToken("sb_api_")
		t.ingestHash, t.apiHash = hashToken(v.IngestToken), hashToken(v.APIToken)
		s.Engine.SetSeriesLimit(id, limits.MaxSeries)
		if !s.tenants.add(t) {
			http.Error(w, "tenant created concurrently; retry", http.StatusConflict)
			return
		}
		status = http.StatusCreated
	} else {
		t.mu.Lock()
		t.limits = limits
		t.tokens = min(t.tokens, limits.Burst)
		t.mu.Unlock()
		s.Engine.SetSeriesLimit(id, limits.MaxSeries)
		if rotate {
			v.IngestToken, v.APIToken = newToken("sb_in_"), newToken("sb_api_")
			s.tenants.setTokens(t, hashToken(v.IngestToken), hashToken(v.APIToken))
		}
	}
	v.Created = t.Created
	s.markDirty()
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	writeJSON(w, v)
}

func (s *Server) handleDeleteTenant(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == model.DefaultTenant {
		http.Error(w, "the default tenant cannot be deleted", http.StatusBadRequest)
		return
	}
	t := s.tenants.remove(id)
	if t == nil {
		http.Error(w, "unknown tenant", http.StatusNotFound)
		return
	}
	s.Engine.DropTenant(id)
	t.mu.Lock()
	for name := range t.receivers {
		s.Outbox.RemoveReceiver(id, name)
	}
	t.mu.Unlock()
	s.markDirty()
	writeJSON(w, map[string]string{"deleted": id})
}

func (s *Server) handleListTenants(w http.ResponseWriter, _ *http.Request) {
	out := []tenantView{}
	for _, t := range s.tenants.all() {
		t.mu.Lock()
		v := tenantView{ID: t.ID, Created: t.Created, Limits: t.limits}
		t.mu.Unlock()
		v.Usage = s.usageOf(t)
		out = append(out, v)
	}
	writeJSON(w, map[string]any{"tenants": out})
}

func (s *Server) handleUsage(w http.ResponseWriter, _ *http.Request, c caller) {
	c.t.mu.Lock()
	v := tenantView{ID: c.t.ID, Created: c.t.Created, Limits: c.t.limits}
	c.t.mu.Unlock()
	v.Usage = s.usageOf(c.t)
	writeJSON(w, v)
}

// ---- rules and receivers (per tenant) -----------------------------------

type ruleView struct {
	Name     string   `json:"name"`
	Scope    string   `json:"scope"` // platform: from the config file, applies to every tenant
	When     string   `json:"when"`
	Window   string   `json:"window"`
	For      string   `json:"for"`
	GroupBy  []string `json:"group_by"`
	Severity string   `json:"severity"`
	Notify   []string `json:"notify"`
}

func viewRule(r config.Rule, scope string) ruleView {
	return ruleView{r.Name, scope, r.When, r.Window.String(), r.For.String(), r.GroupBy, r.Severity, r.NotifyAll}
}

func (s *Server) handleRules(w http.ResponseWriter, _ *http.Request, c caller) {
	out := []ruleView{}
	for _, r := range s.cfg.Rules {
		out = append(out, viewRule(r, "platform"))
	}
	for _, r := range s.Engine.TenantRules(c.t.ID) {
		out = append(out, viewRule(r, "tenant"))
	}
	writeJSON(w, map[string]any{"tenant": c.t.ID, "rules": out})
}

func (s *Server) isPlatformRule(name string) bool {
	for _, r := range s.cfg.Rules {
		if r.Name == name {
			return true
		}
	}
	return false
}

// handlePutRule creates or replaces one of the tenant's rules. The body is
// the rule as YAML or JSON, in the same form as the config file.
func (s *Server) handlePutRule(w http.ResponseWriter, r *http.Request, c caller) {
	name := r.PathValue("name")
	raw, err := readBody(r, 64<<10)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	spec, err := withName(raw, name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rule, err := config.ParseRule([]byte(spec))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if rule.Name != name {
		http.Error(w, "the name in the body differs from the one in the path", http.StatusBadRequest)
		return
	}
	if s.isPlatformRule(name) {
		http.Error(w, "a platform rule already has this name", http.StatusConflict)
		return
	}
	for _, n := range rule.NotifyAll {
		if !s.Outbox.Has(c.t.ID, n) {
			http.Error(w, "unknown receiver "+n+": create it first with PUT /api/receivers/"+n, http.StatusBadRequest)
			return
		}
	}
	rule.Notify = rule.NotifyAll
	t := c.t
	t.mu.Lock()
	if _, exists := t.rules[name]; !exists && t.limits.MaxRules > 0 && len(t.rules) >= t.limits.MaxRules {
		t.mu.Unlock()
		http.Error(w, "rule limit reached for this tenant", http.StatusForbidden)
		return
	}
	t.rules[name] = spec
	t.mu.Unlock()
	s.Engine.SetTenantRule(t.ID, rule, time.Now())
	s.markDirty()
	writeJSON(w, viewRule(rule, "tenant"))
}

func (s *Server) handleDeleteRule(w http.ResponseWriter, r *http.Request, c caller) {
	name := r.PathValue("name")
	c.t.mu.Lock()
	_, ok := c.t.rules[name]
	delete(c.t.rules, name)
	c.t.mu.Unlock()
	if !ok || !s.Engine.DeleteTenantRule(c.t.ID, name) {
		http.Error(w, "unknown rule", http.StatusNotFound)
		return
	}
	s.markDirty()
	writeJSON(w, map[string]string{"deleted": name})
}

type receiverView struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Host string `json:"host,omitempty"` // URLs carry secrets; only the host is shown
}

func viewReceiver(r config.Receiver) receiverView {
	v := receiverView{Name: r.Name, Type: r.Type}
	if u, err := url.Parse(r.URL); err == nil {
		v.Host = u.Host
	}
	return v
}

func (s *Server) handleReceivers(w http.ResponseWriter, _ *http.Request, c caller) {
	out := []receiverView{}
	if c.t.ID == model.DefaultTenant {
		for _, r := range s.cfg.Receivers {
			out = append(out, viewReceiver(r))
		}
	}
	for _, r := range s.Outbox.Receivers(c.t.ID) {
		out = append(out, viewReceiver(r))
	}
	writeJSON(w, map[string]any{"tenant": c.t.ID, "receivers": out})
}

func (s *Server) isFileReceiver(name string) bool {
	for _, r := range s.cfg.Receivers {
		if r.Name == name {
			return true
		}
	}
	return false
}

func (s *Server) handlePutReceiver(w http.ResponseWriter, r *http.Request, c caller) {
	name := r.PathValue("name")
	raw, err := readBody(r, 16<<10)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	spec, err := withName(raw, name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	rc, err := config.ParseReceiver([]byte(spec))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if rc.Name != name {
		http.Error(w, "the name in the body differs from the one in the path", http.StatusBadRequest)
		return
	}
	if c.t.ID == model.DefaultTenant && s.isFileReceiver(name) {
		http.Error(w, "the config file defines this receiver", http.StatusConflict)
		return
	}
	t := c.t
	t.mu.Lock()
	if _, exists := t.receivers[name]; !exists && t.limits.MaxReceivers > 0 && len(t.receivers) >= t.limits.MaxReceivers {
		t.mu.Unlock()
		http.Error(w, "receiver limit reached for this tenant", http.StatusForbidden)
		return
	}
	t.receivers[name] = spec
	t.mu.Unlock()
	s.Outbox.SetReceiver(t.ID, rc)
	s.markDirty()
	writeJSON(w, viewReceiver(rc))
}

func (s *Server) handleDeleteReceiver(w http.ResponseWriter, r *http.Request, c caller) {
	name := r.PathValue("name")
	for _, rule := range s.Engine.TenantRules(c.t.ID) {
		for _, n := range rule.NotifyAll {
			if n == name {
				http.Error(w, "rule "+rule.Name+" sends to this receiver; change or delete the rule first", http.StatusConflict)
				return
			}
		}
	}
	c.t.mu.Lock()
	_, ok := c.t.receivers[name]
	delete(c.t.receivers, name)
	c.t.mu.Unlock()
	if !ok {
		http.Error(w, "unknown receiver", http.StatusNotFound)
		return
	}
	s.Outbox.RemoveReceiver(c.t.ID, name)
	s.markDirty()
	writeJSON(w, map[string]string{"deleted": name})
}

// withName reads a YAML or JSON object (JSON is YAML) and returns it as
// YAML, with the name from the path filled in when the body has none.
func withName(raw []byte, name string) (string, error) {
	var m map[string]any
	if err := yaml.Unmarshal(raw, &m); err != nil {
		return "", fmt.Errorf("body must be a YAML or JSON object: %w", err)
	}
	if m == nil {
		m = map[string]any{}
	}
	if _, ok := m["name"]; !ok {
		m["name"] = name
	}
	b, err := yaml.Marshal(m)
	return string(b), err
}

// ---- persistence ----------------------------------------------------------

// savedTenant is what the state file keeps of a tenant: token hashes, never
// tokens, and rules and receivers as the tenant sent them, validated again
// on load.
type savedTenant struct {
	ID         string              `json:"id"`
	Created    time.Time           `json:"created"`
	Limits     config.TenantLimits `json:"limits"`
	IngestHash string              `json:"ingest_hash,omitempty"`
	APIHash    string              `json:"api_hash,omitempty"`
	Rules      map[string]string   `json:"rules,omitempty"`
	Receivers  map[string]string   `json:"receivers,omitempty"`
}

func (s *Server) snapshotTenants() []savedTenant {
	var out []savedTenant
	for _, t := range s.tenants.all() {
		ih, ah := s.tenants.hashes(t)
		t.mu.Lock()
		st := savedTenant{ID: t.ID, Created: t.Created, Limits: t.limits, IngestHash: ih, APIHash: ah,
			Rules: map[string]string{}, Receivers: map[string]string{}}
		for k, v := range t.rules {
			st.Rules[k] = v
		}
		for k, v := range t.receivers {
			st.Receivers[k] = v
		}
		t.mu.Unlock()
		if t.ID == model.DefaultTenant {
			st.IngestHash, st.APIHash = "", "" // its tokens come from the config file
			if len(st.Rules) == 0 && len(st.Receivers) == 0 {
				continue
			}
		}
		out = append(out, st)
	}
	return out
}

// restoreTenants loads tenants with their receivers first, then rules
// (which name receivers). A rule or receiver that no longer validates is
// skipped with a warning rather than stopping startup.
func (s *Server) restoreTenants(saved []savedTenant) {
	for _, st := range saved {
		t := s.tenants.get(st.ID)
		if t == nil {
			t = newTenant(st.ID, st.Limits, time.Now())
			t.Created, t.ingestHash, t.apiHash = st.Created, st.IngestHash, st.APIHash
		}
		for name, spec := range st.Receivers {
			rc, err := config.ParseReceiver([]byte(spec))
			if err != nil {
				s.log.Warn("saved receiver skipped", "tenant", st.ID, "receiver", name, "err", err)
				continue
			}
			t.receivers[name] = spec
			s.Outbox.SetReceiver(st.ID, rc)
		}
		for name, spec := range st.Rules {
			rule, err := config.ParseRule([]byte(spec))
			if err != nil {
				s.log.Warn("saved rule skipped", "tenant", st.ID, "rule", name, "err", err)
				continue
			}
			rule.Notify = rule.NotifyAll
			t.rules[name] = spec
			s.Engine.SetTenantRule(st.ID, rule, time.Time{})
		}
		s.tenants.put(t)
		s.Engine.SetSeriesLimit(st.ID, t.limits.MaxSeries)
	}
}
