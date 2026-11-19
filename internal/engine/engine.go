// Package engine evaluates alert rules over sliding windows of the record
// stream, in memory, as records arrive. Storage is never queried, so an alert
// fires seconds after the evidence lands and keeps working when storage is down.
package engine

import (
	"fmt"
	"hash/fnv"
	"hash/maphash"
	"math"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shanurwan/signalbox/internal/config"
	"github.com/shanurwan/signalbox/internal/model"
)

type State uint8

const (
	Inactive State = iota
	Pending
	Firing
)

func (s State) String() string {
	return [...]string{"inactive", "pending", "firing"}[s]
}

// Event is a notification-worthy transition.
type Event struct {
	Tenant      string            `json:"tenant,omitempty"`
	Rule        string            `json:"rule"`
	Status      string            `json:"status"` // firing | resolved | reminder
	Severity    string            `json:"severity"`
	Summary     string            `json:"summary"`
	Labels      map[string]string `json:"labels"`
	Metric      string            `json:"metric"`
	Op          string            `json:"op"`
	Threshold   float64           `json:"threshold"`
	Value       float64           `json:"value"`
	NoData      bool              `json:"no_data,omitempty"`
	Window      string            `json:"window"`
	StartsAt    time.Time         `json:"starts_at"`
	EndsAt      time.Time         `json:"ends_at,omitzero"`
	Fingerprint string            `json:"fingerprint"`
	Receivers   []string          `json:"-"`
}

type rule struct {
	config.Rule
	// id is unique in the engine: the name for platform rules (from the
	// config file, applied to every tenant), tenant/name for a tenant's own.
	id       string
	tenant   string    // owner of a tenant rule; "" for platform rules
	created  time.Time // zero for rules loaded at start
	retired  atomic.Bool
	services map[string]bool
	envs     map[string]bool
	minSev   model.Severity
	body     *regexp.Regexp
	path     *regexp.Regexp
	absence  bool // op is < or <=: silence is the signal
}

type series struct {
	rule   *rule
	tenant string
	key    string
	labels map[string]string
	win    *window

	state        State
	activeSince  time.Time
	firingSince  time.Time
	falseSince   time.Time
	lastNotified time.Time
	lastSeen     time.Time
	value        float64
}

const nShards = 64

type shard struct {
	mu     sync.Mutex
	series map[string]*series
}

// ruleSet is replaced, never changed, so Observe reads it without a lock.
type ruleSet struct {
	platform []*rule
	tenant   map[string][]*rule
	byID     map[string]*rule
}

// usage counts one tenant's series against its limit.
type usage struct {
	series    atomic.Int64
	maxSeries atomic.Int64 // 0: no limit
	limited   atomic.Uint64
	gone      atomic.Bool
}

func (u *usage) reserve() bool {
	n := u.series.Add(1)
	if m := u.maxSeries.Load(); m > 0 && n > m {
		u.series.Add(-1)
		u.limited.Add(1)
		return false
	}
	return true
}

type Engine struct {
	rules    atomic.Pointer[ruleSet]
	rulesMu  sync.Mutex // serialises writers of rules
	usage    sync.Map   // tenant -> *usage
	shards   [nShards]shard
	seed     maphash.Seed
	started  time.Time
	lateness time.Duration
	emit     func(Event)

	Late    atomic.Uint64 // records too old for their window
	Matched atomic.Uint64
}

func New(rules []config.Rule, maxLateness time.Duration, emit func(Event)) *Engine {
	e := &Engine{seed: maphash.MakeSeed(), started: time.Now(), lateness: maxLateness, emit: emit}
	for i := range e.shards {
		e.shards[i].series = map[string]*series{}
	}
	rs := &ruleSet{tenant: map[string][]*rule{}, byID: map[string]*rule{}}
	for _, cr := range rules {
		r := compile(cr, "", time.Time{})
		rs.platform = append(rs.platform, r)
		rs.byID[r.id] = r
	}
	e.rules.Store(rs)
	return e
}

func compile(cr config.Rule, tenant string, created time.Time) *rule {
	r := &rule{Rule: cr, id: cr.Name, tenant: tenant, created: created}
	if tenant != "" {
		r.id = tenant + "/" + cr.Name
	}
	if len(cr.Match.Service) > 0 {
		r.services = setOf(cr.Match.Service)
	}
	if len(cr.Match.Environment) > 0 {
		r.envs = setOf(cr.Match.Environment)
	}
	r.minSev = model.Severity(config.SeverityNumber(cr.Match.MinSeverity))
	if cr.Match.Body != "" {
		r.body = regexp.MustCompile(cr.Match.Body)
	}
	if cr.Match.Path != "" {
		r.path = regexp.MustCompile(cr.Match.Path)
	}
	r.absence = cr.Op == "<" || cr.Op == "<="
	return r
}

// update copies the rule set, lets fn change the copy, and publishes it.
func (e *Engine) update(fn func(rs *ruleSet)) {
	e.rulesMu.Lock()
	defer e.rulesMu.Unlock()
	old := e.rules.Load()
	rs := &ruleSet{platform: old.platform, tenant: make(map[string][]*rule, len(old.tenant)), byID: make(map[string]*rule, len(old.byID))}
	for k, v := range old.tenant {
		rs.tenant[k] = v
	}
	for k, v := range old.byID {
		rs.byID[k] = v
	}
	fn(rs)
	e.rules.Store(rs)
}

// SetTenantRule adds or replaces one of a tenant's own rules. The rule must
// already be validated (config.ParseRule). Series of a replaced rule are
// closed at the next evaluation: firing ones resolve, then the new rule
// starts from an empty window. A zero created time means the rule is being
// restored at start and warms from process start.
func (e *Engine) SetTenantRule(tenant string, cr config.Rule, created time.Time) {
	r := compile(cr, tenant, created)
	e.update(func(rs *ruleSet) {
		list := make([]*rule, 0, len(rs.tenant[tenant])+1)
		for _, old := range rs.tenant[tenant] {
			if old.Name == cr.Name {
				old.retired.Store(true)
				continue
			}
			list = append(list, old)
		}
		rs.tenant[tenant] = append(list, r)
		rs.byID[r.id] = r
	})
}

// DeleteTenantRule removes a tenant rule; firing series resolve at the next
// evaluation. It reports whether the rule existed.
func (e *Engine) DeleteTenantRule(tenant, name string) bool {
	found := false
	e.update(func(rs *ruleSet) {
		var list []*rule
		for _, old := range rs.tenant[tenant] {
			if old.Name == name {
				old.retired.Store(true)
				delete(rs.byID, old.id)
				found = true
				continue
			}
			list = append(list, old)
		}
		if len(list) == 0 {
			delete(rs.tenant, tenant)
		} else {
			rs.tenant[tenant] = list
		}
	})
	return found
}

// TenantRules returns a tenant's own rules, sorted by name.
func (e *Engine) TenantRules(tenant string) []config.Rule {
	var out []config.Rule
	for _, r := range e.rules.Load().tenant[tenant] {
		out = append(out, r.Rule)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (e *Engine) usageOf(tenant string) *usage {
	if u, ok := e.usage.Load(tenant); ok {
		return u.(*usage)
	}
	u, _ := e.usage.LoadOrStore(tenant, &usage{})
	return u.(*usage)
}

// SetSeriesLimit caps how many series a tenant may hold; 0 is no limit.
// Records that would start a series over the cap are counted, not evaluated.
func (e *Engine) SetSeriesLimit(tenant string, max int) {
	u := e.usageOf(tenant)
	u.maxSeries.Store(int64(max))
	u.gone.Store(false)
}

// DropTenant forgets a tenant: its rules go at once, its series at the next
// evaluation, without notifications (its receivers are gone too).
func (e *Engine) DropTenant(tenant string) {
	e.update(func(rs *ruleSet) {
		for _, r := range rs.tenant[tenant] {
			r.retired.Store(true)
			delete(rs.byID, r.id)
		}
		delete(rs.tenant, tenant)
	})
	e.usageOf(tenant).gone.Store(true)
}

// TenantUsage is what one tenant holds in the engine.
type TenantUsage struct {
	Series        int64  `json:"series"`
	SeriesLimited uint64 `json:"series_limited"`
	Rules         int    `json:"rules"`
}

func (e *Engine) Usage(tenant string) TenantUsage {
	u := e.usageOf(tenant)
	return TenantUsage{Series: u.series.Load(), SeriesLimited: u.limited.Load(), Rules: len(e.rules.Load().tenant[tenant])}
}

func setOf(xs []string) map[string]bool {
	m := make(map[string]bool, len(xs))
	for _, x := range xs {
		m[x] = true
	}
	return m
}

func (r *rule) matches(rec *model.Record) bool {
	if r.Match.HTTPOnly && rec.HTTP == nil {
		return false
	}
	if r.services != nil && !r.services[rec.Service] {
		return false
	}
	if r.envs != nil && !r.envs[rec.Environment] {
		return false
	}
	if r.minSev != 0 && rec.Severity < r.minSev {
		return false
	}
	if r.path != nil && (rec.HTTP == nil || !r.path.MatchString(rec.HTTP.Path)) {
		return false
	}
	if r.body != nil {
		// A request log's body is its path; body patterns are for log lines.
		if rec.HTTP != nil || !r.body.MatchString(rec.Body) {
			return false
		}
	}
	return true
}

// groupValues writes the record's group values into dst and reports
// whether they equal the previous record's (so the key can be reused).
func (r *rule) groupValues(rec *model.Record, dst []string) bool {
	same := true
	for i, g := range r.GroupBy {
		var v string
		switch g {
		case "service":
			v = rec.Service
		case "environment":
			v = rec.Environment
		case "path":
			if rec.HTTP != nil {
				v = rec.HTTP.Path
			}
		case "method":
			if rec.HTTP != nil {
				v = rec.HTTP.Method
			}
		default:
			v = rec.Attrs[g[len("attr."):]]
		}
		if dst[i] != v {
			same = false
			dst[i] = v
		}
	}
	return same
}

func (r *rule) keyOf(vals []string) string {
	var b strings.Builder
	for i, g := range r.GroupBy {
		b.WriteString(g)
		b.WriteByte('=')
		b.WriteString(vals[i])
		b.WriteByte(0)
	}
	return b.String()
}

func (r *rule) labelsOf(vals []string) map[string]string {
	m := make(map[string]string, len(vals))
	for i, g := range r.GroupBy {
		m[g] = vals[i]
	}
	return m
}

func (e *Engine) shardFor(id string) *shard {
	return &e.shards[maphash.String(e.seed, id)%nShards]
}

// Observe feeds a batch into every rule it matches. Safe for concurrent use.
//
// Records in a batch usually come from one source, so consecutive records
// tend to share a tenant and group: rules are looked up once per run of the
// same tenant, the shard lock is taken once per run of the same group, and
// shared counters are updated once per batch. That keeps senders on many
// cores from queueing on the same lock and cache line.
//
// Observe splits the batch into runs of the same tenant. The pipeline knows
// each batch has one tenant (the one whose token sent it) and calls
// ObserveTenant, which skips that scan.
func (e *Engine) Observe(recs []model.Record, now time.Time) {
	for start := 0; start < len(recs); {
		end := start + 1
		for end < len(recs) && recs[end].Tenant == recs[start].Tenant {
			end++
		}
		e.ObserveTenant(recs[start].Tenant, recs[start:end], now)
		start = end
	}
}

// ObserveTenant feeds a batch whose records all belong to tenant.
func (e *Engine) ObserveTenant(tenant string, recs []model.Record, now time.Time) {
	if tenant == "" {
		tenant = model.DefaultTenant
	}
	rs := e.rules.Load()
	u := e.usageOf(tenant)
	var matched, late uint64
	for _, r := range rs.platform {
		m, l := e.observeRule(r, tenant, u, recs, now)
		matched, late = matched+m, late+l
	}
	for _, r := range rs.tenant[tenant] {
		m, l := e.observeRule(r, tenant, u, recs, now)
		matched, late = matched+m, late+l
	}
	if matched > 0 {
		e.Matched.Add(matched)
	}
	if late > 0 {
		e.Late.Add(late)
	}
}

// observeRule returns how many records matched and how many were too late,
// kept in locals rather than incremented through pointers in the hot loop.
func (e *Engine) observeRule(r *rule, tenant string, u *usage, recs []model.Record, now time.Time) (matched, late uint64) {
	floor := now.Add(-r.Window)
	if oldest := now.Add(-e.lateness); oldest.After(floor) {
		floor = oldest
	}
	vals := make([]string, len(r.GroupBy))
	first := true
	var sh *shard
	var cur *series
	for i := range recs {
		rec := &recs[i]
		if !r.matches(rec) {
			continue
		}
		matched++
		t := rec.Time
		if t.After(now) {
			t = now // clock skew at the source: count it now rather than in the future
		}
		if t.Before(floor) {
			late++
			continue
		}
		if !r.groupValues(rec, vals) || first {
			first = false
			if sh != nil {
				sh.mu.Unlock()
			}
			key := r.keyOf(vals)
			id := seriesID(r.id, tenant, key)
			sh = e.shardFor(id)
			sh.mu.Lock()
			cur = sh.series[id]
			if cur == nil && u.reserve() {
				cur = &series{rule: r, tenant: tenant, key: key, labels: r.labelsOf(vals), win: newWindow(r.Window, r.Quantile > 0)}
				sh.series[id] = cur
			}
		}
		if cur == nil {
			continue // over the tenant's series limit, counted by reserve
		}
		status, dur, isHTTP := 0, -1.0, rec.HTTP != nil
		if isHTTP {
			status, dur = rec.HTTP.Status, rec.HTTP.DurationMs
		}
		if !cur.win.add(t.UnixNano(), isHTTP, status, dur) {
			late++
		}
		if rec.Time.After(cur.lastSeen) {
			cur.lastSeen = rec.Time
		}
	}
	if sh != nil {
		sh.mu.Unlock()
	}
	return matched, late
}

func seriesID(ruleID, tenant, key string) string {
	return ruleID + "\x00" + tenant + "\x00" + key
}

// Evaluate runs every series' state machine once.
func (e *Engine) Evaluate(now time.Time) {
	var a agg
	var events []Event
	for i := range e.shards {
		sh := &e.shards[i]
		sh.mu.Lock()
		for id, s := range sh.series {
			u := e.usageOf(s.tenant)
			if gone := u.gone.Load(); gone || s.rule.retired.Load() {
				// The rule was changed or deleted: close what it opened, so
				// no incident stays open downstream with nothing to resolve it.
				if s.state == Firing && !gone {
					ev := e.event(s, "resolved", now)
					ev.EndsAt = now
					ev.Summary = "Closed because the rule was changed or deleted."
					events = append(events, ev)
				}
				delete(sh.series, id)
				u.series.Add(-1)
				continue
			}
			if ev, ok := e.step(s, now, &a); ok {
				events = append(events, ev)
			}
			if e.collectable(s, now) {
				delete(sh.series, id)
				u.series.Add(-1)
			}
		}
		sh.mu.Unlock()
	}
	for _, ev := range events {
		e.emit(ev)
	}
}

// warming is true while the window still holds time from before this
// process started: its sums are an undercount, so "too few" conditions must
// not start and "too many" conditions must not resolve.
//
// A rule created at runtime warms from its creation: its window starts empty.
func (e *Engine) warming(r *rule, now time.Time) bool {
	start := e.started
	if r.created.After(start) {
		start = r.created
	}
	return now.Sub(start) < r.Window
}

func (e *Engine) step(s *series, now time.Time, a *agg) (Event, bool) {
	r := s.rule
	s.win.sum(now.UnixNano(), a)
	v, ok := value(r, a)
	cond := ok && compare(v, r.Op, r.Threshold)
	s.value = v
	warm := e.warming(r, now)
	switch {
	case cond:
		s.falseSince = time.Time{}
		switch s.state {
		case Inactive:
			if r.absence && warm {
				return Event{}, false
			}
			s.state, s.activeSince = Pending, now
			fallthrough
		case Pending:
			if now.Sub(s.activeSince) >= r.For {
				s.state, s.firingSince, s.lastNotified = Firing, now, now
				return e.event(s, "firing", now), true
			}
		case Firing:
			if r.Repeat > 0 && now.Sub(s.lastNotified) >= r.Repeat {
				s.lastNotified = now
				return e.event(s, "reminder", now), true
			}
		}
	default:
		switch s.state {
		case Pending:
			if !r.absence && warm {
				return Event{}, false // partial window: "not enough yet" is not "fine"
			}
			s.state = Inactive
		case Firing:
			if !r.absence && warm {
				return Event{}, false
			}
			if s.falseSince.IsZero() {
				s.falseSince = now
			}
			if now.Sub(s.falseSince) >= r.ResolveAfter {
				ev := e.event(s, "resolved", now)
				ev.EndsAt = now
				s.state, s.falseSince = Inactive, time.Time{}
				return ev, true
			}
		}
	}
	return Event{}, false
}

// value computes the rule's metric. ok is false when there is not enough
// evidence to judge (no requests, or fewer than min_requests).
func value(r *rule, a *agg) (float64, bool) {
	switch r.Metric {
	case "count":
		return float64(a.count), true
	case "rate":
		return float64(a.count) / r.Window.Seconds(), true
	case "error_ratio":
		if a.total == 0 || a.total < uint64(r.MinRequests) {
			return math.NaN(), false
		}
		return float64(a.errors) / float64(a.total), true
	default: // quantiles
		if a.timed == 0 || a.timed < uint64(r.MinRequests) {
			return math.NaN(), false
		}
		return a.quantile(r.Quantile), true
	}
}

func compare(v float64, op string, t float64) bool {
	switch op {
	case ">":
		return v > t
	case ">=":
		return v >= t
	case "<":
		return v < t
	case "<=":
		return v <= t
	}
	return false
}

// collectable drops series that hold nothing worth keeping. Series of
// absence rules are kept for a day after the group was last seen, so a
// service that goes silent is still known about.
func (e *Engine) collectable(s *series, now time.Time) bool {
	if s.state != Inactive {
		return false
	}
	keep := s.rule.Window
	if s.rule.absence {
		keep = 24 * time.Hour
	}
	return now.Sub(s.lastSeen) > keep && now.Sub(s.lastNotified) > keep
}

func (e *Engine) event(s *series, status string, now time.Time) Event {
	r := s.rule
	v, noData := s.value, math.IsNaN(s.value)
	if noData {
		v = 0
	}
	receivers := r.Notify
	if s.tenant != model.DefaultTenant {
		receivers = r.NotifyAll // looked up among the tenant's own receivers
	}
	return Event{
		Tenant:      s.tenant,
		Rule:        r.Name,
		Status:      status,
		Severity:    r.Severity,
		Summary:     r.Summary,
		Labels:      s.labels,
		Metric:      r.Metric,
		Op:          r.Op,
		Threshold:   r.Threshold,
		Value:       v,
		NoData:      noData,
		Window:      r.Window.String(),
		StartsAt:    s.firingSince,
		Fingerprint: fingerprint(s.fingerprintName(), s.key),
		Receivers:   receivers,
	}
}

// fingerprintName is the rule name, prefixed by the tenant for every tenant
// but the default one, whose fingerprints stay what they were before tenants.
func (s *series) fingerprintName() string {
	if s.tenant == model.DefaultTenant {
		return s.rule.Name
	}
	return s.tenant + "/" + s.rule.Name
}

// fingerprint is FNV-1a over rule and group, stable across restarts and
// versions, so a resolve after a restart closes the same incident downstream.
func fingerprint(rule, key string) string {
	h := fnv.New64a()
	h.Write([]byte(rule))
	h.Write([]byte{0})
	h.Write([]byte(key))
	return fmt.Sprintf("%016x", h.Sum64())
}

// Alert is the public view of a non-inactive series.
type Alert struct {
	Tenant      string            `json:"tenant"`
	Rule        string            `json:"rule"`
	State       string            `json:"state"`
	Labels      map[string]string `json:"labels"`
	Value       float64           `json:"value"`
	ActiveSince time.Time         `json:"active_since"`
	FiringSince time.Time         `json:"firing_since,omitzero"`
}

// Alerts lists pending and firing alerts of one tenant, or of all when
// tenant is "".
func (e *Engine) Alerts(tenant string) []Alert {
	var out []Alert
	for i := range e.shards {
		sh := &e.shards[i]
		sh.mu.Lock()
		for _, s := range sh.series {
			if s.state == Inactive || (tenant != "" && s.tenant != tenant) {
				continue
			}
			v := s.value
			if math.IsNaN(v) {
				v = -1
			}
			out = append(out, Alert{Tenant: s.tenant, Rule: s.rule.Name, State: s.state.String(), Labels: s.labels,
				Value: v, ActiveSince: s.activeSince, FiringSince: s.firingSince})
		}
		sh.mu.Unlock()
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Tenant != out[j].Tenant {
			return out[i].Tenant < out[j].Tenant
		}
		if out[i].Rule != out[j].Rule {
			return out[i].Rule < out[j].Rule
		}
		return out[i].ActiveSince.Before(out[j].ActiveSince)
	})
	return out
}

// Counts returns how many series are pending and firing.
func (e *Engine) Counts() (series, pending, firing int) {
	for i := range e.shards {
		sh := &e.shards[i]
		sh.mu.Lock()
		series += len(sh.series)
		for _, s := range sh.series {
			switch s.state {
			case Pending:
				pending++
			case Firing:
				firing++
			}
		}
		sh.mu.Unlock()
	}
	return
}
