package engine

import (
	"math"
	"math/rand/v2"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shanurwan/signalbox/internal/config"
	"github.com/shanurwan/signalbox/internal/model"
)

var t0 = time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)

func mustRules(t *testing.T, yaml string) []config.Rule {
	t.Helper()
	c, err := config.Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	return c.Rules
}

type recorder struct{ events []Event }

func (r *recorder) emit(e Event) { r.events = append(r.events, e) }
func (r *recorder) statuses() []string {
	var s []string
	for _, e := range r.events {
		s = append(s, e.Status)
	}
	return s
}

func newTestEngine(t *testing.T, yaml string) (*Engine, *recorder) {
	rec := &recorder{}
	e := New(mustRules(t, yaml), 10*time.Minute, rec.emit)
	e.started = t0.Add(-time.Hour) // not warming unless a test says so
	return e, rec
}

func errLog(svc string, at time.Time) model.Record {
	return model.Record{Time: at, Service: svc, Environment: "production", Severity: model.SevError, Body: "boom"}
}

func req(svc string, at time.Time, status int, ms float64) model.Record {
	return model.Record{Time: at, Service: svc, Environment: "production", Severity: model.SevInfo,
		HTTP: &model.HTTP{Status: status, DurationMs: ms, Path: "/"}}
}

func TestCountFiresAfterForAndResolvesAfterHysteresis(t *testing.T) {
	e, rec := newTestEngine(t, `
rules:
  - name: errors
    match: {min_severity: error}
    when: count > 5
    window: 1m
    for: 30s
    resolve_after: 20s
`)
	now := t0
	for i := 0; i < 10; i++ {
		e.Observe([]model.Record{errLog("api", now)}, now)
	}
	e.Evaluate(now) // pending
	if len(rec.events) != 0 {
		t.Fatalf("fired before for elapsed: %v", rec.statuses())
	}
	e.Evaluate(now.Add(29 * time.Second))
	if len(rec.events) != 0 {
		t.Fatalf("fired at 29s: %v", rec.statuses())
	}
	e.Evaluate(now.Add(30 * time.Second))
	if got := rec.statuses(); len(got) != 1 || got[0] != "firing" {
		t.Fatalf("want firing at 30s, got %v", got)
	}
	ev := rec.events[0]
	if ev.Value != 10 || ev.Labels["service"] != "api" || ev.Labels["environment"] != "production" {
		t.Fatalf("event: %+v", ev)
	}
	// The errors age out of the 1m window at t0+61s; resolve needs 20s more of "false".
	e.Evaluate(now.Add(62 * time.Second))
	e.Evaluate(now.Add(75 * time.Second))
	if len(rec.events) != 1 {
		t.Fatalf("resolved before resolve_after: %v", rec.statuses())
	}
	e.Evaluate(now.Add(83 * time.Second))
	if got := rec.statuses(); len(got) != 2 || got[1] != "resolved" {
		t.Fatalf("want resolved, got %v", got)
	}
	if rec.events[1].Fingerprint != ev.Fingerprint {
		t.Fatal("resolve must carry the firing fingerprint")
	}
}

func TestPendingClearsWhenConditionDrops(t *testing.T) {
	e, rec := newTestEngine(t, `
rules:
  - name: errors
    match: {min_severity: error}
    when: count > 1
    window: 10s
    for: 30s
`)
	e.Observe([]model.Record{errLog("api", t0), errLog("api", t0)}, t0)
	e.Evaluate(t0)
	e.Evaluate(t0.Add(15 * time.Second)) // window empty: pending -> inactive
	e.Observe([]model.Record{errLog("api", t0.Add(40*time.Second)), errLog("api", t0.Add(40*time.Second))}, t0.Add(40*time.Second))
	e.Evaluate(t0.Add(40 * time.Second))
	if len(rec.events) != 0 {
		t.Fatalf("a blip that cleared must not fire: %v", rec.statuses())
	}
}

func TestErrorRatioNeedsMinRequests(t *testing.T) {
	e, rec := newTestEngine(t, `
rules:
  - name: api-5xx
    when: error_ratio > 0.05
    min_requests: 100
    window: 5m
`)
	var batch []model.Record
	for i := 0; i < 50; i++ {
		batch = append(batch, req("api", t0, 500, 10)) // 100% errors but only 50 requests
	}
	e.Observe(batch, t0)
	e.Evaluate(t0)
	if len(rec.events) != 0 {
		t.Fatal("fired below min_requests")
	}
	batch = batch[:0]
	for i := 0; i < 100; i++ {
		batch = append(batch, req("api", t0, 200, 10))
	}
	e.Observe(batch, t0)
	e.Evaluate(t0.Add(time.Second))
	if len(rec.events) != 1 || math.Abs(rec.events[0].Value-50.0/150) > 1e-9 {
		t.Fatalf("want firing at 33%%, got %+v", rec.events)
	}
}

func TestLogRulesIgnoreRequestOnlyMetricsAndViceVersa(t *testing.T) {
	e, rec := newTestEngine(t, `
rules:
  - name: slow
    when: p99_ms > 100
    window: 1m
`)
	e.Observe([]model.Record{errLog("api", t0)}, t0) // a plain log line must not count as a request
	e.Evaluate(t0)
	if s, _, _ := e.Counts(); s != 0 || len(rec.events) != 0 {
		t.Fatalf("plain log created a latency series")
	}
}

func TestP99WithinTenPercent(t *testing.T) {
	e, rec := newTestEngine(t, `
rules:
  - name: slow
    when: p99_ms > 0
    window: 1m
`)
	rng := rand.New(rand.NewPCG(1, 2))
	var vals []float64
	var batch []model.Record
	for i := 0; i < 20000; i++ {
		v := math.Exp(rng.NormFloat64()*1.0 + 4) // log-normal, median ~55ms
		vals = append(vals, v)
		batch = append(batch, req("api", t0, 200, v))
	}
	e.Observe(batch, t0)
	e.Evaluate(t0)
	sort.Float64s(vals)
	exact := vals[int(math.Ceil(0.99*float64(len(vals))))-1]
	got := rec.events[0].Value
	if got < exact || got > exact*1.1+1 {
		t.Fatalf("p99 estimate %.1f, exact %.1f: want within +10%%", got, exact)
	}
}

func TestLateAndOutOfOrderRecords(t *testing.T) {
	e, rec := newTestEngine(t, `
rules:
  - name: errors
    match: {min_severity: error}
    when: count >= 3
    window: 1m
`)
	now := t0.Add(5 * time.Minute)
	// Arrive out of order, all inside the window: all count.
	e.Observe([]model.Record{errLog("api", now.Add(-10*time.Second)), errLog("api", now.Add(-50*time.Second)), errLog("api", now.Add(-30*time.Second))}, now)
	// Older than the window: counted as late, not as evidence.
	e.Observe([]model.Record{errLog("api", now.Add(-2*time.Minute))}, now)
	e.Evaluate(now)
	if len(rec.events) != 1 || rec.events[0].Value != 3 {
		t.Fatalf("want firing with 3, got %+v", rec.events)
	}
	if e.Late.Load() != 1 {
		t.Fatalf("late = %d, want 1", e.Late.Load())
	}
}

func TestAbsenceRuleNoFalseAlarmWhileWarming(t *testing.T) {
	e, rec := newTestEngine(t, `
rules:
  - name: worker-silent
    match: {service: [worker]}
    when: count < 1
    window: 2m
    for: 0s
    resolve_after: 0s
`)
	e.Observe([]model.Record{{Time: t0, Service: "worker", Severity: model.SevInfo}}, t0)
	e.Evaluate(t0.Add(time.Minute))
	if len(rec.events) != 0 {
		t.Fatal("fired while the worker was logging")
	}
	e.Evaluate(t0.Add(3 * time.Minute))
	if got := rec.statuses(); len(got) != 1 || got[0] != "firing" {
		t.Fatalf("want firing after 2m of silence, got %v", got)
	}
	at := t0.Add(4 * time.Minute)
	e.Observe([]model.Record{{Time: at, Service: "worker", Severity: model.SevInfo}}, at)
	e.Evaluate(at)
	if got := rec.statuses(); len(got) != 2 || got[1] != "resolved" {
		t.Fatalf("want resolved when logs return, got %v", got)
	}

	// After a restart the window is empty; the worker must not be called silent
	// until a whole window has passed since start.
	saved := e.Snapshot()
	rec2 := &recorder{}
	e2 := New(mustRules(t, `
rules:
  - name: worker-silent
    match: {service: [worker]}
    when: count < 1
    window: 2m
    for: 0s
`), 10*time.Minute, rec2.emit)
	e2.started = at.Add(10 * time.Second)
	if n := e2.Restore(saved); n != 1 {
		t.Fatalf("restored %d series", n)
	}
	e2.Evaluate(at.Add(30 * time.Second))
	if len(rec2.events) != 0 {
		t.Fatalf("false alarm while warming: %v", rec2.statuses())
	}
}

func TestRestartDoesNotRepageOrResolveEarly(t *testing.T) {
	yaml := `
rules:
  - name: errors
    match: {min_severity: error}
    when: count > 5
    window: 5m
    for: 0s
    resolve_after: 0s
    repeat: 4h
`
	e, rec := newTestEngine(t, yaml)
	var batch []model.Record
	for i := 0; i < 10; i++ {
		batch = append(batch, errLog("api", t0))
	}
	e.Observe(batch, t0)
	e.Evaluate(t0)
	if len(rec.events) != 1 {
		t.Fatalf("want 1 firing, got %v", rec.statuses())
	}
	saved := e.Snapshot()

	// New process, one minute later: empty windows.
	rec2 := &recorder{}
	e2 := New(mustRules(t, yaml), 10*time.Minute, rec2.emit)
	start := t0.Add(time.Minute)
	e2.started = start
	e2.Restore(saved)
	for i := 0; i < 4; i++ {
		e2.Evaluate(start.Add(time.Duration(i) * time.Minute))
	}
	if len(rec2.events) != 0 {
		t.Fatalf("restart produced %v; want no re-page and no early resolve", rec2.statuses())
	}
	// Once a full window has been observed with no errors, it resolves.
	e2.Evaluate(start.Add(5 * time.Minute))
	if got := rec2.statuses(); len(got) != 1 || got[0] != "resolved" {
		t.Fatalf("want resolved after warm-up, got %v", got)
	}
	if rec2.events[0].Fingerprint != rec.events[0].Fingerprint {
		t.Fatal("fingerprint changed across restart")
	}
}

func TestReminderRepeats(t *testing.T) {
	e, rec := newTestEngine(t, `
rules:
  - name: errors
    match: {min_severity: error}
    when: count > 0
    window: 10m
    repeat: 2m
`)
	for i := 0; i <= 5; i++ {
		at := t0.Add(time.Duration(i) * time.Minute)
		e.Observe([]model.Record{errLog("api", at)}, at)
		e.Evaluate(at)
	}
	if got := rec.statuses(); len(got) != 3 || got[0] != "firing" || got[1] != "reminder" || got[2] != "reminder" {
		t.Fatalf("got %v", got)
	}
}

func TestGroupByAttribute(t *testing.T) {
	e, rec := newTestEngine(t, `
rules:
  - name: per-tenant
    match: {min_severity: error}
    group_by: [attr.tenant]
    when: count > 1
    window: 1m
`)
	mk := func(tenant string) model.Record {
		r := errLog("api", t0)
		r.Attrs = map[string]string{"tenant": tenant}
		return r
	}
	e.Observe([]model.Record{mk("a"), mk("a"), mk("b")}, t0)
	e.Evaluate(t0)
	if len(rec.events) != 1 || rec.events[0].Labels["attr.tenant"] != "a" {
		t.Fatalf("got %+v", rec.events)
	}
}

func benchEngine() *Engine {
	c, _ := config.Parse([]byte(`
rules:
  - name: errors
    match: {min_severity: error}
    when: count > 50
    window: 5m
  - name: api-5xx
    when: error_ratio > 0.05
    window: 5m
  - name: api-p99
    when: p99_ms > 800
    window: 5m
`))
	return New(c.Rules, 10*time.Minute, func(Event) {})
}

func benchBatch(now time.Time, svc func(i int) string) []model.Record {
	batch := make([]model.Record, 1000)
	for i := range batch {
		batch[i] = model.Record{Time: now, Service: svc(i), Environment: "production", Severity: model.SevInfo,
			HTTP: &model.HTTP{Status: 200 + (i%40)/39*300, DurationMs: float64(i % 700)}}
	}
	return batch
}

// BenchmarkObserve: each batch comes from one service, as an OTLP export
// grouped by resource does. 20 services, 3 rules.
func BenchmarkObserve(b *testing.B) {
	e := benchEngine()
	now := time.Now()
	batches := make([][]model.Record, 20)
	for s := range batches {
		name := "svc" + itoa(s)
		batches[s] = benchBatch(now, func(int) string { return name })
	}
	var n atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			e.Observe(batches[n.Add(1)%20], now)
		}
	})
	b.ReportMetric(float64(b.N)*1000/b.Elapsed().Seconds(), "records/s")
}

// BenchmarkObserveInterleaved is the worst case: every record in a batch
// belongs to a different group than the one before it.
func BenchmarkObserveInterleaved(b *testing.B) {
	e := benchEngine()
	now := time.Now()
	batch := benchBatch(now, func(i int) string { return "svc" + itoa(i%20) })
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			e.Observe(batch, now)
		}
	})
	b.ReportMetric(float64(b.N)*1000/b.Elapsed().Seconds(), "records/s")
}
