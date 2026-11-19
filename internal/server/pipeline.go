package server

import (
	"hash/maphash"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/shanurwan/signalbox/internal/engine"
	"github.com/shanurwan/signalbox/internal/model"
	"github.com/shanurwan/signalbox/internal/sink"
	"github.com/shanurwan/signalbox/internal/tail"
)

// Pipeline is the one path every record takes, whatever protocol it came
// in on: dedupe, then the alert engine, live tail, derived metrics and
// storage. Everything in it is non-blocking except the engine, which only
// touches memory.
type Pipeline struct {
	engine *engine.Engine
	hub    *tail.Hub
	store  *sink.ClickHouse // may be nil
	dedupe *dedupe
	red    *redMetrics

	Records    atomic.Uint64
	Duplicates atomic.Uint64
}

// Submit runs one tenant's batch through the pipeline. The tenant comes
// from the token that delivered the batch and overrides anything in it.
func (p *Pipeline) Submit(tenant string, recs []model.Record) {
	now := time.Now()
	for i := range recs {
		recs[i].Tenant = tenant
	}
	if p.dedupe != nil {
		kept := recs[:0]
		for _, r := range recs {
			if r.HTTP != nil && r.HTTP.RequestID != "" && p.dedupe.seen(tenant, r.HTTP.RequestID, now) {
				p.Duplicates.Add(1)
				continue
			}
			kept = append(kept, r)
		}
		recs = kept
	}
	if len(recs) == 0 {
		return
	}
	p.Records.Add(uint64(len(recs)))
	p.engine.ObserveTenant(tenant, recs, now)
	p.hub.Publish(recs)
	p.red.observe(recs)
	if p.store != nil {
		p.store.Offer(recs)
	}
}

// dedupe remembers request IDs, as 64-bit hashes of tenant and ID (so one
// tenant cannot suppress another's records by reusing its request IDs), in
// two generations that
// rotate every window, so an ID is remembered for between one and two
// windows. Each generation is capped (maxPerGen): under a flood it rotates
// early, which shortens the window instead of growing memory.
type dedupe struct {
	window    time.Duration
	seed      maphash.Seed
	maxPerGen int
	shards    [32]struct {
		mu       sync.Mutex
		cur, old map[uint64]struct{}
		rotated  time.Time
	}
}

func newDedupe(window time.Duration, maxEntries int) *dedupe {
	d := &dedupe{window: window, seed: maphash.MakeSeed(), maxPerGen: maxEntries / 32}
	now := time.Now()
	for i := range d.shards {
		d.shards[i].cur = map[uint64]struct{}{}
		d.shards[i].old = map[uint64]struct{}{}
		d.shards[i].rotated = now
	}
	return d
}

func (d *dedupe) seen(tenant, id string, now time.Time) bool {
	var mh maphash.Hash
	mh.SetSeed(d.seed)
	mh.WriteString(tenant)
	mh.WriteByte(0)
	mh.WriteString(id)
	h := mh.Sum64()
	sh := &d.shards[h%uint64(len(d.shards))]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if now.Sub(sh.rotated) >= d.window || len(sh.cur) >= d.maxPerGen {
		sh.old, sh.cur, sh.rotated = sh.cur, make(map[uint64]struct{}, len(sh.cur)), now
	}
	if _, ok := sh.cur[h]; ok {
		return true
	}
	if _, ok := sh.old[h]; ok {
		return true
	}
	sh.cur[h] = struct{}{}
	return false
}

// redMetrics are request rate, errors and duration per service, for
// dashboards and for any Prometheus-compatible store to scrape. Label sets
// are capped so a misconfigured source cannot explode cardinality.
type redMetrics struct {
	records  *prometheus.CounterVec
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec

	mu    sync.Mutex
	known map[[3]string]bool
	limit int
}

func newRED(reg prometheus.Registerer) *redMetrics {
	m := &redMetrics{
		records: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "signalbox_log_records_total", Help: "Log records received, by tenant, service, environment and severity.",
		}, []string{"tenant", "service", "environment", "severity"}),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "signalbox_http_requests_total", Help: "Request logs received, by tenant, service, environment and status class.",
		}, []string{"tenant", "service", "environment", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "signalbox_http_request_duration_seconds", Help: "Request duration reported by request logs.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
		}, []string{"tenant", "service", "environment"}),
		known: map[[3]string]bool{},
		limit: 2000,
	}
	reg.MustRegister(m.records, m.requests, m.duration)
	return m
}

func (m *redMetrics) labels(tenant, svc, env string) (string, string, string) {
	k := [3]string{tenant, svc, env}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.known[k] {
		return tenant, svc, env
	}
	if len(m.known) >= m.limit {
		return "_other", "_other", "_other"
	}
	m.known[k] = true
	return tenant, svc, env
}

func (m *redMetrics) observe(recs []model.Record) {
	type rk struct{ tenant, svc, env, sev string }
	type qk struct{ tenant, svc, env, code string }
	recCount := map[rk]float64{}
	reqCount := map[qk]float64{}
	for i := range recs {
		r := &recs[i]
		tn, svc, env := m.labels(r.Tenant, r.Service, r.Environment)
		recCount[rk{tn, svc, env, r.Severity.String()}]++
		if h := r.HTTP; h != nil {
			reqCount[qk{tn, svc, env, strconv.Itoa(h.Status/100) + "xx"}]++
			if h.DurationMs >= 0 {
				m.duration.WithLabelValues(tn, svc, env).Observe(h.DurationMs / 1000)
			}
		}
	}
	for k, v := range recCount {
		m.records.WithLabelValues(k.tenant, k.svc, k.env, k.sev).Add(v)
	}
	for k, v := range reqCount {
		m.requests.WithLabelValues(k.tenant, k.svc, k.env, k.code).Add(v)
	}
}
