package engine

import (
	"strings"
	"testing"
	"time"

	"github.com/shanurwan/signalbox/internal/config"
	"github.com/shanurwan/signalbox/internal/model"
)

func tenantLogs(tenant, svc string, n int, at time.Time) []model.Record {
	out := make([]model.Record, n)
	for i := range out {
		out[i] = errLog(svc, at)
		out[i].Tenant = tenant
	}
	return out
}

func mustRule(t *testing.T, yaml string) config.Rule {
	t.Helper()
	r, err := config.ParseRule([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

const platformErrors = `
rules:
  - name: errors
    match: {min_severity: error}
    when: count > 3
    window: 1m
`

// The same service name in two tenants is two series: one tenant's errors
// never count toward, or fire for, another.
func TestTenantsAreIsolated(t *testing.T) {
	e, rec := newTestEngine(t, platformErrors)
	batch := append(tenantLogs("acme", "api", 5, t0), tenantLogs("globex", "api", 2, t0)...)
	e.Observe(batch, t0)
	e.Observe(tenantLogs("", "api", 2, t0), t0) // the default tenant
	e.Evaluate(t0)
	if len(rec.events) != 1 || rec.events[0].Tenant != "acme" || rec.events[0].Status != "firing" {
		t.Fatalf("want one firing for acme, got %+v", rec.events)
	}
	if got := e.Alerts("globex"); len(got) != 0 {
		t.Fatalf("globex sees %+v", got)
	}
	if got := e.Alerts("acme"); len(got) != 1 || got[0].Tenant != "acme" {
		t.Fatalf("acme alerts %+v", got)
	}

	// Fingerprints differ per tenant (PagerDuty would otherwise merge two
	// tenants' incidents), and the default tenant's are what they were
	// before tenants existed.
	e.Observe(tenantLogs("", "api", 5, t0), t0)
	e.Evaluate(t0)
	if len(rec.events) != 2 || rec.events[1].Tenant != model.DefaultTenant {
		t.Fatalf("default tenant should fire too: %+v", rec.events)
	}
	key := "service=api\x00environment=production\x00"
	if rec.events[1].Fingerprint != fingerprint("errors", key) {
		t.Fatal("default tenant fingerprint changed")
	}
	if rec.events[0].Fingerprint == rec.events[1].Fingerprint {
		t.Fatal("two tenants share a fingerprint")
	}
}

func TestTenantRuleAppliesOnlyToItsTenant(t *testing.T) {
	e, rec := newTestEngine(t, "rules: []")
	e.SetTenantRule("acme", mustRule(t, "name: crash\nmatch: {body: 'panic:'}\nwhen: count > 0\nwindow: 1m\n"), time.Time{})
	recs := []model.Record{
		{Tenant: "globex", Time: t0, Service: "w", Body: "panic: nil map"},
		{Tenant: "acme", Time: t0, Service: "w", Body: "panic: nil map"},
	}
	e.Observe(recs, t0)
	e.Evaluate(t0)
	if len(rec.events) != 1 || rec.events[0].Tenant != "acme" || rec.events[0].Rule != "crash" {
		t.Fatalf("%+v", rec.events)
	}
	if u := e.Usage("globex"); u.Series != 0 {
		t.Fatalf("globex holds %d series of acme's rule", u.Series)
	}
}

// Changing a rule while it fires resolves the open alert, so nothing stays
// open downstream with no rule left to resolve it.
func TestChangingAFiringRuleResolvesIt(t *testing.T) {
	e, rec := newTestEngine(t, "rules: []")
	spec := "name: errs\nmatch: {min_severity: error}\nwhen: count > 1\nwindow: 1m\n"
	e.SetTenantRule("acme", mustRule(t, spec), time.Time{})
	e.Observe(tenantLogs("acme", "api", 3, t0), t0)
	e.Evaluate(t0)
	e.SetTenantRule("acme", mustRule(t, strings.Replace(spec, "count > 1", "count > 100", 1)), t0)
	e.Evaluate(t0.Add(time.Second))
	if got := rec.statuses(); len(got) != 2 || got[1] != "resolved" || !strings.Contains(rec.events[1].Summary, "changed or deleted") {
		t.Fatalf("%v %+v", got, rec.events)
	}
	if u := e.Usage("acme"); u.Series != 0 || u.Rules != 1 {
		t.Fatalf("usage after change: %+v", u)
	}
	if e.DeleteTenantRule("acme", "errs") != true || e.DeleteTenantRule("acme", "errs") != false {
		t.Fatal("delete should report whether the rule existed")
	}
}

// A rule created at runtime has seen only what arrived since: its window is
// short of history. A "too few" rule must not read that as an outage. Here a
// worker logs 20 lines a minute; "fewer than 50 in 5 minutes" is false for
// it, but one minute after the rule is created the window holds only 20.
func TestNewTooFewRuleWarmsFromCreation(t *testing.T) {
	e, rec := newTestEngine(t, "rules: []")
	e.SetTenantRule("acme", mustRule(t, "name: quiet\nwhen: count < 50\nwindow: 5m\n"), t0)
	for m := 0; m <= 6; m++ {
		at := t0.Add(time.Duration(m) * time.Minute)
		var batch []model.Record
		for i := 0; i < 20; i++ {
			batch = append(batch, model.Record{Tenant: "acme", Time: at, Service: "worker", Severity: model.SevInfo})
		}
		e.Observe(batch, at)
		e.Evaluate(at.Add(30 * time.Second))
	}
	if len(rec.events) != 0 {
		t.Fatalf("a healthy worker paged because the rule was new: %+v", rec.events)
	}
	// Once warm, real silence still fires.
	for m := 7; m <= 13; m++ {
		e.Evaluate(t0.Add(time.Duration(m) * time.Minute))
	}
	if len(rec.events) != 1 || rec.events[0].Status != "firing" {
		t.Fatalf("should fire once warm and quiet: %+v", rec.events)
	}
}

func TestSeriesLimitPerTenant(t *testing.T) {
	e, _ := newTestEngine(t, platformErrors)
	e.SetSeriesLimit("acme", 2)
	var batch []model.Record
	for _, svc := range []string{"a", "b", "c", "d", "a"} {
		batch = append(batch, tenantLogs("acme", svc, 1, t0)...)
	}
	batch = append(batch, tenantLogs("globex", "x", 1, t0)...)
	e.Observe(batch, t0)
	u := e.Usage("acme")
	if u.Series != 2 || u.SeriesLimited != 2 {
		t.Fatalf("acme: %+v", u)
	}
	if g := e.Usage("globex"); g.Series != 1 || g.SeriesLimited != 0 {
		t.Fatalf("one tenant's limit must not touch another: %+v", g)
	}
	// Collected series free their place.
	e.Evaluate(t0.Add(5 * time.Minute))
	e.Observe(tenantLogs("acme", "c", 1, t0.Add(5*time.Minute)), t0.Add(5*time.Minute))
	if u := e.Usage("acme"); u.Series != 1 {
		t.Fatalf("after collection: %+v", u)
	}
}

func TestDropTenantForgetsItQuietly(t *testing.T) {
	e, rec := newTestEngine(t, platformErrors)
	e.SetTenantRule("acme", mustRule(t, "name: any\nwhen: count > 0\nwindow: 1m\n"), time.Time{})
	e.Observe(tenantLogs("acme", "api", 5, t0), t0)
	e.Evaluate(t0)
	n := len(rec.events)
	e.DropTenant("acme")
	e.Evaluate(t0.Add(time.Second))
	if len(rec.events) != n {
		t.Fatalf("a deleted tenant has no receivers; nothing should be sent: %+v", rec.events[n:])
	}
	if u := e.Usage("acme"); u.Series != 0 || u.Rules != 0 {
		t.Fatalf("%+v", u)
	}
}

func TestTenantSeriesSurviveRestart(t *testing.T) {
	spec := mustRule(t, "name: errs\nmatch: {min_severity: error}\nwhen: count > 1\nwindow: 1m\n")
	e, _ := newTestEngine(t, platformErrors)
	e.SetTenantRule("acme", spec, time.Time{})
	e.Observe(tenantLogs("acme", "api", 5, t0), t0)
	e.Evaluate(t0)
	saved := e.Snapshot()

	e2, rec2 := newTestEngine(t, platformErrors)
	e2.SetTenantRule("acme", spec, time.Time{})
	if n := e2.Restore(saved); n != 2 {
		t.Fatalf("restored %d series, want 2 (platform and tenant rule)", n)
	}
	got := e2.Alerts("acme")
	if len(got) != 2 || got[0].State != "firing" || got[1].State != "firing" {
		t.Fatalf("%+v", got)
	}
	e2.Observe(tenantLogs("acme", "api", 5, t0.Add(time.Second)), t0.Add(time.Second))
	e2.Evaluate(t0.Add(time.Second))
	if len(rec2.events) != 0 {
		t.Fatalf("restart re-paged: %+v", rec2.events)
	}
	if u := e2.Usage("acme"); u.Series != 2 {
		t.Fatalf("usage not restored: %+v", u)
	}
}
