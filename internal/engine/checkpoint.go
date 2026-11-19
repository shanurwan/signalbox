package engine

import (
	"time"

	"github.com/shanurwan/signalbox/internal/model"
)

// SavedSeries is the durable part of a series: its alert state and, for
// absence rules, the fact that the group exists. Window contents are not
// saved; after a restart the engine treats windows as warming (see warming).
type SavedSeries struct {
	Tenant       string            `json:"tenant,omitempty"` // empty: the default tenant
	Rule         string            `json:"rule"`             // rule id: name, or tenant/name for a tenant's own rule
	Key          string            `json:"key"`
	Labels       map[string]string `json:"labels"`
	State        State             `json:"state"`
	ActiveSince  time.Time         `json:"active_since,omitzero"`
	FiringSince  time.Time         `json:"firing_since,omitzero"`
	FalseSince   time.Time         `json:"false_since,omitzero"`
	LastNotified time.Time         `json:"last_notified,omitzero"`
	LastSeen     time.Time         `json:"last_seen,omitzero"`
}

// Snapshot returns every series worth restoring.
func (e *Engine) Snapshot() []SavedSeries {
	var out []SavedSeries
	for i := range e.shards {
		sh := &e.shards[i]
		sh.mu.Lock()
		for _, s := range sh.series {
			if s.state == Inactive && !s.rule.absence {
				continue
			}
			tenant := s.tenant
			if tenant == model.DefaultTenant {
				tenant = ""
			}
			out = append(out, SavedSeries{
				Tenant: tenant, Rule: s.rule.id, Key: s.key, Labels: s.labels, State: s.state,
				ActiveSince: s.activeSince, FiringSince: s.firingSince, FalseSince: s.falseSince,
				LastNotified: s.lastNotified, LastSeen: s.lastSeen,
			})
		}
		sh.mu.Unlock()
	}
	return out
}

// Restore loads saved series. Tenant rules must be set first. Series of
// rules that no longer exist, or whose grouping changed, are dropped. It
// returns how many were restored.
func (e *Engine) Restore(saved []SavedSeries) int {
	rs := e.rules.Load()
	n := 0
	for _, ss := range saved {
		r := rs.byID[ss.Rule]
		if r == nil || !sameKeys(r.GroupBy, ss.Labels) {
			continue
		}
		tenant := ss.Tenant
		if tenant == "" {
			tenant = model.DefaultTenant
		}
		if r.tenant != "" && r.tenant != tenant {
			continue
		}
		s := &series{
			rule: r, tenant: tenant, key: ss.Key, labels: ss.Labels, win: newWindow(r.Window, r.Quantile > 0),
			state: ss.State, activeSince: ss.ActiveSince, firingSince: ss.FiringSince,
			falseSince: ss.FalseSince, lastNotified: ss.LastNotified, lastSeen: ss.LastSeen,
		}
		id := seriesID(r.id, tenant, ss.Key)
		sh := e.shardFor(id)
		sh.mu.Lock()
		if _, dup := sh.series[id]; !dup {
			sh.series[id] = s
			e.usageOf(tenant).series.Add(1)
		}
		sh.mu.Unlock()
		n++
	}
	return n
}

func sameKeys(groupBy []string, labels map[string]string) bool {
	if len(groupBy) != len(labels) {
		return false
	}
	for _, g := range groupBy {
		if _, ok := labels[g]; !ok {
			return false
		}
	}
	return true
}
