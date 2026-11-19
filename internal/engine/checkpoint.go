package engine

import (
	"time"
)

// SavedSeries is the durable part of a series: its alert state and, for
// absence rules, the fact that the group exists. Window contents are not
// saved; after a restart the engine treats windows as warming (see warming).
type SavedSeries struct {
	Rule         string            `json:"rule"`
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
			out = append(out, SavedSeries{
				Rule: s.rule.Name, Key: s.key, Labels: s.labels, State: s.state,
				ActiveSince: s.activeSince, FiringSince: s.firingSince, FalseSince: s.falseSince,
				LastNotified: s.lastNotified, LastSeen: s.lastSeen,
			})
		}
		sh.mu.Unlock()
	}
	return out
}

// Restore loads saved series. Series of rules that no longer exist, or whose
// grouping changed, are dropped. It returns how many were restored.
func (e *Engine) Restore(saved []SavedSeries) int {
	byName := map[string]*rule{}
	for _, r := range e.rules {
		byName[r.Name] = r
	}
	n := 0
	for _, ss := range saved {
		r := byName[ss.Rule]
		if r == nil || !sameKeys(r.GroupBy, ss.Labels) {
			continue
		}
		s := &series{
			rule: r, key: ss.Key, labels: ss.Labels, win: newWindow(r.Window, r.Quantile > 0),
			state: ss.State, activeSince: ss.ActiveSince, firingSince: ss.FiringSince,
			falseSince: ss.FalseSince, lastNotified: ss.LastNotified, lastSeen: ss.LastSeen,
		}
		sh := e.shardFor(r.idx, ss.Key)
		sh.mu.Lock()
		sh.series[seriesID(r.idx, ss.Key)] = s
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
