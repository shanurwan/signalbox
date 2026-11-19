// Package tail fans live records out to subscribers. A slow subscriber
// loses records (and is told how many); it never slows ingestion.
package tail

import (
	"regexp"
	"sync"
	"sync/atomic"

	"github.com/shanurwan/signalbox/internal/model"
)

type Filter struct {
	Service     string
	Environment string
	MinSeverity model.Severity
	Body        *regexp.Regexp
	HTTPOnly    bool
}

func (f *Filter) match(r *model.Record) bool {
	if f.Service != "" && r.Service != f.Service {
		return false
	}
	if f.Environment != "" && r.Environment != f.Environment {
		return false
	}
	if r.Severity < f.MinSeverity {
		return false
	}
	if f.HTTPOnly && r.HTTP == nil {
		return false
	}
	if f.Body != nil && !f.Body.MatchString(r.Body) {
		return false
	}
	return true
}

type Sub struct {
	C       chan model.Record
	filter  Filter
	dropped atomic.Uint64
}

// TakeDropped returns and resets the number of records this subscriber missed.
func (s *Sub) TakeDropped() uint64 { return s.dropped.Swap(0) }

type Hub struct {
	mu     sync.RWMutex
	subs   map[*Sub]struct{}
	active atomic.Int32
}

func NewHub() *Hub { return &Hub{subs: map[*Sub]struct{}{}} }

func (h *Hub) Subscribe(f Filter, buffer int) *Sub {
	s := &Sub{C: make(chan model.Record, buffer), filter: f}
	h.mu.Lock()
	h.subs[s] = struct{}{}
	h.mu.Unlock()
	h.active.Add(1)
	return s
}

func (h *Hub) Unsubscribe(s *Sub) {
	h.mu.Lock()
	delete(h.subs, s)
	h.mu.Unlock()
	h.active.Add(-1)
}

func (h *Hub) Subscribers() int { return int(h.active.Load()) }

// Publish never blocks.
func (h *Hub) Publish(recs []model.Record) {
	if h.active.Load() == 0 {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for s := range h.subs {
		for i := range recs {
			if !s.filter.match(&recs[i]) {
				continue
			}
			select {
			case s.C <- recs[i]:
			default:
				s.dropped.Add(1)
			}
		}
	}
}
