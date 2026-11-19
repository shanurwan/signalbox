package server

import (
	"net/http"
	"strconv"
	"time"

	"github.com/shanurwan/signalbox/internal/config"
	"github.com/shanurwan/signalbox/internal/model"
	"github.com/shanurwan/signalbox/internal/sink"
)

// The query endpoints read stored records for dashboards. They always
// filter by the caller's tenant (bound as a parameter, never pasted into
// SQL) and refuse ranges that would make one tenant's request expensive
// for everyone else.

func durationParam(r *http.Request, name string, def, max time.Duration) (time.Duration, bool) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return def, true
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < time.Second || d > max {
		return 0, false
	}
	return d.Truncate(time.Second), true
}

func (s *Server) queryTenant(w http.ResponseWriter, r *http.Request, c caller) (string, bool) {
	if s.Store == nil {
		http.Error(w, "storage is not configured: set clickhouse in the config", http.StatusNotImplemented)
		return "", false
	}
	if c.admin && r.URL.Query().Get("tenant") == "" {
		return model.DefaultTenant, true
	}
	return c.t.ID, true
}

// handleQueryRequests: GET /api/requests?service=&environment=&path=&since=1h&step=1m
func (s *Server) handleQueryRequests(w http.ResponseWriter, r *http.Request, c caller) {
	tenant, ok := s.queryTenant(w, r, c)
	if !ok {
		return
	}
	since, ok1 := durationParam(r, "since", time.Hour, sink.MaxRange)
	step, ok2 := durationParam(r, "step", 0, sink.MaxRange)
	if !ok1 || !ok2 {
		http.Error(w, "since and step are durations such as 1h or 30s, at least 1s, since at most 744h", http.StatusBadRequest)
		return
	}
	if step == 0 {
		step = max(since/120, time.Second).Truncate(time.Second)
	}
	if since/step > sink.MaxPoints {
		http.Error(w, "too many points: make step larger", http.StatusBadRequest)
		return
	}
	q := r.URL.Query()
	rows, err := s.Store.Requests(r.Context(), sink.RequestQuery{Tenant: tenant, Service: q.Get("service"),
		Environment: q.Get("environment"), Path: q.Get("path"), Since: since, Step: step})
	if err != nil {
		s.log.Warn("query requests", "tenant", tenant, "err", err)
		http.Error(w, "storage query failed", http.StatusBadGateway)
		return
	}
	writeJSON(w, map[string]any{"tenant": tenant, "since": since.String(), "step": step.String(), "points": rows})
}

// handleQueryLogs: GET /api/logs?service=&environment=&level=&q=&since=15m&limit=100
func (s *Server) handleQueryLogs(w http.ResponseWriter, r *http.Request, c caller) {
	tenant, ok := s.queryTenant(w, r, c)
	if !ok {
		return
	}
	since, ok := durationParam(r, "since", 15*time.Minute, sink.MaxRange)
	if !ok {
		http.Error(w, "since is a duration such as 15m, at most 744h", http.StatusBadRequest)
		return
	}
	q := r.URL.Query()
	limit := 100
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > sink.MaxLogRows {
			http.Error(w, "limit is 1 to 1000", http.StatusBadRequest)
			return
		}
		limit = n
	}
	var sev uint8
	if lv := q.Get("level"); lv != "" {
		n := config.SeverityNumber(lv)
		if n == 0 {
			http.Error(w, "level is trace, debug, info, warn, error or fatal", http.StatusBadRequest)
			return
		}
		sev = uint8(n)
	}
	text := q.Get("q")
	if len(text) > 256 {
		http.Error(w, "q is at most 256 bytes", http.StatusBadRequest)
		return
	}
	rows, err := s.Store.Logs(r.Context(), sink.LogQuery{Tenant: tenant, Service: q.Get("service"),
		Environment: q.Get("environment"), Text: text, MinSeverity: sev, Since: since, Limit: limit})
	if err != nil {
		s.log.Warn("query logs", "tenant", tenant, "err", err)
		http.Error(w, "storage query failed", http.StatusBadGateway)
		return
	}
	writeJSON(w, map[string]any{"tenant": tenant, "records": rows})
}
