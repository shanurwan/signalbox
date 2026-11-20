// Package railway reads a Railway environment's logs straight from
// Railway's GraphQL API: deployment logs for every service, and HTTP request
// logs for every live deployment. No forwarder or sidecar is needed; the
// records enter the same pipeline as OTLP and JSON input.
package railway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/shanurwan/signalbox/internal/model"
)

// Config selects what to read. Exactly one of Token (account or workspace
// token, sent as a bearer token) and ProjectToken (sent as
// Project-Access-Token) is set.
type Config struct {
	Token         string `yaml:"token"`
	ProjectToken  string `yaml:"project_token"`
	EnvironmentID string `yaml:"environment_id"`
	Tenant        string `yaml:"tenant"`
	HTTPLogs      *bool  `yaml:"http_logs"`   // default true
	DeployLogs    *bool  `yaml:"deploy_logs"` // default true
	APIURL        string `yaml:"api_url"`
	StreamURL     string `yaml:"stream_url"`
	// Refresh is how often the list of live deployments is re-read.
	Refresh time.Duration `yaml:"refresh"`
	// Overlap is how far before the newest record seen a resubscription
	// starts. Railway's live streams skip some records (they show up in a
	// fresh subscription, not in the live push), so the source resubscribes
	// every Resync, re-reading the last Overlap, and drops repeats by identity.
	Overlap time.Duration `yaml:"overlap"`
	Resync  time.Duration `yaml:"resync"`
}

func (c *Config) Defaults() {
	if c.APIURL == "" {
		c.APIURL = "https://backboard.railway.app/graphql/v2"
	}
	if c.StreamURL == "" {
		// The log subscriptions live on this endpoint; it is the one the
		// Railway dashboard and Locomotive use.
		c.StreamURL = "wss://backboard.railway.app/graphql/internal"
	}
	if c.Refresh == 0 {
		c.Refresh = 15 * time.Second
	}
	if c.Overlap == 0 {
		c.Overlap = time.Minute
	}
	if c.Resync == 0 {
		c.Resync = 30 * time.Second
	}
	if c.Tenant == "" {
		c.Tenant = model.DefaultTenant
	}
}

func (c *Config) Validate() error {
	if (c.Token == "") == (c.ProjectToken == "") {
		return errors.New("railway source: set exactly one of token and project_token")
	}
	if c.EnvironmentID == "" {
		return errors.New("railway source: environment_id is required (Railway sets RAILWAY_ENVIRONMENT_ID in every service)")
	}
	return nil
}

func on(b *bool) bool { return b == nil || *b }

// Stats are counters for signalbox's own metrics.
type Stats struct {
	HTTPRecords, DeployRecords atomic.Uint64
	Reconnects, Errors         atomic.Uint64
	Repeats                    atomic.Uint64 // records seen before, dropped
	Streams                    atomic.Int64
	// LagMs records how long after its own timestamp each record reached
	// signalbox: Railway's log pipeline latency as seen from outside.
	lagMu sync.Mutex
	lag   []float64
}

func (s *Stats) observeLag(ms float64) {
	s.lagMu.Lock()
	if len(s.lag) < 100000 {
		s.lag = append(s.lag, ms)
	}
	s.lagMu.Unlock()
}

// TakeLag returns and clears the lag samples collected since the last call.
func (s *Stats) TakeLag() []float64 {
	s.lagMu.Lock()
	defer s.lagMu.Unlock()
	out := s.lag
	s.lag = nil
	return out
}

type Source struct {
	cfg    Config
	log    *slog.Logger
	submit func(tenant string, recs []model.Record)
	client *http.Client
	Stats  Stats

	mu       sync.RWMutex
	services map[string]string // service id -> name
	envName  string
}

func New(cfg Config, log *slog.Logger, submit func(string, []model.Record)) *Source {
	cfg.Defaults()
	return &Source{cfg: cfg, log: log.With("source", "railway"), submit: submit,
		client: &http.Client{Timeout: 20 * time.Second}, services: map[string]string{}}
}

func (s *Source) authHeader() http.Header {
	h := http.Header{}
	if s.cfg.ProjectToken != "" {
		h.Set("Project-Access-Token", s.cfg.ProjectToken)
	} else {
		h.Set("Authorization", "Bearer "+s.cfg.Token)
	}
	return h
}

// ---- GraphQL queries -------------------------------------------------------

func (s *Source) query(ctx context.Context, q string, vars map[string]any, out any) error {
	body, _ := json.Marshal(map[string]any{"query": q, "variables": vars})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.APIURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	for k, v := range s.authHeader() {
		req.Header[k] = v
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode != 200 {
		return fmt.Errorf("railway api %s: %s", resp.Status, strings.TrimSpace(string(raw[:min(len(raw), 300)])))
	}
	var env struct {
		Data   json.RawMessage `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return err
	}
	if len(env.Errors) > 0 {
		return fmt.Errorf("railway api: %s", env.Errors[0].Message)
	}
	return json.Unmarshal(env.Data, out)
}

const environmentQuery = `query environment($id: String!) {
  environment(id: $id) {
    name
    projectId
    deployments { edges { node { id serviceId status createdAt } } }
  }
}`

const projectQuery = `query project($id: String!) {
  project(id: $id) { services { edges { node { id name } } } }
}`

type deployment struct {
	ID        string    `json:"id"`
	ServiceID string    `json:"serviceId"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"createdAt"`
}

// liveDeployments returns, per service, the newest deployment that is
// running, and refreshes service names.
func (s *Source) liveDeployments(ctx context.Context) (map[string]deployment, error) {
	var env struct {
		Environment struct {
			Name        string `json:"name"`
			ProjectID   string `json:"projectId"`
			Deployments struct {
				Edges []struct {
					Node deployment `json:"node"`
				} `json:"edges"`
			} `json:"deployments"`
		} `json:"environment"`
	}
	if err := s.query(ctx, environmentQuery, map[string]any{"id": s.cfg.EnvironmentID}, &env); err != nil {
		return nil, err
	}
	var proj struct {
		Project struct {
			Services struct {
				Edges []struct {
					Node struct{ ID, Name string } `json:"node"`
				} `json:"edges"`
			} `json:"services"`
		} `json:"project"`
	}
	if err := s.query(ctx, projectQuery, map[string]any{"id": env.Environment.ProjectID}, &proj); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.envName = env.Environment.Name
	for _, e := range proj.Project.Services.Edges {
		s.services[e.Node.ID] = e.Node.Name
	}
	s.mu.Unlock()
	live := map[string]deployment{}
	for _, e := range env.Environment.Deployments.Edges {
		d := e.Node
		if d.Status != "SUCCESS" && d.Status != "SLEEPING" {
			continue
		}
		if cur, ok := live[d.ServiceID]; !ok || d.CreatedAt.After(cur.CreatedAt) {
			live[d.ServiceID] = d
		}
	}
	return live, nil
}

func (s *Source) names(serviceID string) (string, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	name := s.services[serviceID]
	if name == "" {
		name = serviceID
	}
	return name, s.envName
}

// seen remembers the identities of records delivered, so overlapping
// subscriptions don't deliver a record twice. It is keyed to the records'
// own timestamps, not to when they were seen: a resubscription returns
// records newer than (newest - overlap), so every key in that range must be
// kept, however long the stream has been idle. Owned by one goroutine.
type seen struct {
	overlap time.Duration
	m       map[string]time.Time // key -> record timestamp
}

func newSeen(overlap time.Duration) *seen { return &seen{overlap: overlap, m: map[string]time.Time{}} }

// first reports whether key is new, and remembers it with the record's time.
func (s *seen) first(key string, ts time.Time) bool {
	if _, ok := s.m[key]; ok {
		return false
	}
	s.m[key] = ts
	return true
}

// forget drops keys too old for any resubscription to return again.
func (s *seen) forget(newest time.Time) {
	floor := newest.Add(-s.overlap - time.Minute)
	for k, t := range s.m {
		if t.Before(floor) {
			delete(s.m, k)
		}
	}
}

// ---- run ---------------------------------------------------------------------

// Run streams until ctx ends.
func (s *Source) Run(ctx context.Context) {
	var wg sync.WaitGroup
	if on(s.cfg.DeployLogs) {
		wg.Add(1)
		go func() { defer wg.Done(); s.deployLogs(ctx) }()
	}
	if on(s.cfg.HTTPLogs) {
		wg.Add(1)
		go func() { defer wg.Done(); s.httpLogs(ctx) }()
	}
	wg.Wait()
}

// httpLogs keeps one stream per live deployment, following redeploys.
func (s *Source) httpLogs(ctx context.Context) {
	running := map[string]context.CancelFunc{}
	defer func() {
		for _, cancel := range running {
			cancel()
		}
	}()
	tick := time.NewTicker(s.cfg.Refresh)
	defer tick.Stop()
	for {
		live, err := s.liveDeployments(ctx)
		if err != nil {
			s.Stats.Errors.Add(1)
			s.log.Warn("listing deployments", "err", err)
		} else {
			wanted := map[string]bool{}
			for _, d := range live {
				wanted[d.ID] = true
				if _, ok := running[d.ID]; !ok {
					dctx, cancel := context.WithCancel(ctx)
					running[d.ID] = cancel
					go s.httpStream(dctx, d)
					s.log.Info("streaming HTTP logs", "deployment", d.ID, "service", d.ServiceID)
				}
			}
			for id, cancel := range running {
				if !wanted[id] {
					cancel()
					delete(running, id)
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

const httpLogsSub = `subscription streamHttpLogs($deploymentId: String!, $filter: String, $beforeLimit: Int!, $beforeDate: String) {
  httpLogs(deploymentId: $deploymentId, filter: $filter, beforeLimit: $beforeLimit, beforeDate: $beforeDate) {
    requestId timestamp method path host httpStatus totalDuration upstreamRqDuration edgeRegion
  }
}`

type httpLog struct {
	RequestID          string    `json:"requestId"`
	Timestamp          time.Time `json:"timestamp"`
	Method             string    `json:"method"`
	Path               string    `json:"path"`
	Host               string    `json:"host"`
	HTTPStatus         int       `json:"httpStatus"`
	TotalDuration      float64   `json:"totalDuration"`
	UpstreamRqDuration float64   `json:"upstreamRqDuration"`
	EdgeRegion         string    `json:"edgeRegion"`
}

func (s *Source) httpStream(ctx context.Context, d deployment) {
	// beforeDate is the exclusive lower bound: the stream returns logs newer
	// than it. Each (re)subscription starts Overlap before the newest log seen,
	// because logs do not arrive in timestamp order; request IDs drop repeats.
	newest := time.Now()
	dup := newSeen(s.cfg.Overlap)
	vars := func() map[string]any {
		return map[string]any{"deploymentId": d.ID, "filter": "", "beforeLimit": 5000,
			"beforeDate": newest.Add(-s.cfg.Overlap).UTC().Format(time.RFC3339Nano)}
	}
	s.stream(ctx, "http", httpLogsSub, vars, "httpLogs", func(raw json.RawMessage) {
		var logs []httpLog
		if err := json.Unmarshal(raw, &logs); err != nil {
			s.Stats.Errors.Add(1)
			return
		}
		now := time.Now()
		recs := make([]model.Record, 0, len(logs))
		for _, l := range logs {
			key := l.RequestID
			if key == "" {
				key = l.Timestamp.String() + l.Method + l.Path
			}
			if !dup.first(key, l.Timestamp) {
				s.Stats.Repeats.Add(1)
				continue
			}
			svc, env := s.names(d.ServiceID)
			recs = append(recs, model.Record{
				Time: l.Timestamp, Service: svc, Environment: env, Severity: model.SevInfo, Body: l.Path,
				Attrs: map[string]string{"host": l.Host, "edge_region": l.EdgeRegion, "deployment_id": d.ID},
				HTTP: &model.HTTP{Method: l.Method, Path: l.Path, Status: l.HTTPStatus,
					DurationMs: l.TotalDuration, RequestID: l.RequestID},
			})
			s.Stats.observeLag(float64(now.Sub(l.Timestamp).Milliseconds()))
		}
		for _, l := range logs {
			if l.Timestamp.After(newest) {
				newest = l.Timestamp
			}
		}
		dup.forget(newest)
		if len(recs) > 0 {
			s.Stats.HTTPRecords.Add(uint64(len(recs)))
			s.submit(s.cfg.Tenant, recs)
		}
	})
}

const environmentLogsSub = `subscription streamEnvironmentLogs($environmentId: String!, $filter: String, $beforeLimit: Int!, $beforeDate: String) {
  environmentLogs(environmentId: $environmentId, filter: $filter, beforeLimit: $beforeLimit, beforeDate: $beforeDate) {
    timestamp message severity
    tags { serviceId deploymentId }
    attributes { key value }
  }
}`

type environmentLog struct {
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
	Severity  string    `json:"severity"`
	Tags      struct {
		ServiceID    string `json:"serviceId"`
		DeploymentID string `json:"deploymentId"`
	} `json:"tags"`
	Attributes []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	} `json:"attributes"`
}

func (s *Source) deployLogs(ctx context.Context) {
	// Service names come from the deployment listing; fill them before the
	// first logs arrive, and keep them fresh for new services.
	go func() {
		for {
			if _, err := s.liveDeployments(ctx); err != nil {
				s.log.Warn("listing services", "err", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Minute):
			}
		}
	}()
	newest := time.Now()
	dup := newSeen(s.cfg.Overlap)
	vars := func() map[string]any {
		return map[string]any{"environmentId": s.cfg.EnvironmentID, "filter": "", "beforeLimit": 5000,
			"beforeDate": newest.Add(-s.cfg.Overlap).UTC().Format(time.RFC3339Nano)}
	}
	s.stream(ctx, "deploy", environmentLogsSub, vars, "environmentLogs", func(raw json.RawMessage) {
		var logs []environmentLog
		if err := json.Unmarshal(raw, &logs); err != nil {
			s.Stats.Errors.Add(1)
			return
		}
		now := time.Now()
		recs := make([]model.Record, 0, len(logs))
		for _, l := range logs {
			if l.Tags.ServiceID == "" {
				continue
			}
			// Deploy logs carry no ID; their timestamp has nanosecond
			// precision, so with the deployment and text it identifies a line.
			if !dup.first(l.Tags.DeploymentID+"|"+l.Timestamp.Format(time.RFC3339Nano)+"|"+l.Message, l.Timestamp) {
				s.Stats.Repeats.Add(1)
				continue
			}
			svc, env := s.names(l.Tags.ServiceID)
			attrs := map[string]string{"deployment_id": l.Tags.DeploymentID}
			for _, a := range l.Attributes {
				attrs[a.Key] = unquote(a.Value)
			}
			sev, ok := model.ParseSeverity(strings.ToLower(l.Severity))
			if !ok {
				sev = model.SevInfo
			}
			if lv, ok := model.ParseSeverity(strings.ToLower(attrs["level"])); ok {
				sev = lv
			}
			recs = append(recs, model.Record{Time: l.Timestamp, Service: svc, Environment: env, Severity: sev,
				Body: l.Message, Attrs: attrs})
			s.Stats.observeLag(float64(now.Sub(l.Timestamp).Milliseconds()))
		}
		for _, l := range logs {
			if l.Timestamp.After(newest) {
				newest = l.Timestamp
			}
		}
		dup.forget(newest)
		if len(recs) > 0 {
			s.Stats.DeployRecords.Add(uint64(len(recs)))
			s.submit(s.cfg.Tenant, recs)
		}
	})
}

// Railway returns structured attribute values JSON-encoded ("\"error\"").
func unquote(v string) string {
	if len(v) >= 2 && v[0] == '"' {
		if u, err := strconv.Unquote(v); err == nil {
			return u
		}
	}
	return v
}

// ---- graphql-transport-ws ------------------------------------------------------

type wsMsg struct {
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// stream runs one subscription until ctx ends: it resubscribes on the same
// connection when the server completes the stream, and reconnects with
// jittered backoff when the connection fails.
func (s *Source) stream(ctx context.Context, kind, query string, vars func() map[string]any, field string, handle func(json.RawMessage)) {
	s.Stats.Streams.Add(1)
	defer s.Stats.Streams.Add(-1)
	back := time.Second
	for attempt := 0; ctx.Err() == nil; attempt++ {
		if attempt > 0 {
			s.Stats.Reconnects.Add(1)
			wait := back/2 + time.Duration(rand.Int64N(int64(back)))
			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
			}
			back = min(back*2, 30*time.Second)
		}
		got, err := s.session(ctx, query, vars, field, handle)
		if got {
			back = time.Second
		}
		if err != nil && ctx.Err() == nil {
			s.Stats.Errors.Add(1)
			s.log.Warn("stream ended", "kind", kind, "err", err)
		}
	}
}

func (s *Source) session(ctx context.Context, query string, vars func() map[string]any, field string, handle func(json.RawMessage)) (bool, error) {
	dctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	conn, _, err := websocket.Dial(dctx, s.cfg.StreamURL, &websocket.DialOptions{
		Subprotocols: []string{"graphql-transport-ws"}, HTTPHeader: s.authHeader()})
	cancel()
	if err != nil {
		return false, err
	}
	conn.SetReadLimit(64 << 20)
	defer conn.CloseNow()
	write := func(m wsMsg) error {
		b, _ := json.Marshal(m)
		wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return conn.Write(wctx, websocket.MessageText, b)
	}
	read := func(timeout time.Duration) (wsMsg, error) {
		rctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		_, b, err := conn.Read(rctx)
		if err != nil {
			return wsMsg{}, err
		}
		var m wsMsg
		return m, json.Unmarshal(b, &m)
	}
	if err := write(wsMsg{Type: "connection_init"}); err != nil {
		return false, err
	}
	for {
		m, err := read(15 * time.Second)
		if err != nil {
			return false, fmt.Errorf("waiting for connection_ack: %w", err)
		}
		if m.Type == "connection_ack" {
			break
		}
	}
	// Reads run in their own goroutine: the websocket library closes the
	// connection when a read's context expires, so a timer can't be a read
	// timeout. A quiet stream is normal; Railway's pings keep it alive.
	msgs := make(chan wsMsg, 64)
	readErr := make(chan error, 1)
	rctx, stopReads := context.WithCancel(ctx)
	defer stopReads()
	go func() {
		for {
			_, b, err := conn.Read(rctx)
			if err != nil {
				readErr <- err
				return
			}
			var m wsMsg
			if json.Unmarshal(b, &m) == nil {
				select {
				case msgs <- m:
				case <-rctx.Done():
					return
				}
			}
		}
	}()

	delivered := false
	seq := 0
	subscribe := func() (string, error) {
		seq++
		id := strconv.Itoa(seq)
		payload, _ := json.Marshal(map[string]any{"query": query, "variables": vars()})
		return id, write(wsMsg{ID: id, Type: "subscribe", Payload: payload})
	}
	id, err := subscribe()
	if err != nil {
		return delivered, err
	}
	// Railway's live push skips some records that a fresh subscription
	// returns, so the subscription is replaced every Resync, re-reading the
	// last Overlap; repeats are dropped by identity downstream.
	resync := time.NewTicker(s.cfg.Resync)
	defer resync.Stop()
	for {
		select {
		case <-ctx.Done():
			return delivered, ctx.Err()
		case err := <-readErr:
			return delivered, err
		case <-resync.C:
			if err := write(wsMsg{ID: id, Type: "complete"}); err != nil {
				return delivered, err
			}
			if id, err = subscribe(); err != nil {
				return delivered, err
			}
		case m := <-msgs:
			switch m.Type {
			case "ping":
				if err := write(wsMsg{Type: "pong"}); err != nil {
					return delivered, err
				}
			case "next":
				if m.ID != id {
					continue // late data from a subscription already replaced
				}
				var p struct {
					Data map[string]json.RawMessage `json:"data"`
				}
				if json.Unmarshal(m.Payload, &p) == nil {
					if raw, ok := p.Data[field]; ok {
						delivered = true
						handle(raw)
					}
				}
			case "error":
				if m.ID == id {
					return delivered, fmt.Errorf("subscription error: %s", string(m.Payload))
				}
			case "complete":
				// The server ends streams from time to time; start a new
				// one, from the newest record seen minus the overlap.
				if m.ID == id {
					if id, err = subscribe(); err != nil {
						return delivered, err
					}
				}
			}
		}
	}
}
