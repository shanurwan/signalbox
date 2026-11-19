// Package server wires ingestion, the engine, notifications, storage and
// the HTTP/gRPC endpoints together.
package server

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/encoding/gzip" // accept gzip-compressed exports
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/shanurwan/signalbox/internal/config"
	"github.com/shanurwan/signalbox/internal/engine"
	"github.com/shanurwan/signalbox/internal/ingest"
	"github.com/shanurwan/signalbox/internal/model"
	"github.com/shanurwan/signalbox/internal/notify"
	"github.com/shanurwan/signalbox/internal/sink"
	"github.com/shanurwan/signalbox/internal/tail"
)

const maxBody = 32 << 20 // decompressed bytes per request

type Server struct {
	cfg     *config.Config
	log     *slog.Logger
	conv    *ingest.Converter
	Engine  *engine.Engine
	Outbox  *notify.Outbox
	Store   *sink.ClickHouse
	Hub     *tail.Hub
	Pipe    *Pipeline
	reg     *prometheus.Registry
	tenants *registry
	started time.Time
	dirty   chan struct{} // a transition or delivery happened: save soon
}

func New(cfg *config.Config, log *slog.Logger) *Server {
	s := &Server{cfg: cfg, log: log, conv: ingest.NewConverter(cfg.Mapping), Hub: tail.NewHub(), started: time.Now(),
		dirty: make(chan struct{}, 1)}
	s.tenants = newRegistry()
	def := newTenant(model.DefaultTenant, config.TenantLimits{}, s.started)
	if cfg.IngestToken != "" {
		def.ingestHash = hashToken(cfg.IngestToken)
	}
	s.tenants.put(def)
	s.Outbox = notify.New(cfg.Receivers, cfg.TenantReceiversAllowPrivate, log)
	s.Outbox.Changed = s.markDirty
	s.Engine = engine.New(cfg.Rules, cfg.MaxLateness, func(ev engine.Event) {
		log.Info("alert", "rule", ev.Rule, "status", ev.Status, "labels", ev.Labels, "value", ev.Value)
		s.Outbox.Enqueue(ev)
		s.markDirty()
	})
	if cfg.ClickHouse != nil && cfg.ClickHouse.URL != "" {
		s.Store = sink.NewClickHouse(*cfg.ClickHouse, log)
	}
	s.reg = prometheus.NewRegistry()
	s.reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	s.Pipe = &Pipeline{engine: s.Engine, hub: s.Hub, store: s.Store, red: newRED(s.reg)}
	if cfg.DedupeWindow > 0 {
		s.Pipe.dedupe = newDedupe(cfg.DedupeWindow, cfg.DedupeMaxEntries)
	}
	s.reg.MustRegister(&selfCollector{s: s})
	return s
}

// ---- checkpoint -------------------------------------------------------

func (s *Server) markDirty() {
	select {
	case s.dirty <- struct{}{}:
	default:
	}
}

type checkpoint struct {
	Version int                  `json:"version"`
	SavedAt time.Time            `json:"saved_at"`
	Tenants []savedTenant        `json:"tenants,omitempty"`
	Series  []engine.SavedSeries `json:"series"`
	Outbox  []notify.Item        `json:"outbox"`
}

func (s *Server) LoadState() error {
	if s.cfg.StateFile == "" {
		return nil
	}
	b, err := os.ReadFile(s.cfg.StateFile)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var cp checkpoint
	if err := json.Unmarshal(b, &cp); err != nil {
		return fmt.Errorf("state file %s: %w", s.cfg.StateFile, err)
	}
	// Tenants first: series and queued notifications refer to their rules
	// and receivers.
	s.restoreTenants(cp.Tenants)
	n := s.Engine.Restore(cp.Series)
	s.Outbox.Restore(cp.Outbox)
	s.log.Info("state restored", "tenants", len(cp.Tenants), "series", n, "outbox", len(cp.Outbox), "saved_at", cp.SavedAt)
	return nil
}

// SaveState writes atomically: a crash mid-write leaves the previous file.
func (s *Server) SaveState() error {
	if s.cfg.StateFile == "" {
		return nil
	}
	b, err := json.Marshal(checkpoint{Version: 2, SavedAt: time.Now(), Tenants: s.snapshotTenants(),
		Series: s.Engine.Snapshot(), Outbox: s.Outbox.Snapshot()})
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.cfg.StateFile)
	f, err := os.CreateTemp(dir, ".state-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if _, err := f.Write(b); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, s.cfg.StateFile)
}

// ---- run loop ---------------------------------------------------------

// Run serves until ctx ends, then stops intake, evaluates once more, saves
// state and flushes storage.
func (s *Server) Run(ctx context.Context) error {
	if err := s.LoadState(); err != nil {
		return err
	}
	bg, stopBG := context.WithCancel(context.Background())
	defer stopBG()
	done := make(chan struct{}, 2)
	go func() { s.Outbox.Run(bg); done <- struct{}{} }()
	if s.Store != nil {
		go func() {
			if err := s.Store.Init(bg); err == nil {
				s.log.Info("clickhouse ready", "table", s.cfg.ClickHouse.Database+"."+s.cfg.ClickHouse.Table)
			}
		}()
		go func() { s.Store.Run(bg); done <- struct{}{} }()
	} else {
		done <- struct{}{}
	}

	httpSrv := &http.Server{Addr: s.cfg.Listen, Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second}
	errc := make(chan error, 2)
	go func() { errc <- httpSrv.ListenAndServe() }()
	s.log.Info("http listening", "addr", s.cfg.Listen)

	var grpcSrv *grpc.Server
	if s.cfg.GRPCListen != "" {
		lis, err := net.Listen("tcp", s.cfg.GRPCListen)
		if err != nil {
			return err
		}
		grpcSrv = grpc.NewServer(grpc.MaxRecvMsgSize(maxBody))
		collogs.RegisterLogsServiceServer(grpcSrv, &grpcLogs{s: s})
		go func() { errc <- grpcSrv.Serve(lis) }()
		s.log.Info("grpc listening", "addr", s.cfg.GRPCListen)
	}

	eval := time.NewTicker(s.cfg.EvalInterval)
	save := time.NewTicker(5 * time.Second)
	defer eval.Stop()
	defer save.Stop()
	for {
		select {
		case now := <-eval.C:
			s.Engine.Evaluate(now)
		case <-save.C:
			if err := s.SaveState(); err != nil {
				s.log.Error("save state", "err", err)
			}
		case <-s.dirty:
			// Alert transitions and deliveries are saved at once: a crash
			// between sending and saving is what would make a restart page twice.
			if err := s.SaveState(); err != nil {
				s.log.Error("save state", "err", err)
			}
		case err := <-errc:
			if !errors.Is(err, http.ErrServerClosed) {
				return err
			}
		case <-ctx.Done():
			s.log.Info("shutting down")
			sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_ = httpSrv.Shutdown(sctx)
			cancel()
			if grpcSrv != nil {
				grpcSrv.GracefulStop()
			}
			s.Engine.Evaluate(time.Now())
			time.Sleep(200 * time.Millisecond) // give the outbox a moment to send what the last evaluation produced
			stopBG()
			<-done
			<-done
			return s.SaveState()
		}
	}
}

// ---- HTTP -------------------------------------------------------------

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	// Ingest: the token picks the tenant.
	mux.HandleFunc("POST /v1/logs", s.ingest(s.handleOTLP))
	mux.HandleFunc("POST /ingest", s.ingest(s.handleNDJSON))
	// Reads and settings: a tenant's API token sees only that tenant.
	mux.HandleFunc("GET /tail", s.api(s.handleTail))
	mux.HandleFunc("GET /api/alerts", s.api(s.handleAlerts))
	mux.HandleFunc("GET /api/rules", s.api(s.handleRules))
	mux.HandleFunc("PUT /api/rules/{name}", s.api(s.handlePutRule))
	mux.HandleFunc("DELETE /api/rules/{name}", s.api(s.handleDeleteRule))
	mux.HandleFunc("GET /api/receivers", s.api(s.handleReceivers))
	mux.HandleFunc("PUT /api/receivers/{name}", s.api(s.handlePutReceiver))
	mux.HandleFunc("DELETE /api/receivers/{name}", s.api(s.handleDeleteReceiver))
	mux.HandleFunc("POST /api/test-notify", s.api(s.handleTestNotify))
	mux.HandleFunc("GET /api/usage", s.api(s.handleUsage))
	mux.HandleFunc("GET /api/requests", s.api(s.handleQueryRequests))
	mux.HandleFunc("GET /api/logs", s.api(s.handleQueryLogs))
	// Operator only: the api_token from the config file.
	mux.HandleFunc("GET /api/tenants", s.adminOnly(s.handleListTenants))
	mux.HandleFunc("PUT /api/tenants/{id}", s.adminOnly(s.handlePutTenant))
	mux.HandleFunc("DELETE /api/tenants/{id}", s.adminOnly(s.handleDeleteTenant))
	mux.Handle("GET /metrics", s.adminOnly(promhttp.HandlerFor(s.reg, promhttp.HandlerOpts{}).ServeHTTP))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "ok\n") })
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, "signalbox: POST /v1/logs (OTLP), POST /ingest (NDJSON), GET /tail, GET /api/alerts, GET /metrics\n")
	})
	return mux
}

// ingest authenticates a sender and hands its tenant to the handler.
func (s *Server) ingest(h func(http.ResponseWriter, *http.Request, *tenant)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		t := s.ingestTenant(bearer(r))
		if t == nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		h(w, r, t)
	}
}

// admit applies the tenant's rate limit. Over it, the sender gets 429 with
// Retry-After, which OTLP exporters and log forwarders treat as "retry
// later": back-pressure rather than silent loss.
func (s *Server) admit(w http.ResponseWriter, t *tenant, n int) bool {
	if t.allow(n, time.Now()) {
		return true
	}
	w.Header().Set("Retry-After", "1")
	http.Error(w, "rate limit for this tenant exceeded, retry later", http.StatusTooManyRequests)
	return false
}

func body(r *http.Request) (io.Reader, error) {
	var rd io.Reader = http.MaxBytesReader(nil, r.Body, maxBody)
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		zr, err := gzip.NewReader(rd)
		if err != nil {
			return nil, err
		}
		rd = zr
	}
	return io.LimitReader(rd, maxBody+1), nil
}

func (s *Server) handleOTLP(w http.ResponseWriter, r *http.Request, t *tenant) {
	rd, err := body(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	raw, err := io.ReadAll(rd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(raw) > maxBody {
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}
	req := &collogs.ExportLogsServiceRequest{}
	isJSON := strings.HasPrefix(r.Header.Get("Content-Type"), "application/json")
	if isJSON {
		err = protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(raw, req)
	} else {
		err = proto.Unmarshal(raw, req)
	}
	if err != nil {
		http.Error(w, "bad OTLP payload: "+err.Error(), http.StatusBadRequest)
		return
	}
	recs := s.conv.FromOTLP(req, time.Now())
	if !s.admit(w, t, len(recs)) {
		return
	}
	s.Pipe.Submit(t.ID, recs)
	resp := &collogs.ExportLogsServiceResponse{}
	if isJSON {
		w.Header().Set("Content-Type", "application/json")
		b, _ := protojson.Marshal(resp)
		w.Write(b)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	b, _ := proto.Marshal(resp)
	w.Write(b)
}

// handleNDJSON accepts one JSON object per line, or one JSON array.
func (s *Server) handleNDJSON(w http.ResponseWriter, r *http.Request, t *tenant) {
	rd, err := body(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	br := bufio.NewReaderSize(rd, 64<<10)
	now := time.Now()
	var recs []model.Record
	first, _ := br.Peek(1)
	if len(first) == 1 && first[0] == '[' {
		var arr []map[string]any
		dec := json.NewDecoder(br)
		dec.UseNumber()
		if err := dec.Decode(&arr); err != nil {
			http.Error(w, "bad JSON array: "+err.Error(), http.StatusBadRequest)
			return
		}
		for _, o := range arr {
			recs = append(recs, s.conv.FromJSON(o, now))
		}
	} else {
		dec := json.NewDecoder(br)
		dec.UseNumber()
		for line := 1; ; line++ {
			var o map[string]any
			if err := dec.Decode(&o); err == io.EOF {
				break
			} else if err != nil {
				http.Error(w, fmt.Sprintf("bad JSON on record %d: %v", line, err), http.StatusBadRequest)
				return
			}
			recs = append(recs, s.conv.FromJSON(o, now))
		}
	}
	if !s.admit(w, t, len(recs)) {
		return
	}
	s.Pipe.Submit(t.ID, recs)
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "{\"accepted\":%d}\n", len(recs))
}

func (s *Server) handleTail(w http.ResponseWriter, r *http.Request, c caller) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	q := r.URL.Query()
	f := tail.Filter{Tenant: c.scope(r), Service: q.Get("service"), Environment: q.Get("environment"), HTTPOnly: q.Get("http") == "1"}
	if lv := q.Get("level"); lv != "" {
		f.MinSeverity = model.Severity(config.SeverityNumber(lv))
	}
	if expr := q.Get("q"); expr != "" {
		re, err := regexp.Compile(expr)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.Body = re
	}
	sub := s.Hub.Subscribe(f, 4096)
	defer s.Hub.Unsubscribe(sub)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	io.WriteString(w, ": connected\n\n")
	fl.Flush()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	enc := json.NewEncoder(w)
	for {
		select {
		case <-r.Context().Done():
			return
		case rec := <-sub.C:
			io.WriteString(w, "data: ")
			enc.Encode(tailView(rec)) // Encode ends with \n
			io.WriteString(w, "\n")
			// drain what is already queued before flushing, to batch writes
		batch:
			for i := 0; i < 256; i++ {
				select {
				case rec = <-sub.C:
					io.WriteString(w, "data: ")
					enc.Encode(tailView(rec))
					io.WriteString(w, "\n")
				default:
					break batch
				}
			}
			fl.Flush()
		case <-tick.C:
			if n := sub.TakeDropped(); n > 0 {
				fmt.Fprintf(w, "event: dropped\ndata: {\"dropped\":%d}\n\n", n)
			} else {
				io.WriteString(w, ": keepalive\n\n")
			}
			fl.Flush()
		}
	}
}

type tailRecord struct {
	Time        time.Time         `json:"time"`
	Service     string            `json:"service,omitempty"`
	Environment string            `json:"environment,omitempty"`
	Level       string            `json:"level"`
	Body        string            `json:"body"`
	Status      int               `json:"status,omitempty"`
	Method      string            `json:"method,omitempty"`
	Path        string            `json:"path,omitempty"`
	DurationMs  *float64          `json:"duration_ms,omitempty"`
	Attrs       map[string]string `json:"attrs,omitempty"`
}

func tailView(r model.Record) tailRecord {
	t := tailRecord{Time: r.Time, Service: r.Service, Environment: r.Environment, Level: r.Severity.String(), Body: r.Body, Attrs: r.Attrs}
	if h := r.HTTP; h != nil {
		t.Status, t.Method, t.Path = h.Status, h.Method, h.Path
		if h.DurationMs >= 0 {
			d := h.DurationMs
			t.DurationMs = &d
		}
	}
	return t
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

func (s *Server) handleAlerts(w http.ResponseWriter, r *http.Request, c caller) {
	alerts := s.Engine.Alerts(c.scope(r))
	if alerts == nil {
		alerts = []engine.Alert{}
	}
	out := map[string]any{"alerts": alerts}
	if c.admin {
		out["notifications_pending"] = s.Outbox.Pending()
	}
	writeJSON(w, out)
}

func (s *Server) handleTestNotify(w http.ResponseWriter, r *http.Request, c caller) {
	name := r.URL.Query().Get("receiver")
	if !s.Outbox.Has(c.t.ID, name) {
		http.Error(w, "unknown receiver", http.StatusNotFound)
		return
	}
	now := time.Now()
	s.Outbox.Enqueue(engine.Event{
		Tenant: c.t.ID, Rule: "signalbox-test", Status: "firing", Severity: "info",
		Summary: "Test notification from signalbox. If you can read this, the receiver works.",
		Labels:  map[string]string{"receiver": name}, Metric: "count", Op: ">", Threshold: 0, Value: 1,
		Window: "1m0s", StartsAt: now, Fingerprint: fmt.Sprintf("test-%d", now.UnixNano()), Receivers: []string{name},
	})
	writeJSON(w, map[string]string{"queued": name})
}

// ---- gRPC -------------------------------------------------------------

type grpcLogs struct {
	collogs.UnimplementedLogsServiceServer
	s *Server
}

func (g *grpcLogs) Export(ctx context.Context, req *collogs.ExportLogsServiceRequest) (*collogs.ExportLogsServiceResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	tok := ""
	if v := md.Get("authorization"); len(v) > 0 {
		var ok bool
		if tok, ok = strings.CutPrefix(v[0], "Bearer "); !ok {
			tok = "\x00invalid"
		}
	}
	t := g.s.ingestTenant(tok)
	if t == nil {
		return nil, status.Error(codes.Unauthenticated, "bad or missing bearer token")
	}
	recs := g.s.conv.FromOTLP(req, time.Now())
	if !t.allow(len(recs), time.Now()) {
		// With RetryInfo, OTLP exporters retry after the delay instead of
		// dropping the batch.
		st, _ := status.New(codes.ResourceExhausted, "rate limit for this tenant exceeded").
			WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(time.Second)})
		return nil, st.Err()
	}
	g.s.Pipe.Submit(t.ID, recs)
	return &collogs.ExportLogsServiceResponse{}, nil
}

// ---- self metrics -----------------------------------------------------

type selfCollector struct{ s *Server }

var (
	dRecords = prometheus.NewDesc("signalbox_ingested_records_total", "Records accepted into the pipeline.", nil, nil)
	dDup     = prometheus.NewDesc("signalbox_duplicate_records_total", "Request logs dropped as duplicates by request ID.", nil, nil)
	dLate    = prometheus.NewDesc("signalbox_late_records_total", "Matched records too old for their rule's window.", nil, nil)
	dSeries  = prometheus.NewDesc("signalbox_alert_series", "Alert series held in memory, by state.", []string{"state"}, nil)
	dNotify  = prometheus.NewDesc("signalbox_notifications_total", "Notification attempts, by receiver and result.", []string{"receiver", "result"}, nil)
	dOutbox  = prometheus.NewDesc("signalbox_outbox_pending", "Notifications waiting to be delivered.", nil, nil)
	dTail    = prometheus.NewDesc("signalbox_tail_subscribers", "Open live-tail connections.", nil, nil)
	dStored  = prometheus.NewDesc("signalbox_storage_rows_total", "Rows sent to storage, by result.", []string{"result"}, nil)
	dQueue   = prometheus.NewDesc("signalbox_storage_queue_rows", "Rows waiting to be written to storage.", nil, nil)
	dFlush   = prometheus.NewDesc("signalbox_storage_last_flush_seconds", "Duration of the last successful storage write.", nil, nil)
	dTenants = prometheus.NewDesc("signalbox_tenants", "Tenants, the default one included.", nil, nil)
	dReject  = prometheus.NewDesc("signalbox_tenant_rejected_records_total", "Records refused by a tenant's rate limit (tenants with any).", []string{"tenant"}, nil)
	dLimited = prometheus.NewDesc("signalbox_tenant_series_limited_total", "Records not evaluated because the tenant's series limit was reached (tenants with any).", []string{"tenant"}, nil)
)

func (c *selfCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{dRecords, dDup, dLate, dSeries, dNotify, dOutbox, dTail, dStored, dQueue, dFlush, dTenants, dReject, dLimited} {
		ch <- d
	}
}

func (c *selfCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.s
	ch <- prometheus.MustNewConstMetric(dRecords, prometheus.CounterValue, float64(s.Pipe.Records.Load()))
	ch <- prometheus.MustNewConstMetric(dDup, prometheus.CounterValue, float64(s.Pipe.Duplicates.Load()))
	ch <- prometheus.MustNewConstMetric(dLate, prometheus.CounterValue, float64(s.Engine.Late.Load()))
	total, pending, firing := s.Engine.Counts()
	ch <- prometheus.MustNewConstMetric(dSeries, prometheus.GaugeValue, float64(total-pending-firing), "inactive")
	ch <- prometheus.MustNewConstMetric(dSeries, prometheus.GaugeValue, float64(pending), "pending")
	ch <- prometheus.MustNewConstMetric(dSeries, prometheus.GaugeValue, float64(firing), "firing")
	// Tenants' receivers are summed into one label value: one series per
	// tenant receiver would grow without bound.
	stats := map[string]notify.Stats{}
	for name, st := range s.Outbox.Stats() {
		if strings.Contains(name, "/") {
			name = "_tenants"
		}
		a := stats[name]
		a.Sent, a.Failed, a.Dropped = a.Sent+st.Sent, a.Failed+st.Failed, a.Dropped+st.Dropped
		stats[name] = a
	}
	for name, st := range stats {
		ch <- prometheus.MustNewConstMetric(dNotify, prometheus.CounterValue, float64(st.Sent), name, "sent")
		ch <- prometheus.MustNewConstMetric(dNotify, prometheus.CounterValue, float64(st.Failed), name, "retry")
		ch <- prometheus.MustNewConstMetric(dNotify, prometheus.CounterValue, float64(st.Dropped), name, "dropped")
	}
	all := s.tenants.all()
	ch <- prometheus.MustNewConstMetric(dTenants, prometheus.GaugeValue, float64(len(all)))
	for _, t := range all {
		if n := t.rejected.Load(); n > 0 {
			ch <- prometheus.MustNewConstMetric(dReject, prometheus.CounterValue, float64(n), t.ID)
		}
		if n := s.Engine.Usage(t.ID).SeriesLimited; n > 0 {
			ch <- prometheus.MustNewConstMetric(dLimited, prometheus.CounterValue, float64(n), t.ID)
		}
	}
	ch <- prometheus.MustNewConstMetric(dOutbox, prometheus.GaugeValue, float64(s.Outbox.Pending()))
	ch <- prometheus.MustNewConstMetric(dTail, prometheus.GaugeValue, float64(s.Hub.Subscribers()))
	if st := s.Store; st != nil {
		ch <- prometheus.MustNewConstMetric(dStored, prometheus.CounterValue, float64(st.Written.Load()), "written")
		ch <- prometheus.MustNewConstMetric(dStored, prometheus.CounterValue, float64(st.Dropped.Load()), "dropped")
		ch <- prometheus.MustNewConstMetric(dQueue, prometheus.GaugeValue, float64(st.Queued()))
		ch <- prometheus.MustNewConstMetric(dFlush, prometheus.GaugeValue, float64(st.LastFlushMs.Load())/1000)
	}
}
