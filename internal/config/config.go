// Package config loads and validates signalbox.yaml.
package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/shanurwan/signalbox/internal/source/railway"
)

type Config struct {
	Listen       string        `yaml:"listen"`
	GRPCListen   string        `yaml:"grpc_listen"`
	IngestToken  string        `yaml:"ingest_token"`
	APIToken     string        `yaml:"api_token"`
	StateFile    string        `yaml:"state_file"`
	EvalInterval time.Duration `yaml:"eval_interval"`
	MaxLateness  time.Duration `yaml:"max_lateness"`
	DedupeWindow time.Duration `yaml:"dedupe_window"`
	// DedupeMaxEntries caps remembered request IDs per generation.
	DedupeMaxEntries int `yaml:"dedupe_max_entries"`
	// TenantDefaults are the limits a tenant created through the API gets
	// unless the request sets its own. The default tenant has no limits.
	TenantDefaults TenantLimits `yaml:"tenant_defaults"`
	// TenantReceiversAllowPrivate lets receivers created by tenants point at
	// loopback, private and link-local addresses. Off by default: otherwise
	// any tenant could make signalbox send requests into the network it runs
	// in. Receivers in this file are trusted and never restricted.
	TenantReceiversAllowPrivate bool        `yaml:"tenant_receivers_allow_private"`
	Mapping                     Mapping     `yaml:"mapping"`
	ClickHouse                  *ClickHouse `yaml:"clickhouse"`
	Sources                     Sources     `yaml:"sources"`
	Receivers                   []Receiver  `yaml:"receivers"`
	Rules                       []Rule      `yaml:"rules"`

	// Warnings are problems that do not stop startup.
	Warnings []string `yaml:"-"`
}

// Mapping lists, in order of preference, the attribute keys that carry each
// field. The defaults cover OpenTelemetry semantic conventions and the field
// names common log forwarders use.
type Mapping struct {
	Service        []string `yaml:"service"`
	Environment    []string `yaml:"environment"`
	HTTPStatus     []string `yaml:"http_status"`
	HTTPMethod     []string `yaml:"http_method"`
	HTTPPath       []string `yaml:"http_path"`
	HTTPDurationMs []string `yaml:"http_duration_ms"`
	HTTPDurationS  []string `yaml:"http_duration_s"`
	RequestID      []string `yaml:"request_id"`
}

// Sources are inputs signalbox pulls from, besides what is pushed to it.
type Sources struct {
	Railway *railway.Config `yaml:"railway"`
}

// TenantLimits keep one tenant from using up what others share. Zero means
// no limit.
type TenantLimits struct {
	// RecordsPerSecond is the average ingest rate. A batch is accepted while
	// the tenant's balance is positive, so large batches pass and the
	// average still holds; Burst caps how much unused rate can be saved up.
	RecordsPerSecond float64 `yaml:"records_per_second" json:"records_per_second"`
	Burst            float64 `yaml:"burst" json:"burst"`
	MaxSeries        int     `yaml:"max_series" json:"max_series"`
	MaxRules         int     `yaml:"max_rules" json:"max_rules"`
	MaxReceivers     int     `yaml:"max_receivers" json:"max_receivers"`
}

// Merge fills unset fields of l from d.
func (l TenantLimits) Merge(d TenantLimits) TenantLimits {
	if l.RecordsPerSecond == 0 {
		l.RecordsPerSecond = d.RecordsPerSecond
	}
	if l.Burst == 0 {
		l.Burst = d.Burst
	}
	if l.Burst == 0 {
		l.Burst = 2 * l.RecordsPerSecond
	}
	if l.MaxSeries == 0 {
		l.MaxSeries = d.MaxSeries
	}
	if l.MaxRules == 0 {
		l.MaxRules = d.MaxRules
	}
	if l.MaxReceivers == 0 {
		l.MaxReceivers = d.MaxReceivers
	}
	return l
}

type ClickHouse struct {
	URL           string        `yaml:"url"`
	Database      string        `yaml:"database"`
	Table         string        `yaml:"table"`
	User          string        `yaml:"user"`
	Password      string        `yaml:"password"`
	RetentionDays int           `yaml:"retention_days"`
	BatchRows     int           `yaml:"batch_rows"`
	FlushInterval time.Duration `yaml:"flush_interval"`
	QueueRows     int           `yaml:"queue_rows"`
	Writers       int           `yaml:"writers"`
}

type Receiver struct {
	Name       string            `yaml:"name"`
	Type       string            `yaml:"type"` // slack | discord | webhook | pagerduty
	URL        string            `yaml:"url"`
	RoutingKey string            `yaml:"routing_key"`
	Headers    map[string]string `yaml:"headers"`
}

type Match struct {
	Service     []string `yaml:"service"`
	Environment []string `yaml:"environment"`
	MinSeverity string   `yaml:"min_severity"`
	Body        string   `yaml:"body"` // regular expression
	Path        string   `yaml:"path"` // regular expression, request logs only
	HTTPOnly    bool     `yaml:"http_only"`
}

type Rule struct {
	Name            string         `yaml:"name"`
	Match           Match          `yaml:"match"`
	GroupBy         []string       `yaml:"group_by"`
	When            string         `yaml:"when"`
	Window          time.Duration  `yaml:"window"`
	For             time.Duration  `yaml:"for"`
	ResolveAfterSet *time.Duration `yaml:"resolve_after"`
	RepeatSet       *time.Duration `yaml:"repeat"`
	MinRequests     int            `yaml:"min_requests"`
	Severity        string         `yaml:"severity"`
	Summary         string         `yaml:"summary"`
	Notify          []string       `yaml:"notify"`

	// NotifyAll is Notify as written. Notify keeps only the receivers that
	// are on; a platform rule looks NotifyAll up among each tenant's own
	// receivers.
	NotifyAll []string `yaml:"-"`

	// Filled from the fields above; unset means the default, 0s means zero.
	ResolveAfter time.Duration `yaml:"-"`
	Repeat       time.Duration `yaml:"-"` // 0 disables reminders

	// Parsed from When.
	Metric    string  `yaml:"-"`
	Op        string  `yaml:"-"`
	Threshold float64 `yaml:"-"`
	Quantile  float64 `yaml:"-"`
}

var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// Load reads a YAML file, expands ${VAR} references from the environment,
// fills defaults and validates.
//
// SIGNALBOX_CONFIG_YAML, when set, is used instead of the file, for
// platforms where setting a variable is easier than mounting a file.
func Load(path string) (*Config, error) {
	if inline := os.Getenv("SIGNALBOX_CONFIG_YAML"); inline != "" {
		return Parse([]byte(inline))
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(raw)
}

func Parse(raw []byte) (*Config, error) {
	// Unset variables expand to "", which validation reports where it matters.
	expanded := envRef.ReplaceAllStringFunc(string(raw), func(m string) string {
		return os.Getenv(envRef.FindStringSubmatch(m)[1])
	})
	var c Config
	dec := yaml.NewDecoder(strings.NewReader(expanded))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.defaults()
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) defaults() {
	if c.Listen == "" {
		c.Listen = ":8080"
	}
	if c.EvalInterval == 0 {
		c.EvalInterval = 5 * time.Second
	}
	if c.MaxLateness == 0 {
		c.MaxLateness = 2 * time.Minute
	}
	if c.DedupeWindow == 0 {
		c.DedupeWindow = 2 * time.Minute
	}
	if c.DedupeMaxEntries == 0 {
		c.DedupeMaxEntries = 2_000_000
	}
	m := &c.Mapping
	def := func(dst *[]string, keys ...string) {
		if len(*dst) == 0 {
			*dst = keys
		}
	}
	def(&m.Service, "service.name", "service", "_metadata.service_name")
	def(&m.Environment, "deployment.environment.name", "deployment.environment", "environment", "_metadata.environment_name")
	def(&m.HTTPStatus, "http.response.status_code", "http.status_code", "httpStatus", "status")
	def(&m.HTTPMethod, "http.request.method", "http.method", "method")
	def(&m.HTTPPath, "url.path", "http.target", "path")
	def(&m.HTTPDurationMs, "totalDuration", "duration_ms", "latency_ms")
	def(&m.HTTPDurationS, "http.server.request.duration")
	def(&m.RequestID, "http.request.id", "requestId", "request_id")
	if ch := c.ClickHouse; ch != nil {
		if ch.Database == "" {
			ch.Database = "default"
		}
		if ch.Table == "" {
			ch.Table = "signalbox_logs"
		}
		if ch.RetentionDays == 0 {
			ch.RetentionDays = 30
		}
		if ch.BatchRows == 0 {
			ch.BatchRows = 50000
		}
		if ch.FlushInterval == 0 {
			ch.FlushInterval = time.Second
		}
		if ch.QueueRows == 0 {
			ch.QueueRows = 1000000
		}
		if ch.Writers == 0 {
			ch.Writers = 4
		}
	}
	if d := &c.TenantDefaults; d.Burst == 0 {
		d.Burst = 2 * d.RecordsPerSecond
	}
	for i := range c.Rules {
		c.Rules[i].defaults()
	}
}

func (r *Rule) defaults() {
	r.ResolveAfter = time.Minute
	if r.ResolveAfterSet != nil {
		r.ResolveAfter = *r.ResolveAfterSet
	}
	r.Repeat = 4 * time.Hour
	if r.RepeatSet != nil {
		r.Repeat = *r.RepeatSet
	}
	if len(r.GroupBy) == 0 {
		r.GroupBy = []string{"service", "environment"}
	}
	if r.Severity == "" {
		r.Severity = "warning"
	}
}

var whenRe = regexp.MustCompile(`^\s*([a-z0-9_]+)\s*(>=|<=|>|<)\s*([0-9.eE+-]+)\s*$`)

func (c *Config) validate() error {
	// A receiver whose URL or key is empty (usually an unset environment
	// variable) is switched off with a warning, so one template can offer
	// every receiver type and people fill in only the ones they use.
	recv := map[string]bool{}
	off := map[string]bool{}
	var active []Receiver
	for _, r := range c.Receivers {
		if r.Name == "" {
			return fmt.Errorf("receiver without a name")
		}
		if recv[r.Name] || off[r.Name] {
			return fmt.Errorf("receiver %q defined twice", r.Name)
		}
		if err := r.check(); errors.Is(err, errEmpty) {
			off[r.Name] = true
			c.Warnings = append(c.Warnings, fmt.Sprintf("receiver %q is off: %v", r.Name, err))
			continue
		} else if err != nil {
			return err
		}
		recv[r.Name] = true
		active = append(active, r)
	}
	c.Receivers = active
	if r := c.Sources.Railway; r != nil {
		r.Defaults()
		if err := r.Validate(); err != nil {
			return err
		}
	}
	names := map[string]bool{}
	for i := range c.Rules {
		r := &c.Rules[i]
		if r.Name == "" {
			return fmt.Errorf("rule %d has no name", i)
		}
		if names[r.Name] {
			return fmt.Errorf("rule %q defined twice", r.Name)
		}
		names[r.Name] = true
		if err := r.check(); err != nil {
			return err
		}
		var notify []string
		for _, n := range r.Notify {
			switch {
			case recv[n]:
				notify = append(notify, n)
			case off[n]:
			default:
				// Not an error: with tenants, a platform rule can name a
				// receiver that each tenant defines for itself.
				c.Warnings = append(c.Warnings, fmt.Sprintf("rule %q: receiver %q is not in this file; only tenants "+
					"that create a receiver with that name get these alerts", r.Name, n))
			}
		}
		r.NotifyAll, r.Notify = r.Notify, notify
	}
	if len(c.Rules) > 0 && len(c.Receivers) == 0 {
		c.Warnings = append(c.Warnings, "no receiver is configured: alerts show in /api/alerts and the logs only")
	}
	return nil
}

func parseSev(s string) (int, bool) {
	switch strings.ToLower(s) {
	case "trace":
		return 1, true
	case "debug":
		return 5, true
	case "info":
		return 9, true
	case "warn", "warning":
		return 13, true
	case "error":
		return 17, true
	case "fatal":
		return 21, true
	}
	return 0, false
}

// SeverityNumber returns the OpenTelemetry number for a severity word.
func SeverityNumber(s string) int { n, _ := parseSev(s); return n }

var errEmpty = errors.New("empty")

func (r *Receiver) check() error {
	switch r.Type {
	case "slack", "discord", "webhook":
		if r.URL == "" {
			return fmt.Errorf("url is %w", errEmpty)
		}
		if !strings.HasPrefix(r.URL, "https://") && !strings.HasPrefix(r.URL, "http://") {
			return fmt.Errorf("receiver %q: url must start with http:// or https://", r.Name)
		}
	case "pagerduty":
		if r.RoutingKey == "" {
			return fmt.Errorf("routing_key is %w", errEmpty)
		}
	default:
		return fmt.Errorf("receiver %q: unknown type %q", r.Name, r.Type)
	}
	return nil
}

// check validates a rule and parses its condition. It does not look at
// receivers, which depend on who owns the rule.
func (r *Rule) check() error {
	m := whenRe.FindStringSubmatch(r.When)
	if m == nil {
		return fmt.Errorf("rule %q: when %q must look like \"count > 20\"", r.Name, r.When)
	}
	r.Metric, r.Op = m[1], m[2]
	v, err := strconv.ParseFloat(m[3], 64)
	if err != nil {
		return fmt.Errorf("rule %q: threshold: %w", r.Name, err)
	}
	r.Threshold = v
	switch r.Metric {
	case "count", "rate":
	case "error_ratio":
		r.Match.HTTPOnly = true
	case "p50_ms", "p90_ms", "p95_ms", "p99_ms":
		r.Match.HTTPOnly = true
		q, _ := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(r.Metric, "p"), "_ms"))
		r.Quantile = float64(q) / 100
	default:
		return fmt.Errorf("rule %q: unknown metric %q (count, rate, error_ratio, p50_ms, p90_ms, p95_ms, p99_ms)", r.Name, r.Metric)
	}
	if r.Window < time.Second {
		return fmt.Errorf("rule %q: window must be at least 1s", r.Name)
	}
	if r.Window > 24*time.Hour {
		return fmt.Errorf("rule %q: window must be at most 24h", r.Name)
	}
	if r.Match.MinSeverity != "" {
		if _, ok := parseSev(r.Match.MinSeverity); !ok {
			return fmt.Errorf("rule %q: unknown min_severity %q", r.Name, r.Match.MinSeverity)
		}
	}
	for _, re := range []string{r.Match.Body, r.Match.Path} {
		if re != "" {
			if _, err := regexp.Compile(re); err != nil {
				return fmt.Errorf("rule %q: %w", r.Name, err)
			}
		}
	}
	for _, g := range r.GroupBy {
		if g != "service" && g != "environment" && g != "path" && g != "method" && !strings.HasPrefix(g, "attr.") {
			return fmt.Errorf("rule %q: group_by %q (service, environment, path, method or attr.<key>)", r.Name, g)
		}
	}
	return nil
}

var nameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,62}$`)

// ValidName reports whether s can name a tenant, rule or receiver created
// through the API: lower case, digits, '_', '.' and '-', up to 63 bytes.
func ValidName(s string) bool { return nameRe.MatchString(s) }

func strictDecode(raw []byte, v any) error {
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	return dec.Decode(v)
}

// ParseRule reads one rule as YAML or JSON (the API accepts either), fills
// defaults and validates it. Receivers named in notify are left to the caller.
func ParseRule(raw []byte) (Rule, error) {
	var r Rule
	if err := strictDecode(raw, &r); err != nil {
		return r, fmt.Errorf("parse rule: %w", err)
	}
	if !ValidName(r.Name) {
		return r, fmt.Errorf("rule name %q: use lower case letters, digits, '_', '.' or '-'", r.Name)
	}
	if len(r.Match.Body) > 1024 || len(r.Match.Path) > 1024 {
		return r, fmt.Errorf("rule %q: patterns are limited to 1024 bytes", r.Name)
	}
	r.defaults()
	if err := r.check(); err != nil {
		return r, err
	}
	r.NotifyAll = r.Notify
	return r, nil
}

// ParseReceiver reads one receiver as YAML or JSON and validates it.
func ParseReceiver(raw []byte) (Receiver, error) {
	var r Receiver
	if err := strictDecode(raw, &r); err != nil {
		return r, fmt.Errorf("parse receiver: %w", err)
	}
	if !ValidName(r.Name) {
		return r, fmt.Errorf("receiver name %q: use lower case letters, digits, '_', '.' or '-'", r.Name)
	}
	if err := r.check(); err != nil {
		if errors.Is(err, errEmpty) {
			return r, fmt.Errorf("receiver %q: %v", r.Name, err)
		}
		return r, err
	}
	return r, nil
}
