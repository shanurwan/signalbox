// Package config loads and validates signalbox.yaml.
package config

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
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
	DedupeMaxEntries int         `yaml:"dedupe_max_entries"`
	Mapping          Mapping     `yaml:"mapping"`
	ClickHouse       *ClickHouse `yaml:"clickhouse"`
	Receivers        []Receiver  `yaml:"receivers"`
	Rules            []Rule      `yaml:"rules"`

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
	for i := range c.Rules {
		r := &c.Rules[i]
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
		switch r.Type {
		case "slack", "discord", "webhook":
			if r.URL == "" {
				off[r.Name] = true
				c.Warnings = append(c.Warnings, fmt.Sprintf("receiver %q is off: url is empty", r.Name))
				continue
			}
		case "pagerduty":
			if r.RoutingKey == "" {
				off[r.Name] = true
				c.Warnings = append(c.Warnings, fmt.Sprintf("receiver %q is off: routing_key is empty", r.Name))
				continue
			}
		default:
			return fmt.Errorf("receiver %q: unknown type %q", r.Name, r.Type)
		}
		recv[r.Name] = true
		active = append(active, r)
	}
	c.Receivers = active
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
		var notify []string
		for _, n := range r.Notify {
			switch {
			case recv[n]:
				notify = append(notify, n)
			case off[n]:
			default:
				return fmt.Errorf("rule %q: unknown receiver %q", r.Name, n)
			}
		}
		r.Notify = notify
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
