// Package notify delivers alert events through an outbox: at least once,
// in order per receiver, retried with backoff, and saved with the
// checkpoint so a restart does not lose a page.
package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/shanurwan/signalbox/internal/config"
	"github.com/shanurwan/signalbox/internal/engine"
	"github.com/shanurwan/signalbox/internal/model"
)

const (
	maxAge     = 24 * time.Hour
	maxBackoff = time.Minute
)

// PagerDutyURL is a variable so tests can point it elsewhere.
var PagerDutyURL = "https://events.pagerduty.com/v2/enqueue"

type Item struct {
	ID       uint64       `json:"id"`
	Receiver string       `json:"receiver"`
	Event    engine.Event `json:"event"`
	Attempts int          `json:"attempts"`
	NextAt   time.Time    `json:"next_at"`
	Created  time.Time    `json:"created"`
}

type Stats struct {
	Sent, Failed, Dropped uint64
}

// Key names a receiver in the outbox. The default tenant's receivers (the
// ones in the config file) keep their plain names; a tenant's own are
// tenant/name, so two tenants can both have a receiver called "slack".
func Key(tenant, name string) string {
	if tenant == "" || tenant == model.DefaultTenant {
		return name
	}
	return tenant + "/" + name
}

type receiver struct {
	config.Receiver
	restricted bool // created by a tenant: only public addresses
}

// maxConcurrent caps deliveries in flight, so many tenants with slow or dead
// receivers cannot use up connections or goroutines.
const maxConcurrent = 64

type Outbox struct {
	mu         sync.Mutex
	items      []*Item
	nextID     uint64
	receivers  map[string]receiver
	client     *http.Client
	restricted *http.Client
	log        *slog.Logger
	wake       chan struct{}
	stats      map[string]*Stats

	// Changed, when set, is called after deliveries change the outbox, so
	// the owner can persist it before a crash can resend what was sent.
	Changed func()
}

// New creates an outbox with the config file's receivers. allowPrivate lets
// receivers created by tenants reach private addresses (for development).
func New(receivers []config.Receiver, allowPrivate bool, log *slog.Logger) *Outbox {
	o := &Outbox{
		receivers:  map[string]receiver{},
		client:     &http.Client{Timeout: 10 * time.Second},
		restricted: publicOnlyClient(allowPrivate),
		log:        log,
		wake:       make(chan struct{}, 1),
		stats:      map[string]*Stats{},
	}
	for _, r := range receivers {
		o.receivers[r.Name] = receiver{Receiver: r}
		o.stats[r.Name] = &Stats{}
	}
	return o
}

// SetReceiver adds or replaces a tenant's receiver.
func (o *Outbox) SetReceiver(tenant string, r config.Receiver) {
	k := Key(tenant, r.Name)
	o.mu.Lock()
	defer o.mu.Unlock()
	o.receivers[k] = receiver{Receiver: r, restricted: true}
	if o.stats[k] == nil {
		o.stats[k] = &Stats{}
	}
}

// RemoveReceiver deletes a tenant's receiver and what was waiting for it.
func (o *Outbox) RemoveReceiver(tenant, name string) {
	k := Key(tenant, name)
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.receivers, k)
	delete(o.stats, k)
	kept := o.items[:0]
	for _, it := range o.items {
		if it.Receiver != k {
			kept = append(kept, it)
		}
	}
	o.items = kept
}

// Receivers returns a tenant's own receivers, sorted by name.
func (o *Outbox) Receivers(tenant string) []config.Receiver {
	prefix := Key(tenant, "")
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []config.Receiver
	for k, r := range o.receivers {
		if r.restricted && strings.HasPrefix(k, prefix) && k[len(prefix):] == r.Name {
			out = append(out, r.Receiver)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Has reports whether the tenant can send to a receiver of that name.
func (o *Outbox) Has(tenant, name string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	_, ok := o.receivers[Key(tenant, name)]
	return ok
}

// Enqueue adds one item per receiver of the event, looked up among the
// event's tenant's receivers. Names the tenant has no receiver for are
// skipped: the alert still shows in the API.
func (o *Outbox) Enqueue(ev engine.Event) {
	now := time.Now()
	o.mu.Lock()
	for _, name := range ev.Receivers {
		k := Key(ev.Tenant, name)
		r, ok := o.receivers[k]
		if !ok || (r.Type == "pagerduty" && ev.Status == "reminder") {
			continue // no such receiver, or the incident is already open there
		}
		o.nextID++
		o.items = append(o.items, &Item{ID: o.nextID, Receiver: k, Event: ev, NextAt: now, Created: now})
	}
	o.mu.Unlock()
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

func (o *Outbox) Snapshot() []Item {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]Item, len(o.items))
	for i, it := range o.items {
		out[i] = *it
	}
	return out
}

func (o *Outbox) Restore(items []Item) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for i := range items {
		it := items[i]
		if _, ok := o.receivers[it.Receiver]; !ok {
			continue
		}
		if it.ID > o.nextID {
			o.nextID = it.ID
		}
		o.items = append(o.items, &it)
	}
	sort.Slice(o.items, func(i, j int) bool { return o.items[i].ID < o.items[j].ID })
}

func (o *Outbox) Stats() map[string]Stats {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := map[string]Stats{}
	for k, v := range o.stats {
		out[k] = *v
	}
	return out
}

func (o *Outbox) Pending() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.items)
}

// Run delivers until ctx ends. Each receiver gets its oldest item first and
// nothing newer is sent to it until that one succeeds or is dropped, so a
// resolve never overtakes its firing.
func (o *Outbox) Run(ctx context.Context) {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-o.wake:
		}
		o.deliverDue(ctx)
	}
}

func (o *Outbox) deliverDue(ctx context.Context) {
	now := time.Now()
	o.mu.Lock()
	heads := map[string]*Item{}
	for _, it := range o.items {
		if _, seen := heads[it.Receiver]; !seen {
			heads[it.Receiver] = it
		}
	}
	o.mu.Unlock()

	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrent)
	for _, it := range heads {
		if it.NextAt.After(now) {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(it *Item) {
			defer wg.Done()
			defer func() { <-sem }()
			err, retry := o.send(ctx, it)
			o.mu.Lock()
			defer o.mu.Unlock()
			st := o.stats[it.Receiver]
			if st == nil { // the receiver was deleted while sending
				st = &Stats{}
			}
			switch {
			case err == nil:
				st.Sent++
				o.remove(it.ID)
			case !retry || time.Since(it.Created) > maxAge:
				st.Dropped++
				o.remove(it.ID)
				o.log.Error("notification dropped", "receiver", it.Receiver, "rule", it.Event.Rule, "err", err)
			default:
				st.Failed++
				it.Attempts++
				back := time.Duration(math.Min(float64(maxBackoff), float64(time.Second)*math.Pow(2, float64(it.Attempts-1))))
				it.NextAt = time.Now().Add(back)
				o.log.Warn("notification failed, will retry", "receiver", it.Receiver, "rule", it.Event.Rule, "attempt", it.Attempts, "in", back, "err", err)
			}
		}(it)
	}
	wg.Wait()
	if len(heads) > 0 && o.Changed != nil {
		o.Changed()
	}
}

func (o *Outbox) remove(id uint64) {
	for i, it := range o.items {
		if it.ID == id {
			o.items = append(o.items[:i], o.items[i+1:]...)
			return
		}
	}
}

// send returns whether a failure is worth retrying.
func (o *Outbox) send(ctx context.Context, it *Item) (error, bool) {
	o.mu.Lock()
	r, ok := o.receivers[it.Receiver]
	o.mu.Unlock()
	if !ok {
		return fmt.Errorf("receiver %q no longer exists", it.Receiver), false
	}
	client := o.client
	if r.restricted {
		client = o.restricted
	}
	url, body, err := Render(r.Receiver, it.Event)
	if err != nil {
		return err, false
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err, false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "signalbox")
	for k, v := range r.Headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err, !errors.Is(err, errNotPublic) // a blocked address stays blocked
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil, false
	}
	err = fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(msg)))
	return err, resp.StatusCode == 429 || resp.StatusCode >= 500 || resp.StatusCode == 408
}

// Render builds the request for one receiver type.
func Render(r config.Receiver, ev engine.Event) (string, []byte, error) {
	switch r.Type {
	case "slack":
		b, err := json.Marshal(map[string]string{"text": Text(ev)})
		return r.URL, b, err
	case "discord":
		t := Text(ev)
		if len(t) > 1900 {
			t = t[:1900] + "…"
		}
		b, err := json.Marshal(map[string]any{"content": t, "allowed_mentions": map[string]any{"parse": []string{}}})
		return r.URL, b, err
	case "webhook":
		b, err := json.Marshal(ev)
		return r.URL, b, err
	case "pagerduty":
		action := "trigger"
		if ev.Status == "resolved" {
			action = "resolve"
		}
		sev := map[string]string{"critical": "critical", "page": "critical", "error": "error", "warning": "warning", "info": "info"}[ev.Severity]
		if sev == "" {
			sev = "warning"
		}
		b, err := json.Marshal(map[string]any{
			"routing_key":  r.RoutingKey,
			"event_action": action,
			"dedup_key":    ev.Fingerprint,
			"payload": map[string]any{
				"summary":        oneLine(ev),
				"source":         labelString(ev.Labels),
				"severity":       sev,
				"custom_details": ev,
			},
		})
		return PagerDutyURL, b, err
	}
	return "", nil, fmt.Errorf("unknown receiver type %q", r.Type)
}

func oneLine(ev engine.Event) string {
	s := fmt.Sprintf("%s: %s = %s (%s %g over %s)", ev.Rule, ev.Metric, fmtValue(ev), ev.Op, ev.Threshold, ev.Window)
	if l := labelString(ev.Labels); l != "" {
		s += " [" + l + "]"
	}
	return s
}

// Text is the chat message body.
func Text(ev engine.Event) string {
	icon := map[string]string{"firing": "🔴 FIRING", "resolved": "✅ RESOLVED", "reminder": "🔁 STILL FIRING"}[ev.Status]
	var b strings.Builder
	fmt.Fprintf(&b, "%s  %s\n", icon, oneLine(ev))
	if ev.Summary != "" {
		b.WriteString(ev.Summary + "\n")
	}
	switch ev.Status {
	case "resolved":
		fmt.Fprintf(&b, "Firing for %s.", ev.EndsAt.Sub(ev.StartsAt).Round(time.Second))
	case "reminder":
		fmt.Fprintf(&b, "Firing since %s.", ev.StartsAt.UTC().Format(time.RFC3339))
	}
	return strings.TrimSpace(b.String())
}

func fmtValue(ev engine.Event) string {
	metric, v := ev.Metric, ev.Value
	switch {
	case ev.NoData || math.IsNaN(v):
		return "no data"
	case metric == "error_ratio":
		return fmt.Sprintf("%.2f%%", v*100)
	case strings.HasSuffix(metric, "_ms"):
		return fmt.Sprintf("%.0fms", v)
	case metric == "rate":
		return fmt.Sprintf("%.2f/s", v)
	}
	return fmt.Sprintf("%g", v)
}

func labelString(l map[string]string) string {
	keys := make([]string, 0, len(l))
	for k := range l {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		if l[k] != "" {
			parts = append(parts, k+"="+l[k])
		}
	}
	return strings.Join(parts, " ")
}
