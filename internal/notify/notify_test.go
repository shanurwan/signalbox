package notify

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/shanurwan/signalbox/internal/config"
	"github.com/shanurwan/signalbox/internal/engine"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func ev(status string) engine.Event {
	return engine.Event{Rule: "api-5xx", Status: status, Severity: "page", Labels: map[string]string{"service": "api"},
		Metric: "error_ratio", Op: ">", Threshold: 0.05, Value: 0.12, Window: "5m0s", Fingerprint: "f00d", Receivers: []string{"hook"}}
}

func TestRetriesInOrderUntilDelivered(t *testing.T) {
	var mu sync.Mutex
	var got []string
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls <= 2 {
			w.WriteHeader(503) // receiver down for the first two attempts
			return
		}
		var e engine.Event
		json.NewDecoder(r.Body).Decode(&e)
		got = append(got, e.Status)
	}))
	defer srv.Close()

	o := New([]config.Receiver{{Name: "hook", Type: "webhook", URL: srv.URL}}, quiet())
	o.Enqueue(ev("firing"))
	o.Enqueue(ev("resolved"))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	go o.Run(ctx)
	for o.Pending() > 0 && ctx.Err() == nil {
		time.Sleep(50 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 || got[0] != "firing" || got[1] != "resolved" {
		t.Fatalf("delivered %v; want firing then resolved", got)
	}
	if st := o.Stats()["hook"]; st.Sent != 2 || st.Failed != 2 {
		t.Fatalf("stats %+v", st)
	}
}

func TestClientErrorIsNotRetried(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) }))
	defer srv.Close()
	o := New([]config.Receiver{{Name: "hook", Type: "webhook", URL: srv.URL}}, quiet())
	o.Enqueue(ev("firing"))
	o.deliverDue(context.Background())
	if o.Pending() != 0 || o.Stats()["hook"].Dropped != 1 {
		t.Fatalf("a 404 must be dropped, not retried: %+v", o.Stats())
	}
}

func TestPagerDutyTriggerResolvePairing(t *testing.T) {
	r := config.Receiver{Name: "pd", Type: "pagerduty", RoutingKey: "rk"}
	_, b, err := Render(r, ev("firing"))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	json.Unmarshal(b, &m)
	if m["event_action"] != "trigger" || m["dedup_key"] != "f00d" || m["routing_key"] != "rk" {
		t.Fatalf("%s", b)
	}
	if m["payload"].(map[string]any)["severity"] != "critical" {
		t.Fatalf("severity mapping: %s", b)
	}
	_, b, _ = Render(r, ev("resolved"))
	json.Unmarshal(b, &m)
	if m["event_action"] != "resolve" || m["dedup_key"] != "f00d" {
		t.Fatalf("%s", b)
	}
	o := New([]config.Receiver{r}, quiet())
	e := ev("reminder")
	e.Receivers = []string{"pd"}
	o.Enqueue(e)
	if o.Pending() != 0 {
		t.Fatal("reminders must not re-trigger PagerDuty")
	}
}

func TestNoDataEventStillEncodes(t *testing.T) {
	e := ev("resolved")
	e.Value, e.NoData = 0, true
	for _, typ := range []string{"slack", "discord", "webhook", "pagerduty"} {
		if _, _, err := Render(config.Receiver{Type: typ, URL: "http://x", RoutingKey: "k"}, e); err != nil {
			t.Fatalf("%s: %v", typ, err)
		}
	}
	if txt := Text(e); txt == "" || !contains(txt, "no data") {
		t.Fatalf("text %q", txt)
	}
}

func TestOutboxSurvivesRestart(t *testing.T) {
	recv := []config.Receiver{{Name: "hook", Type: "webhook", URL: "http://127.0.0.1:1"}}
	o := New(recv, quiet())
	o.Enqueue(ev("firing"))
	saved := o.Snapshot()
	o2 := New(recv, quiet())
	o2.Restore(saved)
	if o2.Pending() != 1 || o2.Snapshot()[0].Event.Status != "firing" {
		t.Fatal("pending notification lost across restart")
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
