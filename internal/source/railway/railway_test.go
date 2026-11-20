package railway

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/shanurwan/signalbox/internal/model"
)

// fakeRailway serves the two GraphQL queries over HTTP and the log
// subscriptions over graphql-transport-ws. Each HTTP-log subscription gets
// the next scripted step: logs to send, then "complete" or a dropped
// connection.
type fakeRailway struct {
	t      *testing.T
	t0     time.Time
	mu     sync.Mutex
	subs   []map[string]any // variables of every subscribe received
	authOK bool
}

func (f *fakeRailway) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/graphql/v2", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.authOK = r.Header.Get("Project-Access-Token") == "tok"
		f.mu.Unlock()
		var req struct{ Query string }
		json.NewDecoder(r.Body).Decode(&req)
		if strings.Contains(req.Query, "environment(") {
			io.WriteString(w, `{"data":{"environment":{"name":"production","projectId":"p1","deployments":{"edges":[
				{"node":{"id":"d-old","serviceId":"s1","status":"REMOVED","createdAt":"2026-11-20T00:00:00Z"}},
				{"node":{"id":"d1","serviceId":"s1","status":"SUCCESS","createdAt":"2026-11-21T00:00:00Z"}}]}}}}`)
			return
		}
		io.WriteString(w, `{"data":{"project":{"services":{"edges":[{"node":{"id":"s1","name":"checkout"}}]}}}}`)
	})
	mux.HandleFunc("/graphql/internal", func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"graphql-transport-ws"}})
		if err != nil {
			return
		}
		defer c.CloseNow()
		ctx := r.Context()
		send := func(v any) { b, _ := json.Marshal(v); c.Write(ctx, websocket.MessageText, b) }
		for {
			_, b, err := c.Read(ctx)
			if err != nil {
				return
			}
			var m struct {
				ID      string
				Type    string
				Payload struct {
					Query     string
					Variables map[string]any
				}
			}
			json.Unmarshal(b, &m)
			switch m.Type {
			case "connection_init":
				send(map[string]string{"type": "connection_ack"})
				send(map[string]string{"type": "ping"})
			case "subscribe":
				if !strings.Contains(m.Payload.Query, "httpLogs") {
					continue // deploy logs: leave that stream quiet in this test
				}
				f.mu.Lock()
				f.subs = append(f.subs, m.Payload.Variables)
				n := len(f.subs)
				f.mu.Unlock()
				ts := func(s int) string { return f.t0.Add(time.Duration(s) * time.Second).UTC().Format(time.RFC3339Nano) }
				log := func(id string, s, status int) map[string]any {
					return map[string]any{"requestId": id, "timestamp": ts(s), "method": "POST", "path": "/pay",
						"host": "checkout.up.railway.app", "httpStatus": status, "totalDuration": 120, "edgeRegion": "asia"}
				}
				next := func(logs ...any) {
					send(map[string]any{"id": m.ID, "type": "next", "payload": map[string]any{"data": map[string]any{"httpLogs": logs}}})
				}
				switch n {
				case 1:
					// As on Railway, a later batch can hold a log older than one
					// already delivered: r2 (t+2) arrives after r3 (t+3).
					next(log("r1", 1, 200), log("r3", 3, 200))
					next(log("r2", 2, 502))
					send(map[string]any{"id": m.ID, "type": "complete"})
				case 2: // resubscribed with an overlap: r2 and r3 again, plus r4; then the connection drops
					next(log("r2", 2, 502), log("r3", 3, 200), log("r4", 4, 503))
					return
				default: // reconnected: r4 again, plus r5
					next(log("r4", 4, 503), log("r5", 5, 200))
				}
			}
		}
	})
	return mux
}

func TestRailwaySourceStreamsResubscribesAndReconnects(t *testing.T) {
	f := &fakeRailway{t: t, t0: time.Now().Add(time.Second)} // logs are newer than the source's start
	srv := httptest.NewServer(f.handler())
	defer srv.Close()

	var mu sync.Mutex
	var got []model.Record
	src := New(Config{ProjectToken: "tok", EnvironmentID: "e1", Tenant: "acme",
		APIURL: srv.URL + "/graphql/v2", StreamURL: "ws" + strings.TrimPrefix(srv.URL, "http") + "/graphql/internal",
		Refresh: time.Hour, Overlap: 10 * time.Second}, slog.New(slog.NewTextHandler(io.Discard, nil)),
		func(tenant string, recs []model.Record) {
			if tenant != "acme" {
				t.Errorf("tenant %q", tenant)
			}
			mu.Lock()
			got = append(got, recs...)
			mu.Unlock()
		})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { src.Run(ctx); close(done) }()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= 5 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	var ids []string
	for _, r := range got {
		ids = append(ids, r.HTTP.RequestID)
	}
	if strings.Join(ids, ",") != "r1,r3,r2,r4,r5" {
		t.Fatalf("records %v, want r1,r3,r2,r4,r5: the late r2 kept, repeats dropped", ids)
	}
	r := got[2]
	if r.Service != "checkout" || r.Environment != "production" || r.HTTP.Status != 502 || r.HTTP.DurationMs != 120 ||
		r.HTTP.Method != "POST" || r.Attrs["deployment_id"] != "d1" {
		t.Fatalf("mapping: %+v %+v", r, r.HTTP)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.authOK {
		t.Fatal("project token not sent as Project-Access-Token")
	}
	if len(f.subs) < 3 {
		t.Fatalf("%d subscriptions, want a resubscribe and a reconnect", len(f.subs))
	}
	if f.subs[0]["deploymentId"] != "d1" {
		t.Fatalf("subscribed to %v, want the live deployment d1", f.subs[0]["deploymentId"])
	}
	// Each new subscription starts one overlap before the newest log seen.
	if want := f.t0.Add(3*time.Second - 10*time.Second).UTC().Format(time.RFC3339Nano); f.subs[1]["beforeDate"] != want {
		t.Fatalf("resubscribe from %v, want %s", f.subs[1]["beforeDate"], want)
	}
	if want := f.t0.Add(4*time.Second - 10*time.Second).UTC().Format(time.RFC3339Nano); f.subs[2]["beforeDate"] != want {
		t.Fatalf("reconnect from %v, want %s", f.subs[2]["beforeDate"], want)
	}
	if src.Stats.Reconnects.Load() < 1 || src.Stats.HTTPRecords.Load() != 5 || src.Stats.Repeats.Load() != 3 {
		t.Fatalf("stats: reconnects %d records %d repeats %d", src.Stats.Reconnects.Load(), src.Stats.HTTPRecords.Load(), src.Stats.Repeats.Load())
	}
}

func TestUnquoteStructuredAttributes(t *testing.T) {
	if unquote(`"error"`) != "error" || unquote("42") != "42" || unquote(`"`) != `"` {
		t.Fatal("unquote")
	}
}
