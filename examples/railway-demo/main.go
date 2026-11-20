// railway-demo is a small service for trying signalbox's Railway source:
// it serves traffic, and on request returns errors, slows down, prints a
// crash line, or floods its own logs. Every second it prints a log line
// carrying its own send time, so log delivery delay can be measured.
package main

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	errRate atomic.Uint64 // per mille
	slowMs  atomic.Int64
)

func main() {
	admin := os.Getenv("DEMO_ADMIN_TOKEN")
	go func() {
		for range time.Tick(time.Second) {
			line, _ := json.Marshal(map[string]any{"level": "info", "msg": "tick", "sent_at": time.Now().UnixNano()})
			fmt.Println(string(line))
		}
	}()
	authed := func(r *http.Request) bool {
		return admin != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Admin-Token")), []byte(admin)) == 1
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if d := slowMs.Load(); d > 0 {
			time.Sleep(time.Duration(d) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(5+rand.IntN(30)) * time.Millisecond)
		}
		if uint64(rand.IntN(1000)) < errRate.Load() {
			http.Error(w, "upstream failed", http.StatusBadGateway)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("POST /mode", func(w http.ResponseWriter, r *http.Request) {
		if !authed(r) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		e, _ := strconv.ParseFloat(r.URL.Query().Get("err"), 64)
		s, _ := strconv.Atoi(r.URL.Query().Get("slow_ms"))
		errRate.Store(uint64(e * 1000))
		slowMs.Store(int64(s))
		line, _ := json.Marshal(map[string]any{"level": "warn", "msg": "mode changed", "err": e, "slow_ms": s})
		fmt.Println(string(line))
		fmt.Fprintf(w, "err=%.3f slow_ms=%d\n", e, s)
	})
	mux.HandleFunc("POST /panic", func(w http.ResponseWriter, r *http.Request) {
		if !authed(r) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		fmt.Fprintln(os.Stderr, "panic: runtime error: invalid memory address or nil pointer dereference (simulated)")
		fmt.Fprintln(w, "printed")
	})
	mux.HandleFunc("POST /burst", func(w http.ResponseWriter, r *http.Request) {
		if !authed(r) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		n, _ := strconv.Atoi(r.URL.Query().Get("n"))
		id := r.URL.Query().Get("id")
		start := time.Now()
		for i := 0; i < n; i++ {
			fmt.Printf("{\"level\":\"info\",\"msg\":\"burst\",\"burst\":%q,\"seq\":%d}\n", id, i)
		}
		fmt.Fprintf(w, "wrote %d lines in %s\n", n, time.Since(start))
	})
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
