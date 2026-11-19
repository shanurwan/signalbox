// Command signalbox receives logs and request logs, alerts on what they say,
// and optionally stores them in ClickHouse.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/shanurwan/signalbox/internal/config"
	"github.com/shanurwan/signalbox/internal/server"
)

var version = "dev"

func main() {
	path := flag.String("config", envOr("SIGNALBOX_CONFIG", "signalbox.yaml"), "path to the config file")
	check := flag.Bool("check", false, "validate the config and exit")
	showVersion := flag.Bool("version", false, "print the version and exit")
	pprofAddr := flag.String("pprof", os.Getenv("SIGNALBOX_PPROF"), "serve net/http/pprof on this address (e.g. 127.0.0.1:6060)")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := config.Load(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(2)
	}
	if *check {
		fmt.Printf("config ok: %d rules, %d receivers\n", len(cfg.Rules), len(cfg.Receivers))
		return
	}
	for _, w := range cfg.Warnings {
		log.Warn(w)
	}
	if *pprofAddr != "" {
		go func() { log.Warn("pprof", "err", http.ListenAndServe(*pprofAddr, nil)) }()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Info("starting signalbox", "version", version, "rules", len(cfg.Rules), "receivers", len(cfg.Receivers),
		"clickhouse", cfg.ClickHouse != nil && cfg.ClickHouse.URL != "", "state_file", cfg.StateFile)
	if err := server.New(cfg, log).Run(ctx); err != nil {
		log.Error("exit", "err", err)
		os.Exit(1)
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
