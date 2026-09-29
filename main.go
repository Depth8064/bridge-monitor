package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Depth8064/bridge-monitor/internal/config"
	"github.com/Depth8064/bridge-monitor/internal/monitor"
	"github.com/Depth8064/bridge-monitor/internal/web"
)

func main() {
	cfgPath := flag.String("config", "config.json", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if errors.Is(err, os.ErrNotExist) {
		log.Fatalf("config %s not found: copy config.example.json to %s and edit the targets", *cfgPath, *cfgPath)
	}
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	store, err := monitor.OpenStore(cfg.DataDir, cfg.Targets)
	if err != nil {
		log.Fatalf("data dir: %v", err)
	}
	defer store.Close()

	eng := monitor.NewEngine(cfg)
	n, err := monitor.Replay(cfg.DataDir, time.Now().Add(-cfg.Retention.Duration), eng.Record)
	if err != nil {
		log.Printf("replay history: %v", err)
	}
	log.Printf("loaded %d rounds of history from %s", n, cfg.DataDir)

	eng.OnOutage = func(o monitor.Outage) {
		log.Printf("OUTAGE %s (%s): %.1fs, %d probes lost, %s -> %s",
			o.Target, o.Role, o.Duration, o.Lost, o.Start.Format(time.TimeOnly), o.End.Format(time.TimeOnly))
		if err := store.WriteOutage(o); err != nil {
			log.Printf("write outage: %v", err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{Addr: cfg.Listen, Handler: web.Handler(eng), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http: %v", err)
		}
	}()
	log.Printf("dashboard: %s", dashboardURL(cfg.Listen))
	log.Printf("probing %d targets every %s (timeout %s)", len(cfg.Targets), cfg.Interval, cfg.Timeout)

	monitor.New(cfg, eng, store).Run(ctx)

	log.Print("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
}

func dashboardURL(listen string) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "http://" + listen
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "localhost"
	}
	return "http://" + net.JoinHostPort(host, port)
}
