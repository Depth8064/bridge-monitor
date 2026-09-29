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
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/Depth8064/bridge-monitor/internal/config"
	"github.com/Depth8064/bridge-monitor/internal/monitor"
	"github.com/Depth8064/bridge-monitor/internal/storage"
	"github.com/Depth8064/bridge-monitor/internal/web"
)

func main() {
	cfgPath := flag.String("config", "config.json", "path to config file")
	listen := flag.String("listen", "", "override listen from config")
	dataDir := flag.String("data-dir", "", "override data_dir from config")
	health := flag.String("healthcheck", "", "GET this URL and exit 0 on HTTP 200 (for container health checks)")
	flag.Parse()

	if *health != "" {
		os.Exit(healthcheck(*health))
	}

	cfg, err := config.Load(*cfgPath)
	if errors.Is(err, os.ErrNotExist) {
		log.Fatalf("config %s not found: copy config.example.json to %s and edit the targets", *cfgPath, *cfgPath)
	}
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	if *listen != "" {
		cfg.Listen = *listen
	}
	if *dataDir != "" {
		cfg.DataDir = *dataDir
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbPath := filepath.Join(cfg.DataDir, "bridge-monitor.db")
	db, err := storage.Open(dbPath, cfg)
	if err != nil {
		log.Fatalf("open %s: %v", dbPath, err)
	}
	defer db.Close()

	if n, err := db.ImportCSV(ctx, cfg.DataDir); err != nil {
		log.Printf("import csv: %v", err)
	} else if n > 0 {
		log.Printf("imported %d samples from CSV files in %s", n, cfg.DataDir)
	}

	eng := monitor.NewEngine(cfg)
	n, err := db.Replay(ctx, time.Now().Add(-cfg.Retention.Duration), eng.Record)
	if err != nil {
		log.Printf("replay history: %v", err)
	}
	log.Printf("loaded %d rounds of history from %s", n, dbPath)

	eng.OnOutage = func(o monitor.Outage) {
		log.Printf("OUTAGE %s (%s): %.1fs, %d probes lost, %s -> %s",
			o.Target, o.Role, o.Duration, o.Lost, o.Start.Format(time.TimeOnly), o.End.Format(time.TimeOnly))
		if err := db.WriteOutage(o); err != nil {
			log.Printf("write outage: %v", err)
		}
	}

	var bg sync.WaitGroup
	bg.Add(1)
	go func() {
		defer bg.Done()
		storage.NewDownsampler(db, cfg).Run(ctx)
	}()

	hist := storage.NewHistory(db, eng, cfg)
	srv := &http.Server{Addr: cfg.Listen, Handler: web.Handler(eng, hist, db), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http: %v", err)
		}
	}()
	log.Printf("dashboard: %s", dashboardURL(cfg.Listen))
	log.Printf("probing %d targets every %s (timeout %s)", len(cfg.Targets), cfg.Interval, cfg.Timeout)

	monitor.New(cfg, eng, db).Run(ctx)

	log.Print("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shutdownCtx)
	bg.Wait()
}

func healthcheck(url string) int {
	c := http.Client{Timeout: 3 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		log.Print(err)
		return 1
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("%s: %s", url, resp.Status)
		return 1
	}
	return 0
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
