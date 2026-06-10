package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
)

// version is stamped at build time via -ldflags "-X main.version=<tag>"; it
// stays "dev" for local builds.
var version = "dev"

// conversationTTL is how long a conversation stays pinned to its backend before
// the KV-cache affinity is assumed stale.
const conversationTTL = 10 * time.Minute

func main() {
	setupLogging()

	cfgPath := flag.String("config", "", "path to config file (optional — auto-discovers local Ollama if omitted)")
	flag.Parse()

	cfg := loadConfig(*cfgPath)

	pool := NewBackendPool(cfg.Backends, cfg.BackendAPIKey)
	go pool.HealthLoop(5 * time.Second)

	store := NewStateStore()
	scraper := NewScraper(pool, store)
	go scraper.Run(2 * time.Second)

	conv := NewConversationStore(conversationTTL)
	go conv.ReapLoop(conversationTTL)

	gate := NewGate(0, cfg.queueLimits())
	// The gate's global limit is the live sum of every backend's capacity, so it
	// engages for priority ordering exactly when all backends are full — never
	// rejecting while a backend is free, never admitting past real capacity. When
	// any backend is unlimited the sum is 0 and the gate is a no-op, so there is
	// no separate on/off switch to forget.
	gate.SetLimitFunc(pool.TotalConcurrency)
	sched := NewScheduler(pool, store, conv)
	metrics := NewMetrics()
	proxy := NewProxy(sched, gate, metrics, cfg.responseHeaderTimeout())

	mux := http.NewServeMux()
	// Proxy and status carry routing/state detail, so they sit behind auth.
	authed := func(h http.Handler) http.Handler { return authMiddleware(cfg.APIKeys, h) }
	mux.Handle("/v1/", authed(proxy))
	mux.Handle("/api/", authed(proxy))
	mux.Handle("/status", authed(statusHandler(pool, store, gate)))
	// /metrics and /health stay open for Prometheus scraping and load-balancer probes.
	mux.Handle("/metrics", metricsHandler(pool, store, gate, metrics))
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	})

	server := &http.Server{
		Addr:    cfg.Listen,
		Handler: mux,
		// Bound only the header read: a total ReadTimeout/WriteTimeout would
		// truncate long uploads and streaming responses, but ReadHeaderTimeout
		// still closes the Slowloris hole where a client dribbles request headers
		// to hold a connection open indefinitely.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// Trap SIGINT/SIGTERM so a rolling deploy or autoscaler can stop us without
	// cutting off in-flight token streams mid-response.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		slog.Info("starting", "version", version, "listen", cfg.Listen,
			"backends", len(cfg.Backends), "auth", len(cfg.APIKeys) > 0)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server exited", "err", err.Error())
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	stop() // restore default handling so a second signal force-quits a stuck drain
	timeout := cfg.shutdownTimeout()
	slog.Info("shutting down, draining in-flight requests", "timeout", timeout.String())
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("graceful shutdown timed out, forcing exit", "err", err.Error())
		os.Exit(1)
	}
	slog.Info("shutdown complete")
}

func loadConfig(path string) Config {
	if path == "" {
		return autoDiscover()
	}

	data, err := os.ReadFile(path)
	if err != nil {
		slog.Error("read config", "path", path, "err", err.Error())
		os.Exit(1)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		slog.Error("parse config", "err", err.Error())
		os.Exit(1)
	}
	if cfg.Listen == "" {
		cfg.Listen = ":8080"
	}
	return cfg
}

// autoDiscover builds a Config by probing well-known local ports and reading
// environment variables. Lets users run the scheduler with zero configuration.
func autoDiscover() Config {
	cfg := Config{Listen: ":8080"}

	// SCHEDULER_BACKENDS env var: comma-separated URLs
	// e.g. SCHEDULER_BACKENDS=http://localhost:11434,http://gpu-box:11434
	if env := os.Getenv("SCHEDULER_BACKENDS"); env != "" {
		for i, url := range splitTrim(env, ",") {
			cfg.Backends = append(cfg.Backends, BackendConfig{
				Name: "backend-" + strconv.Itoa(i+1),
				URL:  url,
			})
		}
		slog.Info("auto-discover from env", "backends", len(cfg.Backends))
		return cfg
	}

	// Probe well-known Ollama ports on localhost.
	defaultPorts := []string{"11434", "11435", "11436", "11437"}
	client := &http.Client{Timeout: 500 * time.Millisecond}
	for _, port := range defaultPorts {
		url := "http://localhost:" + port
		resp, err := client.Get(url + "/api/version")
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			cfg.Backends = append(cfg.Backends, BackendConfig{
				Name: "ollama-" + port,
				URL:  url,
			})
			slog.Info("auto-discover found Ollama", "url", url)
		}
	}

	if len(cfg.Backends) == 0 {
		slog.Error("auto-discover: no backends found; run with -config or set SCHEDULER_BACKENDS")
		os.Exit(1)
	}
	return cfg
}

// splitTrim splits s on sep and drops empty/space-only fields.
func splitTrim(s, sep string) []string {
	var out []string
	for _, part := range strings.Split(s, sep) {
		if t := strings.TrimSpace(part); t != "" {
			out = append(out, t)
		}
	}
	return out
}
