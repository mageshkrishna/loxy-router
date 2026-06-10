package main

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
)

// Metrics is a tiny counter collector for cumulative request totals. The proxy
// increments it per completed request; metricsHandler reads a snapshot. Gauges
// (health, inflight, warmth, per-token latency) are read live from the pool and
// store instead, so only monotonic counters need to live here.
type Metrics struct {
	mu       sync.Mutex
	requests map[requestKey]int64
}

type requestKey struct {
	backend string
	status  int
}

func NewMetrics() *Metrics {
	return &Metrics{requests: make(map[requestKey]int64)}
}

// IncRequest records one completed (forwarded) request with its HTTP status.
// Safe on a nil receiver so code paths without a collector (e.g. tests) are fine.
func (m *Metrics) IncRequest(backend string, status int) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.requests[requestKey{backend, status}]++
	m.mu.Unlock()
}

func (m *Metrics) snapshot() map[requestKey]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[requestKey]int64, len(m.requests))
	for k, v := range m.requests {
		out[k] = v
	}
	return out
}

// metricsHandler returns a Prometheus text-format handler exposing backend
// health/inflight, the richer LLM state from the Scraper, admission-gate
// pressure, and cumulative request totals.
func metricsHandler(pool *BackendPool, store *StateStore, gate *Gate, metrics *Metrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")

		var b strings.Builder

		// ── Basic health ──────────────────────────────────────────────────────

		b.WriteString("# HELP scheduler_backend_up Backend health (1=up, 0=down)\n")
		b.WriteString("# TYPE scheduler_backend_up gauge\n")
		for _, backend := range pool.All() {
			val := 0
			if backend.Status() == StatusUp {
				val = 1
			}
			fmt.Fprintf(&b, "scheduler_backend_up{backend=%q} %d\n", backend.Name, val)
		}

		b.WriteString("# HELP scheduler_backend_inflight In-flight requests per backend\n")
		b.WriteString("# TYPE scheduler_backend_inflight gauge\n")
		for _, backend := range pool.All() {
			fmt.Fprintf(&b, "scheduler_backend_inflight{backend=%q} %d\n", backend.Name, backend.Inflight())
		}

		// ── LLM-specific state ────────────────────────────────────────────────

		b.WriteString("# HELP scheduler_backend_model_warm 1 if the backend has a model loaded in VRAM\n")
		b.WriteString("# TYPE scheduler_backend_model_warm gauge\n")
		for _, backend := range pool.All() {
			val := 0
			if st, ok := store.Get(backend.Name); ok && st.ModelStatus == ModelWarm {
				val = 1
			}
			fmt.Fprintf(&b, "scheduler_backend_model_warm{backend=%q} %d\n", backend.Name, val)
		}

		b.WriteString("# HELP scheduler_backend_vram_used_pct Fraction of VRAM in use (0–1); 0 means unknown\n")
		b.WriteString("# TYPE scheduler_backend_vram_used_pct gauge\n")
		for _, backend := range pool.All() {
			val := 0.0
			if st, ok := store.Get(backend.Name); ok {
				val = st.VRAMUsedPct
			}
			fmt.Fprintf(&b, "scheduler_backend_vram_used_pct{backend=%q} %.4f\n", backend.Name, val)
		}

		b.WriteString("# HELP scheduler_backend_queue_depth Requests waiting to be served\n")
		b.WriteString("# TYPE scheduler_backend_queue_depth gauge\n")
		for _, backend := range pool.All() {
			val := 0
			if st, ok := store.Get(backend.Name); ok {
				val = st.QueueDepth
			}
			fmt.Fprintf(&b, "scheduler_backend_queue_depth{backend=%q} %d\n", backend.Name, val)
		}

		b.WriteString("# HELP scheduler_backend_per_token_ms EWMA decode latency per generated token (ms); 0 if unmeasured\n")
		b.WriteString("# TYPE scheduler_backend_per_token_ms gauge\n")
		for _, backend := range pool.All() {
			fmt.Fprintf(&b, "scheduler_backend_per_token_ms{backend=%q} %d\n", backend.Name, backend.PerTokenMs())
		}

		// ── Admission gate ────────────────────────────────────────────────────

		gs := gate.Stats()
		b.WriteString("# HELP scheduler_gate_max_inflight Global dispatch limit (live sum of backend caps); 0 = unlimited\n")
		b.WriteString("# TYPE scheduler_gate_max_inflight gauge\n")
		fmt.Fprintf(&b, "scheduler_gate_max_inflight %d\n", gs.MaxInflight)

		b.WriteString("# HELP scheduler_gate_inflight Requests currently dispatched through the gate\n")
		b.WriteString("# TYPE scheduler_gate_inflight gauge\n")
		fmt.Fprintf(&b, "scheduler_gate_inflight %d\n", gs.Inflight)

		b.WriteString("# HELP scheduler_gate_queued Requests waiting in the admission queue, by priority\n")
		b.WriteString("# TYPE scheduler_gate_queued gauge\n")
		for _, p := range []Priority{PriorityInteractive, PriorityBatch, PriorityBackground} {
			fmt.Fprintf(&b, "scheduler_gate_queued{priority=%q} %d\n", p.String(), gs.Queued[p.String()])
		}

		// ── Cumulative request totals ─────────────────────────────────────────

		b.WriteString("# HELP scheduler_requests_total Forwarded requests by backend and HTTP status\n")
		b.WriteString("# TYPE scheduler_requests_total counter\n")
		snap := metrics.snapshot()
		keys := make([]requestKey, 0, len(snap))
		for k := range snap {
			keys = append(keys, k)
		}
		// Stable ordering so the output is deterministic across scrapes.
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].backend != keys[j].backend {
				return keys[i].backend < keys[j].backend
			}
			return keys[i].status < keys[j].status
		})
		for _, k := range keys {
			fmt.Fprintf(&b, "scheduler_requests_total{backend=%q,status=\"%d\"} %d\n",
				k.backend, k.status, snap[k])
		}

		w.Write([]byte(b.String()))
	})
}
