package main

import (
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type BackendStatus string

const (
	StatusUp   BackendStatus = "up"
	StatusDown BackendStatus = "down"
)

type Backend struct {
	Name          string
	URL           string
	Models        []string // empty = accepts all models
	Type          string   // "ollama" or "vllm"
	VRAMTotalMB   int64    // 0 = unknown; used to compute VRAMUsedPct for Ollama backends
	maxConcurrent int      // from config; 0 = auto-compute from VRAM+model size
	apiKey        string   // credential sent to this backend; "" = forward client headers unchanged

	status         BackendStatus
	inflight       atomic.Int64 // requests currently in flight
	perTokenMs     atomic.Int64 // EWMA of per-token latency (ms/token) — size-independent speed
	autoConcurrent atomic.Int64 // computed by scraper from free VRAM / kv-cache estimate
	mu             sync.RWMutex
}

func (b *Backend) Status() BackendStatus {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.status
}

func (b *Backend) setStatus(s BackendStatus) {
	b.mu.Lock()
	b.status = s
	b.mu.Unlock()
}

func (b *Backend) Inflight() int64 { return b.inflight.Load() }
func (b *Backend) incInflight()    { b.inflight.Add(1) }
func (b *Backend) decInflight()    { b.inflight.Add(-1) }

// effectiveConcurrent returns the concurrency limit in force for this backend.
// Config-supplied maxConcurrent wins; if zero, the scraper-computed value is
// used; if that is also zero the backend is treated as unlimited.
func (b *Backend) effectiveConcurrent() int {
	if b.maxConcurrent > 0 {
		return b.maxConcurrent
	}
	if auto := b.autoConcurrent.Load(); auto > 0 {
		return int(auto)
	}
	return 0
}

// hasCapacity reports whether the backend can accept another request under its
// effective concurrency limit. An unlimited backend always has capacity. This is
// a non-reserving check used by the scheduler to filter candidates; the actual
// slot is claimed atomically by tryReserve to avoid a check-then-act race.
func (b *Backend) hasCapacity() bool {
	limit := b.effectiveConcurrent()
	if limit <= 0 {
		return true
	}
	return b.inflight.Load() < int64(limit)
}

// tryReserve atomically claims one inflight slot if the backend is below its
// effective concurrency limit, returning true on success. It uses a
// compare-and-swap loop so concurrent callers can never push inflight past the
// limit — making max_concurrent a hard cap rather than best-effort. An unlimited
// backend (limit <= 0) always succeeds. Pair every success with decInflight.
func (b *Backend) tryReserve() bool {
	limit := b.effectiveConcurrent()
	if limit <= 0 {
		b.inflight.Add(1)
		return true
	}
	for {
		cur := b.inflight.Load()
		if cur >= int64(limit) {
			return false
		}
		if b.inflight.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

// SetAutoConcurrent stores the scraper-computed concurrency estimate.
func (b *Backend) SetAutoConcurrent(n int64) {
	b.autoConcurrent.Store(n)
}

// RecordPerTokenMs updates the exponentially weighted moving average of per-token
// latency (ms per generated token) using alpha=0.2 (new = 0.8×old + 0.2×sample).
// Per-token rather than per-request so the metric reflects true backend speed and
// is not skewed by how long each answer happened to be. Thread-safe via CAS.
func (b *Backend) RecordPerTokenMs(ms int64) {
	for {
		old := b.perTokenMs.Load()
		var next int64
		if old == 0 {
			next = ms
		} else {
			next = (old*4 + ms) / 5
		}
		if b.perTokenMs.CompareAndSwap(old, next) {
			return
		}
	}
}

// PerTokenMs returns the current EWMA per-token latency in milliseconds.
// Returns 0 if no requests have completed yet.
func (b *Backend) PerTokenMs() int64 {
	return b.perTokenMs.Load()
}

func (b *Backend) supportsModel(model string) bool {
	if len(b.Models) == 0 {
		return true
	}
	// b.Models are pre-normalized in NewBackendPool; model is normalized by PickExcluding.
	return slices.Contains(b.Models, model)
}

func (b *Backend) healthEndpoint() string {
	if b.Type == "vllm" {
		return "/health"
	}
	return "/api/version"
}

func (b *Backend) ping(client *http.Client) bool {
	resp, err := client.Get(b.URL + b.healthEndpoint())
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// BackendPool holds all configured backends and manages their health state.
type BackendPool struct {
	backends []*Backend
	client   *http.Client
}

// NewBackendPool builds the pool. defaultAPIKey is the global backend_api_key:
// any backend that does not set its own api_key inherits it. A backend's
// concurrency is taken from its own max_concurrent; when unset (0) the scraper
// computes one from VRAM, so there is no global concurrency default to inherit.
func NewBackendPool(cfgs []BackendConfig, defaultAPIKey string) *BackendPool {
	p := &BackendPool{
		client: &http.Client{Timeout: 3 * time.Second},
	}
	for _, c := range cfgs {
		typ := c.Type
		if typ == "" {
			typ = "ollama"
		}
		models := make([]string, len(c.Models))
		for i, m := range c.Models {
			models[i] = normalizeModel(m)
		}
		// 0 means "no explicit cap"; the scraper fills in an auto-computed value.
		limit := c.MaxConcurrent
		key := c.APIKey
		if key == "" {
			key = defaultAPIKey
		}
		p.backends = append(p.backends, &Backend{
			Name:          c.Name,
			URL:           c.URL,
			Models:        models,
			Type:          typ,
			VRAMTotalMB:   c.VRAMTotalMB,
			maxConcurrent: limit,
			apiKey:        key,
			status:        StatusDown, // pessimistic start; health loop will bring it up
		})
	}
	return p
}

// HealthLoop pings every backend on the given interval and updates their status.
func (p *BackendPool) HealthLoop(interval time.Duration) {
	for {
		for _, b := range p.backends {
			prev := b.Status()
			if b.ping(p.client) {
				b.setStatus(StatusUp)
				if prev == StatusDown {
					slog.Info("backend up", "backend", b.Name)
				}
			} else {
				b.setStatus(StatusDown)
				if prev == StatusUp {
					slog.Warn("backend down", "backend", b.Name)
				}
			}
		}
		time.Sleep(interval)
	}
}

// Available returns all UP backends that support the given model and have
// capacity remaining under their per-backend concurrency limit.
func (p *BackendPool) Available(model string) []*Backend {
	var out []*Backend
	for _, b := range p.backends {
		if b.Status() == StatusUp && b.supportsModel(model) && b.hasCapacity() {
			out = append(out, b)
		}
	}
	return out
}

func (p *BackendPool) All() []*Backend {
	return p.backends
}

// TotalConcurrency returns the combined concurrency of all UP backends — the sum
// of each one's effective limit. This is the system's true total capacity and is
// what the admission gate uses as its global limit, so the gate engages for
// priority ordering exactly when every backend is full. If any UP backend is
// unlimited (effective limit 0), the total is unlimited and 0 is returned.
func (p *BackendPool) TotalConcurrency() int {
	total := 0
	for _, b := range p.backends {
		if b.Status() != StatusUp {
			continue
		}
		limit := b.effectiveConcurrent()
		if limit <= 0 {
			return 0 // an unlimited backend makes the whole system unlimited
		}
		total += limit
	}
	return total
}
