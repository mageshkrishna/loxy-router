package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestProxyConcurrentLoad drives the whole request path — admission gate,
// scheduler pick, atomic slot reservation, and streaming — from many goroutines
// at once. The existing unit tests cover each piece in isolation; this is the
// only test that proves they compose under real concurrency without deadlocking,
// double-counting inflight, or breaching the per-backend hard cap. Run under -race
// it also guards the shared counters along that path.
func TestProxyConcurrentLoad(t *testing.T) {
	const (
		backendCap = 4
		total      = 200
	)

	// The mock backend records the peak number of handlers running at once, so we
	// can assert the cap was a hard ceiling and not a best-effort hint.
	var inflight, peak int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt64(&inflight, 1)
		for {
			old := atomic.LoadInt64(&peak)
			if n <= old || atomic.CompareAndSwapInt64(&peak, old, n) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond) // hold the slot so requests genuinely overlap
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"response":"ok","done":true,"eval_count":3,"eval_duration":3000000}`)
		atomic.AddInt64(&inflight, -1)
	}))
	defer srv.Close()

	pool := NewBackendPool([]BackendConfig{
		{Name: "b", URL: srv.URL, MaxConcurrent: backendCap},
	}, "")
	b := pool.All()[0]
	b.setStatus(StatusUp)

	store := NewStateStore()
	store.Set("b", BackendState{ModelStatus: ModelWarm})
	conv := NewConversationStore(time.Minute)

	// Gate limit follows total backend capacity (as in main), and the queues are
	// deep enough to hold every waiter — so all requests should be served, none
	// rejected, exercising the queue→admit handoff under load.
	deep := [numPriorities]int{PriorityInteractive: total, PriorityBatch: total, PriorityBackground: total}
	gate := NewGate(0, deep)
	gate.SetLimitFunc(pool.TotalConcurrency)
	sched := NewScheduler(pool, store, conv)
	proxy := NewProxy(sched, gate, NewMetrics(), 5*time.Second)

	var ok, other int64
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest("POST", "/v1/chat/completions",
				strings.NewReader(`{"model":"llama3.2:1b","messages":[]}`))
			rec := httptest.NewRecorder()
			proxy.ServeHTTP(rec, req)
			if rec.Code == http.StatusOK {
				atomic.AddInt64(&ok, 1)
			} else {
				atomic.AddInt64(&other, 1)
			}
		}()
	}
	wg.Wait()

	if peak > backendCap {
		t.Fatalf("per-backend hard cap breached: peak concurrent handlers = %d, cap = %d", peak, backendCap)
	}
	if ok != total {
		t.Fatalf("all requests should succeed with a deep queue: ok=%d other=%d total=%d", ok, other, total)
	}
	if got := b.Inflight(); got != 0 {
		t.Fatalf("backend inflight must settle to 0 after drain, got %d", got)
	}
	if got := gate.Stats().Inflight; got != 0 {
		t.Fatalf("gate inflight must settle to 0 after drain, got %d", got)
	}
}

// TestProxyShallowQueueRejects confirms backpressure: when the queue is too small
// to hold the burst, excess requests are rejected fast with 503 rather than
// piling up unbounded, and every request is accounted for (served or rejected)
// with the cap still never breached.
func TestProxyShallowQueueRejects(t *testing.T) {
	const (
		backendCap = 2
		total      = 100
	)

	var inflight, peak int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt64(&inflight, 1)
		for {
			old := atomic.LoadInt64(&peak)
			if n <= old || atomic.CompareAndSwapInt64(&peak, old, n) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"response":"ok","done":true,"eval_count":1,"eval_duration":1000000}`)
		atomic.AddInt64(&inflight, -1)
	}))
	defer srv.Close()

	pool := NewBackendPool([]BackendConfig{
		{Name: "b", URL: srv.URL, MaxConcurrent: backendCap},
	}, "")
	pool.All()[0].setStatus(StatusUp)

	store := NewStateStore()
	store.Set("b", BackendState{ModelStatus: ModelWarm})
	// Shallow queues: only a handful of waiters allowed before 503.
	shallow := [numPriorities]int{PriorityInteractive: 5, PriorityBatch: 5, PriorityBackground: 5}
	gate := NewGate(0, shallow)
	gate.SetLimitFunc(pool.TotalConcurrency)
	sched := NewScheduler(pool, store, NewConversationStore(time.Minute))
	proxy := NewProxy(sched, gate, NewMetrics(), 5*time.Second)

	var ok, rejected, unexpected int64
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest("POST", "/v1/chat/completions",
				strings.NewReader(`{"model":"llama3.2:1b","messages":[]}`))
			rec := httptest.NewRecorder()
			proxy.ServeHTTP(rec, req)
			switch rec.Code {
			case http.StatusOK:
				atomic.AddInt64(&ok, 1)
			case http.StatusServiceUnavailable:
				atomic.AddInt64(&rejected, 1)
			default:
				atomic.AddInt64(&unexpected, 1)
			}
		}()
	}
	wg.Wait()

	if unexpected != 0 {
		t.Fatalf("unexpected (non-200, non-503) responses: %d", unexpected)
	}
	if peak > backendCap {
		t.Fatalf("per-backend hard cap breached under overload: peak = %d, cap = %d", peak, backendCap)
	}
	if ok+rejected != total {
		t.Fatalf("every request must be accounted for: ok=%d + rejected=%d != total=%d", ok, rejected, total)
	}
	if rejected == 0 {
		t.Fatal("a burst far exceeding capacity+queue must produce some 503 rejections")
	}
	if got := pool.All()[0].Inflight(); got != 0 {
		t.Fatalf("backend inflight must settle to 0 after drain, got %d", got)
	}
}
