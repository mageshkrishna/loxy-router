package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNormalizeModel(t *testing.T) {
	tests := []struct {
		name, in, want string
	}{
		{"adds latest tag", "llama3.2", "llama3.2:latest"},
		{"keeps explicit tag", "llama3.2:1b", "llama3.2:1b"},
		{"lowercases", "Llama3.2:1B", "llama3.2:1b"},
		{"trims space", "  llama3.2:1b  ", "llama3.2:1b"},
		{"empty stays empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeModel(tt.in); got != tt.want {
				t.Errorf("normalizeModel(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestParsePriority(t *testing.T) {
	tests := []struct {
		in   string
		want Priority
	}{
		{"interactive", PriorityInteractive},
		{"batch", PriorityBatch},
		{"background", PriorityBackground},
		{"BATCH", PriorityBatch},
		{"", PriorityInteractive},
		{"nonsense", PriorityInteractive},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			if got := parsePriority(tt.in); got != tt.want {
				t.Errorf("parsePriority(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestGateAdmissionAndPriority(t *testing.T) {
	// One slot; interactive may queue up to 1, batch up to 1.
	var limits [numPriorities]int
	limits[PriorityInteractive] = 1
	limits[PriorityBatch] = 1
	g := NewGate(1, limits)

	bg := context.Background()

	if !g.Acquire(PriorityInteractive, bg) {
		t.Fatal("first acquire should succeed (slot free)")
	}

	// Slot is taken; queue one batch waiter (fills batch queue).
	got := make(chan Priority, 2)
	go func() { g.Acquire(PriorityBatch, bg); got <- PriorityBatch }()
	waitFor(t, func() bool { return g.Stats().Queued["batch"] == 1 })

	// Batch queue is now full (limit 1) — a second batch must be rejected.
	if g.Acquire(PriorityBatch, bg) {
		t.Fatal("batch queue full, acquire should be rejected")
	}

	// Queue one interactive waiter; it must be served before the batch waiter.
	go func() { g.Acquire(PriorityInteractive, bg); got <- PriorityInteractive }()
	waitFor(t, func() bool { return g.Stats().Queued["interactive"] == 1 })

	// Release the held slot — highest priority (interactive) wins.
	g.Release()
	if first := <-got; first != PriorityInteractive {
		t.Fatalf("expected interactive served first, got %v", first)
	}

	// Release again — now the batch waiter proceeds.
	g.Release()
	if second := <-got; second != PriorityBatch {
		t.Fatalf("expected batch served second, got %v", second)
	}
}

func TestGateDisabled(t *testing.T) {
	g := NewGate(0, [numPriorities]int{})
	for i := 0; i < 5; i++ {
		if !g.Acquire(PriorityBackground, context.Background()) {
			t.Fatal("disabled gate must always admit")
		}
	}
	g.Release() // must not panic or block
}

func TestGateContextCancel(t *testing.T) {
	g := NewGate(1, [numPriorities]int{PriorityInteractive: 5})

	// Fill the one slot.
	if !g.Acquire(PriorityInteractive, context.Background()) {
		t.Fatal("first acquire must succeed")
	}

	// Queue a waiter with a cancellable context.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() {
		done <- g.Acquire(PriorityInteractive, ctx)
	}()

	// Wait for it to be in the queue, then cancel.
	waitFor(t, func() bool { return g.Stats().Queued["interactive"] == 1 })
	cancel()

	// The acquire must return false due to context cancellation.
	if result := <-done; result {
		t.Fatal("cancelled acquire must return false")
	}

	// Queue must be empty after cancellation.
	if q := g.Stats().Queued["interactive"]; q != 0 {
		t.Fatalf("queue should be empty after cancel, got %d", q)
	}
}

func TestConversationStoreTTL(t *testing.T) {
	c := NewConversationStore(20 * time.Millisecond)

	if _, ok := c.Get(""); ok {
		t.Error("empty id should never resolve")
	}

	c.Set("conv-1", "ollama-2")
	if got, ok := c.Get("conv-1"); !ok || got != "ollama-2" {
		t.Fatalf("Get(conv-1) = %q,%v; want ollama-2,true", got, ok)
	}

	time.Sleep(30 * time.Millisecond)
	if _, ok := c.Get("conv-1"); ok {
		t.Error("entry should have expired after TTL")
	}
}

func TestPerBackendCapacity(t *testing.T) {
	b := &Backend{maxConcurrent: 2}

	if !b.hasCapacity() {
		t.Fatal("fresh backend must have capacity")
	}
	b.incInflight()
	b.incInflight()
	if b.hasCapacity() {
		t.Fatal("backend at max_concurrent must report no capacity")
	}
	b.decInflight()
	if !b.hasCapacity() {
		t.Fatal("backend below max_concurrent must have capacity again")
	}
}

func TestTryReserve(t *testing.T) {
	b := &Backend{maxConcurrent: 2}

	if !b.tryReserve() {
		t.Fatal("first reservation should succeed under limit 2")
	}
	if !b.tryReserve() {
		t.Fatal("second reservation should succeed under limit 2")
	}
	if b.tryReserve() {
		t.Fatal("third reservation must fail at the limit")
	}
	b.decInflight()
	if !b.tryReserve() {
		t.Fatal("reservation should succeed again after a release")
	}

	// Unlimited backend always reserves.
	u := &Backend{}
	for i := 0; i < 100; i++ {
		if !u.tryReserve() {
			t.Fatal("unlimited backend must always reserve")
		}
	}
}

func TestTryReserveConcurrent(t *testing.T) {
	// Hammer a limit-of-5 backend from many goroutines; inflight must never
	// exceed the limit (the CAS loop is what guarantees this).
	b := &Backend{maxConcurrent: 5}
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b.tryReserve() {
				if got := b.Inflight(); got > 5 {
					t.Errorf("inflight exceeded limit: %d", got)
				}
			}
		}()
	}
	wg.Wait()
	if got := b.Inflight(); got != 5 {
		t.Fatalf("exactly 5 slots should be held, got %d", got)
	}
}

func TestGateInflightStaysConsistent(t *testing.T) {
	// Regression guard for the double-decrement bug: repeatedly queue a waiter
	// and cancel it; inflight must return to a sane state (never negative) and
	// end at 1 (only the held slot remains).
	g := NewGate(1, [numPriorities]int{PriorityInteractive: 100})
	if !g.Acquire(PriorityInteractive, context.Background()) {
		t.Fatal("seed acquire should hold the only slot")
	}

	for i := 0; i < 50; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan bool, 1)
		go func() { done <- g.Acquire(PriorityInteractive, ctx) }()
		waitFor(t, func() bool { return g.Stats().Queued["interactive"] == 1 })
		cancel()
		<-done
		if got := g.Stats().Inflight; got < 0 {
			t.Fatalf("iteration %d: inflight went negative: %d", i, got)
		}
	}
	if got := g.Stats().Inflight; got != 1 {
		t.Fatalf("only the seed slot should remain, got inflight=%d", got)
	}
}

func TestGateReleaseCancelRace(t *testing.T) {
	// Cover the woken-AND-cancelled branch (the double-decrement bug): a queued
	// waiter is woken by Release() at the same instant its context is cancelled.
	// Whichever path the select takes, inflight must settle back to 0 — never -1.
	g := NewGate(1, [numPriorities]int{PriorityInteractive: 1000})

	for i := 0; i < 3000; i++ {
		if !g.Acquire(PriorityInteractive, context.Background()) {
			t.Fatalf("iter %d: seed acquire failed", i)
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			if g.Acquire(PriorityInteractive, ctx) {
				g.Release() // got the slot — give it straight back
			}
			close(done)
		}()
		waitFor(t, func() bool { return g.Stats().Queued["interactive"] == 1 })

		// Cancel FIRST (synchronously) so ctx.Done is already closed, then wake the
		// waiter. Now the waiter's select sees both ctx.Done and ready closed and
		// may take the inner woken-and-cancelled path — the one the bug lived in.
		cancel()
		g.Release() // releases the seed; transfers the slot to the waiter
		<-done

		if inf := g.Stats().Inflight; inf != 0 {
			t.Fatalf("iter %d: inflight should settle to 0, got %d", i, inf)
		}
	}
}

func TestMultiModelWarm(t *testing.T) {
	store := NewStateStore()
	store.Set("ollama-1", BackendState{
		ModelStatus:  ModelWarm,
		LoadedModels: []string{"llama3.2:latest", "mistral:latest"},
	})
	b := &Backend{Name: "ollama-1", Type: "ollama"}
	s := NewScheduler(nil, store, nil)

	if !s.isWarm(b, "mistral:latest") {
		t.Fatal("backend with mistral resident must be warm for mistral (not just the first model)")
	}
	if !s.isWarm(b, "llama3.2:latest") {
		t.Fatal("backend must be warm for the first model too")
	}
	if s.isWarm(b, "codellama:latest") {
		t.Fatal("backend must be cold for a model that is not resident")
	}
}

// TestColdPlacementAvoidsEviction reproduces the 4×A40 GPU finding: a cold
// request must prefer an empty backend over one whose VRAM is mostly occupied
// by another model, otherwise the load evicts a model someone else keeps warm.
func TestColdPlacementAvoidsEviction(t *testing.T) {
	store := NewStateStore()
	// busy holds a 32B model (77% VRAM) warm; empty has nothing resident.
	store.Set("busy", BackendState{ModelStatus: ModelWarm, LoadedModels: []string{"qwen3:32b"}, VRAMUsedPct: 0.77})
	store.Set("empty", BackendState{ModelStatus: ModelCold})
	busy := &Backend{Name: "busy", Type: "ollama"}
	empty := &Backend{Name: "empty", Type: "ollama"}
	s := NewScheduler(nil, store, nil)

	// Both are cold for llama3.1 — the empty backend must score higher even
	// though the busy one comes first in the candidate list.
	got, err := s.pick("llama3.1:8b", []*Backend{busy, empty})
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "empty" {
		t.Fatalf("cold request routed to %q (would evict qwen3:32b); want the empty backend", got.Name)
	}

	// Sanity: for the model the busy backend already holds, it must still win.
	got, err = s.pick("qwen3:32b", []*Backend{busy, empty})
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "busy" {
		t.Fatalf("warm request routed to %q; want the backend already holding the model", got.Name)
	}
}

func TestAutoConcurrent(t *testing.T) {
	const MB = int64(1024 * 1024)
	const GB = 1024 * MB

	tests := []struct {
		name         string
		vramTotalMB  int64
		modelBytes   int64
		isCPU        bool
		wantMin      int64
		wantMax      int64
	}{
		{
			name:        "3B model on A100 40GB GPU",
			vramTotalMB: 40 * 1024,
			modelBytes:  6 * GB,
			isCPU:       false,
			wantMin:     32, wantMax: 32, // capped at 32 (34GB free / 600MB kv = 56, capped)
		},
		{
			name:        "7B Q4 on 8GB GPU",
			vramTotalMB: 8 * 1024,
			modelBytes:  5 * GB,
			isCPU:       false,
			wantMin:     5, wantMax: 7, // ~3GB free / 500MB kv ≈ 6
		},
		{
			name:        "CPU inference halves the value",
			vramTotalMB: 16 * 1024,
			modelBytes:  2 * GB,
			isCPU:       true,
			wantMin:     16, wantMax: 16, // 14GB free / 200MB kv = 70 → cap 32 → halve = 16
		},
		{
			name:        "model barely fits, tiny free VRAM",
			vramTotalMB: 8 * 1024,
			modelBytes:  7800 * MB,
			isCPU:       false,
			wantMin:     1, wantMax: 1, // almost no free VRAM
		},
		{
			name:        "no VRAM config, GPU fallback",
			vramTotalMB: 0,
			modelBytes:  5 * GB,
			isCPU:       false,
			wantMin:     4, wantMax: 4,
		},
		{
			name:        "no VRAM config, CPU fallback",
			vramTotalMB: 0,
			modelBytes:  0,
			isCPU:       true,
			wantMin:     2, wantMax: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Single resident model: occupied VRAM == that model's footprint.
			got := autoConcurrent(tt.vramTotalMB, tt.modelBytes, tt.modelBytes, tt.isCPU)
			if got < tt.wantMin || got > tt.wantMax {
				t.Errorf("autoConcurrent(vram=%dMB, model=%dMB, cpu=%v) = %d, want [%d, %d]",
					tt.vramTotalMB, tt.modelBytes/(1024*1024), tt.isCPU, got, tt.wantMin, tt.wantMax)
			}
		})
	}
}

func TestBackendConcurrency(t *testing.T) {
	// No global default: an explicit max_concurrent is used as-is; an unset
	// backend is unlimited (0) until the scraper computes an auto value.
	pool := NewBackendPool([]BackendConfig{
		{Name: "explicit", URL: "http://a", MaxConcurrent: 8},
		{Name: "auto", URL: "http://b"},
	}, "")

	byName := map[string]*Backend{}
	for _, b := range pool.All() {
		byName[b.Name] = b
	}
	if got := byName["explicit"].effectiveConcurrent(); got != 8 {
		t.Errorf("explicit: want 8, got %d", got)
	}
	if got := byName["auto"].effectiveConcurrent(); got != 0 {
		t.Errorf("auto pre-scrape: want 0 (unlimited until scraped), got %d", got)
	}
	// Once the scraper computes a value, that becomes the effective cap.
	byName["auto"].SetAutoConcurrent(3)
	if got := byName["auto"].effectiveConcurrent(); got != 3 {
		t.Errorf("auto post-scrape: want 3, got %d", got)
	}
}

func TestBackendAPIKeyDefault(t *testing.T) {
	// Global backend_api_key applies unless the backend sets its own.
	pool := NewBackendPool([]BackendConfig{
		{Name: "inherits", URL: "http://a"},
		{Name: "overrides", URL: "http://b", APIKey: "own-key"},
	}, "global-key")

	got := map[string]string{}
	for _, b := range pool.All() {
		got[b.Name] = b.apiKey
	}
	if got["inherits"] != "global-key" {
		t.Errorf("inherits: want global-key, got %q", got["inherits"])
	}
	if got["overrides"] != "own-key" {
		t.Errorf("overrides: want own-key, got %q", got["overrides"])
	}
}

func TestForwardBackendAuth(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	p := &Proxy{client: &http.Client{}}

	// Backend with its own key: client's Authorization is stripped and replaced.
	keyed := &Backend{URL: srv.URL, apiKey: "backend-secret"}
	req := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	req.Header.Set("Authorization", "Bearer client-key")
	resp, err := p.forward(req, keyed, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if gotAuth != "Bearer backend-secret" {
		t.Fatalf("keyed backend should receive its own key, got %q", gotAuth)
	}

	// Backend with no key: client's Authorization passes through unchanged.
	open := &Backend{URL: srv.URL}
	req2 := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	req2.Header.Set("Authorization", "Bearer client-key")
	resp2, err := p.forward(req2, open, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if gotAuth != "Bearer client-key" {
		t.Fatalf("keyless backend should pass client auth through, got %q", gotAuth)
	}
}

func TestTotalConcurrency(t *testing.T) {
	pool := NewBackendPool([]BackendConfig{
		{Name: "a", URL: "http://a", MaxConcurrent: 4}, // 4
		{Name: "b", URL: "http://b", MaxConcurrent: 4}, // 4
		{Name: "c", URL: "http://c", MaxConcurrent: 8}, // 8
	}, "")

	// All start DOWN — no capacity counted.
	if n := pool.TotalConcurrency(); n != 0 {
		t.Fatalf("all-down total: want 0, got %d", n)
	}

	// Bring two up: 4 + 8 = 12.
	pool.All()[0].setStatus(StatusUp)
	pool.All()[2].setStatus(StatusUp)
	if n := pool.TotalConcurrency(); n != 12 {
		t.Fatalf("two-up total: want 12, got %d", n)
	}

	// Third comes up: 4 + 4 + 8 = 16.
	pool.All()[1].setStatus(StatusUp)
	if n := pool.TotalConcurrency(); n != 16 {
		t.Fatalf("all-up total: want 16, got %d", n)
	}
}

func TestGateDynamicLimit(t *testing.T) {
	// A dynamic gate fed by a changing capacity function admits up to the live
	// limit and rejects beyond it (with no queue room configured).
	capacity := 2
	g := NewGate(1, [numPriorities]int{}) // static seed ignored once limitFn is set
	g.SetLimitFunc(func() int { return capacity })
	bg := context.Background()

	if !g.Acquire(PriorityInteractive, bg) {
		t.Fatal("first acquire should succeed (limit 2)")
	}
	if !g.Acquire(PriorityInteractive, bg) {
		t.Fatal("second acquire should succeed (limit 2)")
	}
	// Limit grows to 3 — a third acquire now fits.
	capacity = 3
	if !g.Acquire(PriorityInteractive, bg) {
		t.Fatal("acquire should succeed after limit grew to 3")
	}
	if got := g.Stats().MaxInflight; got != 3 {
		t.Fatalf("Stats.MaxInflight should reflect live limit 3, got %d", got)
	}
}

func TestPerTokenEWMA(t *testing.T) {
	b := &Backend{}

	if b.PerTokenMs() != 0 {
		t.Fatal("fresh backend must return 0")
	}

	b.RecordPerTokenMs(1000)
	if b.PerTokenMs() != 1000 {
		t.Fatalf("first sample: want 1000, got %d", b.PerTokenMs())
	}

	// Second sample: EWMA(1000, 500, alpha=0.2) = (1000*4+500)/5 = 900
	b.RecordPerTokenMs(500)
	if b.PerTokenMs() != 900 {
		t.Fatalf("second sample: want 900, got %d", b.PerTokenMs())
	}
}

func TestTokenCount(t *testing.T) {
	tests := []struct {
		name  string
		feed  []string // fed to observe() in pieces, to exercise chunk boundaries
		want  int
	}{
		{
			name: "ollama streaming uses exact eval_count",
			feed: []string{
				`{"response":"Hel","done":false}` + "\n",
				`{"response":"lo","done":false}` + "\n",
				`{"response":"","done":true,"eval_count":42,"prompt_eval_count":7}` + "\n",
			},
			want: 42,
		},
		{
			name: "ollama non-streaming single body",
			feed: []string{`{"response":"hello there","done":true,"eval_count":3}`},
			want: 3,
		},
		{
			name: "openai SSE with usage uses completion_tokens",
			feed: []string{
				"data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\n",
				"data: {\"choices\":[],\"usage\":{\"completion_tokens\":15}}\n\n",
				"data: [DONE]\n\n",
			},
			want: 15,
		},
		{
			name: "openai SSE without usage falls back to event count",
			feed: []string{
				"data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n",
				"data: {\"choices\":[{\"delta\":{\"content\":\"b\"}}]}\n\n",
				"data: [DONE]\n\n",
			},
			want: 3, // 6 newlines / 2 events
		},
		{
			name: "empty body never returns zero",
			feed: []string{""},
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := &tokenCounter{}
			for _, chunk := range tt.feed {
				tc.observe([]byte(chunk))
			}
			if got := tc.count(); got != tt.want {
				t.Errorf("count() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestPerTokenDecodeLatency(t *testing.T) {
	// The whole point of the fix: per-token latency must reflect DECODE speed and
	// ignore one-time costs (model load / prompt / queue), which live in wall-clock.
	t.Run("ollama eval_duration ignores wall-clock", func(t *testing.T) {
		tc := &tokenCounter{}
		// 450ms of decode over 9 tokens = 50 ms/token, regardless of elapsed time.
		tc.observe([]byte(`{"done":true,"eval_count":9,"eval_duration":450000000}`))
		// Pretend the first byte arrived 5s ago (a cold load would do this) — it
		// must NOT leak into the figure.
		res := tc.result(time.Now().Add(-5 * time.Second))
		if res.tokens != 9 {
			t.Fatalf("tokens: want 9, got %d", res.tokens)
		}
		if res.perTokenMs != 50 {
			t.Fatalf("per-token: want 50 (decode only), got %d", res.perTokenMs)
		}
	})

	t.Run("sub-ms decode clamps to 1, never falls back", func(t *testing.T) {
		tc := &tokenCounter{}
		tc.observe([]byte(`{"done":true,"eval_count":10,"eval_duration":1000000}`)) // 0.1 ms/token
		res := tc.result(time.Now().Add(-5 * time.Second))
		if res.perTokenMs != 1 {
			t.Fatalf("per-token: want 1 (clamped), got %d", res.perTokenMs)
		}
	})

	t.Run("streamed non-ollama uses first-token→last window", func(t *testing.T) {
		tc := &tokenCounter{}
		// Two SSE chunks → genuinely streamed (chunks > 1), 2 events ≈ 2 tokens.
		tc.observe([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n"))
		tc.observe([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"b\"}}]}\n\n"))
		res := tc.result(time.Now().Add(-200 * time.Millisecond)) // ~200ms window
		// ~200ms / 2 tokens ≈ 100ms; allow scheduling slack.
		if res.perTokenMs < 80 || res.perTokenMs > 140 {
			t.Fatalf("per-token: want ~100 from window, got %d", res.perTokenMs)
		}
	})

	t.Run("single-shot body yields no decode signal", func(t *testing.T) {
		tc := &tokenCounter{}
		// One chunk, no eval_duration (e.g. vLLM stream:false) → can't measure
		// decode; perTokenMs stays 0 so the caller falls back to wall-clock.
		tc.observe([]byte(`{"usage":{"completion_tokens":12}}`))
		res := tc.result(time.Now())
		if res.perTokenMs != 0 {
			t.Fatalf("per-token: want 0 (no signal), got %d", res.perTokenMs)
		}
	})
}

func TestMetricsEndpoint(t *testing.T) {
	pool := NewBackendPool([]BackendConfig{
		{Name: "b1", URL: "http://x", MaxConcurrent: 4},
	}, "")
	pool.All()[0].setStatus(StatusUp)
	pool.All()[0].RecordPerTokenMs(37)

	store := NewStateStore()
	store.Set("b1", BackendState{ModelStatus: ModelWarm, VRAMUsedPct: 0.5, QueueDepth: 2})

	gate := NewGate(0, [numPriorities]int{})
	gate.SetLimitFunc(pool.TotalConcurrency) // → 4
	if !gate.Acquire(PriorityInteractive, context.Background()) {
		t.Fatal("acquire should succeed")
	}

	metrics := NewMetrics()
	metrics.IncRequest("b1", 200)
	metrics.IncRequest("b1", 200)
	metrics.IncRequest("b1", 503)

	rec := httptest.NewRecorder()
	metricsHandler(pool, store, gate, metrics).ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()

	want := []string{
		`scheduler_backend_per_token_ms{backend="b1"} 37`,
		`scheduler_gate_max_inflight 4`,
		`scheduler_gate_inflight 1`,
		`scheduler_gate_queued{priority="interactive"} 0`,
		`scheduler_requests_total{backend="b1",status="200"} 2`,
		`scheduler_requests_total{backend="b1",status="503"} 1`,
	}
	for _, w := range want {
		if !strings.Contains(body, w) {
			t.Errorf("metrics output missing line:\n  %s\n--- full body ---\n%s", w, body)
		}
	}
}

func TestScrapeVLLMMetrics(t *testing.T) {
	// vLLM ≥0.10 renamed gpu_cache_usage_perc → kv_cache_usage_perc. The scraper
	// must read the current name (and still accept the old one) or the VRAM guard
	// and load signals silently read 0 for every vLLM backend.
	body := `# HELP vllm:kv_cache_usage_perc GPU KV-cache usage
# TYPE vllm:kv_cache_usage_perc gauge
vllm:kv_cache_usage_perc{engine="0",model_name="Qwen/Qwen2.5-0.5B-Instruct"} 0.42
vllm:num_requests_running{engine="0"} 3.0
vllm:num_requests_waiting{engine="0"} 5.0
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	pool := NewBackendPool([]BackendConfig{{Name: "v", URL: srv.URL, Type: "vllm"}}, "")
	s := NewScraper(pool, NewStateStore())

	var state BackendState
	if err := s.scrapeVLLMMetrics(pool.All()[0], &state); err != nil {
		t.Fatalf("scrapeVLLMMetrics: %v", err)
	}
	if state.VRAMUsedPct != 0.42 {
		t.Errorf("VRAMUsedPct: want 0.42 from kv_cache_usage_perc, got %v", state.VRAMUsedPct)
	}
	if state.Inflight != 3 {
		t.Errorf("Inflight: want 3, got %d", state.Inflight)
	}
	if state.QueueDepth != 5 {
		t.Errorf("QueueDepth: want 5, got %d", state.QueueDepth)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}
