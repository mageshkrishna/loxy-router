package main

import (
	"context"
	"math"
	"strings"
	"sync"
)

// Priority orders requests competing for limited backend capacity. Lower values
// are served first.
type Priority int

const (
	PriorityInteractive Priority = iota // a user is waiting on a response
	PriorityBatch                       // a job that can tolerate queueing
	PriorityBackground                  // best-effort; first to be shed
	numPriorities
)

func parsePriority(s string) Priority {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "batch":
		return PriorityBatch
	case "background":
		return PriorityBackground
	default:
		return PriorityInteractive
	}
}

func (p Priority) String() string {
	switch p {
	case PriorityBatch:
		return "batch"
	case PriorityBackground:
		return "background"
	default:
		return "interactive"
	}
}

// Gate bounds the number of requests dispatched to backends concurrently and,
// when at capacity, serves waiting requests in priority order. Each tier has a
// queue-length limit; exceeding it rejects the request (admission control) so an
// overloaded scheduler fails fast instead of letting latency grow without bound.
//
// A maxInflight <= 0 disables the gate entirely: Acquire always admits and
// Release is a no-op. This preserves the pre-Phase-3 behavior when unconfigured.
type Gate struct {
	maxInflight int            // static limit; used when limitFn is nil (tests)
	limitFn     func() int     // dynamic limit: live sum of backend capacity (preferred)
	limits      [numPriorities]int

	mu       sync.Mutex
	inflight int
	queues   [numPriorities][]chan struct{}
}

func NewGate(maxInflight int, limits [numPriorities]int) *Gate {
	return &Gate{maxInflight: maxInflight, limits: limits}
}

// SetLimitFunc makes the gate's global limit dynamic: it is recomputed on every
// Acquire/Release from fn (the live sum of all backend limits). This keeps the
// gate's capacity in lock-step with the backends — adding a GPU or losing one
// adjusts the limit automatically.
func (g *Gate) SetLimitFunc(fn func() int) { g.limitFn = fn }

// limit returns the current global limit. <= 0 means the gate is disabled.
//
// A dynamic gate (limitFn set) is always considered enabled: if the live sum is
// momentarily 0 (e.g. every backend is briefly DOWN) it returns a very large
// number rather than 0, so a request admitted earlier still gets its slot
// released. Without this, inflight tracking could desync when capacity flips to
// zero between a request's Acquire and Release.
func (g *Gate) limit() int {
	if g.limitFn != nil {
		if n := g.limitFn(); n > 0 {
			return n
		}
		return math.MaxInt32
	}
	return g.maxInflight
}

// Acquire blocks until a dispatch slot is free, then returns true. It returns
// false immediately if the caller's priority queue is already full, or if ctx
// is cancelled while the request is waiting (e.g. client disconnected).
func (g *Gate) Acquire(p Priority, ctx context.Context) bool {
	limit := g.limit()
	if limit <= 0 {
		return true
	}

	g.mu.Lock()

	// Fast path: capacity is free and nobody is already waiting ahead of us.
	if g.inflight < limit && g.totalQueued() == 0 {
		g.inflight++
		g.mu.Unlock()
		return true
	}

	// Admission control: reject when this tier's queue is full.
	if g.limits[p] > 0 && len(g.queues[p]) >= g.limits[p] {
		g.mu.Unlock()
		return false
	}

	ready := make(chan struct{})
	g.queues[p] = append(g.queues[p], ready)
	g.mu.Unlock()

	select {
	case <-ready:
		// Release transferred the slot to us.
		return true
	case <-ctx.Done():
		// Client disconnected — remove ourselves from the queue so the slot
		// isn't phantom-held. Guard against a simultaneous Release() that may
		// have already closed ready.
		g.mu.Lock()
		select {
		case <-ready:
			// We were woken just as the context was cancelled — we now own the
			// slot. Release() alone returns it correctly (it decrements inflight
			// and hands the slot to the next waiter). Do NOT also decrement here,
			// or the slot is double-counted and inflight drifts negative.
			g.mu.Unlock()
			g.Release()
		default:
			// Not yet woken; remove from queue.
			for i, ch := range g.queues[p] {
				if ch == ready {
					g.queues[p] = append(g.queues[p][:i], g.queues[p][i+1:]...)
					break
				}
			}
			g.mu.Unlock()
		}
		return false
	}
}

// Release returns a slot and hands it to the highest-priority waiter, if any.
func (g *Gate) Release() {
	if g.limit() <= 0 {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	g.inflight--
	for p := Priority(0); p < numPriorities; p++ {
		if len(g.queues[p]) > 0 {
			ready := g.queues[p][0]
			g.queues[p] = g.queues[p][1:]
			g.inflight++ // transfer the slot to the woken waiter
			close(ready)
			return
		}
	}
}

func (g *Gate) totalQueued() int {
	n := 0
	for p := Priority(0); p < numPriorities; p++ {
		n += len(g.queues[p])
	}
	return n
}

// GateStats is a point-in-time snapshot for the /status endpoint.
type GateStats struct {
	MaxInflight int            `json:"max_inflight"`
	Inflight    int            `json:"inflight"`
	Queued      map[string]int `json:"queued"`
}

func (g *Gate) Stats() GateStats {
	// Report the raw limit for display: the live sum (or static value), without
	// the MaxInt32 "momentarily unbounded" sentinel that limit() substitutes.
	maxInflight := g.maxInflight
	if g.limitFn != nil {
		maxInflight = g.limitFn()
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	return GateStats{
		MaxInflight: maxInflight,
		Inflight:    g.inflight,
		Queued: map[string]int{
			PriorityInteractive.String(): len(g.queues[PriorityInteractive]),
			PriorityBatch.String():       len(g.queues[PriorityBatch]),
			PriorityBackground.String():  len(g.queues[PriorityBackground]),
		},
	}
}
