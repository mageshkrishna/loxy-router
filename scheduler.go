package main

import (
	"errors"
	"fmt"
	"strings"
)

var ErrNoBackend = errors.New("no available backend")

// normalizeModel lowercases the name and appends ":latest" when no tag is present,
// so "llama3.2" and "llama3.2:latest" resolve to the same backend.
func normalizeModel(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	if name != "" && !strings.Contains(name, ":") {
		name += ":latest"
	}
	return name
}

// vramGuardThreshold: skip backends using more than this fraction of VRAM.
// Only applied when the backend reports a VRAMUsedPct > 0.
const vramGuardThreshold = 0.90

// coldLoadMs is the estimated penalty (ms) for loading a model from disk.
// A cold backend must pay this cost before serving the first token.
// 25 seconds is a conservative estimate for a 7B model on SSD.
const coldLoadMs = 25_000

// defaultPerTokenMs is the assumed per-token latency when a backend has no
// history yet (~20 ms/token ≈ 50 tokens/sec, typical for a local model).
const defaultPerTokenMs = 20

// expectedTokens is the assumed answer length used to turn a per-token latency
// into an estimated full-request duration. It only needs to be roughly right —
// it scales every backend equally, so it affects the warm-vs-cold tradeoff
// (how big a queue makes a cold backend worth it), not which warm backend wins.
const expectedTokens = 256

type Scheduler struct {
	pool   *BackendPool
	store  *StateStore
	conv   *ConversationStore
	plugin SchedulerPlugin // optional — nil means built-in logic only
}

func NewScheduler(pool *BackendPool, store *StateStore, conv *ConversationStore, plugins ...SchedulerPlugin) *Scheduler {
	s := &Scheduler{pool: pool, store: store, conv: conv}
	if len(plugins) > 0 {
		s.plugin = plugins[0]
	}
	return s
}

// Pick returns the best backend for the request.
func (s *Scheduler) Pick(req clientRequest) (*Backend, error) {
	return s.PickExcluding(req, nil)
}

// PickExcluding is like Pick but skips backends in the exclude set.
// The proxy uses this to retry on a different backend after a connection failure.
//
// Routing order:
//  1. Plugin override (if registered)
//  2. Session affinity — stick to the backend holding this conversation's KV cache
//     (only when that backend is still warm and VRAM-safe)
//  3. Weighted score — every remaining safe backend gets a score that models
//     expected time-to-first-token; highest score wins.
//
// Score formula: 1 / expectedMs, where reqMs = expectedTokens × perTokenMs:
//
//	expectedMs = (load+1) × reqMs          for warm backends
//	expectedMs = coldLoadMs + load × reqMs  for cold backends
//
// perTokenMs is the EWMA of per-token latency, so a backend that served long
// answers is not mistaken for a slow one. This naturally prefers warm-and-lightly-
// loaded backends but routes to a cold-but-idle backend when the warm one is
// overloaded enough that the swap penalty is cheaper than waiting in queue.
func (s *Scheduler) PickExcluding(req clientRequest, exclude map[string]bool) (*Backend, error) {
	model := normalizeModel(req.model)

	all := s.pool.Available(model)
	candidates := all
	if len(exclude) > 0 {
		candidates = make([]*Backend, 0, len(all))
		for _, b := range all {
			if !exclude[b.Name] {
				candidates = append(candidates, b)
			}
		}
	}

	if len(candidates) == 0 {
		return nil, fmt.Errorf("%w for model %q", ErrNoBackend, model)
	}

	if s.plugin != nil {
		name, err := s.plugin.Pick(model, candidates)
		if err != nil {
			return nil, err
		}
		if name != "" {
			for _, b := range candidates {
				if b.Name == name {
					return b, nil
				}
			}
		}
	}

	// Session affinity: route back to the sticky backend only when it still has
	// the model warm and is VRAM-safe. A cold backend's KV cache is already gone.
	if b := s.affinityPick(req, model, candidates); b != nil {
		return b, nil
	}

	return s.pick(model, candidates)
}

// affinityPick returns the sticky backend for req's conversation if it is among
// candidates, warm for the model, and VRAM-safe; otherwise nil.
func (s *Scheduler) affinityPick(req clientRequest, model string, candidates []*Backend) *Backend {
	if s.conv == nil || req.conversationID == "" {
		return nil
	}
	name, ok := s.conv.Get(req.conversationID)
	if !ok {
		return nil
	}
	for _, b := range candidates {
		if b.Name == name && s.isWarm(b, model) && s.isSafe(b) {
			return b
		}
	}
	return nil
}

// RecordConversation remembers that backend served req's conversation, so the
// next turn can route back to it. A no-op when affinity is unused.
func (s *Scheduler) RecordConversation(req clientRequest, backend string) {
	if s.conv != nil {
		s.conv.Set(req.conversationID, backend)
	}
}

func (s *Scheduler) pick(model string, candidates []*Backend) (*Backend, error) {
	// Filter out backends near VRAM limit to avoid OOM crashes.
	safe := s.applyVRAMGuard(candidates)
	if len(safe) == 0 {
		return nil, fmt.Errorf("%w: all backends near VRAM limit (≥%.0f%%)", ErrNoBackend, vramGuardThreshold*100)
	}

	return s.bestScore(model, safe), nil
}

// bestScore picks the backend with the highest expected-throughput score.
func (s *Scheduler) bestScore(model string, backends []*Backend) *Backend {
	var best *Backend
	var bestScore float64
	for _, b := range backends {
		sc := s.score(b, model)
		if best == nil || sc > bestScore {
			best = b
			bestScore = sc
		}
	}
	return best
}

// score computes an expected-TTFT-based score for a backend.
// Higher score = better choice. Returns 1/expectedMs so that lower expected
// latency maps to a higher score.
//
// The warmth vs load tradeoff is handled automatically: a warm-but-overloaded
// backend accumulates enough expected wait time that a cold-but-idle backend
// scores higher once the queue grows past ~(coldLoadMs / avgResponseMs) requests.
func (s *Scheduler) score(b *Backend, model string) float64 {
	st, _ := s.store.Get(b.Name)

	perTok := b.PerTokenMs()
	if perTok == 0 {
		perTok = defaultPerTokenMs
	}
	// Estimated time for one full request: assumed answer length × per-token speed.
	// Using per-token latency here means a backend that drew long answers is not
	// mistaken for a slow one.
	reqMs := float64(expectedTokens) * float64(perTok)

	// Use whichever load measure is larger: our inflight counter (reliable for
	// Ollama) or the queue depth reported by the backend (accurate for vLLM).
	load := int64(st.QueueDepth)
	if inf := b.Inflight(); inf > load {
		load = inf
	}

	var expectedMs float64
	if s.isWarm(b, model) {
		// Warm: pay (load+1) × reqMs — the +1 accounts for this new request.
		expectedMs = float64(load+1) * reqMs
	} else {
		// Cold: pay the swap penalty up front, then queue behind existing load.
		// The penalty scales with the target's VRAM usage: loading onto a fuller
		// backend risks evicting a model someone else keeps warm (and paying its
		// reload later), so among cold candidates an empty backend wins. Loading
		// where another model is resident costs up to 2× the base penalty.
		expectedMs = coldLoadMs*(1+st.VRAMUsedPct) + float64(load)*reqMs
	}

	return 1.0 / expectedMs
}

// applyVRAMGuard filters out backends that are above the VRAM threshold.
// Backends with no VRAM data (VRAMUsedPct == 0) always pass.
func (s *Scheduler) applyVRAMGuard(backends []*Backend) []*Backend {
	out := make([]*Backend, 0, len(backends))
	for _, b := range backends {
		if s.isSafe(b) {
			out = append(out, b)
		}
	}
	return out
}

// isSafe reports whether the backend is below the VRAM guard threshold.
// A backend with no VRAM data (VRAMUsedPct == 0) is treated as safe.
func (s *Scheduler) isSafe(b *Backend) bool {
	st, ok := s.store.Get(b.Name)
	return !(ok && st.VRAMUsedPct > 0 && st.VRAMUsedPct >= vramGuardThreshold)
}

// isWarm reports whether the backend currently has the requested model loaded.
//   - vLLM: always warm (single model, loaded at startup)
//   - Ollama: warm only when /api/ps shows the model in VRAM
func (s *Scheduler) isWarm(b *Backend, model string) bool {
	if b.Type == "vllm" {
		return true
	}
	st, ok := s.store.Get(b.Name)
	if !ok || st.ModelStatus != ModelWarm {
		return false
	}
	// A backend may hold several models resident at once — warm if any matches.
	for _, m := range st.LoadedModels {
		if m == model {
			return true
		}
	}
	return false
}
