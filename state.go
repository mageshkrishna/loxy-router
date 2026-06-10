package main

import (
	"sync"
	"time"
)

type ModelStatus string

const (
	ModelWarm ModelStatus = "warm" // model loaded in VRAM, ready to serve immediately
	ModelCold ModelStatus = "cold" // backend up, model not loaded
)

// BackendState holds the last-known LLM-specific state for one backend.
// Populated by the Scraper goroutine; read by the Scheduler.
type BackendState struct {
	ModelStatus   ModelStatus
	LoadedModels  []string // all models currently resident in VRAM (normalized); Ollama can hold several at once
	VRAMUsedBytes int64
	VRAMUsedPct   float64 // 0.0–1.0; only valid when VRAMTotalMB is configured or backend reports it
	QueueDepth    int     // requests waiting, not yet running
	Inflight      int64   // requests currently running
	LastScraped   time.Time
}

// StateStore is a thread-safe map of backend name → BackendState.
type StateStore struct {
	mu     sync.RWMutex
	states map[string]BackendState
}

func NewStateStore() *StateStore {
	return &StateStore{states: make(map[string]BackendState)}
}

func (s *StateStore) Set(name string, state BackendState) {
	s.mu.Lock()
	s.states[name] = state
	s.mu.Unlock()
}

func (s *StateStore) Get(name string) (BackendState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.states[name]
	return st, ok
}

func (s *StateStore) All() map[string]BackendState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]BackendState, len(s.states))
	for k, v := range s.states {
		out[k] = v
	}
	return out
}
