package main

import (
	"sync"
	"time"
)

// ConversationStore remembers which backend last served a given conversation so
// follow-up turns route back to the backend that still holds the conversation's
// KV cache — avoiding a full re-prefill of the prompt history.
//
// Entries expire after a TTL: once a model is evicted (or the conversation goes
// idle long enough), the cache is gone and the affinity is worthless, so we let
// it lapse rather than pin traffic to a stale choice.
type ConversationStore struct {
	ttl     time.Duration
	mu      sync.Mutex
	entries map[string]conversationEntry
}

type conversationEntry struct {
	backend string
	seen    time.Time
}

func NewConversationStore(ttl time.Duration) *ConversationStore {
	return &ConversationStore{ttl: ttl, entries: make(map[string]conversationEntry)}
}

// Get returns the backend last associated with id, or ("", false) if there is
// no live entry. An expired entry is treated as absent.
func (c *ConversationStore) Get(id string) (string, bool) {
	if id == "" {
		return "", false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[id]
	if !ok || time.Since(e.seen) > c.ttl {
		return "", false
	}
	return e.backend, true
}

// Set records that backend served conversation id, refreshing its TTL.
func (c *ConversationStore) Set(id, backend string) {
	if id == "" {
		return
	}
	c.mu.Lock()
	c.entries[id] = conversationEntry{backend: backend, seen: time.Now()}
	c.mu.Unlock()
}

// Reap drops expired entries. Call periodically to bound memory; correctness
// does not depend on it because Get already ignores expired entries.
func (c *ConversationStore) Reap() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, e := range c.entries {
		if time.Since(e.seen) > c.ttl {
			delete(c.entries, id)
		}
	}
}

// ReapLoop runs Reap on the given interval. Launch as a goroutine.
func (c *ConversationStore) ReapLoop(interval time.Duration) {
	for {
		time.Sleep(interval)
		c.Reap()
	}
}
