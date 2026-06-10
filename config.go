package main

import "time"

// Config is the top-level YAML configuration.
type Config struct {
	Listen   string          `yaml:"listen"`
	Backends []BackendConfig `yaml:"backends"`

	// APIKeys, when non-empty, requires every proxy/status request to present
	// a matching "Authorization: Bearer <key>". Empty means auth is disabled.
	// This guards the INBOUND door (client → scheduler).
	APIKeys []string `yaml:"api_keys"`

	// BackendAPIKey is the default credential the scheduler presents to EACH
	// backend (OUTBOUND door: scheduler → backend). A backend that sets its own
	// api_key overrides this. When set for a backend, the scheduler strips the
	// client's Authorization and sends this key instead, so client credentials
	// never leak downstream. Empty leaves backend requests unauthenticated
	// (correct for default Ollama, which has no auth).
	BackendAPIKey string `yaml:"backend_api_key"`

	// Queue sets per-priority queue-length limits used by admission control.
	Queue QueueConfig `yaml:"queue"`

	// Timeouts bound the network deadlines that protect against hung connections.
	Timeouts TimeoutConfig `yaml:"timeouts"`
}

// TimeoutConfig tunes the deadlines that catch a dead or wedged backend without
// truncating legitimate long-running LLM responses. Zero in any field selects
// the default.
type TimeoutConfig struct {
	// ResponseHeaderSecs bounds how long to wait for a backend to send response
	// headers after the request is written. It must comfortably exceed a cold
	// model load (~25s) yet still catch a backend gone silent. It does NOT limit
	// streaming once headers arrive, so token streams may run for minutes. A
	// non-streaming request that computes for longer than this before sending
	// any headers will be cut off; raise it if you serve those.
	ResponseHeaderSecs int `yaml:"response_header_secs"`

	// ShutdownSecs bounds how long graceful shutdown waits for in-flight requests
	// to drain before forcing exit.
	ShutdownSecs int `yaml:"shutdown_secs"`
}

const (
	defaultResponseHeaderSecs = 120
	defaultShutdownSecs       = 30
)

// responseHeaderTimeout is the backend response-header deadline, with the default
// applied when unset.
func (c Config) responseHeaderTimeout() time.Duration {
	s := c.Timeouts.ResponseHeaderSecs
	if s <= 0 {
		s = defaultResponseHeaderSecs
	}
	return time.Duration(s) * time.Second
}

// shutdownTimeout is the graceful-drain deadline, with the default applied when unset.
func (c Config) shutdownTimeout() time.Duration {
	s := c.Timeouts.ShutdownSecs
	if s <= 0 {
		s = defaultShutdownSecs
	}
	return time.Duration(s) * time.Second
}

// QueueConfig bounds how many requests may wait per priority tier before new
// arrivals of that tier are rejected. The gate (and thus these limits) engages
// automatically whenever every backend has a finite concurrency cap.
type QueueConfig struct {
	Interactive int `yaml:"interactive"`
	Batch       int `yaml:"batch"`
	Background  int `yaml:"background"`
}

type BackendConfig struct {
	Name          string   `yaml:"name"`
	URL           string   `yaml:"url"`
	Models        []string `yaml:"models"`
	Type          string   `yaml:"type"`           // "ollama" (default) or "vllm"
	VRAMTotalMB   int64    `yaml:"vram_total_mb"`  // optional: total GPU VRAM in MB, enables VRAM guard for Ollama
	MaxConcurrent int      `yaml:"max_concurrent"` // optional: hard concurrency cap for this backend; 0 = auto-compute from VRAM
	APIKey        string   `yaml:"api_key"`        // optional: credential sent to this backend; overrides backend_api_key; "" = inherit global
}

// queueLimits returns the per-priority limits, applying sensible defaults for any
// tier left unset. The gate engages on its own once every backend is capped.
func (c Config) queueLimits() [numPriorities]int {
	q := c.Queue
	if q.Interactive == 0 {
		q.Interactive = 50
	}
	if q.Batch == 0 {
		q.Batch = 200
	}
	if q.Background == 0 {
		q.Background = 100
	}
	var limits [numPriorities]int
	limits[PriorityInteractive] = q.Interactive
	limits[PriorityBatch] = q.Batch
	limits[PriorityBackground] = q.Background
	return limits
}
