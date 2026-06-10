package main

import (
	"encoding/json"
	"net/http"
	"time"
)

func statusHandler(pool *BackendPool, store *StateStore, gate *Gate) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		type backendEntry struct {
			Name               string    `json:"name"`
			URL                string    `json:"url"`
			Type               string    `json:"type"`
			Status             string    `json:"status"`
			Warm               bool      `json:"warm"`
			LoadedModels       []string  `json:"loaded_models,omitempty"`
			VRAMUsedPct        float64   `json:"vram_used_pct"`
			QueueDepth         int       `json:"queue_depth"`
			Inflight           int64     `json:"inflight"`
			EffectiveConcurrent int      `json:"effective_concurrent"` // 0 = unlimited
			PerTokenMs         int64     `json:"per_token_ms"`
			LastScraped        time.Time `json:"last_scraped,omitempty"`
		}
		type response struct {
			Gate     GateStats      `json:"gate"`
			Backends []backendEntry `json:"backends"`
		}

		resp := response{Gate: gate.Stats()}
		for _, b := range pool.All() {
			entry := backendEntry{
				Name:                b.Name,
				URL:                 b.URL,
				Type:                b.Type,
				Status:              string(b.Status()),
				Inflight:            b.Inflight(),
				EffectiveConcurrent: b.effectiveConcurrent(),
				PerTokenMs:          b.PerTokenMs(),
			}
			if st, ok := store.Get(b.Name); ok {
				entry.Warm = st.ModelStatus == ModelWarm
				entry.LoadedModels = st.LoadedModels
				entry.VRAMUsedPct = st.VRAMUsedPct
				entry.QueueDepth = st.QueueDepth
				entry.LastScraped = st.LastScraped
			}
			resp.Backends = append(resp.Backends, entry)
		}

		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(resp)
	}
}
