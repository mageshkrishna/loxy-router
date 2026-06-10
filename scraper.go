package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Scraper polls each backend on a fixed interval and writes LLM-specific
// state (model warmth, VRAM usage, queue depth) into the StateStore.
type Scraper struct {
	pool   *BackendPool
	store  *StateStore
	client *http.Client
}

func NewScraper(pool *BackendPool, store *StateStore) *Scraper {
	return &Scraper{
		pool:   pool,
		store:  store,
		client: &http.Client{Timeout: 3 * time.Second},
	}
}

// Run starts a polling loop. Call as a goroutine.
func (s *Scraper) Run(interval time.Duration) {
	for {
		for _, b := range s.pool.All() {
			go s.scrape(b)
		}
		time.Sleep(interval)
	}
}

func (s *Scraper) scrape(b *Backend) {
	var (
		state BackendState
		err   error
	)
	switch b.Type {
	case "vllm":
		state, err = s.scrapeVLLM(b)
	default:
		state, err = s.scrapeOllama(b)
	}
	if err != nil {
		slog.Warn("scrape failed", "backend", b.Name, "err", err.Error())
		return
	}
	state.LastScraped = time.Now()
	s.store.Set(b.Name, state)
}

// ── Ollama ────────────────────────────────────────────────────────────────────

type ollamaPSResponse struct {
	Models []struct {
		Name     string `json:"name"`
		Size     int64  `json:"size"`
		SizeVRAM int64  `json:"size_vram"`
	} `json:"models"`
}

func (s *Scraper) scrapeOllama(b *Backend) (BackendState, error) {
	resp, err := s.client.Get(b.URL + "/api/ps")
	if err != nil {
		return BackendState{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return BackendState{}, err
	}

	var ps ollamaPSResponse
	if err := json.Unmarshal(body, &ps); err != nil {
		return BackendState{}, err
	}

	state := BackendState{
		ModelStatus: ModelCold,
		Inflight:    b.Inflight(),
	}

	// A backend with no configured cap must never be left unlimited: an unlimited
	// backend makes TotalConcurrency report 0, which disables the admission gate
	// for the whole system. When nothing is resident yet (so we can't size from a
	// model) seed a conservative baseline — but only if we've never computed one,
	// so a real value learned while warm survives the model later unloading.
	if b.maxConcurrent == 0 && len(ps.Models) == 0 && b.autoConcurrent.Load() == 0 {
		b.SetAutoConcurrent(defaultColdConcurrent)
	}

	if len(ps.Models) > 0 {
		state.ModelStatus = ModelWarm

		// Ollama can keep several models resident at once (OLLAMA_MAX_LOADED_MODELS).
		// Record them all and sum their footprint, so warmth and VRAM are correct
		// rather than reflecting just the first model.
		var sumVRAM, sumSize, largest int64
		for _, m := range ps.Models {
			state.LoadedModels = append(state.LoadedModels, normalizeModel(m.Name))
			sumVRAM += m.SizeVRAM
			sumSize += m.Size
			// per-model footprint: VRAM when on GPU, total size on CPU.
			footprint := m.SizeVRAM
			if footprint == 0 {
				footprint = m.Size
			}
			if footprint > largest {
				largest = footprint
			}
		}

		// size_vram is 0 on CPU-only systems; fall back to total size (RAM usage).
		isCPU := sumVRAM == 0
		usedBytes := sumVRAM
		if usedBytes == 0 {
			usedBytes = sumSize
		}
		state.VRAMUsedBytes = usedBytes

		if b.VRAMTotalMB > 0 {
			totalBytes := b.VRAMTotalMB * 1024 * 1024
			state.VRAMUsedPct = float64(usedBytes) / float64(totalBytes)
		}

		// Auto-tune per-backend concurrency when the operator has not set it.
		// Free VRAM is total minus everything resident; KV-cache-per-request is
		// sized from the largest resident model (the heaviest a request could hit).
		if b.maxConcurrent == 0 {
			b.SetAutoConcurrent(autoConcurrent(b.VRAMTotalMB, usedBytes, largest, isCPU))
		}
	}

	return state, nil
}

// ── Auto-concurrency tuning ───────────────────────────────────────────────────

// defaultColdConcurrent is the placeholder cap given to an uncapped backend that
// has no model resident yet, so it is never treated as unlimited. It is replaced
// by a VRAM-derived value as soon as a model loads and the scraper can size one.
const defaultColdConcurrent = 4

// autoConcurrent estimates how many requests a backend can handle concurrently.
// usedBytes is the total VRAM already occupied (all resident models); modelBytes
// is the footprint of the largest resident model, used to size the KV cache a
// single request needs.
//
// Formula:
//
//	free_vram  = vram_total - used_vram
//	kv_per_req = largest_model × 0.10   (KV cache ≈ 10% of model size per slot)
//	n          = clamp(free_vram / kv_per_req, 1, 32)
//
// CPU inference (isCPU=true) halves n because CPU parallelism is far slower.
// When no VRAM data is available, conservative defaults are returned.
func autoConcurrent(vramTotalMB, usedBytes, modelBytes int64, isCPU bool) int64 {
	if vramTotalMB <= 0 || modelBytes <= 0 {
		if isCPU {
			return 2
		}
		return 4
	}

	totalBytes := vramTotalMB * 1024 * 1024
	freeBytes := totalBytes - usedBytes
	if freeBytes <= 0 {
		return 1
	}

	kvPerRequest := modelBytes / 10 // 10% of largest model size
	if kvPerRequest == 0 {
		kvPerRequest = 256 << 20 // 256 MB floor
	}

	n := freeBytes / kvPerRequest
	if n < 1 {
		n = 1
	}
	if n > 32 {
		n = 32
	}
	if isCPU {
		n = n / 2
		if n < 1 {
			n = 1
		}
	}
	return n
}

// ── vLLM ─────────────────────────────────────────────────────────────────────

func (s *Scraper) scrapeVLLM(b *Backend) (BackendState, error) {
	state := BackendState{
		ModelStatus: ModelWarm, // vLLM always has its model loaded in VRAM
	}

	// Parse Prometheus /metrics for VRAM and queue state.
	if err := s.scrapeVLLMMetrics(b, &state); err != nil {
		slog.Warn("scrape vllm metrics failed", "backend", b.Name, "err", err.Error())
	}

	// Get the loaded model name (vLLM serves exactly one).
	if model, err := s.vllmLoadedModel(b); err == nil && model != "" {
		state.LoadedModels = []string{normalizeModel(model)}
	} else if len(b.Models) > 0 {
		state.LoadedModels = []string{normalizeModel(b.Models[0])}
	}

	// vLLM uses continuous batching and PagedAttention — a fixed conservative
	// default works better than trying to compute from /metrics.
	if b.maxConcurrent == 0 {
		b.SetAutoConcurrent(20)
	}

	return state, nil
}

func (s *Scraper) scrapeVLLMMetrics(b *Backend, state *BackendState) error {
	resp, err := s.client.Get(b.URL + "/metrics")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		name, val, ok := parsePromLine(line)
		if !ok {
			continue
		}
		switch {
		// vLLM ≥0.10 renamed gpu_cache_usage_perc → kv_cache_usage_perc; accept both
		// so the VRAM guard keeps working across engine versions.
		case strings.HasPrefix(name, "vllm:kv_cache_usage_perc"),
			strings.HasPrefix(name, "vllm:gpu_cache_usage_perc"):
			state.VRAMUsedPct = val
		case strings.HasPrefix(name, "vllm:num_requests_running"):
			state.Inflight = int64(val)
		case strings.HasPrefix(name, "vllm:num_requests_waiting"):
			state.QueueDepth = int(val)
		}
	}
	return nil
}

func (s *Scraper) vllmLoadedModel(b *Backend) (string, error) {
	resp, err := s.client.Get(b.URL + "/v1/models")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var result struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}
	if len(result.Data) > 0 {
		return result.Data[0].ID, nil
	}
	return "", nil
}

// ── Prometheus text parser ────────────────────────────────────────────────────

// parsePromLine parses one Prometheus text-format line:
//
//	metric_name{labels} value [timestamp]
//
// Returns the metric name (without labels), the float64 value, and ok.
func parsePromLine(line string) (name string, value float64, ok bool) {
	// Value is the last space-delimited field (ignore optional timestamp).
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return "", 0, false
	}
	v, err := strconv.ParseFloat(parts[len(parts)-1], 64)
	if err != nil {
		// Last field might be a timestamp; try second-to-last.
		if len(parts) < 3 {
			return "", 0, false
		}
		v, err = strconv.ParseFloat(parts[len(parts)-2], 64)
		if err != nil {
			return "", 0, false
		}
	}
	// Strip label block from the metric name.
	namepart := parts[0]
	if i := strings.Index(namepart, "{"); i >= 0 {
		namepart = namepart[:i]
	}
	return namepart, v, true
}
