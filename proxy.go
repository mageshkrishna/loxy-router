package main

import (
	"bytes"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"
)

const (
	// dialTimeout bounds establishing the TCP connection — a dead host fails fast
	// instead of pinning a request goroutine.
	dialTimeout = 5 * time.Second
	// tlsHandshakeTimeout bounds the TLS handshake for https backends.
	tlsHandshakeTimeout = 10 * time.Second
	// idleConnTimeout closes pooled keep-alive connections after inactivity.
	idleConnTimeout = 90 * time.Second
)

type Proxy struct {
	sched   *Scheduler
	gate    *Gate
	metrics *Metrics
	client  *http.Client
}

func NewProxy(sched *Scheduler, gate *Gate, metrics *Metrics, responseHeaderTimeout time.Duration) *Proxy {
	return &Proxy{
		sched:   sched,
		gate:    gate,
		metrics: metrics,
		// No overall client timeout: a streaming LLM response runs for minutes and
		// a total deadline would truncate it. Instead bound the points where a dead
		// or wedged backend would otherwise pin a request goroutine forever —
		// connect, TLS, and the wait for response headers — while leaving an
		// established token stream free to run as long as it needs.
		client: &http.Client{
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				DialContext:           (&net.Dialer{Timeout: dialTimeout, KeepAlive: 30 * time.Second}).DialContext,
				TLSHandshakeTimeout:   tlsHandshakeTimeout,
				IdleConnTimeout:       idleConnTimeout,
				ResponseHeaderTimeout: responseHeaderTimeout,
				MaxIdleConns:          100,
				MaxIdleConnsPerHost:   100,
			},
		},
	}
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// Cap the body before buffering it so an oversized request can't OOM us.
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	req, body, err := classify(r)
	if err != nil {
		slog.Warn("rejected oversized/unreadable body", "path", r.URL.Path, "err", err.Error())
		writeJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return
	}

	// Admission control: reject fast when the request's priority tier is full.
	// Pass the request context so a client disconnect frees the queued slot.
	if !p.gate.Acquire(req.priority, r.Context()) {
		slog.Warn("admission rejected",
			"model", req.model, "priority", req.priority.String(), "path", r.URL.Path)
		writeJSONError(w, http.StatusServiceUnavailable, "overloaded, try again later")
		return
	}
	defer p.gate.Release()

	tried := map[string]bool{}
	retries := 0
	for {
		backend, err := p.sched.PickExcluding(req, tried)
		if err != nil {
			slog.Warn("no backend", "model", req.model, "err", err.Error(), "retries", retries)
			writeJSONError(w, http.StatusServiceUnavailable, "no available backend")
			return
		}
		tried[backend.Name] = true

		// Reserve a slot atomically (compare-and-increment under the backend's
		// concurrency limit). If we lose the race — another request filled the
		// last slot between selection and reservation — exclude this backend and
		// pick another, so max_concurrent is a hard cap, not a best-effort one.
		if !backend.tryReserve() {
			continue
		}
		resp, ferr := p.forward(r, backend, body)
		if ferr != nil {
			backend.decInflight()
			// If the client disconnected, the forward fails with a cancelled
			// context. Don't retry across every other backend for a request whose
			// caller is already gone.
			if r.Context().Err() != nil {
				return
			}
			retries++
			slog.Warn("backend unreachable, retrying",
				"backend", backend.Name, "err", ferr.Error())
			continue // try a different backend
		}

		status := resp.StatusCode
		res := p.stream(w, resp)
		latencyMs := time.Since(start).Milliseconds()
		backend.decInflight()

		// Per-token latency measures pure generation speed so a long answer doesn't
		// make a backend look slow. Crucially it must EXCLUDE one-time costs — model
		// load, prompt processing, and queue wait — otherwise a backend that just
		// did a cold load looks slow and we route traffic away from the very backend
		// we just warmed. stream() reports the decode-only figure (from the engine's
		// own eval_duration when available, else the first-token→last-token window);
		// we only fall back to wall-clock/tokens if neither was obtainable.
		tokens := res.tokens
		perTokenMs := res.perTokenMs
		if perTokenMs <= 0 {
			perTokenMs = latencyMs / int64(tokens)
		}
		if perTokenMs < 1 {
			perTokenMs = 1
		}

		// Only learn from a SUCCESSFUL response: a fast error (e.g. instant 500/429)
		// would record a tiny per-token latency and make a broken backend look fast,
		// pulling more traffic toward it. For the same reason, only pin the
		// conversation's affinity on success.
		// Skip learning if the client disconnected mid-stream: the token count is
		// truncated, so the per-token latency would be misleadingly high.
		if status >= 200 && status < 300 && r.Context().Err() == nil {
			backend.RecordPerTokenMs(perTokenMs)
			p.sched.RecordConversation(req, backend.Name)
		}

		// Count every forwarded response (success or backend error) by status, so
		// rate() and error-ratio queries work in Grafana.
		p.metrics.IncRequest(backend.Name, status)

		slog.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"model", req.model,
			"backend", backend.Name,
			"priority", req.priority.String(),
			"conversation_id", req.conversationID,
			"status", status,
			"retries", retries,
			"latency_ms", latencyMs,
			"tokens", tokens,
			"per_token_ms", perTokenMs,
		)
		return
	}
}

func (p *Proxy) forward(r *http.Request, b *Backend, body []byte) (*http.Response, error) {
	targetURL := b.URL + r.URL.Path
	if r.URL.RawQuery != "" {
		targetURL += "?" + r.URL.RawQuery
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for key, vals := range r.Header {
		// When the backend has its own credential, never forward the client's
		// Authorization downstream — we replace it below. Otherwise pass headers
		// through unchanged (default Ollama needs no auth).
		if b.apiKey != "" && http.CanonicalHeaderKey(key) == "Authorization" {
			continue
		}
		for _, v := range vals {
			req.Header.Add(key, v)
		}
	}
	if b.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+b.apiKey)
	}
	return p.client.Do(req)
}

// streamResult reports what stream() observed: the token count and a decode-only
// per-token latency (ms/token) that excludes model load, prompt processing, and
// queue wait. perTokenMs is 0 when no decode-timing signal was obtainable, so the
// caller can fall back to a coarser estimate.
type streamResult struct {
	tokens     int
	perTokenMs int64
}

// stream copies the backend response to the client, flushing each chunk for live
// streaming, and reports the token count plus a decode-only per-token latency.
func (p *Proxy) stream(w http.ResponseWriter, resp *http.Response) streamResult {
	defer resp.Body.Close()

	for key, vals := range resp.Header {
		for _, v := range vals {
			w.Header().Add(key, v)
		}
	}
	w.WriteHeader(resp.StatusCode)

	// http.Flusher.Flush() pushes each chunk immediately — critical for SSE/streaming tokens.
	flusher, canFlush := w.(http.Flusher)
	tc := &tokenCounter{}
	var firstByteAt time.Time // when generation started streaming (excludes load/prompt)
	buf := make([]byte, 4096)
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if firstByteAt.IsZero() {
				firstByteAt = time.Now()
			}
			tc.observe(buf[:n])
			if _, writeErr := w.Write(buf[:n]); writeErr != nil {
				return tc.result(firstByteAt) // client disconnected mid-stream
			}
			if canFlush {
				flusher.Flush()
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			slog.Warn("stream read error", "err", readErr.Error())
			break
		}
	}
	return tc.result(firstByteAt)
}

// tokenCounter estimates how many tokens a response generated by watching the
// bytes flow past, without buffering the whole body. It works across formats:
//
//   - Ollama streams newline-delimited JSON; the final object carries the exact
//     "eval_count". Non-streaming Ollama puts "eval_count" in the single body.
//   - OpenAI-compatible (vLLM) streams SSE "data:" events; with usage enabled the
//     final chunk carries "completion_tokens". Without it, each event ≈ one token,
//     so we fall back to counting events (two newlines per SSE event).
//
// Exact fields are preferred; the newline estimate is the fallback. The result
// is only used to normalize latency, so an approximate count is fine.
type tokenCounter struct {
	newlines int
	chunks   int // number of non-empty reads — >1 means the response was actually streamed
	sse      bool
	tail     []byte // last bytes, where the exact token-count field lives
}

const tokenTailMax = 2048

func (t *tokenCounter) observe(b []byte) {
	t.chunks++
	for _, c := range b {
		if c == '\n' {
			t.newlines++
		}
	}
	if !t.sse && bytes.Contains(b, []byte("data:")) {
		t.sse = true
	}
	t.tail = append(t.tail, b...)
	if len(t.tail) > tokenTailMax {
		t.tail = t.tail[len(t.tail)-tokenTailMax:]
	}
}

// result computes the token count and a decode-only per-token latency in ms.
// It prefers the engine's own generation time — Ollama reports eval_duration
// (nanoseconds) over eval_count — which is the exact decode time and excludes
// model load and prompt processing. Otherwise it uses the wall-clock streaming
// window (first token → now), valid only when the response was actually streamed
// (more than one chunk); a single-shot body carries no usable decode timing, so
// perTokenMs stays 0 and the caller falls back to a coarser estimate.
func (t *tokenCounter) result(firstByteAt time.Time) streamResult {
	tokens := t.count()
	res := streamResult{tokens: tokens}

	if evalNs, ok := jsonIntField(t.tail, "eval_duration"); ok && evalNs > 0 {
		denom := tokens
		if ec, ok := jsonIntField(t.tail, "eval_count"); ok && ec > 0 {
			denom = ec
		}
		res.perTokenMs = int64(evalNs) / int64(denom) / 1_000_000
		if res.perTokenMs < 1 {
			res.perTokenMs = 1 // sub-ms/token still beats falling back to wall-clock
		}
		return res
	}

	if t.chunks > 1 && !firstByteAt.IsZero() && tokens > 0 {
		ms := time.Since(firstByteAt).Milliseconds() / int64(tokens)
		if ms < 1 {
			ms = 1
		}
		res.perTokenMs = ms
	}
	return res
}

func (t *tokenCounter) count() int {
	if n, ok := jsonIntField(t.tail, "eval_count"); ok && n > 0 {
		return n
	}
	if n, ok := jsonIntField(t.tail, "completion_tokens"); ok && n > 0 {
		return n
	}
	if t.sse && t.newlines > 1 {
		return t.newlines / 2 // ~two newlines per SSE event
	}
	if t.newlines > 0 {
		return t.newlines
	}
	return 1 // avoid divide-by-zero; treat as a single unit of work
}

// jsonIntField finds the last "key": <int> in b and returns the integer. It
// scans manually rather than unmarshalling, because the tail may be a partial
// JSON fragment. Returns ok=false if the key or a following integer is absent.
func jsonIntField(b []byte, key string) (int, bool) {
	needle := []byte(`"` + key + `"`)
	i := bytes.LastIndex(b, needle)
	if i < 0 {
		return 0, false
	}
	j := i + len(needle)
	for j < len(b) && (b[j] == ' ' || b[j] == ':') {
		j++
	}
	start := j
	for j < len(b) && b[j] >= '0' && b[j] <= '9' {
		j++
	}
	if j == start {
		return 0, false
	}
	n, err := strconv.Atoi(string(b[start:j]))
	if err != nil {
		return 0, false
	}
	return n, true
}

// writeJSONError sends a minimal JSON error body with the given status code.
// Messages are short, controlled, ASCII literals, so strconv.Quote yields valid
// JSON without reaching for encoding/json.
func writeJSONError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write([]byte(`{"error":` + strconv.Quote(msg) + `}`))
}
