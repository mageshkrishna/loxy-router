---
name: go-style
description: Use when writing, editing, or reviewing Go code in the inference-scheduler project. Follows the Google Go Style Guide (https://google.github.io/styleguide/go/guide) plus this project's local conventions.
---

# Go style for inference-scheduler

This project follows the **Google Go Style Guide**:
- Guide (principles): https://google.github.io/styleguide/go/guide
- Decisions (specific rules): https://google.github.io/styleguide/go/decisions
- Best practices: https://google.github.io/styleguide/go/best-practices

When Google's guide and these local notes ever conflict, Google's guide wins —
except where a rule below states a project-specific reason (the guide explicitly
allows local conventions for things like channel/mutex patterns).

## The five principles, in priority order
1. **Clarity** — the purpose and rationale are obvious to the reader.
2. **Simplicity** — the most straightforward solution; no clever code.
3. **Concision** — high signal-to-noise; no repetition or dead abstraction.
4. **Maintainability** — easy for the next person to change correctly.
5. **Consistency** — looks and behaves like the rest of this codebase.

Lower-priority principles break ties; they don't override higher ones.

## Core rules from the guide
- **gofmt is mandatory.** `gofmt -l .` must report nothing. Run `gofmt -w .`.
- **MixedCaps**, never underscores. Exported `MaxLength`, unexported `maxLength`.
- **No fixed line length.** Don't split a line just to fit a width, and never
  break a string literal across lines to shorten it. Refactor instead if a line
  is genuinely doing too much.
- **Comments explain *why*, not *what*.** Skip comments that restate the code.
  Do comment language nuances, business-logic subtleties, and perf-critical paths.
- **Names avoid repetition in context.** A `Backend` field isn't `BackendName`,
  it's `Name`. Don't repeat the package name in identifiers.
- **Same concept, same name** across receivers and parameters.
- **Least mechanism**: reach for core language constructs before the stdlib,
  and the stdlib before a new dependency.
- **Standard error handling should be instantly recognizable.** Flag any
  deviation from the normal pattern with a comment.

## Verify before "done"
- `go build ./...` — no output.
- `gofmt -l .` — no output.
- `go vet ./...` — clean.
- `go test ./...` — passing, if logic changed.

## Project-local conventions (valid under the guide's "local consistency")
- **Errors**: wrap with `%w` and context — `fmt.Errorf("read config %q: %w", path, err)`.
  Sentinel errors as package vars (`ErrNoBackend`), checked with `errors.Is`.
  `log.Fatalf` only in startup/`main`; library code returns errors.
- **Concurrency**: cross-goroutine counters use `sync/atomic` (see `Backend.inflight`);
  shared maps use `sync.RWMutex` with `defer` unlocks (see `StateStore`); methods
  returning shared state return a copy (`StateStore.All`); background workers expose
  `Run(interval time.Duration)` launched as a goroutine from `main`.
- **File layout**: one concern per file; constructors `NewX(...) *X`; exported types
  get a doc comment starting with the type name; `── Section ────` banner comments
  separate logical groups (see `scraper.go`).
- **HTTP**: `http.NewRequestWithContext` propagating `r.Context()`; short timeout on
  scraper/health clients, **no timeout** on the proxy client (LLM responses take
  minutes); stream with a 4096-byte buffer + `http.Flusher.Flush()` per chunk.
- **Tests**: table-driven with `t.Run` subtests; test pure routing/normalization
  functions directly; no network in unit tests.
- **Dependencies**: resist them. The edge is "single binary, no Kubernetes, runs in
  5 minutes." Get explicit sign-off before adding a module.
