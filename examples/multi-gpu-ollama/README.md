# Multi-GPU Ollama with LoxyRouter

Ollama never loads the same model twice, even with idle GPUs available
([ollama#9054](https://github.com/ollama/ollama/issues/9054)). The maintainers'
recommended workaround: run one Ollama server per GPU (`CUDA_VISIBLE_DEVICES` /
device pinning) and put a load-balancing proxy in front.

This example is that exact setup — four GPU-pinned Ollama instances — with
LoxyRouter as the proxy instead of nginx `least_conn`.

## Why not just nginx?

For a single model on N identical GPUs, nginx works. What it can't see costs
you the moment your setup grows:

| | nginx `least_conn` | LoxyRouter |
|---|---|---|
| Routing signal | open connections | which backend has the model **loaded in VRAM** (`/api/ps`), live queue depth, measured per-token speed |
| Multiple models | scatters them — every misroute forces a 25–120s model reload | pins each model where it's warm |
| SSE streaming | buffers by default (breaks token streaming until configured) | streams through untouched |
| VRAM exhaustion | routes into it | 503s before the backend OOMs |
| Concurrency cap | `max_conns` (best-effort) | atomic reservation (hard ceiling) |
| Priorities | — | `interactive` > `batch` > `background` via `X-Priority` |

The multi-model case is not hypothetical — it's [the very next question in the
same issue](https://github.com/ollama/ollama/issues/9054#issuecomment-2848270519):
*"8 GPUs, two classes of models... it would clearly be more efficient to load 4
instances of each."* A connection-counting proxy can't do that placement; a
model-aware one does it by default.

## Run it

Requires the [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/latest/install-guide.html)
(`nvidia-smi` must work, and `docker info` should list the `nvidia` runtime).

```bash
docker compose up -d

# Pull a model once (shared volume — all instances see it)
docker compose exec ollama-1 ollama pull llama3.2:1b

# Backends start as "down" and flip to "up" on the first scrape (~5 s).
# Wait until all four show "status": "up" before sending traffic:
curl -s localhost:11434/status

# LoxyRouter listens on the host's 11434, so existing clients work unchanged
curl localhost:11434/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"llama3.2:1b","messages":[{"role":"user","content":"hi"}]}'
```

Verify routing state — each backend, its loaded models, and live load:

```bash
curl localhost:11434/status
```

## Adapting

- **Fewer/more GPUs:** add or remove `ollama-N` services (bump `device_ids`)
  and the matching entry in `loxyrouter.yaml`.
- **Throughput tuning:** raise `OLLAMA_NUM_PARALLEL` and `max_concurrent`
  together. Aggregate tokens/sec rises with parallelism (at some per-request
  latency cost) until the GPU saturates — see the benchmarks in ollama#9054.
- **Multiple models:** list them in `models:` per backend to partition GPUs by
  model class, or leave `models` unset and let warmth-aware routing place them.
