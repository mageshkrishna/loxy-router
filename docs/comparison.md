# LoxyRouter vs. the alternatives — an honest guide

Every tool below is good at what it was built for. This page exists because
"ollama load balancer" returns five different categories of software, and
picking the wrong one costs you either an afternoon or a 70-second model
reload on every other request. Short version: **match the tool to your
topology**, then to your features.

| | nginx / HAProxy | [llama-swap](https://github.com/mostlygeek/llama-swap) | [Olla](https://github.com/thushan/olla) | [LiteLLM](https://github.com/BerriAI/litellm) | LoxyRouter |
|---|---|---|---|---|---|
| Built for | generic HTTP balancing | **one machine**, many models: spawns/swaps the inference process on demand | proxying + failover across local inference backends | unified API over 100+ cloud & local providers; keys, budgets, quotas | **N instances**, many models: route to where the model is already loaded |
| Knows what's in VRAM | no | yes (it loaded it) | no | no | **yes** — polls `/api/ps` (Ollama) and `/metrics` (vLLM) |
| Avoids cold loads across instances | no — scatters requests, forces reloads | n/a (single host) | no — balances by priority / round-robin / least-connections | no | **yes** — warmth-aware scoring; cold-routes only when the swap is provably cheaper than the warm queue |
| VRAM-overload protection | no | TTL-based unload | no | no | guard: fast 503 *before* the backend OOMs |
| Routing signal | connections / requests | requested model name | health + connection counts | provider config, cost rules, fallbacks | warmth + measured decode ms/token + live queue depth |
| Multi-turn KV-cache affinity | cookie/IP hash (model-blind) | n/a | no | no | yes — `X-Conversation-Id` pins to the backend holding the cache |
| Manages backend processes | no | **yes** — starts/stops them for you | no | no | no — backends run however you run them |
| Backends | anything HTTP | any local OpenAI/Anthropic-compatible server it can spawn | Ollama, llama.cpp, vLLM, LM Studio, SGLang, more | 100+ incl. OpenAI/Anthropic/Bedrock + local | Ollama, vLLM (generic OpenAI type planned) |
| Cloud providers / virtual API keys / spend tracking | no | no | no | **yes** — its home turf | no, deliberately |
| Runtime | C, battle-tested | one Go binary | one Go binary | Python + optional Postgres/Redis | one Go binary, in-memory state |
| Horizontal scaling of the proxy itself | yes | single instance | yes | yes (with DB) | **single instance for now** |

## Which one should you run?

**One inference box, you want many models on limited VRAM** →
**llama-swap.** It supervises the backend process itself, loading and
unloading models on demand with TTLs. LoxyRouter does not manage processes;
on a single instance there is nothing for it to route around.

**One box, multiple GPUs, and Ollama only uses one of them** → this is
[ollama#9054](https://github.com/ollama/ollama/issues/9054), and the
maintainers' recommended fix is one Ollama server per GPU with a routing
proxy in front. That proxy is LoxyRouter's home turf — a model-unaware
proxy in that position causes the constant-reload thrash. Ready-to-run
setup: [`examples/multi-gpu-ollama`](../examples/multi-gpu-ollama).

**Several machines/instances serving one identical, always-loaded model** →
**nginx/HAProxy is fine**, honestly. Every backend is always warm, so
warmth-awareness buys you nothing. (Mind SSE buffering and use
`least_conn`.) You'd still get the VRAM guard, hard concurrency caps, and
per-token metrics from LoxyRouter, but you don't *need* it.

**Several instances, more models than fit everywhere at once** →
**LoxyRouter.** This is the case it was built for: the cost of a wrong
routing decision is a 25–120 s reload (we measured 70 s truly cold for
qwen3:32b on an A40), and connection-counting balancers make that wrong
decision constantly.

**You need cloud providers, API-key management, budgets, or
provider-format translation** → **LiteLLM.** That's a gateway problem, not
a placement problem. (They compose: clients → LiteLLM → LoxyRouter → your
local backends, if you want both.)

**You want failover and a unified model registry across mixed local
backends, and cold loads aren't your bottleneck** → **Olla.** Broader
backend support than LoxyRouter today (llama.cpp, LM Studio, SGLang); its
balancing strategies are priority/round-robin/least-connections rather
than model-warmth-based.

## What LoxyRouter deliberately doesn't do

- **Cloud fallback, virtual keys, multi-tenancy** — LiteLLM's lane.
- **Kubernetes-native distributed scheduling** — llm-d / vLLM
  production-stack's lane.
- **Spawning or supervising backends** — llama-swap's lane. LoxyRouter
  assumes your backends are already running and routes between them.
- **Request/response rewriting** — bodies pass through untouched, always.

If this page misrepresents any tool, [open an issue](https://github.com/mageshkrishna/loxy-router/issues) — it's meant to be the page we'd want to find when evaluating.
