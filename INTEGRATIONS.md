# Integrations

LoxyRouter is a transparent proxy that speaks the OpenAI and Ollama API formats.
Any tool that works with Ollama or vLLM works with LoxyRouter by changing one URL.

---

## Install

**Binary (no Go required):**
```bash
# Linux
curl -L https://github.com/mageshkrishna/loxy-router/releases/latest/download/loxy-router-linux-amd64 \
  -o loxy-router && chmod +x loxy-router

# Mac (Apple Silicon)
curl -L https://github.com/mageshkrishna/loxy-router/releases/latest/download/loxy-router-darwin-arm64 \
  -o loxy-router && chmod +x loxy-router
```

**Docker:**
```bash
docker pull ghcr.io/mageshkrishna/loxy-router:latest
```

**From source:**
```bash
go install github.com/mageshkrishna/loxy-router@latest
```

---

## Zero-Config Start

If Ollama is running locally on the default port, no config needed:
```bash
./loxy-router
# auto-discover: found Ollama at http://localhost:11434
# loxy-router listening on :8080
```

Multiple Ollama instances via environment variable:
```bash
SCHEDULER_BACKENDS=http://localhost:11434,http://localhost:11435 ./loxy-router
```

---

## Open WebUI

Open WebUI is the most common Ollama frontend. Point it at LoxyRouter instead:

```bash
docker run -d \
  -e OLLAMA_BASE_URL=http://localhost:8080 \
  -p 3000:8080 \
  ghcr.io/open-webui/open-webui:main
```

No other change needed. Open WebUI talks the OpenAI format — LoxyRouter is transparent.

---

## Continue.dev (VS Code / JetBrains)

In your `~/.continue/config.json`:

```json
{
  "models": [
    {
      "title": "Llama 3 (via LoxyRouter)",
      "provider": "ollama",
      "model": "llama3",
      "apiBase": "http://localhost:8080"
    }
  ]
}
```

---

## Python (openai SDK)

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:8080/v1",
    api_key="not-needed",  # required by the SDK; ignored by LoxyRouter unless api_keys is set
)

response = client.chat.completions.create(
    model="llama3",
    messages=[{"role": "user", "content": "hello"}],
)
print(response.choices[0].message.content)
```

Streaming:
```python
stream = client.chat.completions.create(
    model="llama3",
    messages=[{"role": "user", "content": "count to 10"}],
    stream=True,
)
for chunk in stream:
    print(chunk.choices[0].delta.content or "", end="", flush=True)
```

---

## JavaScript / TypeScript (openai SDK)

```typescript
import OpenAI from "openai";

const client = new OpenAI({
  baseURL: "http://localhost:8080/v1",
  apiKey: "not-needed",
});

const response = await client.chat.completions.create({
  model: "llama3",
  messages: [{ role: "user", content: "hello" }],
});
console.log(response.choices[0].message.content);
```

---

## LiteLLM

Use LoxyRouter as a backend in LiteLLM's proxy config:

```yaml
# litellm_config.yaml
model_list:
  - model_name: llama3
    litellm_params:
      model: ollama/llama3
      api_base: http://localhost:8080
```

---

## LangChain (Python)

```python
from langchain_openai import ChatOpenAI

llm = ChatOpenAI(
    base_url="http://localhost:8080/v1",
    api_key="not-needed",
    model="llama3",
)
response = llm.invoke("hello")
```

---

## curl

```bash
curl http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llama3",
    "messages": [{"role": "user", "content": "hello"}],
    "stream": false
  }'
```

> vLLM backends require `Content-Type: application/json`. LoxyRouter forwards headers
> verbatim, so set it on the client (Ollama is lenient; vLLM is strict).

---

## Priority & Affinity (Optional Headers)

Pass these headers from your client to steer scheduling:

```
X-Priority: interactive      # low latency, preempts batch (default)
X-Priority: batch            # high throughput, deferrable
X-Priority: background       # best effort, shed first under load

X-Conversation-Id: <id>      # pin a conversation to one backend for KV-cache reuse
```

Python example:
```python
response = client.chat.completions.create(
    model="llama3",
    messages=[{"role": "user", "content": "hello"}],
    extra_headers={
        "X-Priority": "batch",
        "X-Conversation-Id": "my-session-123",
    },
)
```

---

## Kubernetes

LoxyRouter keeps all state in memory, so run a **single replica** (see the single-instance
note in the README). Put a TLS-terminating Service/Ingress in front of it.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: loxy-router
spec:
  replicas: 1          # single instance: affinity/admission state is in-memory
  selector:
    matchLabels:
      app: loxy-router
  template:
    metadata:
      labels:
        app: loxy-router
    spec:
      terminationGracePeriodSeconds: 35   # ≥ timeouts.shutdown_secs for graceful drain
      containers:
        - name: loxy-router
          image: ghcr.io/mageshkrishna/loxy-router:latest
          ports:
            - containerPort: 8080
          env:
            - name: SCHEDULER_BACKENDS
              value: "http://ollama-svc-1:11434,http://ollama-svc-2:11434"
---
apiVersion: v1
kind: Service
metadata:
  name: loxy-router
spec:
  selector:
    app: loxy-router
  ports:
    - port: 8080
      targetPort: 8080
```

---

## Custom Routing Plugin

Need routing logic LoxyRouter doesn't ship? Implement `SchedulerPlugin` — no fork required.
Returning `("", nil)` falls through to the built-in scheduler.

```go
package main

import "strings"

// CostRouter sends large models to a dedicated high-VRAM backend.
type CostRouter struct{}

func (c *CostRouter) Pick(model string, backends []*Backend) (string, error) {
    if strings.HasSuffix(model, ":70b") || strings.HasSuffix(model, ":72b") {
        return "high-vram-node", nil // name must match config.yaml
    }
    return "", nil // fall through to the built-in scheduler
}
```

Register it as the optional final argument to `NewScheduler` in `main.go`:
```go
sched := NewScheduler(pool, store, conv, &CostRouter{})
```

---

## Observability

LoxyRouter exposes Prometheus metrics at `/metrics` (open, no auth):

```
scheduler_backend_up{backend="ollama-1"}                 1
scheduler_backend_inflight{backend="ollama-1"}           3
scheduler_backend_model_warm{backend="ollama-1"}         1
scheduler_backend_vram_used_pct{backend="ollama-1"}      0.61
scheduler_backend_queue_depth{backend="ollama-1"}        0
scheduler_backend_per_token_ms{backend="ollama-1"}       37
scheduler_gate_inflight                                  5
scheduler_gate_max_inflight                              16
scheduler_gate_queued{priority="interactive"}            0
scheduler_requests_total{backend="ollama-1",status="200"} 1284
```

Add a Prometheus datasource in Grafana pointed at `http://localhost:8080/metrics`.
(Metric names carry the `scheduler_` prefix from LoxyRouter's internal package name.)
