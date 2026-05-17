<div align="center">

[![Main Repo](https://img.shields.io/badge/Main%20Repo-gits.tysstech.com-blue?logo=gitea)](https://git.tysstech.com/TySS-Dev/ollama-ai-answers-searxng)
[![Mirror Repo](https://img.shields.io/badge/Mirror%20Repo-github.com-blue?logo=github)](https://github.com/TySP-Dev/ollama-ai-answers-searxng)

<div align="left">

# Ollama AI Answers Plugin for SearXNG
**Based on [ai-answers-searxng](https://github.com/cra88y/ai-answers-searxng) by [cra88y](https://github.com/cra88y)**

A SearXNG plugin that generates local AI overviews powered by Ollama, using search results as RAG context.

Features:
- Token-by-token UI streaming
- Clickable inline citations
- Interactive mode: continue summary, ask follow-ups, copy, or regenerate
- Simple response mode with no extras
- Internally called low-latency RAG for follow-ups (bypasses HTTP loopback)
- Native network integration via `searx.network` (respects proxy/SSL settings)
- Stateless conversation persistence/shareability via URL hash
- Model selector in the AI overview widget
- Does not slow down result loading
- One file install
- Real-time streaming via Valkey — responses stream token by token using a background thread + Valkey job queue, working around granian's broken generator support for true streaming feel
- TF-IDF result reranking — fetched page content is scored against the query using BM25-style TF-IDF before being sent to Ollama, surfacing the most relevant sources first
- Smart chunking — pages are split into 512-token overlapping segments and the highest-scoring chunk per page is selected for context
- Intent detection — queries are automatically classified into 8 intent types (factual, howto, technical, comparison, opinion, current, local, general) with tailored system prompts per type
- Conversation memory — 30-minute cross-search conversation history stored in Valkey, so follow-up questions work even after navigating to a new search
- Markdown rendering — AI responses render bold, italic, lists, headers, and inline code natively in the result box
- Intent emoji badge — a small emoji appears next to "AI Overview" indicating the detected query type

## Install

1. Download the plugin:
   ```bash
   curl -o ollama_answers.py https://raw.githubusercontent.com/TySP-Dev/ollama-ai-answers-searxng/master/ollama_answers.py
   ```

2. Copy to your SearXNG plugins directory:
   ```bash
   cp ollama_answers.py ~/searxng/plugins/ollama_answers.py
   ```

3. Add the volume mount to your `docker-compose.yml` under the searxng service:
   ```yaml
   volumes:
     - ./plugins/ollama_answers.py:/usr/local/searxng/searx/plugins/ollama_answers.py:Z
   ```

4. Add environment variables to `docker-compose.yml`:
   ```yaml
   environment:
     - LLM_URL=http://ollama:11434/v1/chat/completions
     - LLM_MODEL=qwen3.5:9b
     - VALKEY_HOST=searxng-valkey
   ```

5. Add to `settings.yml` plugins section:
   ```yaml
   plugins:
     searx.plugins.ollama_answers.SXNGPlugin:
       active: true
   ```

6. Restart SearXNG:
   ```bash
   docker compose up -d --force-recreate core
   ```

## Configuration

Configure via environment variables.

| Variable | Default | Description |
|---|---|---|
| `LLM_URL` | `http://ollama:11434/v1/chat/completions` | Ollama endpoint |
| `LLM_MODEL` | `qwen3.5:9b` | Default model |
| `LLM_MAX_TOKENS` | `200` | Max response tokens |
| `LLM_TEMPERATURE` | `0.2` | Response temperature |
| `LLM_TABS` | `general,science,it,news` | Tabs to show AI overview on |
| `LLM_QUESTION_MARK_REQUIRED` | `false` | Only trigger on queries with `?` |
| `LLM_INTERACTIVE` | `true` | Show copy/regen/follow-up UI |
| `LLM_SYSTEM_PROMPT` | *(built-in)* | Override the system prompt |
| `LLM_CONTEXT_DEEP_COUNT` | `5` | Full-content results to fetch |
| `LLM_CONTEXT_SHALLOW_COUNT` | `15` | Headline-only results |
| `VALKEY_HOST` | `searxng-valkey` | Valkey container hostname |
| `VALKEY_PORT` | `6379` | Valkey port |

## How It Works

1. User performs a search
2. Results return server-side
3. `post_search` plugin hook fires
4. Token-optimized context is extracted from results
5. UI/logic shell injected into the standard answers object
6. Client-side script calls a signed endpoint (`/ai-stream`)
7. Ollama streams a response token-by-token in the UI

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Browser                           │
│  POST /ai-stream → GET /ai-status/{id} (poll 150ms) │
└────────────────┬────────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────────┐
│              SearXNG + Plugin                        │
│                                                      │
│  post_search()                                       │
│    → _enrich_results()  ← ThreadPoolExecutor        │
│      → _fetch_page_text() × 5 parallel              │
│      → _chunk_text() + _tfidf_score()               │
│      → rerank by score                              │
│    → _assemble_context()                            │
│    → inject AI Overview HTML + JS                   │
│                                                      │
│  /ai-stream                                          │
│    → validate token                                  │
│    → _detect_intent() → select system prompt        │
│    → _load_conversation() from Valkey               │
│    → launch stream_to_valkey() thread               │
│    → return {job_id} immediately                    │
│                                                      │
│  stream_to_valkey() [background thread]             │
│    → Ollama stream=True                             │
│    → RPUSH tokens to Valkey                         │
│    → RPUSH __DONE__ when complete                   │
│                                                      │
│  /ai-status/{job_id}                                │
│    → LRANGE chunks from offset                      │
│    → return {chunks, done}                          │
└────────────────┬────────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────────┐
│                  Valkey                              │
│  ai:job:{id}:chunks  (list, TTL 120s)               │
│  ai:job:{id}:status  (string, TTL 120s)             │
│  ai:conv:{session}   (JSON, TTL 1800s)              │
└─────────────────────────────────────────────────────┘
```

## Docker Compose Example

```yaml
services:
  searxng:
    environment:
      - LLM_URL=http://ollama:11434/v1/chat/completions
      - LLM_MODEL=qwen3.5:9b
      - VALKEY_HOST=searxng-valkey
    volumes:
      - ./ollama_answers.py:/usr/local/searxng/searx/plugins/ollama_answers.py

  ollama:
    image: ollama/ollama
    volumes:
      - ollama_data:/root/.ollama

volumes:
  ollama_data:
```

## Remote Ollama

If your Ollama instance is remote or behind a reverse proxy, set `LLM_URL` to the full endpoint and provide an API key if required. The plugin supports Bearer token auth and follows HTTP redirects.

```yaml
environment:
  - LLM_URL=https://ollama.example.com/v1/chat/completions
  - LLM_API_KEY=your-bearer-token
```

## Project Structure

```
ollama-ai-answers-searxng/
├── ollama_answers.py      # single plugin file — all logic here
├── README.md
├── requirements.txt       # flask, flask-babel (for local dev only)
└── tests/
    └── dev.py             # local dev server
```

## Development — Dev Server

A standalone Flask dev server is included in `tests/dev.py`. It mocks the SearXNG plugin environment so you can test the full UI without a running SearXNG instance.

### Setup

```bash
pip install flask flask-babel certifi
```

### Run

```bash
python tests/dev.py
```

Then open [http://127.0.0.1:5000/](http://127.0.0.1:5000/) in your browser.

> **Note:** Use `127.0.0.1:5000`, not `localhost:5000` — macOS AirPlay Receiver can occupy the IPv6 loopback on port 5000.

### Usage

- Type a query in the search bar and hit **Search** to trigger an AI overview.
- Expand **Ollama Configuration** at the top to change the endpoint URL or Bearer token for the current session. Click **Apply** to save and re-run the current query.
- The model selector in the AI overview widget (loaded from `/ai-models`) shows all models available on the configured Ollama server and persists your choice in the session URL.

### Environment Variables (dev)

The dev server reads the same variables as the plugin:

```bash
LLM_URL=http://localhost:11434/v1/chat/completions \
LLM_MODEL=qwen3.5:9b \
python tests/dev.py
```

Or export them before running. Any values set in the config panel at runtime take priority for that session.
