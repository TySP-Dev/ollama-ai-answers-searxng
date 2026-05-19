<div align="center">

[![Main Repo](https://img.shields.io/badge/Main%20Repo-gits.tysstech.com-blue?logo=gitea)](https://git.tysstech.com/TySS-Dev/ollama-ai-answers-searxng)
[![Mirror Repo](https://img.shields.io/badge/Mirror%20Repo-github.com-blue?logo=github)](https://github.com/TySP-Dev/ollama-ai-answers-searxng)

<div align="left">

# Ollama AI Answers Plugin for SearXNG
**Based on [ai-answers-searxng](https://github.com/cra88y/ai-answers-searxng) by [cra88y](https://github.com/cra88y)**

A SearXNG plugin that generates local AI overviews powered by Ollama, using search results as RAG context.

## Features:

- Inline numbered citations
- Interactive mode - Continue summary, ask follow-ups, copy, or regenerate
- Overview of ranked results with prompts based on detected query intent:
  - `How To` `Technical` `Factual` `Comparison` `Opinion` `Current` `Local` `Geneal`
- Internally called RAG for follow-ups
- Native network integration via `searx.network`
- Stateless conversation presistence/shareability via URL hash
- Ollama model selector
- Feeds fetched results to Ollama without slowing down SearXNG  results
- Real-time streaming via Valkey (No waiting for a completed response)
- TF-IDF result ranking before being sent to Ollama
- Smart chunking - Pages are split into 512-token segments and highest-scoring chunk per page used for context
- Conversation memory - 30-minute cross-search conversation history via Valkey for follow-up questions
- Markdown support
- Intent emoji badge showing what intent prompt was used 

## Install

1. Download the plugin:

   ### Main repo (Gitea)
   ```bash
   curl -o ollama_answers.py https://git.tysstech.com/TySS-Dev/ollama-ai-answers-searxng/raw/branch/main/ollama_answers.py
   ```

   ### Mirror repo (Github):
   ```bash
   curl -o ollama_answers.py https://raw.githubusercontent.com/TySP-Dev/ollama-ai-answers-searxng/main/ollama_answers.py
   ```

3. Copy to your SearXNG plugins directory:
   ```bash
   cp ollama_answers.py path_to/searxng/plugins/ollama_answers.py
   ```

4. Add the volume mount to your `docker-compose.yml` under the searxng service:
   ```yaml
   volumes:
     - ./plugins/ollama_answers.py:/usr/local/searxng/searx/plugins/ollama_answers.py:Z
   ```

5. Add environment variables to `docker-compose.yml`:
   ```yaml
   environment:
     - LLM_URL=http://ollama:11434/v1/chat/completions
     - LLM_MODEL=qwen3.5:9b
     - VALKEY_HOST=searxng-valkey
   ```

6. Add to `settings.yml` plugins section:
   ```yaml
   plugins:
     searx.plugins.ollama_answers.SXNGPlugin:
       active: true
   ```

7. Restart SearXNG:
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

## Known Issues

- [ ] Update README with all updates

- [x] When asking a follow up question the previous output disappears

- [x] Parts of the UI are not theme aware resulting in a unpolished look when not using a dark theme

- [x] When SearXNG provides a info blob for a search it appears on top of the overview i.e. `Wikipedia` or `Linux` 

For any issues not stated here please create an issue ticket on [Gitea](https://git.tysstech.com/TySS-Dev/ollama-ai-answers-searxng/issues) or [GitHub](https://github.com/TySP-Dev/ollama-ai-answers-searxng/issues) and add the `bug` tag.

## Roadmap

### Dev Server

- [x] Stream viewer — show tokens arriving in real time in the debug panel as they come out of Valkey, so you can see exactly what the LLM is generating chunk by chunk

- [x] TF-IDF score visualizer — show a table of which URLs were fetched, their scores, and which chunks were selected for context

- [ ] Intent detection display — show what intent was detected and which system prompt was used for each query

- [ ] Saved queries — save/load test queries so you can quickly re-run the same set of searches after making changes to the plugin

- [ ] A/B model comparison — run the same query against two different models simultaneously and show both responses side by side

- [ ] Response time breakdown — show how long each phase took: SearXNG fetch, page fetching, TF-IDF scoring, LLM stream start, stream complete

- [ ] Context inspector — show the full assembled context string that gets sent to the LLM, so you can see exactly what it's working with

- [ ] Prompt viewer — show the full system prompt + user prompt that gets sent to Ollama

- [ ] Export button — copy the full context + prompt + response as a JSON blob for bug reports

- [ ] Per-intent system prompt editor — edit the system prompts for each intent type live without restarting

- [ ] Token counter — show estimated token count of the context being sent

### Plugin

- [ ] Working on feature plans

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Browser                           │
│  POST /ai-stream → GET /ai-status/{id} (poll 150ms) │
└────────────────┬────────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────────┐
│              SearXNG + Plugin                       │
│                                                     │
│  post_search()                                      │
│    → _enrich_results()  ← ThreadPoolExecutor        │
│      → _fetch_page_text() × 5 parallel              │
│      → _chunk_text() + _tfidf_score()               │
│      → rerank by score                              │
│    → _assemble_context()                            │
│    → inject AI Overview HTML + JS                   │
│                                                     │
│  /ai-stream                                         │
│    → validate token                                 │
│    → _detect_intent() → select system prompt        │
│    → _load_conversation() from Valkey               │
│    → launch stream_to_valkey() thread               │
│    → return {job_id} immediately                    │
│                                                     │
│  stream_to_valkey() [background thread]             │
│    → Ollama stream=True                             │
│    → RPUSH tokens to Valkey                         │
│    → RPUSH __DONE__ when complete                   │
│                                                     │
│  /ai-status/{job_id}                                │
│    → LRANGE chunks from offset                      │
│    → return {chunks, done}                          │
└────────────────┬────────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────────┐
│                  Valkey                             │
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
