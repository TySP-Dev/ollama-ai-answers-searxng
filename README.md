<div align="center">

[![Main Repo](https://img.shields.io/badge/Main%20Repo-gits.tysstech.com-blue?logo=gitea)](https://git.tysstech.com/tyler/PacCrypt-Webapp)
[![Mirror Repo](https://img.shields.io/badge/Mirror%20Repo-github.com-blue?logo=github)](https://github.com/TySP-Dev/ollama-ai-answers-searxng)

<div align="left">

# Ollama AI Answers Plugin for SearXNG
**Single file install**  
**Does not block result loading time**  
**Based on [ai-answers-searxng](https://github.com/cra88y/ai-answers-searxng) by [cra88y](https://github.com/cra88y)**

A SearXNG plugin that generates local AI overviews powered by Ollama, using search results as RAG context.

Features:
- token-by-token UI streaming
- clickable inline citations
- interactive mode: continue summary, ask follow-ups, copy, or regenerate
- simple response mode with no extras
- internally called low-latency RAG for follow-ups (bypasses HTTP loopback)
- native network integration via `searx.network` (respects proxy/SSL settings)
- stateless conversation persistence/shareability via URL hash
- model selector in the AI overview widget

## Installation

Place `ai_answers.py` into the `searx/plugins` directory of your SearXNG instance (or mount it in a container) and enable it in `settings.yml`:

```yaml
plugins:
  searx.plugins.ai_answers.SXNGPlugin:
    active: true
```

## Configuration

Configure via environment variables.

### Required

| Variable | Description | Default |
|---|---|---|
| `LLM_URL` | Ollama chat completions endpoint | `http://ollama:11434/v1/chat/completions` |
| `LLM_MODEL` | Model name as listed in Ollama | `qwen3.5:9b` |

### Optional

| Variable | Description | Default |
|---|---|---|
| `LLM_SYSTEM_PROMPT` | Overrides the default system prompt | `You are a direct, citation-accurate search synthesis engine.` |
| `LLM_MAX_TOKENS` | Max tokens in the AI response | `200` |
| `LLM_TEMPERATURE` | Sampling temperature | `0.2` |
| `LLM_CONTEXT_DEEP_COUNT` | Results used with full snippets | `5` |
| `LLM_CONTEXT_SHALLOW_COUNT` | Results with headlines only (breadth) | `15` |
| `LLM_TABS` | Comma-delimited tab whitelist | `general,science,it,news` |
| `LLM_INTERACTIVE` | Interactive UI mode (copy, regenerate, follow-up) | `true` |
| `LLM_QUESTION_MARK_REQUIRED` | Only trigger on queries containing `?` | `false` |

## How It Works

1. User performs a search
2. Results return server-side
3. `post_search` plugin hook fires
4. Token-optimized context is extracted from results
5. UI/logic shell injected into the standard answers object
6. Client-side script calls a signed endpoint (`/ai-stream`)
7. Ollama streams a response token-by-token in the UI

## Docker Compose Example

```yaml
services:
  searxng:
    environment:
      - LLM_URL=http://ollama:11434/v1/chat/completions
      - LLM_MODEL=qwen3.5:9b
    volumes:
      - ./ai_answers.py:/usr/local/searxng/searx/plugins/ai_answers.py

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

## Development — Demo Server

A standalone Flask demo server is included in `tests/demo.py`. It mocks the SearXNG plugin environment so you can test the full UI without a running SearXNG instance.

### Setup

```bash
pip install flask flask-babel certifi
```

### Run

```bash
python tests/demo.py
```

Then open [http://127.0.0.1:5000/](http://127.0.0.1:5000/) in your browser.

> **Note:** Use `127.0.0.1:5000`, not `localhost:5000` — macOS AirPlay Receiver can occupy the IPv6 loopback on port 5000.

### Usage

- Type a query in the search bar and hit **Search** to trigger an AI overview.
- Expand **Ollama Configuration** at the top to change the endpoint URL or Bearer token for the current session. Click **Apply** to save and re-run the current query.
- The model selector in the AI overview widget (loaded from `/ai-models`) shows all models available on the configured Ollama server and persists your choice in the session URL.

### Environment Variables (demo)

The demo reads the same variables as the plugin:

```bash
LLM_URL=http://localhost:11434/v1/chat/completions \
LLM_MODEL=qwen3.5:9b \
python tests/demo.py
```

Or export them before running. Any values set in the config panel at runtime take priority for that session.
