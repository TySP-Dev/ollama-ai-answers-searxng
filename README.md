# Ollama AI Answers Plugin for SearXNG
**Single file install**  
**Does not block result loading time**
**Based on [ai-answers-searxng](https://github.com/cra88y/ai-answers-searxng) by [cra88y](https://github.com/cra88y)

A SearXNG plugin that generates local AI overviews powered by Ollama, using search results as RAG context.

Features:
- token-by-token UI streaming
- clickable inline citations
- interactive mode to continue summary, ask follow ups, copy, or regenerate
- simple response mode with no extras
- internally called low-latency RAG for follow ups (bypasses http loopback)
- native network integration via `searx.network` (respects proxy/SSL settings)
- stateless conversation persistence/sharability via URL


## Installation

Place `ai_answers.py` into the `searx/plugins` directory of your instance (or mount it in a container) and enable it in `settings.yml`:

```yaml
plugins:
  searx.plugins.ai_answers.SXNGPlugin:  
    active: true
```

## Configuration

Configure via environment variables:

### Required

- `LLM_URL`: Ollama chat completions endpoint. Default: `http://ollama:11434/v1/chat/completions`
- `LLM_MODEL`: Model name as listed in Ollama. Default: `llama3.2`

### Optional

- `LLM_SYSTEM_PROMPT`: Overrides the system prompt. Default: `You are a direct, citation-accurate search synthesis engine.`
- `LLM_MAX_TOKENS`: Default `200`.
- `LLM_TEMPERATURE`: Default `0.2`.
- `LLM_CONTEXT_DEEP_COUNT`: Results used as context with full snippets. Default `5`.
- `LLM_CONTEXT_SHALLOW_COUNT`: Results with headlines only (additional breadth). Default `15`.
- `LLM_TABS`: Tab whitelist, comma delimited. Default `general,science,it,news`.
- `LLM_INTERACTIVE`: UI mode. Default `true` (interactive: copy, regenerate, follow up). Set to `false` for simple response only.
- `LLM_QUESTION_MARK_REQUIRED`: Only trigger AI answers when the query contains `?`. Default `false`.

## How It Works
1. User performs initial search
2. Results return server side
3. `post_search` plugin hook fires
4. Token-optimized context extracted from results
5. UI/logic shell injected into the standard results answer object
6. Client-side script calls custom endpoint with a signed token
7. Ollama response renders token by token in the UI

## Example

### Docker Compose
```yaml
environment:
  - LLM_URL=http://ollama:11434/v1/chat/completions
  - LLM_MODEL=llama3.2
```

### Environment variables
```
LLM_URL=http://ollama:11434/v1/chat/completions
LLM_MODEL=llama3.2
```

## Development

```bash
pip install flask flask-babel
python tests/demo.py   # UI demo at localhost:5000
```
