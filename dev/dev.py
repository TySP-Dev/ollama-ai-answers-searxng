"""
dev.py — AI Answers Plugin Development Server
Provides a full-featured local dev environment for ollama_answers.py.

Features:
  - Mock SearXNG environment so the plugin loads without a real SearXNG install
  - Optional live SearXNG backend (set SEARXNG_URL to proxy real searches)
  - Debug console with request/response logging
  - Plugin API explorer (all routes: /ai-stream, /ai-status, /ai-models, /ai-conversation, /ai-auxiliary-search)
  - Dark UI matching SearXNG's default theme with real CSS vars
  - Config panel: LLM endpoint, model, token limits, temperature
  - Live query testing with streaming output in the browser

Usage:
  cd dev/
  python dev.py

  Or with a live SearXNG backend:
  SEARXNG_URL=http://localhost:8080 python dev.py

Environment variables (all optional):
  LLM_URL            Ollama endpoint  (default: http://localhost:11434/v1/chat/completions)
  LLM_MODEL          Model name       (default: qwen3.5:9b)
  LLM_MAX_TOKENS     Max tokens       (default: 200)
  LLM_TEMPERATURE    Temperature      (default: 0.2)
  SEARXNG_URL        Live SearXNG URL (default: None, uses mock results)
  DEV_PORT           Port to listen on (default: 5000)
  DEV_HOST           Host to bind to   (default: 127.0.0.1)
"""

import sys
import os
import json
import time
import logging
import hashlib
import secrets
import urllib.request
import urllib.parse
from types import ModuleType

# ── path setup ──────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── .env loading ─────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    # look for .env in the project root (one level up from dev/)
    _env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
    if os.path.exists(_env_path):
        load_dotenv(_env_path)
        print(f"  Loaded .env from {_env_path}")
except ImportError:
    pass  # python-dotenv not installed, ignore

# ── logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)-8s %(name)s: %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger('dev')

# ── env defaults ─────────────────────────────────────────────────────────────
os.environ.setdefault('LLM_URL', 'http://localhost:11434/v1/chat/completions')
os.environ.setdefault('LLM_MODEL', 'qwen3.5:9b')

SEARXNG_URL = os.getenv('SEARXNG_URL', '').rstrip('/')
DEV_PORT    = int(os.getenv('DEV_PORT', 5000))
DEV_HOST    = os.getenv('DEV_HOST', '127.0.0.1')

# ── SearXNG module mocks ─────────────────────────────────────────────────────
searx_mod         = ModuleType("searx")
searx_plugins_mod = ModuleType("searx.plugins")
searx_results_mod = ModuleType("searx.result_types")
searx_network_mod = ModuleType("searx.network")

class _MockPlugin:
    def __init__(self, cfg):
        self.active = getattr(cfg, 'active', True)

class _MockPluginInfo:
    def __init__(self, **kwargs):
        self.meta = kwargs

class _MockEngineResults:
    def __init__(self):
        self.types = ModuleType("types")
        self.types.Answer = lambda *a, **kw: kw.get('answer', a[0] if a else "")
        self._results = []
    def add(self, res):
        self._results.append(res)

searx_plugins_mod.Plugin     = _MockPlugin
searx_plugins_mod.PluginInfo = _MockPluginInfo
searx_results_mod.EngineResults = _MockEngineResults
searx_mod.settings = {'server': {'secret_key': 'dev-secret-key'}}

# network mock — forwards real HTTP so Ollama calls work
import http.client, ssl as _ssl

def _real_http(method, url, **kwargs):
    from urllib.parse import urlparse
    p = urlparse(url)
    port = p.port or (443 if p.scheme == 'https' else 80)
    path = p.path + (f'?{p.query}' if p.query else '')
    if p.scheme == 'https':
        conn = http.client.HTTPSConnection(p.hostname, port, timeout=30,
                                           context=_ssl.create_default_context())
    else:
        conn = http.client.HTTPConnection(p.hostname, port, timeout=30)
    headers = kwargs.get('headers', {})
    body = json.dumps(kwargs['json']).encode() if kwargs.get('json') else kwargs.get('data')
    if kwargs.get('params'):
        qs = urllib.parse.urlencode(kwargs['params'])
        path += ('&' if '?' in path else '?') + qs
    conn.request(method, path, body=body, headers=headers)
    return conn.getresponse()

class _MockHTTPResponse:
    def __init__(self, r):
        self.status_code = r.status
        self._content = r.read()
        self.text = self._content.decode('utf-8', errors='replace')
    def json(self):
        return json.loads(self.text)

def _mock_get(url, **kw):
    return _MockHTTPResponse(_real_http('GET', url, **kw))

def _mock_stream(method, url, **kw):
    r = _real_http(method, url, **kw)
    class _Resp:
        status_code = r.status
        text = "streaming"
    def _gen():
        while True:
            chunk = r.read(256)
            if not chunk: break
            yield chunk
    return _Resp(), _gen()

searx_network_mod.stream = _mock_stream
searx_network_mod.get    = _mock_get

sys.modules['searx']              = searx_mod
sys.modules['searx.plugins']      = searx_plugins_mod
sys.modules['searx.result_types'] = searx_results_mod
sys.modules['searx.network']      = searx_network_mod

# ── log capture handler for intent / score extraction ────────────────────────
class _ScoreCaptureHandler(logging.Handler):
    """Captures plugin log messages so dev routes can extract intent/scores."""
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(self.format(record))

    def clear(self):
        self.records.clear()

_score_handler = _ScoreCaptureHandler()
_score_handler.setFormatter(logging.Formatter('%(message)s'))
logging.getLogger('ollama_answers').addHandler(_score_handler)

# ── load plugin ───────────────────────────────────────────────────────────────
from flask import Flask, request, redirect, jsonify, Response
from flask_babel import Babel
from ollama_answers import SXNGPlugin

app  = Flask(__name__)
babel = Babel(app)

class _MockCfg:
    active = True

plugin = SXNGPlugin(_MockCfg())
plugin.init(app)
# Override api_key from env if set
env_key = os.getenv('LLM_API_KEY', '').strip()
if env_key:
    plugin.api_key = env_key

# Verify Valkey is reachable
try:
    import valkey as _vk
    _vk_host = os.getenv('VALKEY_HOST', 'localhost')
    _vk_port = int(os.getenv('VALKEY_PORT', 6379))
    _test_vk = _vk.Valkey(host=_vk_host, port=_vk_port,
                           socket_connect_timeout=2)
    _test_vk.ping()
    print(f"  Valkey:      {_vk_host}:{_vk_port} ✓")
except Exception as e:
    print(f"""
  ⚠ Valkey not reachable at {_vk_host}:{_vk_port}
  Streaming will not work without Valkey.

  Start one with Docker:
    docker run -d --name dev-valkey -p 6379:6379 valkey/valkey:9-alpine

  Or set VALKEY_HOST and VALKEY_PORT in your .env file.
  Error: {e}
""")

# ── request/response debug log storage ───────────────────────────────────────
_DEV_SESSION = secrets.token_hex(12)

_dev_log = []   # list of {ts, method, path, status, ms, req_body, resp_body}
MAX_LOG = 100

@app.before_request
def _log_req():
    request._dev_start = time.time()
    request._dev_body  = request.get_data(as_text=True)

@app.after_request
def _log_resp(resp):
    if request.path.startswith('/static'):
        return resp
    ms = round((time.time() - getattr(request, '_dev_start', time.time())) * 1000, 1)
    entry = {
        'ts':       time.strftime('%H:%M:%S'),
        'method':   request.method,
        'path':     request.full_path.rstrip('?'),
        'status':   resp.status_code,
        'ms':       ms,
        'req':      getattr(request, '_dev_body', ''),
        'resp':     resp.get_data(as_text=True)[:2000],
    }
    _dev_log.insert(0, entry)
    if len(_dev_log) > MAX_LOG:
        _dev_log.pop()
    return resp

# ── mock search results ───────────────────────────────────────────────────────
MOCK_RESULTS = {
    'default': [
        {"title": "Wikipedia — General Knowledge",
         "content": "A comprehensive reference covering the query topic in depth with cited sources.",
         "url": "https://en.wikipedia.org/wiki/Main_Page", "publishedDate": "2026-01-15"},
        {"title": "NASA Science — Research Article",
         "content": "Peer-reviewed research providing scientific context and empirical data on the topic.",
         "url": "https://science.nasa.gov/", "publishedDate": "2026-01-10"},
        {"title": "Scientific American — Analysis",
         "content": "In-depth analysis covering recent discoveries and expert commentary.",
         "url": "https://scientificamerican.com/", "publishedDate": "2026-01-01"},
        {"title": "BBC Science — Overview",
         "content": "Accessible explainer covering the fundamentals and current understanding.",
         "url": "https://bbc.com/science", "publishedDate": "2025-12-20"},
        {"title": "MIT OpenCourseWare — Educational Resource",
         "content": "Course materials and lecture notes providing structured educational content.",
         "url": "https://ocw.mit.edu/", "publishedDate": "2025-12-15"},
    ] + [
        {"title": f"Source {i} — Additional Context",
         "content": f"Supplementary information and supporting details from source {i}.",
         "url": f"https://example.com/source-{i}", "publishedDate": "2025-12-01"}
        for i in range(6, 16)
    ]
}

def _get_results(query: str):
    """Return real results from SearXNG if configured, else mock results."""
    if SEARXNG_URL:
        try:
            qs = urllib.parse.urlencode({'q': query, 'format': 'json', 'categories': 'general'})
            url = f"{SEARXNG_URL}/search?{qs}"
            with urllib.request.urlopen(url, timeout=8) as r:
                data = json.loads(r.read())
            results = data.get('results', [])
            logger.info(f"Live SearXNG: {len(results)} results for '{query}'")
            return results, data.get('infoboxes', []), data.get('answers', [])
        except Exception as e:
            logger.warning(f"Live SearXNG failed ({e}), falling back to mock results")
    return MOCK_RESULTS['default'], [], []

# ── search route used by the demo UI ─────────────────────────────────────────
_last_render = {'html': '', 'query': '', 'scores': [], 'intent': '—', 'summary': ''}

@app.route('/dev/search')
def dev_search():
    query = request.args.get('q', 'why is the sky blue')

    global _last_render
    _last_render['intent'] = '…'
    _last_render['query'] = query

    class _MockSearch:
        class _MockSearchQuery:
            pageno     = 1
            lang       = 'en-US'
            categories = ['general']
        search_query = _MockSearchQuery()
        search_query.query = query

        class _MockResultContainer:
            def __init__(self, q):
                self.answers   = set()
                self._q        = q
                self.infoboxes = []
            def get_ordered_results(self):
                results, iboxes, _ = _get_results(self._q)
                self.infoboxes = iboxes
                return results

        result_container = _MockResultContainer(query)

    session_id = secrets.token_hex(12)

    # Clear prior conversation so each search starts fresh
    try:
        from ollama_answers import _get_valkey
        vk = _get_valkey()
        vk.delete(f"ai:conv:{session_id}")
    except Exception:
        pass

    class _MockRequest:
        cookies = {'sxng_ai_session': session_id}
        headers = {}
        args = {}
        script_root = ''

        class _MockForm(dict):
            def get(self, key, default=None):
                return default  # always return html format

        form = _MockForm()

    _score_handler.clear()
    search = _MockSearch()
    try:
        plugin.post_search(_MockRequest(), search)
    except Exception as e:
        logger.error(f"post_search failed: {e}", exc_info=True)
        return f"<p style='color:red'>post_search error: {e}</p>"

    html_payload = ""
    if search.result_container.answers:
        raw = list(search.result_container.answers)[0]
        # Markup objects need str() to get the HTML
        html_payload = str(raw) if raw else ""
        logger.info(f"Got AI answer, length: {len(html_payload)}")
        if html_payload:
            import re as _re
            text_preview = _re.sub(r'<[^>]+>', '', html_payload)
            text_preview = ' '.join(text_preview.split())[:300]
            logger.info(f"AI output preview: {text_preview}")
    else:
        logger.warning("post_search produced no answers")

    # Parse intent, per-URL TF-IDF scores, and rerank summary from captured log records
# Parse intent, per-URL TF-IDF scores, and rerank summary from captured log records
    import re as _re
    detected_intent = '—'

    scores = []
    fallback_domains = set()
    summary = ''

    for msg in _score_handler.records:
        m = _re.match(r'AI Answers: \[(.+?)\] score=([\d.]+) chunks=(\d+)', msg)
        
        if m:
            scores.append({
                'url': m.group(1),
                'score': float(m.group(2)),
                'chunks': int(m.group(3)),
                'fallback': False
            })

        m = _re.match(r'AI Answers: falling back to snippet for \[\d+\] (.+)', msg)
        if m:
            fallback_domains.add(m.group(1))

        m = _re.match(r'AI Answers: reranked (\d+) results, top score=([\d.]+)', msg)
        if m:
            summary = f"{m.group(1)} reranked · top {float(m.group(2)):.4f}"

    for s in scores:
        domain = s['url'].replace('https://', '').replace('http://', '').split('/')[0]

        if domain in fallback_domains:
            s['fallback'] = True

    scores.sort(key=lambda s: s['score'], reverse=True)

    # Refresh latest intent from Valkey
    try:
        from ollama_answers import _get_valkey
        vk = _get_valkey()
        latest_intent = vk.get("ai:last_intent")

        if latest_intent:
            detected_intent = latest_intent
    except Exception:
        pass

    _last_render['html'] = html_payload
    _last_render['query'] = query
    _last_render['intent'] = detected_intent
    _last_render['scores'] = scores
    _last_render['summary'] = summary

    global _last_job

    _last_job['intent'] = detected_intent
    _last_job['query'] = query

    logger.warning(f"RETURNING INTENT TO UI: {repr(_last_job.get('intent'))}")

    return jsonify({'ok': True, 'query': query})

@app.route('/dev/render')
def dev_render():
    html = _last_render.get('html', '')
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8">
<style>
  * {{ box-sizing: border-box; }}
  body {{ margin: 0; padding: 0.5rem;
         background: #1e1e2e; color: #cdd6f4;
         font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         overflow: hidden; }}
  :root {{
    --color-answer-background: #2a2a3e;
    --color-answer-font: #cdd6f4;
    --color-result-link: #89b4fa;
    --color-base-font: #cdd6f4;
    --color-sidebar-bg: #313346;
    --color-result-hover: #414168;
    --color-base-background: #1e1e2e;
    --color-base-background-hover: rgba(255,255,255,0.05);
    --color-search-font: #a6adc8;
    --color-result-border: #414168;
    --color-result-description: #bac2de;
    --color-toolkit-select-background: #313346;
  }}
  #answers {{ display: none; }}
  #results {{ display: block; min-height: 100vh; }}
</style>
</head>
<body>
<div id="answers"><div class="answer"><span>{html}</span></div></div>
<div id="results"></div>
</body>
</html>"""

# ── dev API log endpoint ──────────────────────────────────────────────────────
_last_job = {'job_id': None, 'intent': None, 'model': None,
             'query': None, 'start_ts': None}

def _get_last_job_from_log():
    for entry in _dev_log:
        if '/ai-stream' in entry.get('path', '') and entry.get('method') == 'POST':
            try:
                resp = json.loads(entry.get('resp', '{}'))
                if resp.get('job_id'):
                    return resp['job_id']
            except Exception:
                pass
    return None

@app.route('/dev/log')
def dev_log():
    return jsonify(_dev_log)

@app.route('/dev/log/clear', methods=['POST'])
def dev_log_clear():
    _dev_log.clear()
    return jsonify({'ok': True})

@app.route('/dev/last-job')
def dev_last_job():
    return jsonify(_last_job)

@app.route('/dev/last-scores')
def dev_last_scores():
    return jsonify({
        'scores':  _last_render.get('scores', []),
        'intent':  _last_render.get('intent', '—'),
        'query':   _last_render.get('query', ''),
        'summary': _last_render.get('summary', ''),
    })

@app.route('/dev/stream-watch')
def dev_stream_watch():
    job_id = request.args.get('job_id', '')
    if not job_id:
        return jsonify({'error': 'no job_id'}), 400

    def generate():
        import valkey as _vk
        host = os.getenv('VALKEY_HOST', 'localhost')
        port = int(os.getenv('VALKEY_PORT', 6379))
        v = _vk.Valkey(host=host, port=port,
                       socket_connect_timeout=2,
                       decode_responses=True)
        offset = 0
        max_polls = 600
        polls = 0
        while polls < max_polls:
            polls += 1
            time.sleep(0.1)
            try:
                chunks = v.lrange(f"ai:job:{job_id}:chunks", offset, -1)
                for chunk in chunks:
                    if chunk == '__DONE__':
                        yield "data: __DONE__\n\n"
                        return
                    if chunk.startswith('__ERROR__'):
                        yield f"data: __ERROR__{chunk[9:]}\n\n"
                        return
                    yield f"data: {json.dumps(chunk)}\n\n"
                offset += len(chunks)
            except Exception as e:
                yield f"data: __ERROR__{e}\n\n"
                return
        yield "data: __DONE__\n\n"

    return Response(generate(),
                   mimetype='text/event-stream',
                   headers={
                       'Cache-Control': 'no-cache',
                       'X-Accel-Buffering': 'no'
                   })

# ── dev config update ─────────────────────────────────────────────────────────
@app.route('/dev/config', methods=['POST'])
def dev_config():
    global SEARXNG_URL
    data = request.json or {}
    if 'llm_url' in data:
        url = data.get('llm_url', '').strip()
        if url:
            plugin.endpoint_url = url
            os.environ['LLM_URL'] = url
    if 'model' in data and data['model']:
        plugin.model = data['model'].strip()
        os.environ['LLM_MODEL'] = plugin.model
    if 'max_tokens' in data:
        try:
            plugin.max_tokens = max(1, int(data['max_tokens']))
        except (ValueError, TypeError):
            pass
    if 'temperature' in data:
        try:
            plugin.temperature = float(data['temperature'])
        except (ValueError, TypeError):
            pass
    if 'api_key' in data:
        plugin.api_key = data['api_key'].strip() or 'ollama'
    if 'searxng_url' in data:
        SEARXNG_URL = data['searxng_url'].strip().rstrip('/')
    if 'valkey_host' in data or 'valkey_port' in data:
        host = data.get('valkey_host', os.getenv('VALKEY_HOST', 'localhost')).strip()
        port = int(data.get('valkey_port', os.getenv('VALKEY_PORT', 6379)))
        os.environ['VALKEY_HOST'] = host
        os.environ['VALKEY_PORT'] = str(port)
        # Reset the valkey connection pool so it picks up new settings
        import ollama_answers as _oa
        _oa._VALKEY_POOL = None
    return jsonify({
        'llm_url':     plugin.endpoint_url,
        'model':       plugin.model,
        'max_tokens':  plugin.max_tokens,
        'temperature': plugin.temperature,
        'api_key':     plugin.api_key,
        'searxng_url': SEARXNG_URL or None,
    })

@app.route('/dev/config', methods=['GET'])
def dev_config_get():
    return jsonify({
        'llm_url':     plugin.endpoint_url,
        'model':       plugin.model,
        'max_tokens':  plugin.max_tokens,
        'temperature': plugin.temperature,
        'api_key':     plugin.api_key,
        'searxng_url': SEARXNG_URL or None,
        'interactive': plugin.interactive,
        'allowed_tabs': list(plugin.allowed_tabs),
        'valkey_host': os.getenv('VALKEY_HOST', 'localhost'),
        'valkey_port': int(os.getenv('VALKEY_PORT', 6379)),
    })

# ── main dev UI ───────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return DEV_HTML

DEV_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AI Answers — Dev Server</title>
<style>
  :root {
    --bg: #1e1e2e; --bg2: #2a2a3e; --bg3: #313346;
    --border: #414168; --text: #cdd6f4; --muted: #6c7086;
    --accent: #89b4fa; --green: #a6e3a1; --red: #f38ba8;
    --yellow: #f9e2af; --orange: #fab387;
    font-size: 14px;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; display: flex; flex-direction: column; height: 100vh; overflow: hidden; }

  /* ── top bar ── */
  header { background: var(--bg2); border-bottom: 1px solid var(--border); padding: 0.6rem 1rem; display: flex; align-items: center; gap: 1rem; flex-shrink: 0; }
  header h1 { font-size: 1rem; font-weight: 700; color: var(--accent); }
  header .badge { font-size: 0.7rem; background: var(--bg3); border: 1px solid var(--border); border-radius: 4px; padding: 1px 6px; color: var(--muted); }
  header .badge.live { border-color: var(--green); color: var(--green); }
  header .spacer { flex: 1; }

  /* ── layout ── */
  .layout { display: flex; flex: 1; overflow: hidden; }

  /* ── left panel: search + output ── */
  .main-panel { flex: 1; display: flex; flex-direction: column; overflow: hidden; }
  .search-bar { padding: 0.75rem 1rem; background: var(--bg2); border-bottom: 1px solid var(--border); display: flex; gap: 0.5rem; flex-shrink: 0; }
  .search-bar input { flex: 1; background: var(--bg3); border: 1px solid var(--border); border-radius: 6px; color: var(--text); padding: 0.45rem 0.75rem; font-size: 0.9rem; }
  .search-bar input:focus { outline: none; border-color: var(--accent); }
  .search-bar button { background: var(--accent); color: var(--bg); border: none; border-radius: 6px; padding: 0.45rem 1.1rem; cursor: pointer; font-weight: 600; font-size: 0.85rem; }
  .search-bar button:hover { opacity: 0.85; }
  .search-bar button:disabled { opacity: 0.4; cursor: default; }

  .output-area { flex: 1; overflow-y: auto; padding: 1rem; }
  .output-area #ai-output { min-height: 4rem; }

  /* SearXNG CSS vars so the plugin looks right */
  #ai-output {
    --color-answer-background: #2a2a3e;
    --color-answer-font: #cdd6f4;
    --color-result-link: #89b4fa;
    --color-base-font: #cdd6f4;
    --color-sidebar-bg: #313346;
    --color-result-hover: #414168;
    --color-base-background: #1e1e2e;
    --color-base-background-hover: rgba(255,255,255,0.05);
    --color-search-font: #a6adc8;
    --color-result-border: #414168;
    --color-result-description: #bac2de;
    --color-toolkit-select-background: #313346;
  }

  /* ── right panel: config + debug ── */
  .side-panel { width: 360px; border-left: 1px solid var(--border); display: flex; flex-direction: column; overflow: hidden; flex-shrink: 0; }
  .tabs { display: flex; background: var(--bg2); border-bottom: 1px solid var(--border); flex-shrink: 0; }
  .tab { flex: 1; padding: 0.5rem; text-align: center; font-size: 0.78rem; color: var(--muted); cursor: pointer; border-bottom: 2px solid transparent; }
  .tab.active { color: var(--accent); border-bottom-color: var(--accent); }
  .tab-content { display: none; flex: 1; overflow-y: auto; padding: 0.75rem; flex-direction: column; gap: 0.6rem; }
  .tab-content.active { display: flex; }

  /* ── config tab ── */
  .field label { display: block; font-size: 0.72rem; color: var(--muted); margin-bottom: 3px; text-transform: uppercase; letter-spacing: 0.04em; }
  .field input, .field select { width: 100%; background: var(--bg3); border: 1px solid var(--border); border-radius: 5px; color: var(--text); padding: 0.35rem 0.6rem; font-size: 0.82rem; }
  .field input:focus, .field select:focus { outline: none; border-color: var(--accent); }
  .field input[type=range] { padding: 0; background: none; border: none; }
  .range-row { display: flex; align-items: center; gap: 0.5rem; }
  .range-val { font-size: 0.8rem; color: var(--accent); min-width: 2.5rem; text-align: right; }
  .apply-btn { background: var(--accent); color: var(--bg); border: none; border-radius: 5px; padding: 0.4rem 1rem; cursor: pointer; font-weight: 700; font-size: 0.82rem; align-self: flex-start; }
  .apply-btn:hover { opacity: 0.85; }
  .section-title { font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.06em; color: var(--muted); border-bottom: 1px solid var(--border); padding-bottom: 0.3rem; margin-top: 0.25rem; }
  .info-row { display: flex; justify-content: space-between; font-size: 0.78rem; }
  .info-row span:first-child { color: var(--muted); }
  .info-row span:last-child { color: var(--text); font-family: monospace; }

  /* ── debug log tab ── */
  .log-controls { display: flex; gap: 0.5rem; margin-bottom: 0.5rem; flex-shrink: 0; }
  .log-controls button { background: var(--bg3); border: 1px solid var(--border); color: var(--muted); border-radius: 4px; padding: 3px 10px; cursor: pointer; font-size: 0.75rem; }
  .log-controls button:hover { color: var(--text); border-color: var(--accent); }
  .log-list { flex: 1; overflow-y: auto; display: flex; flex-direction: column; gap: 0.4rem; }
  .log-entry { background: var(--bg3); border: 1px solid var(--border); border-radius: 5px; font-size: 0.75rem; overflow: hidden; }
  .log-entry-header { display: flex; align-items: center; gap: 0.4rem; padding: 0.3rem 0.5rem; cursor: pointer; }
  .log-entry-header:hover { background: var(--bg2); }
  .log-ts { color: var(--muted); font-family: monospace; }
  .log-method { font-weight: 700; font-family: monospace; }
  .log-path { flex: 1; color: var(--text); font-family: monospace; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .log-status { font-family: monospace; font-weight: 700; }
  .log-ms { color: var(--muted); }
  .log-body { display: none; border-top: 1px solid var(--border); padding: 0.5rem; }
  .log-body.open { display: block; }
  .log-body pre { white-space: pre-wrap; word-break: break-all; color: var(--muted); font-size: 0.72rem; max-height: 200px; overflow-y: auto; }
  .s200 { color: var(--green); } .s4xx { color: var(--red); } .s5xx { color: var(--orange); }
  .GET { color: var(--accent); } .POST { color: var(--green); } .DELETE { color: var(--red); }

  /* ── api explorer tab ── */
  .api-route { background: var(--bg3); border: 1px solid var(--border); border-radius: 5px; overflow: hidden; margin-bottom: 0.4rem; }
  .api-route-header { display: flex; align-items: center; gap: 0.5rem; padding: 0.4rem 0.6rem; cursor: pointer; }
  .api-route-header:hover { background: var(--bg2); }
  .api-route-body { display: none; padding: 0.6rem; border-top: 1px solid var(--border); }
  .api-route-body.open { display: block; }
  .api-route-body textarea { width: 100%; background: var(--bg2); border: 1px solid var(--border); border-radius: 4px; color: var(--text); font-family: monospace; font-size: 0.75rem; padding: 0.4rem; resize: vertical; }
  .api-route-body textarea:focus { outline: none; border-color: var(--accent); }
  .api-send { background: var(--accent); color: var(--bg); border: none; border-radius: 4px; padding: 0.3rem 0.8rem; cursor: pointer; font-size: 0.75rem; font-weight: 700; margin-top: 0.4rem; }
  .api-resp { margin-top: 0.4rem; background: var(--bg2); border: 1px solid var(--border); border-radius: 4px; padding: 0.4rem; font-family: monospace; font-size: 0.72rem; color: var(--muted); white-space: pre-wrap; max-height: 180px; overflow-y: auto; display: none; }
  .api-resp.visible { display: block; }
  .method-badge { font-size: 0.68rem; font-weight: 700; padding: 1px 5px; border-radius: 3px; }
  .mb-get { background: #1a3a5c; color: var(--accent); }
  .mb-post { background: #1a3d2b; color: var(--green); }
  .mb-delete { background: #3d1a1a; color: var(--red); }
  .route-name { font-size: 0.8rem; font-family: monospace; }
  .route-desc { font-size: 0.72rem; color: var(--muted); }

  /* ── config panel overlay ── */

  .no-entries { color: var(--muted); font-size: 0.8rem; text-align: center; padding: 1.5rem; }
</style>
</head>
<body>

<header>
  <h1>✦ AI Answers Dev</h1>
  <span class="badge" id="searxng-badge">Mock Results</span>
  <span class="badge" id="ollama-badge">Ollama: checking…</span>
  <span class="badge" id="valkey-badge">Valkey: checking…</span>
  <span class="spacer"></span>
</header>

<div class="layout">
  <div class="main-panel">
    <div class="search-bar">
      <input id="q" type="text" value="why is the sky blue" placeholder="Search query…" onkeydown="if(event.key==='Enter')doSearch()">
      <button id="search-btn" onclick="doSearch()">Search</button>
    </div>
    <div class="output-area">
      <div id="ai-output">
        <p style="color:var(--muted);padding:0.5rem;font-size:0.85rem;">Enter a query above and click Search to test the plugin.</p>
      </div>
      <div id="tfidf-panel" style="display:none;margin-top:0.75rem;background:var(--bg2);border:1px solid var(--border);border-radius:6px;overflow:hidden;">
        <div style="padding:0.4rem 0.75rem;background:var(--bg3);border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;">
          <span style="font-size:0.72rem;text-transform:uppercase;letter-spacing:0.06em;color:var(--muted)">TF-IDF Relevance Scores</span>
          <span id="tfidf-summary" style="font-size:0.72rem;color:var(--accent);font-family:monospace"></span>
        </div>
        <div style="padding:0.5rem 0.75rem">
          <div id="tfidf-bars"></div>
        </div>
      </div>
    </div>
  </div>

  <div class="side-panel">
    <div class="tabs">
      <div class="tab active" onclick="showTab('config')">Config</div>
      <div class="tab" onclick="showTab('debug')">Debug Log</div>
      <div class="tab" onclick="showTab('stream')">Stream</div>
      <div class="tab" onclick="showTab('api')">API Explorer</div>
    </div>

    <!-- Config tab -->
    <div class="tab-content active" id="tab-config">
      <div class="section-title">Plugin Config</div>
      <div class="field" id="info-url"><label>LLM Endpoint</label><span style="font-family:monospace;font-size:0.78rem;color:var(--accent)">loading…</span></div>
      <div class="info-row"><span>Model</span><span id="info-model">—</span></div>
      <div class="info-row"><span>Max Tokens</span><span id="info-tokens">—</span></div>
      <div class="info-row"><span>Temperature</span><span id="info-temp">—</span></div>
      <div class="info-row"><span>Interactive</span><span id="info-interactive">—</span></div>
      <div class="info-row"><span>Allowed Tabs</span><span id="info-tabs">—</span></div>
      <div class="info-row"><span>SearXNG Backend</span><span id="info-searxng">mock</span></div>
      <div class="info-row"><span>Valkey</span><span id="info-valkey">—</span></div>

      <div class="section-title" style="margin-top:0.5rem">Edit Config</div>
      <div class="field"><label>LLM URL</label><input id="cfg-url" placeholder="http://localhost:11434/v1/chat/completions"></div>
      <div class="field">
        <label>Model</label>
        <select id="cfg-model"><option value="">loading…</option></select>
      </div>
      <div class="field"><label>Bearer Token <span style="opacity:0.5;font-size:0.68rem;text-transform:none">(leave empty for Ollama)</span></label><input id="cfg-key" placeholder="sk-… or leave blank"></div>
      <div class="field"><label>SearXNG URL <span style="opacity:0.5;font-size:0.68rem;text-transform:none">(optional — uses mock if blank)</span></label><input id="cfg-searxng" placeholder="http://localhost:8080"></div>
      <div class="field"><label>Valkey Host</label>
        <input id="cfg-valkey-host" placeholder="localhost"></div>
      <div class="field"><label>Valkey Port</label>
        <input id="cfg-valkey-port" type="number" placeholder="6379"></div>
      <div class="field">
        <label>Max Tokens: <span id="cfg-tokens-val">200</span></label>
        <input type="range" id="cfg-tokens" min="50" max="2000" step="50" value="200" oninput="document.getElementById('cfg-tokens-val').textContent=this.value">
      </div>
      <div class="field">
        <label>Temperature: <span id="cfg-temp-val">0.20</span></label>
        <input type="range" id="cfg-temp" min="0" max="2" step="0.05" value="0.2" oninput="document.getElementById('cfg-temp-val').textContent=parseFloat(this.value).toFixed(2)">
      </div>
      <button class="apply-btn" onclick="applyConfig()">Apply Changes</button>
    </div>

    <!-- Debug log tab -->
    <div class="tab-content" id="tab-debug">
      <div class="log-controls">
        <button onclick="refreshLog()">⟳ Refresh</button>
        <button onclick="clearLog()">✕ Clear</button>
        <button onclick="autoRefresh=!autoRefresh;this.textContent=autoRefresh?'⏸ Auto':'▶ Auto'">▶ Auto</button>
      </div>
      <div class="log-list" id="log-list">
        <div class="no-entries">No requests yet</div>
      </div>
    </div>

    <!-- Stream tab -->
    <div class="tab-content" id="tab-stream">
      <div class="log-controls">
        <span id="stream-status" style="font-size:0.75rem;color:var(--muted)">Idle</span>
        <span class="spacer" style="flex:1"></span>
        <button onclick="clearStream()">✕ Clear</button>
      </div>
      <div style="margin-bottom:0.5rem;font-size:0.72rem;color:var(--muted)">
        Shows token chunks arriving from Valkey in real time during streaming.
      </div>
      <div id="stream-meta" style="display:none;margin-bottom:0.5rem;
           background:var(--bg3);border:1px solid var(--border);
           border-radius:4px;padding:0.4rem 0.6rem;font-size:0.75rem;">
        <div class="info-row"><span>Job ID</span><span id="sv-job-id" style="font-family:monospace">—</span></div>
        <div class="info-row"><span>Intent</span><span id="sv-intent">—</span></div>
        <div class="info-row"><span>Model</span><span id="sv-model">—</span></div>
        <div class="info-row"><span>Chunks</span><span id="sv-chunks">0</span></div>
        <div class="info-row"><span>Tokens est.</span><span id="sv-tokens">0</span></div>
        <div class="info-row"><span>Elapsed</span><span id="sv-elapsed">0ms</span></div>
      </div>
      <div id="stream-viewer" style="
           flex:1;overflow-y:auto;
           background:var(--bg3);border:1px solid var(--border);
           border-radius:4px;padding:0.5rem;
           font-family:monospace;font-size:0.75rem;
           line-height:1.6;color:var(--text);
           white-space:pre-wrap;word-break:break-word;
           min-height:200px;">
        <span style="color:var(--muted)">Waiting for next search…</span>
      </div>
      <div style="margin-top:0.4rem;display:flex;gap:0.5rem;font-size:0.72rem;color:var(--muted)">
        <span id="sv-first-token">First token: —</span>
        <span>|</span>
        <span id="sv-total-time">Total: —</span>
      </div>
    </div>

    <!-- API Explorer tab -->
    <div class="tab-content" id="tab-api">
      <p style="font-size:0.75rem;color:var(--muted);margin-bottom:0.5rem">Test plugin routes directly. Token is auto-injected.</p>

      <div class="api-route">
        <div class="api-route-header" onclick="toggleApi(this)">
          <span class="method-badge mb-get">GET</span>
          <span class="route-name">/ai-models</span>
          <span class="route-desc" style="margin-left:auto">List Ollama models</span>
        </div>
        <div class="api-route-body">
          <button class="api-send" onclick="apiCall('GET','/ai-models',null,this)">Send</button>
          <pre class="api-resp"></pre>
        </div>
      </div>

      <div class="api-route">
        <div class="api-route-header" onclick="toggleApi(this)">
          <span class="method-badge mb-post">POST</span>
          <span class="route-name">/ai-stream</span>
          <span class="route-desc" style="margin-left:auto">Start AI stream</span>
        </div>
        <div class="api-route-body">
          <textarea id="api-stream-body" rows="5">{
  "q": "what is linux",
  "lang": "en-US",
  "context": "DEEP SOURCES:\\n[1] example.com: Linux is an open-source OS kernel.",
  "model": ""
}</textarea>
          <button class="api-send" onclick="apiCall('POST','/ai-stream',document.getElementById('api-stream-body').value,this)">Send</button>
          <pre class="api-resp"></pre>
        </div>
      </div>

      <div class="api-route">
        <div class="api-route-header" onclick="toggleApi(this)">
          <span class="method-badge mb-get">GET</span>
          <span class="route-name">/ai-status/{job_id}</span>
          <span class="route-desc" style="margin-left:auto">Poll stream status</span>
        </div>
        <div class="api-route-body">
          <div class="field"><label>Job ID</label><input id="api-status-jobid" placeholder="paste job_id from /ai-stream response"></div>
          <div class="field"><label>Offset</label><input id="api-status-offset" type="number" value="0" min="0"></div>
          <button class="api-send" onclick="apiStatusCall(this)">Send</button>
          <pre class="api-resp"></pre>
        </div>
      </div>

      <div class="api-route">
        <div class="api-route-header" onclick="toggleApi(this)">
          <span class="method-badge mb-get">GET</span>
          <span class="route-name">/ai-conversation</span>
          <span class="route-desc" style="margin-left:auto">Get conversation</span>
        </div>
        <div class="api-route-body">
          <div class="field"><label>Session ID</label><input id="api-conv-get-sid" placeholder="session_id"></div>
          <button class="api-send" onclick="apiConvGet(this)">Send</button>
          <pre class="api-resp"></pre>
        </div>
      </div>

      <div class="api-route">
        <div class="api-route-header" onclick="toggleApi(this)">
          <span class="method-badge mb-delete">DELETE</span>
          <span class="route-name">/ai-conversation</span>
          <span class="route-desc" style="margin-left:auto">Clear conversation</span>
        </div>
        <div class="api-route-body">
          <div class="field"><label>Session ID</label><input id="api-conv-del-sid" placeholder="session_id"></div>
          <button class="api-send" onclick="apiConvDelete(this)">Send</button>
          <pre class="api-resp"></pre>
        </div>
      </div>

    </div>
  </div>
</div>

<script>
let autoRefresh = false;
let _token = null;
let _sid = 'dev-session-' + Math.random().toString(36).slice(2);

// ── token helper ─────────────────────────────────────────────
async function getToken() {
  if (_token) return _token;
  // dev server exposes a token endpoint for convenience
  const r = await fetch('/dev/token');
  const d = await r.json();
  _token = d.token;
  return _token;
}

// ── tab switching ─────────────────────────────────────────────
function showTab(name) {
  document.querySelectorAll('.tab').forEach((t,i) => {
    const names = ['config','debug','stream','api'];
    t.classList.toggle('active', names[i] === name);
  });
  document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
  document.getElementById('tab-'+name).classList.add('active');
  if (name === 'debug') refreshLog();
}

async function loadConfig() {
  const r = await fetch('/dev/config');
  const d = await r.json();
  document.getElementById('info-model').textContent = d.model;
  document.getElementById('info-tokens').textContent = d.max_tokens;
  document.getElementById('info-temp').textContent = d.temperature;
  document.getElementById('info-interactive').textContent = d.interactive ? 'yes' : 'no';
  document.getElementById('info-tabs').textContent = (d.allowed_tabs||[]).join(', ');
  document.getElementById('info-searxng').textContent = d.searxng_url || 'mock';
  const urlEl = document.getElementById('info-url');
  urlEl.innerHTML = `<label>LLM Endpoint</label><span style="font-family:monospace;font-size:0.78rem;color:var(--accent)">${d.llm_url}</span>`;

  const elUrl = document.getElementById('cfg-url'); if(elUrl) elUrl.value = d.llm_url;
  const elKey = document.getElementById('cfg-key'); if(elKey) elKey.value = d.api_key === 'ollama' ? '' : d.api_key;
  const cfgSearxng = document.getElementById('cfg-searxng');
  if (cfgSearxng) cfgSearxng.value = d.searxng_url || '';
  // populate model dropdown from /ai-models
  const modelSel = document.getElementById('cfg-model');
  if (modelSel) {
    try {
      const tk = await getToken();
      const mr = await fetch('/ai-models?tk=' + encodeURIComponent(tk));
      const md = await mr.json();
      const models = (md.models || [d.model]).filter(m => !m.includes('embed') && !m.includes('vl:'));
      modelSel.innerHTML = models.map(m =>
        `<option value="${m}" ${m === d.model ? 'selected' : ''}>${m}</option>`
      ).join('');
    } catch {
      modelSel.innerHTML = `<option value="${d.model}" selected>${d.model}</option>`;
    }
  }
  const elTokens = document.getElementById('cfg-tokens'); if(elTokens) elTokens.value = d.max_tokens;
  document.getElementById('cfg-tokens-val').textContent = d.max_tokens;
  document.getElementById('cfg-temp-val').textContent = parseFloat(d.temperature).toFixed(2);
  const elTemp = document.getElementById('cfg-temp'); if(elTemp) elTemp.value = d.temperature;
  const cfgValkeyHost = document.getElementById('cfg-valkey-host');
  const cfgValkeyPort = document.getElementById('cfg-valkey-port');
  if (cfgValkeyHost) cfgValkeyHost.value = d.valkey_host || 'localhost';
  if (cfgValkeyPort) cfgValkeyPort.value = d.valkey_port || 6379;
  const infoValkey = document.getElementById('info-valkey');
  if (infoValkey) infoValkey.textContent =
      `${d.valkey_host || 'localhost'}:${d.valkey_port || 6379}`;
}

async function applyConfig() {
  const modelEl = document.getElementById('cfg-model');
  const body = {
    llm_url:     document.getElementById('cfg-url')?.value,
    model:       modelEl?.value,
    api_key:     document.getElementById('cfg-key')?.value,
    max_tokens:  parseInt(document.getElementById('cfg-tokens')?.value),
    temperature: parseFloat(document.getElementById('cfg-temp')?.value),
    searxng_url: document.getElementById('cfg-searxng')?.value || '',
    valkey_host: document.getElementById('cfg-valkey-host')?.value || 'localhost',
    valkey_port: parseInt(document.getElementById('cfg-valkey-port')?.value || 6379),
  };
  await fetch('/dev/config', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
  _token = null; // invalidate cached token
  await loadConfig();
  checkValkey();
}

// ── search ────────────────────────────────────────────────────
async function doSearch() {
  const q = document.getElementById('q').value.trim();
  if (!q) return;
  const btn = document.getElementById('search-btn');
  btn.disabled = true;
  btn.textContent = 'Searching…';

  const out = document.getElementById('ai-output');
  out.innerHTML = '<p style="color:var(--muted);font-size:0.85rem;padding:0.5rem">Loading…</p>';

  try {
    const r = await fetch('/dev/search?q=' + encodeURIComponent(q));
    if (!r.ok) throw new Error(r.statusText);
    const d = await r.json();

    if (d.ok) {
      out.innerHTML = '';
      const iframe = document.createElement('iframe');
      iframe.src = '/dev/render?t=' + Date.now();
      iframe.style.cssText = 'width:100%;border:none;background:transparent;overflow:hidden;';
      
      iframe.onload = () => {
        const resize = () => {
          try {
            const doc = iframe.contentDocument || iframe.contentWindow.document;
            doc.body.style.overflow = 'hidden';
            
            // 1. COLLAPSE FIRST: This stops the infinite height-growing loop
            iframe.style.height = '0px';
            
            // 2. MEASURE SECOND: Get the true internal height of the document
            const h = Math.max(
              doc.body.scrollHeight,
              doc.documentElement.scrollHeight
            );
            
            // 3. APPLY THIRD: Apply the height with a 10px buffer to prevent jitter
            if (h > 0) {
              iframe.style.height = (h + 10) + 'px';
            }
          } catch(e) {
            console.error("Cross-origin or sizing error:", e);
          }
        };
        
        resize();
        
        // Keep resizing as content streams in over 30 seconds
        const resizeInterval = setInterval(resize, 200);
        setTimeout(() => clearInterval(resizeInterval), 30000);
        // Keep resizing as content streams in over 30 seconds
        fetch('/dev/last-scores?t=' + Date.now())
          .then(r => r.json())
          .then(meta => {
            console.log('LAST SCORES:', meta);

            const intentEl = document.getElementById('intent');
            if (intentEl) {
              intentEl.textContent = meta.intent || '—';
            }

            const queryEl = document.getElementById('last-query');
            if (queryEl) {
              queryEl.textContent = meta.query || '';
            }

            const summaryEl = document.getElementById('summary');
            if (summaryEl) {
              summaryEl.textContent = meta.summary || '';
            }
          })
          .catch(e => {
            console.error('Failed to refresh metadata:', e);
          });
      };
      out.appendChild(iframe);
    } else {
      out.innerHTML = `<p style="color:var(--red)">Error: ${d.error || 'unknown'}</p>`;
    }
  } catch(e) {
    out.innerHTML = `<p style="color:var(--red)">Error: ${e.message}</p>`;
  } finally {
    btn.disabled = false;
    btn.textContent = 'Search';
  }
}


// ── debug log ─────────────────────────────────────────────────
function statusClass(s) {
  if (s >= 500) return 's5xx';
  if (s >= 400) return 's4xx';
  return 's200';
}

function fmtJson(str) {
  try { return JSON.stringify(JSON.parse(str), null, 2); } catch { return str; }
}

async function refreshLog() {
  const r = await fetch('/dev/log');
  const entries = await r.json();
  const list = document.getElementById('log-list');
  if (!entries.length) {
    list.innerHTML = '<div class="no-entries">No requests yet</div>';
    return;
  }
  list.innerHTML = entries.map((e,i) => `
    <div class="log-entry">
      <div class="log-entry-header" onclick="toggleLogBody(${i})">
        <span class="log-ts">${e.ts}</span>
        <span class="log-method ${e.method}">${e.method}</span>
        <span class="log-path" title="${e.path}">${e.path}</span>
        <span class="log-status ${statusClass(e.status)}">${e.status}</span>
        <span class="log-ms">${e.ms}ms</span>
      </div>
      <div class="log-body" id="log-body-${i}">
        ${e.req ? `<div style="color:var(--muted);font-size:0.7rem;margin-bottom:3px">REQUEST</div><pre>${fmtJson(e.req)}</pre>` : ''}
        ${e.resp ? `<div style="color:var(--muted);font-size:0.7rem;margin:4px 0 3px">RESPONSE</div><pre>${fmtJson(e.resp)}</pre>` : ''}
      </div>
    </div>
  `).join('');
}

function toggleLogBody(i) {
  document.getElementById('log-body-'+i).classList.toggle('open');
}

async function clearLog() {
  await fetch('/dev/log/clear', {method:'POST'});
  refreshLog();
}

setInterval(() => { if (autoRefresh) refreshLog(); }, 1500);

// ── API explorer ──────────────────────────────────────────────
function toggleApi(header) {
  header.nextElementSibling.classList.toggle('open');
}

async function apiCall(method, path, bodyStr, btn) {
  const tk = await getToken();
  let url = path;
  let opts = { method, headers: {'Content-Type':'application/json'} };

  if (method === 'GET') {
    url += (url.includes('?') ? '&' : '?') + 'tk=' + encodeURIComponent(tk);
  } else {
    let body = {};
    try { body = JSON.parse(bodyStr || '{}'); } catch { body = {}; }
    body.tk = tk;
    body.session_id = _sid;
    opts.body = JSON.stringify(body);
  }

  const resp = btn.closest('.api-route-body').querySelector('.api-resp');
  resp.textContent = 'Sending…';
  resp.classList.add('visible');
  try {
    const r = await fetch(url, opts);
    const text = await r.text();
    resp.textContent = fmtJson(text);
  } catch(e) {
    resp.textContent = 'Error: ' + e.message;
  }
}

async function apiStatusCall(btn) {
  const jobId = document.getElementById('api-status-jobid').value.trim();
  const offset = document.getElementById('api-status-offset').value || 0;
  if (!jobId) { alert('Enter a job_id'); return; }
  const tk = await getToken();
  const url = `/ai-status/${jobId}?tk=${encodeURIComponent(tk)}&offset=${offset}`;
  const resp = btn.closest('.api-route-body').querySelector('.api-resp');
  resp.textContent = 'Sending…';
  resp.classList.add('visible');
  try {
    const r = await fetch(url);
    resp.textContent = fmtJson(await r.text());
  } catch(e) {
    resp.textContent = 'Error: ' + e.message;
  }
}

async function apiConvGet(btn) {
  const sid = document.getElementById('api-conv-get-sid').value.trim() || _sid;
  const tk = await getToken();
  const url = `/ai-conversation?tk=${encodeURIComponent(tk)}&session_id=${sid}`;
  const resp = btn.closest('.api-route-body').querySelector('.api-resp');
  resp.textContent = 'Sending…'; resp.classList.add('visible');
  try { resp.textContent = fmtJson(await (await fetch(url)).text()); } catch(e) { resp.textContent = 'Error: '+e.message; }
}

async function apiConvDelete(btn) {
  const sid = document.getElementById('api-conv-del-sid').value.trim() || _sid;
  const tk = await getToken();
  const resp = btn.closest('.api-route-body').querySelector('.api-resp');
  resp.textContent = 'Sending…'; resp.classList.add('visible');
  try {
    const r = await fetch('/ai-conversation', {
      method: 'DELETE',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({tk, session_id: sid})
    });
    resp.textContent = fmtJson(await r.text());
  } catch(e) { resp.textContent = 'Error: '+e.message; }
}

// ── ollama health check ───────────────────────────────────────
async function checkOllama() {
  const cfg = await (await fetch('/dev/config')).json();
  const base = cfg.llm_url.replace('/v1/chat/completions','').replace('/api/chat','');
  try {
    const r = await fetch('/dev/ollama-check');
    const d = await r.json();
    const el = document.getElementById('ollama-badge');
    if (d.ok) {
      el.textContent = `Ollama: ${d.models} model${d.models!==1?'s':''}`;
      el.classList.add('live');
    } else {
      el.textContent = 'Ollama: offline';
    }
  } catch { document.getElementById('ollama-badge').textContent = 'Ollama: offline'; }
}

async function checkValkey() {
    try {
        const r = await fetch('/dev/valkey-check');
        const d = await r.json();
        const el = document.getElementById('valkey-badge');
        if (d.ok) {
            el.textContent = `Valkey: ${d.host}:${d.port}`;
            el.classList.add('live');
        } else {
            el.textContent = 'Valkey: offline';
            el.style.borderColor = 'var(--red)';
            el.style.color = 'var(--red)';
        }
    } catch {
        document.getElementById('valkey-badge').textContent = 'Valkey: offline';
    }
}

// ── stream viewer ─────────────────────────────────────────────
let _streamSource = null;
let _streamStartTs = null;
let _streamFirstTokenTs = null;
let _streamChunkCount = 0;
let _streamText = '';

function clearStream() {
    document.getElementById('stream-viewer').innerHTML =
        '<span style="color:var(--muted)">Waiting for next search…</span>';
    document.getElementById('stream-meta').style.display = 'none';
    document.getElementById('stream-status').textContent = 'Idle';
    document.getElementById('stream-status').style.color = 'var(--muted)';
    document.getElementById('sv-chunks').textContent = '0';
    document.getElementById('sv-tokens').textContent = '0';
    document.getElementById('sv-elapsed').textContent = '0ms';
    document.getElementById('sv-first-token').textContent = 'First token: —';
    document.getElementById('sv-total-time').textContent = 'Total: —';
    _streamText = '';
    _streamChunkCount = 0;
    if (_streamSource) { _streamSource.close(); _streamSource = null; }
}

async function attachStreamViewer() {
    // Find the latest job_id from the debug log
    const r = await fetch('/dev/log');
    const entries = await r.json();
    let jobId = null;
    let model = '—';
    for (const e of entries) {
        if (e.path && e.path.includes('/ai-stream') && e.method === 'POST') {
            try {
                const resp = JSON.parse(e.resp);
                if (resp.job_id) {
                    jobId = resp.job_id;
                    const req = JSON.parse(e.req);
                    model = req.model || '—';
                }
            } catch {}
            break;
        }
    }
    if (!jobId) return;

    if (_streamSource) _streamSource.close();

    _streamStartTs = Date.now();
    _streamFirstTokenTs = null;
    _streamChunkCount = 0;
    _streamText = '';

    // Fetch intent from last search
    let intent = '—';
    try {
        const scoresR = await fetch('/dev/last-scores');
        const scoresD = await scoresR.json();
        intent = scoresD.intent || '—';
    } catch {}

    document.getElementById('stream-meta').style.display = 'block';
    document.getElementById('sv-job-id').textContent = jobId;
    document.getElementById('sv-model').textContent = model;
    document.getElementById('sv-intent').textContent = intent;
    document.getElementById('stream-status').textContent = '● Streaming';
    document.getElementById('stream-status').style.color = 'var(--green)';

    const viewer = document.getElementById('stream-viewer');
    viewer.innerHTML = '';

    _streamSource = new EventSource(`/dev/stream-watch?job_id=${jobId}`);

    const elapsedTimer = setInterval(() => {
        const ms = Date.now() - _streamStartTs;
        document.getElementById('sv-elapsed').textContent = ms + 'ms';
    }, 100);

    _streamSource.onmessage = (e) => {
        if (e.data === '__DONE__') {
            _streamSource.close();
            _streamSource = null;
            clearInterval(elapsedTimer);
            const total = Date.now() - _streamStartTs;
            document.getElementById('stream-status').textContent = '✓ Done';
            document.getElementById('stream-status').style.color = 'var(--green)';
            document.getElementById('sv-total-time').textContent = `Total: ${total}ms`;
            return;
        }
        if (e.data.startsWith('__ERROR__')) {
            _streamSource.close();
            clearInterval(elapsedTimer);
            document.getElementById('stream-status').textContent = '✗ Error';
            document.getElementById('stream-status').style.color = 'var(--red)';
            viewer.innerHTML += `<span style="color:var(--red)">${e.data}</span>`;
            return;
        }
        try {
            const chunk = JSON.parse(e.data);
            if (!_streamFirstTokenTs) {
                _streamFirstTokenTs = Date.now();
                const ttft = _streamFirstTokenTs - _streamStartTs;
                document.getElementById('sv-first-token').textContent = `First token: ${ttft}ms`;
            }
            _streamChunkCount++;
            _streamText += chunk;
            document.getElementById('sv-chunks').textContent = _streamChunkCount;
            document.getElementById('sv-tokens').textContent =
                Math.round(_streamText.length / 4);

            const span = document.createElement('span');
            span.textContent = chunk;
            span.style.cssText = 'background:rgba(137,180,250,0.15);' +
                'border-radius:2px;transition:background 0.5s ease;';
            viewer.appendChild(span);
            setTimeout(() => span.style.background = 'transparent', 500);
            viewer.scrollTop = viewer.scrollHeight;
        } catch {}
    };
}

// ── TF-IDF score visualizer ───────────────────────────────────
async function loadTfidfScores() {
  let d;
  try {
    const r = await fetch('/dev/last-scores');
    d = await r.json();
  } catch { return; }

  const panel = document.getElementById('tfidf-panel');
  const bars  = document.getElementById('tfidf-bars');
  const sumEl = document.getElementById('tfidf-summary');

  if (!d.scores || !d.scores.length) { panel.style.display = 'none'; return; }

  panel.style.display = 'block';
  sumEl.textContent = d.summary || '';

  const maxScore = Math.max(...d.scores.map(s => s.score), 0.0001);
  bars.innerHTML = d.scores.map(s => {
    const pct    = (s.score / maxScore * 100).toFixed(1);
    const domain = s.url.replace(/^https?:\\/\\//, '').split('/')[0];
    const color  = s.fallback ? 'var(--yellow)' : 'var(--accent)';
    const label  = s.fallback ? domain + ' ⚠' : domain;
    const meta   = s.chunks ? ` (${s.chunks}ch)` : '';
    return `<div style="margin-bottom:0.45rem">
      <div style="display:flex;justify-content:space-between;font-size:0.72rem;margin-bottom:3px">
        <span style="color:${color};overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:68%" title="${s.url}">${label}</span>
        <span style="color:var(--muted);font-family:monospace">${s.score.toFixed(4)}${meta}</span>
      </div>
      <div style="height:5px;background:var(--bg3);border-radius:3px;overflow:hidden">
        <div style="height:100%;width:${pct}%;background:${color};border-radius:3px;transition:width 0.3s ease"></div>
      </div>
    </div>`;
  }).join('');
}

// Auto-attach stream viewer when a search completes
const _origDoSearch = doSearch;
doSearch = async function() {
    showTab('stream');
    clearStream();
    document.getElementById('tfidf-panel').style.display = 'none';
    document.getElementById('tfidf-bars').innerHTML = '';
    document.getElementById('tfidf-summary').textContent = '';
    document.getElementById('stream-status').textContent = '⏳ Waiting for job…';
    await _origDoSearch();
    loadTfidfScores();
    setTimeout(attachStreamViewer, 500);
};

// ── init ──────────────────────────────────────────────────────
loadConfig();
checkOllama();
checkValkey();
setTimeout(checkOllama, 5000);
</script>
</body>
</html>
"""

# ── token endpoint (dev only) ─────────────────────────────────────────────────
@app.route('/dev/token')
def dev_token():
    ts = str(time.time())
    sig = hashlib.sha256(f"{ts}{plugin.secret}".encode()).hexdigest()
    return jsonify({'token': f"{ts}.{sig}"})

# ── ollama health check endpoint ──────────────────────────────────────────────
@app.route('/dev/valkey-check')
def dev_valkey_check():
    try:
        import valkey as _vk
        host = os.getenv('VALKEY_HOST', 'localhost')
        port = int(os.getenv('VALKEY_PORT', 6379))
        v = _vk.Valkey(host=host, port=port, socket_connect_timeout=2)
        v.ping()
        return jsonify({'ok': True, 'host': host, 'port': port})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

@app.route('/dev/ollama-check')
def dev_ollama_check():
    try:
        p = urllib.parse.urlparse(plugin.endpoint_url)
        base = f"{p.scheme}://{p.netloc}"
        url = f"{base}/api/tags"
        req = urllib.request.Request(url, headers={'Authorization': f'Bearer {plugin.api_key}'})
        with urllib.request.urlopen(req, timeout=3) as r:
            data = json.loads(r.read())
        models = len(data.get('models', []))
        return jsonify({'ok': True, 'models': models})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)})

# ── entry point ───────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print(f"""
  ✦ AI Answers Dev Server
  ─────────────────────────────────────────
  UI:          http://{DEV_HOST}:{DEV_PORT}
  LLM:         {plugin.endpoint_url}
  Model:       {plugin.model}
  Max tokens:  {plugin.max_tokens}
  Temperature: {plugin.temperature}
  SearXNG:     {SEARXNG_URL or 'mock results'}
  ─────────────────────────────────────────
""")
    app.run(host=DEV_HOST, port=DEV_PORT, debug=False)