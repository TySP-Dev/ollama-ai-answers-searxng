import json, os, logging, base64, time, hashlib, re, http.client, ssl, concurrent.futures, threading
from urllib.parse import urlparse
from searx import network
try:
    from searx.network import get_network
except ImportError:
    get_network = None
from flask import Response, request, abort, jsonify
from searx.plugins import Plugin, PluginInfo
from searx.result_types import EngineResults
from searx import settings
from flask_babel import gettext
from markupsafe import Markup

logger = logging.getLogger(__name__)

try:
    import valkey as _valkey_mod
    _VALKEY_AVAILABLE = True
except ImportError:
    _VALKEY_AVAILABLE = False
    _valkey_mod = None
    logger.warning("AI Answers: valkey package not found. Streaming via Valkey unavailable.")

TOKEN_EXPIRY_SEC = 3600
STREAM_CHUNK_SIZE = 512
STREAM_TIMEOUT_SEC = 60

def _get_streaming_connection(url: str, verify_ssl: bool = True):
    parsed = urlparse(url)
    host = parsed.hostname
    port = parsed.port or (443 if parsed.scheme == 'https' else 80)
    path = parsed.path + ('?' + parsed.query if parsed.query else '')

    if verify_ssl and get_network is not None:
        try:
            net = get_network()
            verify_ssl = getattr(net, 'verify', True)
        except Exception:
            pass

    if parsed.scheme == 'https':
        if not verify_ssl:
            ctx = ssl._create_unverified_context()
        else:
            try:
                import certifi
                ctx = ssl.create_default_context(cafile=certifi.where())
            except ImportError:
                ctx = ssl.create_default_context()
        conn = http.client.HTTPSConnection(host, port, timeout=STREAM_TIMEOUT_SEC, context=ctx)
    else:
        conn = http.client.HTTPConnection(host, port, timeout=STREAM_TIMEOUT_SEC)

    return conn, path


_VALKEY_POOL = None


def _get_valkey_pool():
    global _VALKEY_POOL
    if _VALKEY_POOL is None:
        assert _valkey_mod is not None
        _VALKEY_POOL = _valkey_mod.ConnectionPool(
            host=os.getenv('VALKEY_HOST', 'searxng-valkey'),
            port=int(os.getenv('VALKEY_PORT', 6379)),
            db=0,
            decode_responses=True,
        )
    return _VALKEY_POOL


def _get_valkey():
    if not _VALKEY_AVAILABLE or _valkey_mod is None:
        raise RuntimeError("valkey package not installed")
    return _valkey_mod.Valkey(connection_pool=_get_valkey_pool())


def stream_to_valkey(job_id: str, payload: str, headers: dict, endpoint_url: str, model: str):
    chunks_key = f"ai:job:{job_id}:chunks"
    status_key = f"ai:job:{job_id}:status"
    conn = None
    try:
        vk = _get_valkey()
        url = endpoint_url
        res = None
        for _ in range(3):
            conn, path = _get_streaming_connection(url)
            conn.request("POST", path, body=payload.encode('utf-8'), headers=headers)
            res = conn.getresponse()
            if res.status in (301, 302, 307, 308):
                location = res.getheader('Location', '')
                res.read()
                conn.close()
                conn = None
                if not location:
                    raise RuntimeError(f"Redirect {res.status} with no Location")
                url = location if location.startswith('http') else \
                    f"{urlparse(url).scheme}://{urlparse(url).netloc}{location}"
                continue
            break
        else:
            raise RuntimeError("Too many redirects to Ollama endpoint")

        if res.status != 200:
            body = res.read(1024).decode('utf-8', errors='replace')
            raise RuntimeError(f"Ollama error {res.status}: {body[:200]}")

        think_depth = 0
        pending = ''
        chunk_count = 0

        while True:
            raw_line = res.readline()
            if not raw_line:
                break
            line = raw_line.decode('utf-8', errors='replace').rstrip('\r\n')
            if not line or not line.startswith('data: '):
                continue
            data_str = line[6:]
            if data_str == '[DONE]':
                break
            try:
                obj = json.loads(data_str)
            except (json.JSONDecodeError, ValueError):
                continue
            choices = obj.get('choices', [])
            if not choices:
                continue
            delta = choices[0].get('delta', {})
            text = delta.get('content') or ''
            reasoning = delta.get('reasoning') or ''
            chunk = text if text else reasoning
            if not chunk:
                continue

            pending += chunk
            # Filter <think>...</think> blocks, push clean content immediately
            while True:
                if think_depth == 0:
                    think_start = pending.find('<think>')
                    if think_start == -1:
                        if pending:
                            vk.rpush(chunks_key, pending)
                            vk.expire(chunks_key, 120)
                            chunk_count += 1
                            pending = ''
                        break
                    else:
                        before = pending[:think_start]
                        if before:
                            vk.rpush(chunks_key, before)
                            vk.expire(chunks_key, 120)
                            chunk_count += 1
                        pending = pending[think_start + 7:]
                        think_depth = 1
                else:
                    think_end = pending.find('</think>')
                    if think_end == -1:
                        break
                    else:
                        pending = pending[think_end + 8:]
                        think_depth = 0

        if think_depth == 0 and pending:
            vk.rpush(chunks_key, pending)
            vk.expire(chunks_key, 120)
            chunk_count += 1

        logger.debug(f"AI Answers: stream complete, wrote {chunk_count} chunks")
        vk.rpush(chunks_key, '__DONE__')
        vk.expire(chunks_key, 120)
        vk.set(status_key, 'done', ex=120)

    except Exception as e:
        logger.error(f"AI Answers: stream_to_valkey error for job {job_id}: {e}", exc_info=True)
        try:
            vk2 = _get_valkey()
            vk2.rpush(chunks_key, f"__ERROR__{e}")
            vk2.expire(chunks_key, 120)
            vk2.set(status_key, 'error', ex=120)
        except Exception:
            pass
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass



PLUGIN_NAME = "AI Answers"
DEFAULT_TABS = "general,science,it,news"

# UI assets

INTERACTIVE_CSS = '''
                        @keyframes sxng-fade-in-up {
                            0% { opacity: 0; transform: translateY(10px); }
                            100% { opacity: 1; transform: translateY(0); }
                        }
                        .sxng-footer {
                            display: flex;
                            align-items: center;
                            gap: 0.5rem;
                            margin-top: 1rem;
                            opacity: 0;
                            animation: sxng-fade-in-up 0.5s ease-out forwards;
                        }
                        .sxng-btn {
                            display: inline-flex;
                            align-items: center;
                            justify-content: center;
                            width: 32px;
                            height: 32px;
                            padding: 0;
                            border: none;
                            border-radius: 4px;
                            background: var(--color-sidebar-bg, #424247);
                            color: var(--color-search-url, #bbb);
                            cursor: pointer;
                            vertical-align: middle;
                            line-height: 1.4;
                        }
                        .sxng-btn:hover {
                            background: var(--color-search-url, #303033);
                            color: var(--color-sidebar-bg, #bbb);
                        }
                        .sxng-btn svg { width: 18px; height: 18px; fill: currentColor; }
                        .sxng-input-wrapper {
                            flex-grow: 1;
                            display: flex;
                            height: 32px;
                            align-items: center;
                            margin: 0 0.5rem;
                            position: relative;
                        }
                        .sxng-input {
                            width: 100%;
                            height: -webkit-fill-available;
                            background: var(--color-sidebar-bg, #424247);
                            border: none;
                            color: var(--color-base-font, #cdd6f4);
                            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
                            font-size: 0.78em;
                            padding: 3px 8px;
                            border-radius: 4px;
                            line-height: 1.4;
                            vertical-align: middle;
                        }
                        .sxng-input:focus { outline: none; }
                        .sxng-input::placeholder { color: var(--color-base-font, #333); opacity: 0.35; }
                        .sxng-input-line {
                            position: absolute;
                            bottom: 0;
                            left: 0;
                            width: 0;
                            height: 1px;
                            background: var(--color-result-link, #5e81ac);
                            transition: width 0.3s ease;
                        }
                        .sxng-input:focus + .sxng-input-line { width: 100%; }
                        .sxng-user-msg {
                            display: block;
                            width: fit-content;
                            max-width: 80%;
                            margin: 0.75rem 0 0.75rem auto;
                            padding: 0.25rem 0.6rem 0.25rem 0;
                            border-right: 2px solid var(--color-result-link, #5e81ac);
                            text-align: right;
                            font-size: 0.85rem;
                            line-height: 1.4;
                            opacity: 0.55;
                            animation: sxng-fade-in-up 0.3s ease-out forwards;
                        }
                        .sxng-input-wrapper:focus-within { 
                            opacity: 1; 
                            color: var(--color-result-link, #5e81ac); 
                            background: var(--color-base-background-hover, rgba(0,0,0,0.05)) !important;
                        }
                        .sxng-model-select {
                            appearance: none;
                            -webkit-appearance: none;
                            background: url("data:image/svg+xml;charset=UTF-8,%3C%3Fxml%20version%3D%221.0%22%20encoding%3D%22UTF-8%22%3F%3E%0A%3Csvg%20xmlns%3D%22http%3A%2F%2Fwww.w3.org%2F2000%2Fsvg%22%20viewBox%3D%220%200%20512%20512%22%3E%0A%3Cg%20fill%3D%22%23aaa%22%3E%0A%3Cpolygon%20points%3D%22128%2C192%20256%2C320%20384%2C192%22%2F%3E%3C%2Fg%3E%0A%3C%2Fsvg%3E") calc(100% + 2rem) / 1rem no-repeat content-box border-box;
                            background-color: #424247;
                            text-overflow: ellipsis;
                            border-width: 0 2rem 0 0;
                            border-color: transparent;
                            border-radius: 5px;
                            outline: none;
                            height: 25px;
                            color: var(--color-search-url, #bbb);
                            font-size: .9rem;
                            padding: 1px 10px 1px 10px !important;
                            margin: 0;
                            cursor: pointer;
                            display: none;
                            max-width: 8rem;
                            vertical-align: middle;
                        }
                        .sxng-model-select:hover {
                            background-color: #303033;
                            color: var(--color-search-url, #bbb);
                        }
                        .sxng-reasoning {
                            margin: 0.5rem 0; padding: 0.5rem;
                            border-left: 2px solid var(--color-result-link, #5e81ac);
                            background: var(--color-base-background-hover, rgba(0,0,0,0.03));
                            font-size: 0.85rem; opacity: 0.7; transition: opacity 0.2s;
                        }
                        .sxng-reasoning:hover { opacity: 1; }
                        .sxng-reasoning summary { cursor: pointer; font-weight: bold; color: var(--color-result-link, #5e81ac); }
                        .sxng-thought-content { margin-top: 0.5rem; white-space: pre-wrap; font-family: monospace; }
                        .sxng-citation-footer {
                            margin-top: 0.75rem;
                            padding-top: 0.5rem;
                            border-top: 1px solid var(--color-sidebar-bg, #424247);
                            display: flex;
                            flex-wrap: wrap;
                            gap: 0.4rem 0.75rem;
                        }
                        .sxng-citation-item a {
                            font-size: 0.75em;
                            color: var(--color-result-link, #5e81ac);
                            text-decoration: none;
                            opacity: 0.75;
                        }
                        .sxng-citation-item a:hover {
                            opacity: 1;
                            text-decoration: underline;
                        }
'''

INTERACTIVE_HTML = '''
                    <div id="sxng-footer" class="sxng-footer" style="display:none;">
                        <button class="sxng-btn" id="btn-copy" title="Copy to clipboard">
                            <svg viewBox="0 0 24 24"><path d="M16 1H4C2.9 1 2 1.9 2 3V17H4V3H16V1M19 5H8C6.9 5 6 5.9 6 7V21C6 22.1 6.9 23 8 23H19C20.1 23 21 22.1 21 21V7C21 5.9 20.1 5 19 5M19 21H8V7H19V21Z"/></svg>
                        </button>
                        <button class="sxng-btn" id="btn-regen" title="Regenerate answer">
                            <svg viewBox="0 0 24 24"><path d="M17.65 6.35C16.2 4.9 14.21 4 12 4C7.58 4 4.01 7.58 4.01 12C4.01 16.42 7.58 20 12 20C15.73 20 18.84 17.45 19.73 14H17.65C16.83 16.33 14.61 18 12 18C8.69 18 6 15.31 6 12C6 8.69 8.69 6 12 6C13.66 6 15.14 6.69 16.22 7.78L13 11H20V4L17.65 6.35Z"/></svg>
                        </button>
                        <form id="sxng-action-form" class="sxng-input-wrapper" onsubmit="event.preventDefault();">
                            <input type="text" id="sxng-action-input" class="sxng-input" placeholder="Ask..." aria-label="Ask follow-up" autocomplete="off">
                            <div class="sxng-input-line"></div>
                            </button>
                        </form>
                    </div>
'''

CITATION_HELPER_JS = r'''
                        function renderCitations(text, urls) {
                            const fragment = document.createDocumentFragment();
                            const re = /\[(\d{1,2}(?:\s*,\s*\d{1,2})*)\]/g;
                            let lastIdx = 0;
                            const matches = [...text.matchAll(re)];
                            
                            matches.forEach(match => {
                                if (match.index > lastIdx) {
                                    const s = document.createElement('span');
                                    s.className = 'sxng-chunk';
                                    s.textContent = text.substring(lastIdx, match.index);
                                    fragment.appendChild(s);
                                }
                                match[1].split(/\s*,\s*/).forEach(n => {
                                    const idx = parseInt(n.trim());
                                    if (idx >= 1 && idx <= urls.length) {
                                        const url = urls[idx-1];
                                        if (url) {
                                            const a = document.createElement('a');
                                            a.href = url;
                                            a.target = '_blank';
                                            a.style.cssText = 'text-decoration:none;color:var(--color-result-link);font-weight:bold;';
                                            a.textContent = `[${n.trim()}]`;
                                            a.className = 'sxng-chunk';
                                            fragment.appendChild(a);
                                        } else {
                                            const s = document.createElement('span');
                                            s.className = 'sxng-chunk';
                                            s.textContent = `[${n.trim()}]`;
                                            fragment.appendChild(s);
                                        }
                                    } else {
                                        const s = document.createElement('span');
                                        s.className = 'sxng-chunk';
                                        s.textContent = `[${n.trim()}]`;
                                        fragment.appendChild(s);
                                    }
                                });
                                lastIdx = match.index + match[0].length;
                            });
                            
                            if (lastIdx < text.length) {
                                const s = document.createElement('span');
                                s.className = 'sxng-chunk';
                                // Preserve whitespace by not trimming
                                s.textContent = text.substring(lastIdx);
                                fragment.appendChild(s);
                            }
                            return fragment;
                        }

                        function renderCitationFooter(textContent, urls, container) {
                            const re = /\[(\d{1,2}(?:\s*,\s*\d{1,2})*)\]/g;
                            const usedIndices = new Set();
                            let m;
                            while ((m = re.exec(textContent)) !== null) {
                                m[1].split(/\s*,\s*/).forEach(n => {
                                    const idx = parseInt(n.trim());
                                    if (idx >= 1 && idx <= urls.length && urls[idx - 1]) {
                                        usedIndices.add(idx);
                                    }
                                });
                            }
                            if (usedIndices.size === 0) return;
                            const sorted = [...usedIndices].sort((a, b) => a - b);
                            const footer = document.createElement('div');
                            footer.className = 'sxng-citation-footer';
                            sorted.forEach(n => {
                                const url = urls[n - 1];
                                if (!url) return;
                                let domain;
                                try { domain = new URL(url).hostname.replace('www.', ''); } catch(e) { domain = url; }
                                const item = document.createElement('span');
                                item.className = 'sxng-citation-item';
                                const a = document.createElement('a');
                                a.href = url;
                                a.target = '_blank';
                                a.textContent = `[${n}] ${domain}`;
                                item.appendChild(a);
                                footer.appendChild(item);
                            });
                            container.appendChild(footer);
                        }
'''

INTERACTIVE_JS = r'''
                        const footer = document.getElementById('sxng-footer');
                        const input = document.getElementById('sxng-action-input');
                        if (typeof model_init !== 'undefined' && model_init) {
                            const _ms = document.getElementById('sxng-model-select');
                            if (_ms) {
                                const _o = document.createElement('option');
                                _o.value = model_init;
                                _o.textContent = model_init;
                                _o.selected = true;
                                _ms.appendChild(_o);
                            }
                        }
                        if (window.getComputedStyle && box) {
                            try {
                                const docStyles = getComputedStyle(document.documentElement);
                                let accent = docStyles.getPropertyValue('--color-result-link').trim();
                                if (!accent) {
                                    const a = document.createElement('a');
                                    document.body.appendChild(a);
                                    accent = getComputedStyle(a).color;
                                    document.body.removeChild(a);
                                }
                                if (accent) {
                                    box.style.setProperty('--color-result-link', accent);
                                    box.style.setProperty('--sxng-ai-accent', accent);
                                }
                            } catch(e) {}
                        }

                        // conversation saved as base64 URL fragment.
                        const updateState = () => {
                            try {
                                let state = {
                                    t: conversation.turns.map(t => ({
                                        r: t.role === 'user' ? 'u' : 'a',
                                        c: t.content.replace(/\s+/g, ' ').trim()
                                    })),
                                    u: urls
                                };
                                const encodeB64 = (obj) => {
                                    const u8 = new TextEncoder().encode(JSON.stringify(obj));
                                    let bin = '';
                                    // Use a loop to avoid RangeError: Maximum call stack size exceeded
                                    for (let i = 0; i < u8.byteLength; i++) {
                                        bin += String.fromCharCode(u8[i]);
                                    }
                                    return btoa(bin);
                                };
                                
                                let b64 = encodeB64(state);
                                while (b64.length > 2000 && state.t.length > 2) {
                                    state.t.splice(1, 2); // Delete in Q&A pairs
                                    b64 = encodeB64(state);
                                }
                                
                                history.replaceState(null, null, '#ai=' + b64);
                            } catch(e) {}
                        };

                        if (location.hash.includes('ai=')) {
                            try {
                                const b64 = location.hash.split('ai=')[1];
                                const uint8 = new Uint8Array(atob(b64).split('').map(c => c.charCodeAt(0)));
                                const json = new TextDecoder().decode(uint8);
                                const state = JSON.parse(json);
                                if (state.t && state.t.length > 0) {
                                    // Restore URLs for citation indexing
                                    if (state.u && Array.isArray(state.u)) {
                                        urls = state.u;
                                    }
                                    
                                    conversation.turns = state.t.map(t => ({
                                        role: t.r === 'u' ? 'user' : 'assistant',
                                        content: t.c.trim(),
                                        ts: 0
                                    }));
                                    
                                    const injectCitations = (text) => {
                                        return renderCitations(text, urls);
                                    };
                                    
                                    data.innerHTML = '';
                                    conversation.turns.forEach((turn, i) => {
                                        if (turn.role === 'user') {
                                            if (turn.content !== conversation.originalQuery) {
                                                const u = document.createElement('span');
                                                u.className = 'sxng-user-msg';
                                                u.textContent = turn.content;
                                                data.appendChild(u);
                                                const clr = document.createElement('div');
                                                clr.style.clear = 'both';
                                                data.appendChild(clr);
                                            }
                                        } else {
                                            data.appendChild(injectCitations(turn.content));
                                        }
                                    });
                                    box.style.display = 'block';
                                    if(wrapper) wrapper.style.display = '';
                                    if(footer && is_interactive) footer.style.display = 'flex';
                                    restored = true;
                                }
                            } catch(e) { console.warn('Restore failed', e); }
                        }
                        document.getElementById('btn-copy').onclick = async (e) => {
                            const btn = e.currentTarget;
                            const originalContent = btn.innerHTML;
                            const text = Array.from(data.childNodes)
                                .filter(n => n.nodeType === 3 || n.tagName === 'SPAN')
                                .map(n => n.textContent)
                                .join('');
                            await navigator.clipboard.writeText(text);
                            btn.innerHTML = '<svg viewBox="0 0 24 24" style="color:#a3be8c;"><path d="M9 16.17L4.83 12L3.41 13.41L9 19L21 7L19.59 5.59L9 16.17Z"/></svg>';
                            setTimeout(() => btn.innerHTML = originalContent, 2000);
                        };

                        document.getElementById('btn-regen').onclick = async () => {
                            data.innerHTML = '<span class="sxng-cursor"></span>';
                            footer.style.display = 'none';
                            
                            if (conversation.turns.length > 0 && conversation.turns[conversation.turns.length - 1].role === 'assistant') {
                                conversation.turns.pop();
                            }
                            
                            updateState();
                            
                            if (conversation.turns.length <= 1) {
                                await startStream();
                            } else {
                                const val = conversation.turns[conversation.turns.length - 1].content;
                                const currentText = conversation.turns.slice(0, -1).slice(-6)
                                    .map(t => (t.role === 'user' ? 'Q' : 'A') + ': ' + t.content)
                                    .join('\\n\\n');
                                await startStream(val, currentText);
                            }
                            updateState();
                        };

                        const handleAction = async (e) => {
                            if (e) e.preventDefault();
                            const val = input.value.trim();
                            
                            conversation.turns.push({role: 'user', content: val, ts: Date.now()});
                            updateState();
                            
                            const currentText = conversation.turns.slice(0, -1).slice(-6)
                                .map(t => (t.role === 'user' ? 'Q' : 'A') + ': ' + t.content)
                                .join('\\n\\n');

                            input.value = '';
                            input.blur();
                            footer.style.display = 'none';

                            if (val) {
                                const cursor = data.querySelector('.sxng-cursor');
                                if (cursor) cursor.remove();
                                const userMsg = document.createElement('span');
                                userMsg.className = 'sxng-user-msg';
                                userMsg.textContent = val;
                                data.appendChild(userMsg);
                                const clr = document.createElement('div');
                                clr.style.clear = 'both';
                                data.appendChild(clr);

                                const newCursor = document.createElement('span');
                                newCursor.className = 'sxng-cursor';
                                data.appendChild(newCursor);
                                
                                const synthesized = synthesizeQuery(q_init, val);
                                let auxContext = null;
                                try {
                                    const auxData = await fetch(script_root + '/ai-auxiliary-search', {
                                        method: 'POST',
                                        headers: {'Content-Type': 'application/json'},
                                        body: JSON.stringify({query: synthesized, lang: lang_init, offset: urls.length, tk: tk_init})
                                    }).then(r => r.json());
                                    if (auxData.context) {
                                        const originalBackground = conversation.originalContext.substring(0, 1500);
                                        auxContext = `FRESH SOURCES (most relevant):\\n${auxData.context}\\n\\nBACKGROUND (for reference):\\n${originalBackground}`;
                                        if (auxData.new_urls && Array.isArray(auxData.new_urls)) {
                                            urls = urls.concat(auxData.new_urls);
                                        }
                                    }
                                } catch (err) {}
                                
                                await startStream(val, currentText, auxContext);
                                updateState();
                            } else {
                                const cursor = data.querySelector('.sxng-cursor');
                                if (cursor) cursor.remove();
                                data.appendChild(document.createElement('br'));
                                data.appendChild(document.createElement('br'));
                                const newCursor = document.createElement('span');
                                newCursor.className = 'sxng-cursor';
                                data.appendChild(newCursor);
                                await startStream("Continue", currentText);
                                updateState();
                            }
                        };

                        document.getElementById('sxng-action-form').onsubmit = handleAction;
                        input.onfocus = () => {
                            setTimeout(() => {
                                input.scrollIntoView({behavior: 'smooth', block: 'center'});
                            }, 300);
                        };

                        (function fetchModels() {
                            const _msel2 = document.getElementById('sxng-model-select');
                            if (!_msel2) return;
                            const _modelsUrl = script_root + '/ai-models?tk=' + encodeURIComponent(tk_init);
                            console.log('[AI Answers] Fetching models from', _modelsUrl);
                            fetch(_modelsUrl)
                                .then(r => r.ok ? r.json() : Promise.reject('HTTP ' + r.status))
                                .then(d => {
                                    const models = (d && d.models && d.models.length > 0) ? d.models : [model_init];
                                    const _cur = _msel2.value || model_init;
                                    _msel2.innerHTML = '';
                                    models.forEach(m => {
                                        const o = document.createElement('option');
                                        o.value = m; o.textContent = m;
                                        if (m === _cur) o.selected = true;
                                        _msel2.appendChild(o);
                                    });
                                    _msel2.style.display = 'inline-block';
                                })
                                .catch(() => {
                                    if (model_init) {
                                        const o = document.createElement('option');
                                        o.value = model_init; o.textContent = model_init;
                                        o.selected = true;
                                        _msel2.appendChild(o);
                                        _msel2.style.display = 'inline-block';
                                    }
                                });
                        })();
'''

FRONTEND_JS_TEMPLATE = r"""
(async () => {
    const is_interactive = __IS_INTERACTIVE__;
    const q_init = __JS_Q__;
    const lang_init = __JS_LANG__;
    let urls = __JS_URLS__;
    const b64_init = __B64_CONTEXT__;
    const tk_init = __TK__;
    const script_root = __SCRIPT_ROOT__;
    const model_init = __MODEL_INIT__;
    const conversation = {
        originalQuery: q_init,
        originalContext: new TextDecoder().decode(Uint8Array.from(atob(b64_init), c => c.charCodeAt(0))),
        originalSources: [...urls],
        turns: [{role: 'user', content: q_init, ts: Date.now()}]
    };
    const box = document.getElementById('sxng-stream-box');
    const data = document.getElementById('sxng-stream-data');
    const wrapper = box.closest('.answer');
    if (wrapper) wrapper.style.display = 'none';
    let restored = false;
    let isStreaming = false;
    
    __CITATION_HELPER_JS__

    __INTERACTIVE_JS_INIT__

    function synthesizeQuery(original, followup) {
        const cleanOrig = original.replace(/^(what|how|why|when|where|who|which|is|are|can|does|do)(\s+(is|are|do|does|can|to|a|an|the))?\s+/i, '');
        const origWords = cleanOrig.split(' ').slice(0, 12);
        return `${origWords.join(' ')} ${followup}`.trim();
    }

    __STREAM_FN_SIG__ {
        if (isStreaming) {
            console.warn('[AI Answers] Stream already in progress, ignoring duplicate call');
            return;
        }
        
        isStreaming = true;
        try {
            const ctx = auxContext || conversation.originalContext;
            if (wrapper) wrapper.style.display = '';
            box.style.display = 'block';

            const controller = new AbortController();
            let timeoutId = setTimeout(() => controller.abort(), 90000);
            const finalQ = __STREAM_Q__;

            const _selMdl = (document.getElementById('sxng-model-select') || {value: ''}).value;
            const bodyObj = { q: finalQ, lang: lang_init, context: ctx, tk: tk_init, model: _selMdl__STREAM_BODY__ };
            const res = await fetch(script_root + '/ai-stream', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(bodyObj),
                signal: controller.signal
            });

            clearTimeout(timeoutId);
            if (!res.ok) {
                const errSpan = document.createElement('span');
                errSpan.style.color = '#bf616a';
                errSpan.textContent = "Error: " + res.statusText;
                data.appendChild(errSpan);
                return;
            }

            const respJson = await res.json();

            if (respJson.error) {
                const cursorErr = data.querySelector('.sxng-cursor');
                if (cursorErr) cursorErr.remove();
                const errSpan = document.createElement('span');
                errSpan.style.color = '#bf616a';
                errSpan.textContent = "⚠️ " + respJson.error;
                data.appendChild(errSpan);
                return;
            }

            const jobId = respJson.job_id;
            if (!jobId) {
                const cursorErr = data.querySelector('.sxng-cursor');
                if (cursorErr) cursorErr.remove();
                const errSpan = document.createElement('span');
                errSpan.style.color = '#bf616a';
                errSpan.textContent = 'No job ID returned. Check server logs.';
                data.appendChild(errSpan);
                return;
            }

            let cursor = data.querySelector('.sxng-cursor');
            if (!cursor) {
                cursor = document.createElement('span');
                cursor.className = 'sxng-cursor';
                data.appendChild(cursor);
            }

            let buffer = '';
            let fullText = '';
            const flushBuffer = (force = false) => {
                if (!buffer) return;

                if (force) {
                    const fragment = renderCitations(buffer, urls);
                    if (cursor) cursor.before(fragment);
                    else data.appendChild(fragment);
                    buffer = '';
                    return;
                }

                while (true) {
                    const match = buffer.match(/(\[\d+(?:,\s*\d+)*\])/);

                    if (!match) break;

                    const preText = buffer.substring(0, match.index);
                    if (preText) {
                        const s = document.createElement('span');
                        s.className = 'sxng-chunk';
                        s.textContent = preText;
                        cursor.before(s);
                    }

                    const citationText = match[0];
                    const fragment = renderCitations(citationText, urls);
                    cursor.before(fragment);

                    buffer = buffer.substring(match.index + match[0].length);
                }

                const openIdx = buffer.lastIndexOf('[');
                if (openIdx === -1) {
                    if (buffer) {
                        const s = document.createElement('span');
                        s.className = 'sxng-chunk';
                        s.textContent = buffer;
                        cursor.before(s);
                        buffer = '';
                    }
                } else {
                    const safeChunk = buffer.substring(0, openIdx);
                    if (safeChunk) {
                        const s = document.createElement('span');
                        s.className = 'sxng-chunk';
                        s.textContent = safeChunk;
                        cursor.before(s);
                    }
                    buffer = buffer.substring(openIdx);

                    if (buffer.length > 50) {
                        const s = document.createElement('span');
                        s.className = 'sxng-chunk';
                        s.textContent = buffer[0];
                        cursor.before(s);
                        buffer = buffer.substring(1);
                    }
                }
            };

            let offset = 0;
            const maxPolls = 600;
            let polls = 0;

            while (polls < maxPolls) {
                polls++;
                await new Promise(r => setTimeout(r, 150));

                let statusRes;
                try {
                    statusRes = await fetch(
                        `${script_root}/ai-status/${jobId}?tk=${encodeURIComponent(tk_init)}&offset=${offset}`,
                        { signal: controller.signal }
                    );
                } catch (fetchErr) {
                    if (fetchErr.name === 'AbortError') throw fetchErr;
                    continue;
                }

                if (statusRes.status === 404) {
                    const cursorE = data.querySelector('.sxng-cursor');
                    if (cursorE) cursorE.remove();
                    const expiredSpan = document.createElement('span');
                    expiredSpan.style.color = '#bf616a';
                    expiredSpan.textContent = 'Response expired. Please search again.';
                    data.appendChild(expiredSpan);
                    return;
                }

                if (!statusRes.ok) continue;

                const statusData = await statusRes.json();

                if (statusData.error) {
                    const cursorE = data.querySelector('.sxng-cursor');
                    if (cursorE) cursorE.remove();
                    const errSpan2 = document.createElement('span');
                    errSpan2.style.color = '#bf616a';
                    errSpan2.textContent = '⚠️ ' + statusData.error;
                    data.appendChild(errSpan2);
                    return;
                }

                for (const chunk of (statusData.chunks || [])) {
                    fullText += chunk;
                    buffer += chunk;
                    flushBuffer(false);
                }
                offset += (statusData.chunks || []).length;

                if (statusData.done) {
                    flushBuffer(true);
                    break;
                }
            }

            if (cursor) cursor.remove();

            let last = data.lastChild;
            while (last) {
                if (last.textContent && last.textContent.trim().length === 0) {
                    const prev = last.previousSibling;
                    last.remove();
                    last = prev;
                } else {
                    if (last.textContent) last.textContent = last.textContent.trimEnd();
                    break;
                }
            }

            renderCitationFooter(fullText, urls, data);

            const collectedResponse = fullText;

            __INTERACTIVE_JS_COMPLETE__

            if (collectedResponse) {
                conversation.turns.push({role: 'assistant', content: collectedResponse.trim(), ts: Date.now()});
            }

            if (arguments.length === 0 && typeof updateState === 'function') {
                updateState();
            }

        } catch (e) {
            console.error('[AI Answers] Fatal stream exception:', e);
            const errSpan = document.createElement('span');
            errSpan.style.cssText = 'color: #bf616a; font-weight: bold; display: block; margin-top: 0.5rem;';
            
            if (e.name === 'AbortError') {
                errSpan.textContent = "⚠️ Connection to AI provider timed out.";
            } else {
                errSpan.textContent = "⚠️ AI Widget encountered a fatal error. Check browser console.";
            }
            
            if (data) {
                const cursor = data.querySelector('.sxng-cursor');
                if (cursor) cursor.remove();
                data.appendChild(errSpan);
            }
        } finally {
            isStreaming = false;
        }
    }

    if (!restored) startStream();
})();
"""

import typing
if typing.TYPE_CHECKING:
    from searx.search import SearchWithPlugins
    from searx.extended_types import SXNG_Request
    from . import PluginCfg

class SXNGPlugin(Plugin):
    id = "ai_answers"

    def __init__(self, plg_cfg: "PluginCfg"):
        super().__init__(plg_cfg)
        self.info = PluginInfo(
            id=self.id,
            name=gettext(f"{PLUGIN_NAME} Plugin"),
            description=gettext("Live AI search answers using LLM providers."),
            preference_section="general",
        )
        self._load_config()



    def _load_config(self):
        self.interactive = os.getenv('LLM_INTERACTIVE', 'true').lower().strip() in ('true', '1', 'yes', 'on')
        self.question_mark_required = os.getenv('LLM_QUESTION_MARK_REQUIRED', 'false').lower().strip() in ('true', '1', 'yes', 'on')

        raw_url = os.getenv('LLM_URL', 'http://ollama:11434/v1/chat/completions').strip()
        if not raw_url.startswith(('http://', 'https://')):
            raw_url = f"http://{raw_url}"
        self.endpoint_url = raw_url

        self.api_key = 'ollama'
        self.model = os.getenv('LLM_MODEL', 'qwen3.5:9b').strip()

        try:
            self.max_tokens = max(1, int(os.getenv('LLM_MAX_TOKENS', 200)))
        except ValueError:
            logger.warning(f"{PLUGIN_NAME}: Invalid LLM_MAX_TOKENS value. Enforcing default (200).")
            self.max_tokens = 200
        try:
            self.temperature = float(os.getenv('LLM_TEMPERATURE', 0.2))
        except ValueError:
            logger.warning(f"{PLUGIN_NAME}: Invalid LLM_TEMPERATURE value. Enforcing default (0.2).")
            self.temperature = 0.2
        try:
            self.context_deep_count = max(0, int(os.getenv('LLM_CONTEXT_DEEP_COUNT', 5)))
        except ValueError:
            logger.warning(f"{PLUGIN_NAME}: Invalid LLM_CONTEXT_DEEP_COUNT value. Enforcing default (5).")
            self.context_deep_count = 5
        try:
            self.context_shallow_count = max(0, int(os.getenv('LLM_CONTEXT_SHALLOW_COUNT', 15)))
        except ValueError:
            logger.warning(f"{PLUGIN_NAME}: Invalid LLM_CONTEXT_SHALLOW_COUNT value. Enforcing default (15).")
            self.context_shallow_count = 15

        self.allowed_tabs = set(t.strip() for t in os.getenv('LLM_TABS', DEFAULT_TABS).split(','))

        server_secret = settings.get('server', {}).get('secret_key', '')
        self.secret = hashlib.sha256(f"ai_answers_{server_secret}".encode()).hexdigest()

        self.system_prompt = os.getenv('LLM_SYSTEM_PROMPT', '').strip()

    def _parse_aux_results(self, raw_results, raw_infoboxes, raw_answers):
        results = []
        limit = self.context_deep_count + self.context_shallow_count
        for r in raw_results[:limit]:
            # MainResult (attribute access) and LegacyResult (dict access)
            if hasattr(r, 'title'):
                results.append({
                    'title': getattr(r, 'title', ''),
                    'content': getattr(r, 'content', ''),
                    'url': getattr(r, 'url', ''),
                    'publishedDate': getattr(r, 'publishedDate', '')
                })
            else:
                # Legacy dictionary-style access
                results.append({
                    'title': r.get('title', ''),
                    'content': r.get('content', ''),
                    'url': r.get('url', ''),
                    'publishedDate': r.get('publishedDate', '')
                })

        # SearXNG already merges infoboxes by ID, use first
        infoboxes = []
        for ib in raw_infoboxes[:1]:
            infoboxes.append({
                'name': ib.get('infobox', '') or ib.get('title', ''),
                'content': str(ib.get('content') or '')[:2000],
                'attributes': ib.get('attributes', [])
            })
            
        answers = []
        for a in list(raw_answers)[:2]:
            ans_text = ""
            if hasattr(a, 'answer') and isinstance(getattr(a, 'answer', None), str):
                ans_text = a.answer
            elif isinstance(a, dict) and a.get('answer'):
                ans_text = str(a['answer'])
            if ans_text and 'id="sxng-stream-box"' not in ans_text and not ans_text.strip().startswith('<'):
                answers.append(ans_text)
                   
        return results, infoboxes, answers

    def init(self, app):
        @app.route('/ai-auxiliary-search', methods=['POST'])
        def ai_auxiliary_search():
            if not self.api_key:
                abort(403)
            
            data = request.json or {}
            token = data.get('tk', '')
            
            # Token access control
            try:
                ts, sig = token.rsplit('.', 1)
                expected = hashlib.sha256(f"{ts}{self.secret}".encode()).hexdigest()
                if sig != expected or (time.time() - float(ts)) > TOKEN_EXPIRY_SEC:
                    abort(403)
            except (ValueError, KeyError, AttributeError):
                abort(403)

            aux_ip = request.headers.get('X-Real-IP') or request.headers.get('X-Forwarded-For')
            if aux_ip:
                logger.debug(f"{PLUGIN_NAME}: /ai-auxiliary-search from proxied IP {aux_ip}")

            query = data.get('query', '').strip()
            lang = data.get('lang', 'all')
            categories = data.get('categories', 'general')
            offset = data.get('offset', 0)
            if not query:
                return jsonify({'results': []})
            
            try:
                from searx.search import SearchWithPlugins
                from searx.search.models import SearchQuery
                from searx.query import RawTextQuery
                from searx.webadapter import get_engineref_from_category_list
                
                preferences = getattr(request, 'preferences', None)
                disabled_engines = preferences.engines.get_disabled() if preferences else []
                rtq = RawTextQuery(query, disabled_engines)
                if isinstance(categories, str):
                    category_list = [c.strip() for c in categories.split(',') if c.strip()]
                else:
                    category_list = categories or ['general']
                
                enginerefs = get_engineref_from_category_list(category_list, disabled_engines)
                sq = SearchQuery(
                    query=rtq.getQuery(),
                    engineref_list=enginerefs,
                    lang=lang,
                    pageno=1,
                )
                search_obj = SearchWithPlugins(sq, request, user_plugins=[])
                result_container = search_obj.search()
                
                raw_results = result_container.get_ordered_results()
                raw_infoboxes = getattr(result_container, 'infoboxes', [])
                raw_answers = getattr(result_container, 'answers', [])
                
                results, infoboxes, answers = self._parse_aux_results(raw_results, raw_infoboxes, raw_answers)
                
                context_str, new_urls = self._assemble_context(results, infoboxes, answers, offset)

                return jsonify({
                    'context': context_str,
                    'new_urls': new_urls,
                    'results': results, 
                    'infoboxes': infoboxes,
                    'answers': answers,
                    'query': query
                })

            except Exception as e:
                logger.error(f"{PLUGIN_NAME}: Aux search failed: {e}")
                return jsonify({'results': [], 'error': 'Search failed'}), 500

        @app.route('/ai-models', methods=['GET'])
        def ai_models():
            token = request.args.get('tk', '')

            models_ip = request.headers.get('X-Real-IP') or request.headers.get('X-Forwarded-For')
            if models_ip:
                logger.debug(f"{PLUGIN_NAME}: /ai-models from proxied IP {models_ip}")

            try:
                ts, sig = token.rsplit('.', 1)
                expected = hashlib.sha256(f"{ts}{self.secret}".encode()).hexdigest()
                if sig != expected or (time.time() - float(ts)) > TOKEN_EXPIRY_SEC:
                    abort(403)
            except (ValueError, KeyError, AttributeError):
                abort(403)

            auth_headers = {"Authorization": f"Bearer {self.api_key}"}
            p = urlparse(self.endpoint_url)
            base = f"{p.scheme}://{p.netloc}"

            def fetch_get(start_url):
                url = start_url
                for _ in range(5):
                    conn, path = _get_streaming_connection(url)
                    conn.request("GET", path, headers=auth_headers)
                    res = conn.getresponse()
                    if res.status in (301, 302, 307, 308):
                        location = res.getheader('Location', '')
                        res.read(); conn.close()
                        if not location:
                            return None
                        url = location if location.startswith('http') else f"{urlparse(url).scheme}://{urlparse(url).netloc}{location}"
                        continue
                    return res
                return None

            for models_url, parse_fn in [
                (f"{base}/v1/models", lambda d: [m['id'] for m in d.get('data', [])]),
                (f"{base}/api/tags",  lambda d: [m['name'] for m in d.get('models', [])]),
            ]:
                try:
                    res = fetch_get(models_url)
                    if res and res.status == 200:
                        models = parse_fn(json.loads(res.read().decode('utf-8', errors='replace')))
                        if models:
                            return jsonify({'models': models})
                    elif res:
                        res.read()
                except Exception as e:
                    logger.debug(f"{PLUGIN_NAME}: /ai-models attempt {models_url} failed: {e}")

            return jsonify({'models': [self.model] if self.model else []})

        @app.route('/ai-stream', methods=['POST'])
        def handle_ai_stream():
            data = request.json or {}
            
            token = data.get('tk', '')
            q = data.get('q', '')
            lang = data.get('lang', 'all')
            
            try:
                ts, sig = token.rsplit('.', 1)
                expected = hashlib.sha256(f"{ts}{self.secret}".encode()).hexdigest()
                if sig != expected or (time.time() - float(ts)) > TOKEN_EXPIRY_SEC:
                    abort(403)
            except (ValueError, KeyError, AttributeError):
                abort(403)

            context_text = data.get('context', '')
            prev_answer = (data.get('prev_answer') or '')[-4000:]
            req_model = (data.get('model') or '').strip()
            effective_model = req_model or self.model

            client_ip = request.headers.get('X-Real-IP') or request.headers.get('X-Forwarded-For')
            if client_ip:
                logger.debug(f"{PLUGIN_NAME}: /ai-stream from proxied IP {client_ip}")

            if not self.api_key:
                return Response("Missing API key or query", status=400)
            
            today = time.strftime("%Y-%m-%d")
            lang_instruction = f" Respond in {lang}." if lang not in ('all', 'auto') else ""

            base_sys = self.system_prompt if self.system_prompt else "You are a direct, citation-accurate search synthesis engine."
            SYSTEM = (f"{base_sys} Today is {today}.{lang_instruction} "
                      "Output only your final answer. Do not output your thinking process, "
                      "reasoning steps, or internal monologue. Begin your response with the "
                      "direct answer immediately. "
                      "Be concise. Give a 2-4 sentence overview that directly answers the query. "
                      "The user can ask follow-up questions for more detail. "
                      "Do not enumerate or list everything from the sources.")
            max_source_idx = 0
            if context_text:
                indices = re.findall(r'\[(\d+)\]', context_text)
                if indices:
                    max_source_idx = max(map(int, indices))

            CORE_RULES = [
                "Answer the question directly using the provided context.",
                "MUST CITE SOURCES by tailing a sentence with [n] or [n,n] etc. If citing general knowledge, use [*].",
                "Never explain your process. The user expects a direct response.",
                "Response format must be plain text with no markdown. "
                "Be brief: 2-4 sentences maximum. Lead with the direct answer. "
                "Cite the most relevant source(s) only. Stop after the overview.",
                "If sources and general knowledge are insufficient, respond with 'Insufficient information to answer.'"
            ]

            if q == "Continue":
                task = "CONTINUE: Pick up exactly where previous answer stopped. No repetition. Seamless flow."
            elif prev_answer:
                task = "FOLLOW-UP: Address the new question using prior context. Prioritize the new query."
            else:
                task = "ANSWER FIRST: Lead with the direct answer. No preamble, no context-setting."

            grounding = "GROUNDING: KNOWLEDGE GRAPH > DEEP > SHALLOW." if context_text else "GROUNDING: No sources available. Use general knowledge and cite as [*] which means based on general knowledge."
            history_rule = "HISTORY: Refer to prior exchange for context. Ideally, do not repeat any claims." if prev_answer else None

            instructions = [task] + CORE_RULES + [grounding]
            if history_rule:
                instructions.append(history_rule)

            numbered_instructions = "\n".join(f"{i+1}. {r}" for i, r in enumerate(instructions))
            prompt = f"""<system>{SYSTEM}</system>

<GROUNDING_SOURCES>
{context_text or 'None.'}
</GROUNDING_SOURCES>

<HISTORY>
{prev_answer or 'None.'}
</HISTORY>

<USER_QUERY>{q}</USER_QUERY>

<CORE_DIRECTIVES>
{numbered_instructions}
</CORE_DIRECTIVES>"""

            job_id = hashlib.sha256(f"{time.time()}{q}".encode()).hexdigest()[:16]

            payload_dict = {
                "model": effective_model,
                "messages": [
                    {"role": "system",    "content": SYSTEM},
                    {"role": "user",      "content": prompt},
                    {"role": "assistant", "content": ""},
                ],
                "stream": True,
                "max_tokens": self.max_tokens,
                "temperature": self.temperature,
            }
            stream_payload = json.dumps(payload_dict)
            stream_headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
            }

            try:
                vk = _get_valkey()
                vk.set(f"ai:job:{job_id}:status", "running", ex=120)
            except Exception as e:
                logger.error(f"{PLUGIN_NAME}: Valkey unavailable: {e}", exc_info=True)
                return jsonify({"error": "Streaming service unavailable (Valkey connection failed)."}), 503

            t = threading.Thread(
                target=stream_to_valkey,
                args=(job_id, stream_payload, stream_headers, self.endpoint_url, effective_model),
                daemon=True,
            )
            t.start()

            return jsonify({"job_id": job_id})

        @app.route('/ai-status/<job_id>', methods=['GET'])
        def ai_status(job_id):
            token = request.args.get('tk', '')
            try:
                ts, sig = token.rsplit('.', 1)
                expected = hashlib.sha256(f"{ts}{self.secret}".encode()).hexdigest()
                if sig != expected or (time.time() - float(ts)) > TOKEN_EXPIRY_SEC:
                    abort(403)
            except (ValueError, KeyError, AttributeError):
                abort(403)

            offset = max(0, int(request.args.get('offset', 0)))
            chunks_key = f"ai:job:{job_id}:chunks"
            status_key = f"ai:job:{job_id}:status"

            try:
                vk = _get_valkey()
                status = vk.get(status_key)
                if status is None:
                    return jsonify({"error": "Job not found or expired"}), 404
                raw_chunks = vk.lrange(chunks_key, offset, -1)
            except Exception as e:
                logger.error(f"{PLUGIN_NAME}: Valkey error in /ai-status: {e}", exc_info=True)
                return jsonify({"error": "Stream service temporarily unavailable"}), 503

            done = False
            error = None
            chunks = []
            for chunk in raw_chunks:
                if chunk == '__DONE__':
                    done = True
                    break
                elif chunk.startswith('__ERROR__'):
                    error = chunk[9:]
                    done = True
                    break
                else:
                    chunks.append(chunk)

            return jsonify({"chunks": chunks, "done": done, "error": error})

        return True

    def _fetch_page_text(self, url: str, timeout: int = 5) -> str:
        SKIP_DOMAINS = ('youtube.com', 'twitter.com', 'x.com', 'instagram.com', 'facebook.com', 'reddit.com')
        try:
            if url.endswith('.pdf'):
                return ''
            if any(d in url for d in SKIP_DOMAINS):
                return ''

            current_url = url
            for _ in range(3):  # initial request + up to 2 redirects
                parsed = urlparse(current_url)
                host = parsed.hostname or ''
                if not host:
                    return ''
                port = parsed.port or (443 if parsed.scheme == 'https' else 80)
                path = (parsed.path or '/') + ('?' + parsed.query if parsed.query else '')

                if parsed.scheme == 'https':
                    try:
                        import certifi
                        ctx = ssl.create_default_context(cafile=certifi.where())
                    except ImportError:
                        ctx = ssl.create_default_context()
                    conn = http.client.HTTPSConnection(host, port, timeout=timeout, context=ctx)
                else:
                    conn = http.client.HTTPConnection(host, port, timeout=timeout)

                try:
                    conn.request('GET', path, headers={'User-Agent': 'Mozilla/5.0 (compatible; SearXNG-AI/1.0)'})
                    res = conn.getresponse()

                    if res.status in (301, 302, 303, 307, 308):
                        location = res.getheader('Location', '')
                        res.read()
                        if not location:
                            return ''
                        current_url = location if location.startswith('http') else f"{parsed.scheme}://{parsed.netloc}{location}"
                        continue

                    if res.status != 200:
                        return ''

                    html = res.read(512 * 1024).decode('utf-8', errors='replace')
                finally:
                    conn.close()

                html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
                html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
                html = re.sub(r'<nav[^>]*>.*?</nav>', '', html, flags=re.DOTALL | re.IGNORECASE)
                html = re.sub(r'<header[^>]*>.*?</header>', '', html, flags=re.DOTALL | re.IGNORECASE)
                html = re.sub(r'<footer[^>]*>.*?</footer>', '', html, flags=re.DOTALL | re.IGNORECASE)
                text = re.sub(r'<[^>]+>', '', html)
                text = (text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
                            .replace('&quot;', '"').replace('&#39;', "'").replace('&nbsp;', ' '))
                text = re.sub(r'\s+', ' ', text).strip()

                logger.debug(f"{PLUGIN_NAME}: fetched {len(text)} chars from {url}")
                return text[:2000]

            return ''
        except Exception:
            return ''

    def _enrich_results(self, clean_results: list, query: str) -> list:
        enrich_count = min(3, self.context_deep_count)
        for r in clean_results:
            r['fetched_content'] = ''

        futures_map: dict = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            for r in clean_results[:enrich_count]:
                futures_map[executor.submit(self._fetch_page_text, r.get('url', ''))] = r

            for future, r in futures_map.items():
                try:
                    text = future.result(timeout=6)
                    if text and len(text) > 100:
                        words = query.lower().split()
                        text_lower = text.lower()
                        best_pos = len(text) // 2
                        best_count = -1

                        keyword_positions = []
                        for word in words:
                            start = 0
                            while True:
                                idx = text_lower.find(word, start)
                                if idx == -1:
                                    break
                                keyword_positions.append(idx)
                                start = idx + 1

                        for pos in (keyword_positions or [best_pos]):
                            window_start = max(0, pos - 400)
                            window_end = min(len(text), pos + 400)
                            count = sum(w in text_lower[window_start:window_end] for w in words)
                            if count > best_count:
                                best_count = count
                                best_pos = pos

                        start = max(0, best_pos - 400)
                        r['fetched_content'] = text[start:start + 800]
                except Exception:
                    pass

        return clean_results

    def _assemble_context(self, clean_results, infoboxes, answers, offset=0) -> tuple[str, list]:
        """Builds context string from normalized search data. Returns (context_str, urls)."""
        context_parts = []
        result_urls = []
        
        knowledge_graph_lines = []
        for ib in infoboxes:
            ib_name = ib.get('name', '') or ib.get('infobox', '') or ib.get('title', '')
            ib_content = str(ib.get('content', '')).replace('\n', ' ').strip()
            
            if ib_name:
                parts = [f"INFOBOX [{ib_name}]:"]
                if ib_content:
                    parts.append(ib_content)
                for attr in ib.get('attributes', []):
                    attr_label = attr.get('label', '')
                    attr_value = attr.get('value', '')
                    if attr_label and attr_value:
                        parts.append(f"  {attr_label}: {attr_value}")
                
                knowledge_graph_lines.append(" ".join(parts) if len(parts) == 2 else "\n".join(parts))

        for ans_text in answers:
            if ans_text and not str(ans_text).startswith('<'):
                knowledge_graph_lines.append(f"ANSWER: {str(ans_text)[:300]}")
        
        if knowledge_graph_lines:
            context_parts.append("KNOWLEDGE GRAPH:\n" + "\n".join(knowledge_graph_lines))
        
        deep_lines = []
        for i, r in enumerate(clean_results[:self.context_deep_count]):
            url = r.get('url', '')
            result_urls.append(url)
            domain = urlparse(url).netloc.replace('www.', '')
            date_str = f" ({r.get('publishedDate')})" if r.get('publishedDate') else ""
            title = r.get('title', '').replace('\n', ' ').strip()
            idx = i + 1 + offset
            fetched_content = r.get('fetched_content', '')
            if fetched_content:
                deep_lines.append(f"[{idx}] {domain}{date_str}: {title}: {fetched_content}")
            else:
                logger.debug(f"{PLUGIN_NAME}: falling back to snippet for [{idx}] {domain}")
                content = str(r.get('content', '')).replace('\n', ' ').strip()[:800]
                deep_lines.append(f"[{idx}] {domain}{date_str}: {title}: {content}")
        
        if deep_lines:
            context_parts.append("DEEP SOURCES:\n" + "\n".join(deep_lines))
            
        if self.context_shallow_count > 0:
            shallow_lines = []
            start_idx = self.context_deep_count
            end_idx = self.context_deep_count + self.context_shallow_count
            for i, r in enumerate(clean_results[start_idx:end_idx]):
                url = r.get('url', '')
                result_urls.append(url)
                domain = urlparse(url).netloc.replace('www.', '')
                title = r.get('title', '').replace('\n', ' ').strip()[:60]
                idx = i + 1 + start_idx + offset
                shallow_lines.append(f"[{idx}] {domain}: {title}")
            
            if shallow_lines:
                context_parts.append("SHALLOW SOURCES (headlines):\n" + "\n".join(shallow_lines))
        
        return "\n\n".join(context_parts), result_urls

    def post_search(self, request: "SXNG_Request", search: "SearchWithPlugins") -> EngineResults:
        results = EngineResults()
        try:
            if request and hasattr(request, 'headers') and request.headers.get('X-AI-Auxiliary'):
                return results

            if request and request.form.get('format', 'html') != 'html':
                return results

            if self.question_mark_required and '?' not in search.search_query.query:
                return results

            current_tabs = set(search.search_query.categories)
            if not current_tabs: current_tabs = {'general'}

            if not self.active or not self.api_key or search.search_query.pageno > 1 or not self.allowed_tabs.intersection(current_tabs):
                return results

            raw_results = search.result_container.get_ordered_results()
            raw_infoboxes = getattr(search.result_container, 'infoboxes', [])
            raw_answers = getattr(search.result_container, 'answers', [])
            
            q_clean = search.search_query.query.strip()
            clean_results, infoboxes, answers = self._parse_aux_results(raw_results, raw_infoboxes, raw_answers)
            clean_results = self._enrich_results(clean_results, q_clean)
            context_str, _ = self._assemble_context(clean_results, infoboxes, answers)

            ts = str(int(time.time()))
            lang = search.search_query.lang
            sig = hashlib.sha256(f"{ts}{self.secret}".encode()).hexdigest()
            tk = f"{ts}.{sig}"
            
            # XSS blocking
            safe_json = lambda x: json.dumps(x).replace('<', '\\u003c').replace('>', '\\u003e').replace('&', '\\u0026')
            
            b64_context = base64.b64encode(context_str.encode('utf-8')).decode('utf-8')
            total_context_count = self.context_deep_count + self.context_shallow_count
            
            raw_urls = [r.get('url', '') for r in clean_results[:total_context_count]]
            
            js_q = safe_json(q_clean)
            js_lang = safe_json(lang)
            js_urls = safe_json(raw_urls)
            js_b64_context = safe_json(b64_context)
            js_tk = safe_json(tk)
            js_script_root = safe_json((request.script_root if request else '').rstrip('/'))
            js_model_init = safe_json(self.model)

            is_interactive = self.interactive

            interactive_css = INTERACTIVE_CSS if is_interactive else ''
            interactive_html = INTERACTIVE_HTML if is_interactive else ''
            interactive_js_init = INTERACTIVE_JS if is_interactive else ''

            if is_interactive:
                interactive_js_complete = "footer.style.display = 'flex';"
            else:
                interactive_js_complete = ''
            stream_fn_sig = 'async function startStream(overrideQ = null, prevAnswer = null, auxContext = null)'
            stream_q = 'overrideQ || q_init' if is_interactive else 'q_init'
            stream_body = f'''prev_answer: prevAnswer''' if is_interactive else ''
            
            js_code = FRONTEND_JS_TEMPLATE \
                .replace("__IS_INTERACTIVE__", 'true' if is_interactive else 'false') \
                .replace("__TK__", js_tk) \
                .replace("__SCRIPT_ROOT__", js_script_root) \
                .replace("__MODEL_INIT__", js_model_init) \
                .replace("__CITATION_HELPER_JS__", CITATION_HELPER_JS) \
                .replace("__INTERACTIVE_JS_INIT__", interactive_js_init) \
                .replace("__STREAM_FN_SIG__", stream_fn_sig) \
                .replace("__STREAM_Q__", stream_q) \
                .replace("__STREAM_BODY__", ', ' + stream_body if stream_body else '') \
                .replace("__INTERACTIVE_JS_COMPLETE__", interactive_js_complete) \
                .replace("__JS_LANG__", js_lang) \
                .replace("__JS_URLS__", js_urls) \
                .replace("__B64_CONTEXT__", js_b64_context) \
                .replace("__JS_Q__", js_q)

            html_payload = f'''
                <article id="sxng-stream-box" class="answer" style="display:none; margin-top: 0; margin-bottom: 0;">
                    <style>
                        @keyframes sxng-fade-pulse {{
                            0%, 100% {{ opacity: 0.1; }}
                            50% {{ opacity: 1; }}
                        }}
                        @keyframes sxng-fade-in {{
                            from {{ opacity: 0; }}
                            to {{ opacity: 1; }}
                        }}
                        #sxng-stream-data {{
                            position: relative;
                            margin-top: .5rem !important;
                            min-height: 1.5em;
                        }}
                        .sxng-cursor {{
                            display: inline-block;
                            width: 0.6em;
                            height: 1.2em;
                            background: var(--color-result-link-visited, var(--color-result-link, #b48ead));
                            vertical-align: text-bottom;
                            animation: sxng-fade-pulse 1s ease-in-out infinite;
                            margin-right: 0.2rem;
                            border-radius: 2px;
                        }}
                        .sxng-chunk {{
                            opacity: 1;
                        }}
                        @media (min-width: 769px) {{
                            .sxng-chunk {{
                                animation: sxng-fade-in 0.3s ease-out;
                            }}
                        }}
                        .sxng-ai-header {{
                            display: flex;
                            justify-content: space-between;
                            align-items: center;
                            margin-top: -4px;
                        }}
                        .sxng-ai-label {{
                            font-size: 1.05em;
                            font-weight: 700;
                            letter-spacing: 0.04em;
                            text-transform: none;
                            color: var(--color-base-font, #333);
                        }}
                        {interactive_css}
                    </style>
                    <div class="sxng-ai-header">
                        <span class="sxng-ai-label">
                            <span style="color:#4a9eff;font-size:1.1em;">✦</span> AI Overview
                        </span>
                        <select id="sxng-model-select" class="sxng-model-select" title="Select model"></select>
                    </div>
                    <p id="sxng-stream-data" style="white-space: pre-wrap; color: var(--color-result-description); font-size: 0.95rem; margin:0;"><span class="sxng-cursor"></span></p>
                    {interactive_html}
                    <script>
                    {js_code}
                    </script>
                </article>
            '''
            search.result_container.answers.add(results.types.Answer(answer=Markup(html_payload)))
        except Exception as e:
            logger.error(f"{PLUGIN_NAME}: {e}")
        return results