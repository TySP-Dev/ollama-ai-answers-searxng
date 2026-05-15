import json, os, logging, base64, time, hashlib, re, http.client, ssl
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

TOKEN_EXPIRY_SEC = 3600
STREAM_CHUNK_SIZE = 512
STREAM_TIMEOUT_SEC = 60

def _get_streaming_connection(url: str):
    parsed = urlparse(url)
    host = parsed.hostname
    port = parsed.port or (443 if parsed.scheme == 'https' else 80)
    path = parsed.path + ('?' + parsed.query if parsed.query else '')
    
    verify_ssl = True
    if get_network is not None:
        try:
            net = get_network()
            verify_ssl = getattr(net, 'verify', True)
        except Exception:
            pass
    
    if parsed.scheme == 'https':
        ctx = ssl.create_default_context() if verify_ssl else ssl._create_unverified_context()
        conn = http.client.HTTPSConnection(host, port, timeout=STREAM_TIMEOUT_SEC, context=ctx)
    else:
        conn = http.client.HTTPConnection(host, port, timeout=STREAM_TIMEOUT_SEC)
    
    return conn, path



PLUGIN_NAME = "AI Answers"
DEFAULT_TABS = "general,science,it,news"

PROVIDER_PRESETS = {
    'openai':     {'url': 'https://api.openai.com/v1/chat/completions',       'model': 'gpt-4o-mini'},
    'openrouter': {'url': 'https://openrouter.ai/api/v1/chat/completions',    'model': 'google/gemma-3-27b-it:free'},
    'ollama':     {'url': 'http://localhost:11434/v1/chat/completions',       'model': 'llama3.2'},
    'localai':    {'url': 'http://localhost:8080/v1/chat/completions',        'model': 'gpt-4'},
    'lmstudio':   {'url': 'http://localhost:1234/v1/chat/completions',        'model': 'local-model'},
    'gemini':     {'url': 'https://generativelanguage.googleapis.com/v1beta/models/{model}:streamGenerateContent', 'model': 'gemma-3-27b-it'},
    'azure':      {'url': None,                                               'model': 'azure-deployment'},
    'huggingface': {'url': 'https://api-inference.huggingface.co/models/{model}/v1/chat/completions', 'model': 'meta-llama/Meta-Llama-3-8B-Instruct'}
}

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
                            height: 32px;
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
                                <svg viewBox="0 0 24 24"><path d="M19,7V11H5.83L9.41,7.41L8,6L2,12L8,18L9.41,16.59L5.83,13H21V7H19Z"/></svg>
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
            let timeoutId = setTimeout(() => controller.abort(), 60000);
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

            const fullText = (respJson.text || '').trim();

            if (!fullText) {
                const cursorErr = data.querySelector('.sxng-cursor');
                if (cursorErr) cursorErr.remove();
                const errSpan = document.createElement('span');
                errSpan.style.color = '#bf616a';
                errSpan.textContent = 'No response received. Check API configuration and server logs.';
                data.appendChild(errSpan);
                return;
            }

            let mainText = fullText;
            const thinkMatch = mainText.match(/^<think>([\s\S]*?)<\/think>\s*/);
            if (thinkMatch) {
                const cursorTh = data.querySelector('.sxng-cursor');
                const details = document.createElement('details');
                details.className = 'sxng-reasoning';
                details.innerHTML = '<summary>Thought Process</summary>';
                const thoughtDiv = document.createElement('div');
                thoughtDiv.className = 'sxng-thought-content';
                thoughtDiv.textContent = thinkMatch[1];
                details.appendChild(thoughtDiv);
                if (cursorTh) cursorTh.before(details);
                else data.appendChild(details);
                mainText = mainText.substring(thinkMatch[0].length);
            }

            let cursor = data.querySelector('.sxng-cursor');
            if (!cursor) {
                cursor = document.createElement('span');
                cursor.className = 'sxng-cursor';
                data.appendChild(cursor);
            }

            let buffer = '';
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

            let twPos = 0;
            const twBatch = 4;
            await new Promise(resolve => {
                function twTick() {
                    if (twPos >= mainText.length) {
                        flushBuffer(true);
                        resolve();
                        return;
                    }
                    const end = Math.min(twPos + twBatch, mainText.length);
                    buffer += mainText.substring(twPos, end);
                    twPos = end;
                    flushBuffer(false);
                    setTimeout(twTick, 8);
                }
                twTick();
            });

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

            const collectedResponse = mainText;

            __INTERACTIVE_JS_COMPLETE__

            if (collectedResponse) {
                conversation.turns.push({role: 'assistant', content: collectedResponse.trim(), ts: Date.now()});
            }
            
            // Save state if this was an initial generation or a regeneration
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



    def _ollama_unload_model(self) -> None:
        try:
            if self.provider != 'ollama':
                return
            if not getattr(self, 'ollama_unload_after', False):
                return
            unload_url = (getattr(self, 'ollama_unload_url', '') or '').strip()
            if not unload_url:
                return

            conn = None
            try:
                conn, path = _get_streaming_connection(unload_url)
                conn.timeout = 2.0 
                payload = json.dumps({
                    "model": self.model,
                    "messages": [],
                    "keep_alive": 0
                })
                headers = {"Content-Type": "application/json"}
                if self.api_key and self.api_key not in ('none', 'ollama'):
                    headers["Authorization"] = f"Bearer {self.api_key}"
                conn.request("POST", path, body=payload, headers=headers)
                res = conn.getresponse()
                res.read()
                if res.status >= 400:
                    logger.warning(f"{PLUGIN_NAME}: Ollama unload failed: {res.status} {res.reason}")
            finally:
                if conn:
                    conn.close()
        except Exception as e:
            logger.warning(f"{PLUGIN_NAME}: Ollama unload error: {e}")

    def _load_config(self):
        self.interactive = os.getenv('LLM_INTERACTIVE', 'true').lower().strip() in ('true', '1', 'yes', 'on')
        self.question_mark_required = os.getenv('LLM_QUESTION_MARK_REQUIRED', 'false').lower().strip() in ('true', '1', 'yes', 'on')
        raw_provider = os.getenv('LLM_PROVIDER', '').lower().strip()
        
        raw_url = os.getenv('LLM_URL', '').strip()
        if not raw_provider and raw_url:
            url_lower = raw_url.lower()
            if 'openai.com' in url_lower:
                raw_provider = 'openai'
            elif 'openrouter.ai' in url_lower:
                raw_provider = 'openrouter'
            elif ':11434' in url_lower:
                raw_provider = 'ollama'
            elif 'generativelanguage.googleapis.com' in url_lower:
                raw_provider = 'gemini'
            elif 'openai.azure.com' in url_lower or '.azure.com' in url_lower:
                raw_provider = 'azure'
            elif 'huggingface.co' in url_lower:
                raw_provider = 'huggingface'
            else:
                raw_provider = 'openai'
                logger.info(f"{PLUGIN_NAME}: Using OpenAI-compatible mode for custom URL")
        
        if not raw_provider:
            self.provider = ''
            self.model = ''
            self.is_gemini = False
            self.api_key = ''
            return
        
        if raw_provider not in PROVIDER_PRESETS:
            logger.warning(f"{PLUGIN_NAME}: Unknown provider '{raw_provider}', falling back to 'openai'")
        self.provider = raw_provider if raw_provider in PROVIDER_PRESETS else 'openai'
        self.is_gemini = (self.provider == 'gemini')
        preset = PROVIDER_PRESETS[self.provider]

        self.api_key = os.getenv('LLM_KEY', '')
        if not self.api_key and self.provider in ('ollama', 'localai', 'lmstudio'):
            self.api_key = 'none'
        self.api_key = self.api_key.strip()

        self.model = os.getenv('LLM_MODEL', preset['model']).strip()

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
        
        preset_url = preset['url']
        if preset_url and '{model}' in preset_url:
            preset_url = preset_url.format(model=self.model)
        
        raw_url = os.getenv('LLM_URL', '').strip() or preset_url
        if not raw_url.startswith(('http://', 'https://')):
            raw_url = f"https://{raw_url}"
        self.endpoint_url = raw_url
        
        self.ollama_unload_after = os.getenv('LLM_OLLAMA_UNLOAD_AFTER', 'false').lower().strip() in ('true', '1', 'yes', 'on')
        self.ollama_unload_url = ''
        if self.provider == 'ollama' and self.ollama_unload_after:
            try:
                p = urlparse(self.endpoint_url)
                scheme = p.scheme or 'http'
                host = p.hostname or 'localhost'
                port = p.port
                netloc = f"{host}:{port}" if port else host
                self.ollama_unload_url = f"{scheme}://{netloc}/api/chat"
            except Exception:
                self.ollama_unload_url = "http://localhost:11434/api/chat"
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
        if not self.provider:
            return

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

            if self.provider != 'ollama':
                return jsonify({'models': [self.model] if self.model else []})

            conn = None
            try:
                p = urlparse(self.endpoint_url)
                tags_url = f"{p.scheme}://{p.netloc}/api/tags"
                conn, path = _get_streaming_connection(tags_url)
                conn.request("GET", path)
                res = conn.getresponse()
                body = res.read().decode('utf-8', errors='replace')
                tags_data = json.loads(body)
                models = [m['name'] for m in tags_data.get('models', [])]
                return jsonify({'models': models})
            except Exception as e:
                logger.error(f"{PLUGIN_NAME}: /ai-models error: {e}", exc_info=True)
                return jsonify({'models': [self.model] if self.model else []})
            finally:
                if conn:
                    conn.close()

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

            def call_gemini():
                base = self.endpoint_url.replace('streamGenerateContent', 'generateContent')
                url = f"{base}&key={self.api_key}" if '?' in base else f"{base}?key={self.api_key}"
                conn = None
                try:
                    conn, path = _get_streaming_connection(url)
                    payload = json.dumps({
                        "contents": [{"parts": [{"text": prompt}]}],
                        "generationConfig": {"maxOutputTokens": min(self.max_tokens * 4, 8192), "temperature": self.temperature}
                    })
                    conn.request("POST", path, body=payload.encode('utf-8'), headers={"Content-Type": "application/json"})
                    res = conn.getresponse()
                    if res.status != 200:
                        body = res.read(2048).decode('utf-8', errors='replace')[:500]
                        logger.error(f"{PLUGIN_NAME}: Gemini API {res.status}: {body}")
                        return '', f"API error {res.status}. Check server logs."
                    obj = json.loads(res.read().decode('utf-8', errors='replace'))
                    if obj.get('promptFeedback', {}).get('blockReason'):
                        return '', f"Gemini blocked prompt: {obj['promptFeedback']['blockReason']}"
                    candidates = obj.get('candidates', [])
                    if not candidates:
                        return '', "No candidates in Gemini response."
                    first = candidates[0]
                    if first.get('finishReason') == 'SAFETY':
                        return '', "Gemini stopped generation due to safety filters."
                    parts = first.get('content', {}).get('parts', [])
                    text = ''.join(p.get('text', '') for p in parts if isinstance(p, dict))
                    return text, None
                except Exception as e:
                    logger.error(f"{PLUGIN_NAME}: Gemini call error: {e}", exc_info=True)
                    return '', f"Connection Error: {e}"
                finally:
                    if conn: conn.close()

            def call_openai_compatible():
                conn = None
                try:
                    conn, path = _get_streaming_connection(self.endpoint_url)
                    payload_dict = {
                        "model": effective_model,
                        "messages": [
                            {"role": "system",    "content": SYSTEM},
                            {"role": "user",      "content": prompt},
                            {"role": "assistant", "content": ""},
                        ],
                        "stream": False,
                        "max_tokens": self.max_tokens,
                        "temperature": self.temperature
                    }
                    payload = json.dumps(payload_dict)
                    headers = {
                        "Content-Type": "application/json",
                        "HTTP-Referer": "https://github.com/searxng/searxng",
                        "X-Title": "SearXNG"
                    }
                    if self.provider == 'azure':
                        headers['api-key'] = self.api_key
                    else:
                        headers['Authorization'] = f"Bearer {self.api_key}"
                    conn.request("POST", path, body=payload.encode('utf-8'), headers=headers)
                    res = conn.getresponse()
                    if res.status != 200:
                        body = res.read(2048).decode('utf-8', errors='replace')[:500]
                        logger.error(f"{PLUGIN_NAME}: {self.provider} API {res.status}: {body}")
                        return '', f"API error {res.status}. Check server logs."
                    obj = json.loads(res.read().decode('utf-8', errors='replace'))
                    if "error" in obj:
                        err = obj["error"]
                        msg = err.get("message", str(err)) if isinstance(err, dict) else str(err)
                        return '', f"API Error: {msg}"
                    choices = obj.get("choices", [])
                    if not choices:
                        return '', "No choices in API response."
                    message = choices[0].get("message", {})
                    content = re.sub(r'<think>.*?</think>', '', message.get("content") or "", flags=re.DOTALL).strip()
                    reasoning = message.get("reasoning") or message.get("reasoning_content") or ""
                    if not content and reasoning:
                        logger.warning(f"{PLUGIN_NAME}: {self.provider} returned empty content; extracting answer from reasoning field")
                        header_pat = re.compile(r'^\s*\*?\*?[A-Z][^:]{0,40}:\*?\*?\s*$', re.MULTILINE)
                        matches = list(header_pat.finditer(reasoning))
                        if matches:
                            answer = reasoning[matches[-1].end():].strip()
                        else:
                            paras = [p.strip() for p in re.split(r'\n{2,}', reasoning) if p.strip()]
                            answer = paras[-1] if paras else reasoning.strip()
                        full = answer
                    else:
                        full = (f"<think>\n{reasoning}\n</think>\n\n" if reasoning else "") + content
                    full = re.sub(r'<think>.*?</think>', '', full, flags=re.DOTALL).strip()
                    return full, None
                except Exception as e:
                    logger.error(f"{PLUGIN_NAME}: {self.provider} call error: {e}", exc_info=True)
                    return '', f"Connection Error: {e}"
                finally:
                    if conn: conn.close()

            call_fn = call_gemini if self.is_gemini else call_openai_compatible
            text, error = call_fn()

            if self.provider == 'ollama' and getattr(self, 'ollama_unload_after', False):
                self._ollama_unload_model()

            return jsonify({"text": text, "error": error})
        return True

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
            content = str(r.get('content', '')).replace('\n', ' ').strip()[:800]
            idx = i + 1 + offset
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
            
            clean_results, infoboxes, answers = self._parse_aux_results(raw_results, raw_infoboxes, raw_answers)
            context_str, _ = self._assemble_context(clean_results, infoboxes, answers)

            ts = str(int(time.time()))
            q_clean = search.search_query.query.strip()
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
                interactive_js_complete = (
                    "footer.style.display = 'flex';\n"
                    "                            const _msel2 = document.getElementById('sxng-model-select');\n"
                    "                            if (_msel2 && !_msel2.dataset.loaded) {\n"
                    "                                const _modelsUrl = script_root + '/ai-models?tk=' + encodeURIComponent(tk_init);\n"
                    "                                console.log('[AI Answers] Fetching models from', _modelsUrl);\n"
                    "                                fetch(_modelsUrl)\n"
                    "                                    .then(r => {\n"
                    "                                        console.log('[AI Answers] /ai-models response status:', r.status);\n"
                    "                                        return r.ok ? r.json() : Promise.reject('HTTP ' + r.status);\n"
                    "                                    })\n"
                    "                                    .then(d => {\n"
                    "                                        console.log('[AI Answers] /ai-models payload:', d);\n"
                    "                                        if (!d || !d.models || d.models.length <= 1) {\n"
                    "                                            console.log('[AI Answers] Model selector hidden: need 2+ models, got', d && d.models ? d.models.length : 0);\n"
                    "                                            return;\n"
                    "                                        }\n"
                    "                                        const _cur = _msel2.value;\n"
                    "                                        _msel2.innerHTML = '';\n"
                    "                                        d.models.forEach(m => {\n"
                    "                                            const o = document.createElement('option');\n"
                    "                                            o.value = m; o.textContent = m;\n"
                    "                                            if (m === (_cur || model_init)) o.selected = true;\n"
                    "                                            _msel2.appendChild(o);\n"
                    "                                        });\n"
                    "                                        document.getElementById('sxng-model-select').style.display = 'inline-block';\n"
                    "                                        console.log('[AI Answers] Model selector shown with', d.models.length, 'models');\n"
                    "                                    })\n"
                    "                                    .catch(err => { console.warn('[AI Answers] /ai-models fetch failed:', err); });\n"
                    "                            }"
                )
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