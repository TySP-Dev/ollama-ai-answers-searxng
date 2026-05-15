import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import logging
from types import ModuleType
from flask import Flask, request, redirect

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
os.environ.setdefault('LLM_URL', 'http://localhost:11434/v1/chat/completions')

# SearXNG module mocks
searx = ModuleType("searx")
searx_plugins = ModuleType("searx.plugins")
searx_results = ModuleType("searx.result_types")

class MockPlugin:
    def __init__(self, cfg):
        self.active = getattr(cfg, 'active', True)

class MockPluginInfo:
    def __init__(self, **kwargs):
        self.meta = kwargs

class MockEngineResults:
    def __init__(self):
        self.types = ModuleType("types")
        self.types.Answer = lambda *args, **kwargs: kwargs.get('answer', args[0] if args else "")
        self._results = []
    
    def add(self, res):
        self._results.append(res)

searx_plugins.Plugin = MockPlugin
searx_plugins.PluginInfo = MockPluginInfo
searx_results.EngineResults = MockEngineResults

searx.settings = {'server': {'secret_key': 'demo-secret'}}
searx.network = ModuleType("searx.network")

sys.modules["searx"] = searx
sys.modules["searx.plugins"] = searx_plugins
sys.modules["searx.result_types"] = searx_results

# Network module mock
searx_network = ModuleType("searx.network")
def mock_network_call(method, url, **kwargs):
    import http.client, ssl, json
    from urllib.parse import urlparse
    
    parsed = urlparse(url)
    port = parsed.port or (443 if parsed.scheme=='https' else 80)
    target = f"{parsed.hostname}:{port}"
    
    if parsed.scheme == 'https':
        conn = http.client.HTTPSConnection(target, timeout=30, context=ssl.create_default_context())
    else:
        conn = http.client.HTTPConnection(target, timeout=30)
    
    headers = kwargs.get('headers', {})
    body = None
    if kwargs.get('json'):
        body = json.dumps(kwargs['json'])
    elif kwargs.get('data'):
        body = kwargs['data']

    path = parsed.path
    if parsed.query:
        path += f"?{parsed.query}"
    
    if kwargs.get('params'):
        from urllib.parse import urlencode
        query_str = urlencode(kwargs['params'])
        if '?' in path:
            path += f"&{query_str}"
        else:
            path += f"?{query_str}"

    conn.request(method, path, body=body, headers=headers)
    return conn.getresponse()

def mock_stream(method, url, **kwargs):
    res = mock_network_call(method, url, **kwargs)
    
    class MockResponse:
        def __init__(self, r):
            self.status_code = r.status
            self.text = "Mock Response" # Stub
            self._r = r
    
    def generator():
        while True:
            chunk = res.read(128)
            if not chunk: break
            yield chunk

    return MockResponse(res), generator()

def mock_get(url, **kwargs):
    import json
    res = mock_network_call('GET', url, **kwargs)
    
    class MockResponse:
        def __init__(self, r):
            self.status_code = r.status
            self._content = r.read()
            self.text = self._content.decode('utf-8')
        
        def json(self):
            return json.loads(self.text)
            
    return MockResponse(res)

searx_network.stream = mock_stream
searx_network.get = mock_get
sys.modules["searx.network"] = searx_network

from ai_answers import SXNGPlugin
from flask_babel import Babel

app = Flask(__name__)
babel = Babel(app)

class MockConfig:
    active = True

plugin = SXNGPlugin(MockConfig())
plugin.init(app)

@app.route("/config", methods=["POST"])
def update_config():
    url = request.form.get("url", "").strip()
    bearer = request.form.get("bearer", "").strip()
    model = request.form.get("model", "").strip()
    query = request.form.get("q", "")
    if url:
        plugin.endpoint_url = url
    plugin.api_key = bearer if bearer else "ollama"
    if model:
        plugin.model = model
    redirect_q = f"?q={query}" if query else ""
    return redirect(f"/{redirect_q}")


@app.route("/search")
def mock_search():
    query = request.args.get("q", "")
    format_type = request.args.get("format", "html")
    
    if format_type != "json":
        return "Demo only supports JSON format", 400
    
    results = [
        {"title": f"Result 1 for: {query}", "content": f"This is simulated content about {query}. It contains relevant information.", "url": f"https://example.com/1/{query.replace(' ', '-')}", "publishedDate": "2026-01-18"},
        {"title": f"Result 2 for: {query}", "content": f"Additional information regarding {query}. More context and details.", "url": f"https://example.com/2/{query.replace(' ', '-')}", "publishedDate": "2026-01-17"},
        {"title": f"Result 3 for: {query}", "content": f"Further reading on {query}. Expert analysis.", "url": f"https://example.com/3/{query.replace(' ', '-')}", "publishedDate": "2026-01-16"},
    ]
    
    return {
        "results": results,
        "infoboxes": [],
        "answers": [],
        "suggestions": [f"{query} explained", f"{query} tutorial"]
    }

@app.route("/")
def index():
    query = request.args.get("q", "why is the sky blue")
    
    class MockSearchQuery:
        pageno = 1
        lang = 'en'
        categories = ['general']
    MockSearchQuery.query = query
    
    class MockSearch:
        search_query = MockSearchQuery()
        class MockResultContainer:
            def __init__(self):
                self.answers = set()

            def get_ordered_results(self):
                base_results = [
                    {"title": "Wikipedia", "content": "The sky appears blue due to Rayleigh scattering of sunlight. When sunlight enters the atmosphere, it collides with gas molecules and scatters in all directions. Blue light scatters more than other colors because it travels in shorter waves.", "url": "https://en.wikipedia.org/wiki/Rayleigh_scattering", "publishedDate": "2026-01-15"},
                    {"title": "NASA Science", "content": "Shorter blue wavelengths scatter more than longer red wavelengths. This phenomenon, discovered by Lord Rayleigh in the 1870s, explains why we see a blue sky during the day.", "url": "https://science.nasa.gov/blue-sky", "publishedDate": "2026-01-10"},
                    {"title": "Physics Today", "content": "The atmosphere acts as a filter, scattering blue light in all directions while letting other colors pass through more directly.", "url": "https://physicstoday.org/atmosphere", "publishedDate": "2026-01-01"},
                    {"title": "Scientific American", "content": "At sunset, light travels through more atmosphere, scattering away the blue and leaving reds and oranges.", "url": "https://scientificamerican.com/sunset", "publishedDate": "2025-12-20"},
                    {"title": "National Geographic", "content": "Ocean color also results from light scattering and absorption by water molecules.", "url": "https://nationalgeographic.com/ocean-blue", "publishedDate": "2025-12-15"},
                ]
                broad_results = [
                    {"title": "MIT OpenCourseWare: Atmospheric Physics", "content": "Course materials.", "url": "https://ocw.mit.edu/physics"},
                    {"title": "NOAA: Understanding the Atmosphere", "content": "Educational resource.", "url": "https://noaa.gov/atmosphere"},
                    {"title": "BBC Science: Why is the sky blue?", "content": "Explainer article.", "url": "https://bbc.com/science/sky"},
                    {"title": "Khan Academy: Light and Color", "content": "Video lesson.", "url": "https://khanacademy.org/light"},
                    {"title": "HowStuffWorks: Rayleigh Scattering", "content": "Detailed explanation.", "url": "https://howstuffworks.com/rayleigh"},
                    {"title": "Physics Stack Exchange: Sky color discussion", "content": "Q&A thread.", "url": "https://physics.stackexchange.com/sky"},
                    {"title": "Quora: Atmospheric optics explained", "content": "Community answers.", "url": "https://quora.com/atmosphere"},
                ]
                if 'quantum' in query.lower():
                    return [
                        {"title": "IBM Quantum", "content": "Quantum computers rely on qubits, which can represent 0, 1, or both via superposition. They solve complex problems faster.", "url": "https://www.ibm.com/quantum", "publishedDate": "2026-01-15"},
                        {"title": "Nature Physics", "content": "Entanglement allows qubits to be correlated instantly across distances. This is key for quantum cryptography and teleportation.", "url": "https://nature.com/articles/quantum", "publishedDate": "2026-01-10"},
                        {"title": "Wikipedia", "content": "Quantum computing uses quantum mechanics. Major applications include drug discovery and materials science.", "url": "https://en.wikipedia.org/wiki/Quantum_computing", "publishedDate": "2025-12-01"}
                    ] + broad_results
                return base_results + broad_results
        result_container = MockResultContainer()

    search = MockSearch()
    plugin.post_search(None, search)
    
    injection_html = ""
    if search.result_container.answers:
        injection_html = list(search.result_container.answers)[0]
    
    bearer_display = plugin.api_key if plugin.api_key != "ollama" else ""
    return f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>AI Answers Demo</title>
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                padding: 2rem;
                max-width: 800px;
                margin: 0 auto;
                background: #2e3440;
                color: #eceff4;
            }}
            :root {{
                --color-result-border: #3b4252;
                --color-result-description: #d8dee9;
                --color-base-font: #88c0d0;
                --color-result-link: #81a1c1;
            }}
            .meta {{ color: #81a1c1; font-size: 0.9rem; }}
            hr {{ border-color: #4c566a; }}
            a {{ color: #88c0d0; }}
            .config-panel {{
                background: #3b4252;
                border-radius: 6px;
                padding: 1rem 1.25rem;
                margin-bottom: 1.25rem;
            }}
            .config-panel summary {{
                cursor: pointer;
                font-size: 0.85rem;
                color: #81a1c1;
                user-select: none;
            }}
            .config-panel summary:hover {{ color: #88c0d0; }}
            .config-row {{
                display: flex;
                flex-direction: column;
                gap: 0.5rem;
                margin-top: 0.75rem;
            }}
            .config-row label {{
                font-size: 0.8rem;
                color: #81a1c1;
            }}
            .config-row input {{
                background: #2e3440;
                border: 1px solid #4c566a;
                border-radius: 4px;
                color: #eceff4;
                font-size: 0.85rem;
                padding: 0.4rem 0.6rem;
                width: 100%;
                box-sizing: border-box;
            }}
            .config-row input:focus {{ outline: none; border-color: #81a1c1; }}
            .config-btn {{
                margin-top: 0.75rem;
                background: #4c566a;
                border: none;
                border-radius: 4px;
                color: #eceff4;
                cursor: pointer;
                font-size: 0.85rem;
                padding: 0.4rem 1rem;
            }}
            .config-btn:hover {{ background: #5e81ac; }}
            .search-row {{
                display: flex;
                gap: 0.5rem;
                margin-bottom: 1.25rem;
            }}
            .search-row input {{
                flex: 1;
                background: #3b4252;
                border: 1px solid #4c566a;
                border-radius: 4px;
                color: #eceff4;
                font-size: 0.95rem;
                padding: 0.45rem 0.75rem;
            }}
            .search-row input:focus {{ outline: none; border-color: #81a1c1; }}
            .search-row button {{
                background: #5e81ac;
                border: none;
                border-radius: 4px;
                color: #eceff4;
                cursor: pointer;
                font-size: 0.9rem;
                padding: 0.45rem 1rem;
            }}
            .search-row button:hover {{ background: #81a1c1; }}
        </style>
    </head>
    <body>
        <details class="config-panel" {'open' if not bearer_display and 'localhost' in plugin.endpoint_url else ''}>
            <summary>&#9881; Ollama Configuration</summary>
            <form method="POST" action="/config">
                <input type="hidden" name="q" value="{query}">
                <div class="config-row">
                    <label>Endpoint URL</label>
                    <input type="text" name="url" value="{plugin.endpoint_url}" placeholder="http://localhost:11434/v1/chat/completions">
                </div>
                <div class="config-row">
                    <label>Bearer Token <span style="opacity:0.5;">(optional)</span></label>
                    <input type="text" name="bearer" value="{bearer_display}" placeholder="Leave empty if not required">
                </div>
                <button type="submit" class="config-btn">Apply</button>
            </form>
        </details>

        <form class="search-row" method="GET" action="/">
            <input type="text" name="q" value="{query}" placeholder="Ask something...">
            <button type="submit">Search</button>
        </form>

        <p class="meta">Model: <strong>{plugin.model}</strong></p>
        <hr>
        {injection_html if injection_html else '<p style="color:#f66;">No response — check your Ollama endpoint and token above.</p>'}
    </body>
    </html>
    """

if __name__ == "__main__":
    print("AI Answers - Demo\n")
    print(f"  Endpoint: {plugin.endpoint_url}")
    print(f"  Model:    {plugin.model or 'N/A'}")
    print(f"  Mode:     {'interactive' if plugin.interactive else 'simple'}")
    print(f"\n  http://localhost:5000/?q=why+is+the+sky+blue\n")
    app.run(debug=False, port=5000)
