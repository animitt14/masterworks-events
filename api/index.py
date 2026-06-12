"""Vercel serverless function — live RSVP dashboard.

Renders the dashboard on demand with live HubSpot data instead of serving a
3×/day static snapshot. The HubSpot token lives only in this function's
environment (HUBSPOT_API_KEY), never in the page. Access is gated by HTTP Basic
Auth against DASHBOARD_PASSCODE.

Enrichment runs cache-only here (OFFLINE_ENRICH=1): no external API calls, no
HubSpot writes, no disk writes — the scheduled GitHub Action still does the full
enrichment and commits enrich_cache.json / confirmations.json, which this
function reads.
"""
import base64
import os
import sys
import time
import traceback
from http.server import BaseHTTPRequestHandler
from pathlib import Path

# Must be set BEFORE importing generate_rsvp — the flag is read at import time.
os.environ['OFFLINE_ENRICH'] = '1'

# generate_rsvp.py lives at the repo root, one level above this api/ directory.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import generate_rsvp  # noqa: E402

PASSCODE = os.environ.get('DASHBOARD_PASSCODE', '')

# Short in-process cache: warm instances reuse a render for TTL seconds so rapid
# reloads don't re-hit HubSpot. Cold starts render fresh.
_CACHE = {'html': None, 'ts': 0.0}
_TTL_SECONDS = 45


def _authorized(headers) -> bool:
    """True if the request carries Basic-Auth credentials whose password matches
    DASHBOARD_PASSCODE. If no passcode is configured, access is open."""
    if not PASSCODE:
        return True
    raw = headers.get('Authorization', '')
    if not raw.startswith('Basic '):
        return False
    try:
        decoded = base64.b64decode(raw[6:]).decode('utf-8', 'replace')
    except Exception:
        return False
    _user, _, password = decoded.partition(':')
    return password == PASSCODE


def _render_cached() -> str:
    now = time.time()
    if _CACHE['html'] is not None and (now - _CACHE['ts']) < _TTL_SECONDS:
        return _CACHE['html']
    html = generate_rsvp.render_live()
    _CACHE['html'] = html
    _CACHE['ts'] = now
    return html


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if not _authorized(self.headers):
            self.send_response(401)
            self.send_header('WWW-Authenticate', 'Basic realm="RSVP Dashboard"')
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(b'Authentication required.')
            return

        try:
            html = _render_cached()
        except Exception:
            self.send_response(500)
            self.send_header('Content-Type', 'text/plain; charset=utf-8')
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            self.wfile.write(b'Render error:\n' + traceback.format_exc().encode('utf-8'))
            return

        body = html.encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        # Shared CDN cache for ~45s; never cached when the 401 path is taken.
        self.send_header('Cache-Control', 's-maxage=45, stale-while-revalidate=30')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
