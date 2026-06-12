"""Vercel serverless function — dashboard write-back.

Replaces the old browser → Gist → daily-build → HubSpot relay. The dashboard
posts uninvite / send-confirmation toggles here and this function PATCHes
HubSpot immediately, using the server-side token. Gated by the same Basic-Auth
passcode as the page.

POST body (JSON):
  toggles:    {"contact_id": "123", "action": "uninvite"|"sendconf", "value": true|false}
  set score:  {"contact_id": "123", "action": "wealth_rating", "value": 1..5}
"""
import base64
import json
import os
from http.server import BaseHTTPRequestHandler

import requests

PASSCODE = os.environ.get('DASHBOARD_PASSCODE', '')
HUBSPOT_TOKEN = os.environ.get('HUBSPOT_API_KEY', '')

# Boolean toggles → (property, value when ON, value when OFF)
_TOGGLE_MAP = {
    'uninvite': ('outbound_event_attendee_disqualified', 'Disqualified', ''),
    'sendconf': ('outbound_event_send_confirmation', 'Yes', ''),
    'attended': ('attended_outbound_event', 'yes', ''),
}
# Numeric "set" actions → property. value is the score (1–5) to write, or '' to clear.
_SET_MAP = {
    'wealth_rating': 'outbound_wealth_rating',
}


def _resolve(action: str, value):
    """Map (action, value) to (property_name, new_value). Returns (None, None)
    if the action is unknown or the value is invalid."""
    if action in _TOGGLE_MAP:
        prop, on_value, off_value = _TOGGLE_MAP[action]
        return prop, (on_value if bool(value) else off_value)
    if action in _SET_MAP:
        if value is None or value == '':
            return _SET_MAP[action], ''   # clear
        try:
            n = int(value)
        except (ValueError, TypeError):
            return None, None
        if not (1 <= n <= 5):
            return None, None
        return _SET_MAP[action], n
    return None, None


def _authorized(headers) -> bool:
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


class handler(BaseHTTPRequestHandler):
    def _json(self, status: int, payload: dict):
        body = json.dumps(payload).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if not _authorized(self.headers):
            self.send_response(401)
            self.send_header('WWW-Authenticate', 'Basic realm="RSVP Dashboard"')
            self.send_header('Cache-Control', 'no-store')
            self.end_headers()
            return

        try:
            length = int(self.headers.get('Content-Length') or 0)
            data = json.loads(self.rfile.read(length) or b'{}')
        except Exception:
            return self._json(400, {'ok': False, 'error': 'invalid JSON body'})

        contact_id = str(data.get('contact_id') or '').strip()
        action = str(data.get('action') or '').strip()

        prop, new_value = _resolve(action, data.get('value'))
        if not contact_id or prop is None:
            return self._json(400, {'ok': False, 'error': 'missing/invalid contact_id, action, or value'})
        if not HUBSPOT_TOKEN:
            return self._json(500, {'ok': False, 'error': 'server missing HUBSPOT_API_KEY'})

        try:
            resp = requests.patch(
                f'https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}',
                headers={'Authorization': f'Bearer {HUBSPOT_TOKEN}',
                         'Content-Type': 'application/json'},
                json={'properties': {prop: new_value}},
                timeout=10,
            )
        except Exception as e:
            return self._json(502, {'ok': False, 'error': f'HubSpot request failed: {e}'})

        if not resp.ok:
            return self._json(502, {'ok': False, 'error': f'HubSpot {resp.status_code}',
                                    'detail': resp.text[:500]})

        return self._json(200, {'ok': True, 'contact_id': contact_id,
                                'property': prop, 'value': new_value})
