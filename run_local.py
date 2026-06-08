#!/usr/bin/env python3
"""
run_local.py — offline runner for generate_rsvp.py
Injects pre-fetched HubSpot contact data (from MCP) so the generator
can build the HTML without direct network access to api.hubapi.com.
"""

import json
import os
import sys

# Load .env if present (keeps secrets out of version control)
_env_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(_env_path):
    for _line in open(_env_path):
        _line = _line.strip()
        if _line and not _line.startswith('#') and '=' in _line:
            _k, _v = _line.split('=', 1)
            os.environ.setdefault(_k.strip(), _v.strip())

os.environ.setdefault('DAYS_BACK', '3')

ENRICHMENTS = {
    "224364188294": {"jobtitle": "Corporate Sales Representative", "company": "SweetGiftsbyStar"},
    "224349365265": {"jobtitle": "Attorney",                       "company": "Law Offices of Judith Vargas"},
    "224349822515": {"jobtitle": "Senior Vice President, Balance Sheet Construction", "company": "Merchants Capital"},
    "224345827471": {"jobtitle": "Managing Director",              "company": "Deutsche Bank"},
    "224365697445": {"jobtitle": "Photographer"},
    # Jun 5 2026 enrichments
    "226658753322": {"jobtitle": "EVP & Chief Risk Officer", "company": "The Clearing House"},
}

_contacts_path = os.path.join(os.path.dirname(__file__), 'jun5_contacts.json')
with open(_contacts_path) as _f:
    CONTACTS = json.load(_f)

# Apply enrichments (only fill blank fields)
for c in CONTACTS:
    enrichment = ENRICHMENTS.get(c["id"])
    if enrichment:
        for k, v in enrichment.items():
            if not c["properties"].get(k):
                c["properties"][k] = v
        print(f"[ENRICH] {c['properties'].get('firstname')} {c['properties'].get('lastname','')}: {enrichment}")

MOCK_RESPONSE_MAIN = {"results": CONTACTS, "paging": {}}

# ── Monkeypatch requests ─────────────────────────────────────────────────────
import requests as _req

_orig_post  = _req.post
_orig_patch = _req.patch
_orig_get   = _req.get

SEARCH_URL = 'https://api.hubapi.com/crm/v3/objects/contacts/search'

class _R:
    def __init__(self, data=None, sc=200):
        self._data = data or {}
        self.status_code = sc
        self.ok = sc < 400
    def json(self):    return self._data
    def raise_for_status(self):
        if not self.ok: raise Exception(f'HTTP {self.status_code}')

def _mock_post(url, *args, **kwargs):
    if SEARCH_URL in (url or ''):
        payload = kwargs.get('json', {})
        for fg in payload.get('filterGroups', []):
            for f in fg.get('filters', []):
                if f.get('propertyName') == 'outbound_rsvp_to_event':
                    return _R(MOCK_RESPONSE_MAIN)
        return _R({"results": []})  # host-lookup and other searches
    # Block batch email reads etc. — return empty
    return _R({"results": []})

def _mock_patch(url, *args, **kwargs):
    body = kwargs.get('json', {})
    print(f'[SKIP-PATCH] {url.split("/")[-1]} → {body}')
    return _R({"id": url.split("/")[-1]})

def _mock_get(url, *args, **kwargs):
    # Allow public NYC PLUTO endpoint (cityofnewyork.us) — may work
    if 'cityofnewyork.us' in (url or ''):
        try: return _orig_get(url, *args, **kwargs)
        except: pass
    # Allow Census API
    if 'census.gov' in (url or ''):
        try: return _orig_get(url, *args, **kwargs)
        except: pass
    # Block everything else (LinkedIn, GitHub Gist, RocketReach, etc.)
    return _R({}, 403)

_req.post  = _mock_post
_req.patch = _mock_patch
_req.get   = _mock_get

# ── Run the generator ────────────────────────────────────────────────────────
import generate_rsvp
generate_rsvp.main()
