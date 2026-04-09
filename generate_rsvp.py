#!/usr/bin/env python3
"""
generate_rsvp.py
Masterworks Outbound Events — RSVP Dashboard Generator
Queries HubSpot for contacts RSVPed to events within DAYS_BACK..DAYS_AHEAD,
scores them 1–5, and writes docs/index.html.
"""

import json
import os
import re
import sys
import requests
from datetime import date, timedelta, datetime, timezone
from html import escape
from urllib.parse import quote_plus
from pathlib import Path
from collections import defaultdict

# ─── CONFIG ───────────────────────────────────────────────────────────────────

HUBSPOT_TOKEN = os.environ.get('HUBSPOT_API_KEY', '')
PORTAL_ID       = '5454671'
GITHUB_REPO     = os.environ.get('GITHUB_REPO',     '')  # e.g. 'animitt14/masterworks-events'
GITHUB_WORKFLOW = 'daily.yml'
SHARED_GIST_ID  = '44d6dd7bc96a5cbe2454b65ee55f8cdb'
SEARCH_URL    = 'https://api.hubapi.com/crm/v3/objects/contacts/search'

# Google Custom Search — used to enrich contacts with no title AND no company
GOOGLE_API_KEY     = os.environ.get('GOOGLE_API_KEY', '')
GOOGLE_CSE_ID      = os.environ.get('GOOGLE_CSE_ID', '')
GOOGLE_SEARCH_URL  = 'https://www.googleapis.com/customsearch/v1'
ENRICH_LIMIT       = 95   # stay just under the 100/day free quota

PERSONAL_DOMAINS = {
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'icloud.com',
    'aol.com', 'protonmail.com', 'me.com', 'mac.com', 'live.com',
    'msn.com', 'ymail.com', 'googlemail.com', 'comcast.net', 'verizon.net',
    'att.net', 'sbcglobal.net',
}

# Date window: how many days back/ahead to pull
DAYS_BACK  = int(os.environ.get('DAYS_BACK',  180))
DAYS_AHEAD = int(os.environ.get('DAYS_AHEAD', 30))

OWNERS = {
    '1433036370': 'Blake Martin',
    '77771452':   'Anisha Mittal',
    '73613833':   'Erik Bringsjord',
    '202057506':  'Linna Henry',
    '35397207':   'Michael DelPozzo',
    '62900591':   'Kevin Cox',
}
INACTIVE_OWNERS = {
    '601301427', '165450538', '96133170', '587857010',
    '52728646',  '74718843',  '51117431', '1329531581',
}

FINANCE_DOMAINS = {
    'jpmorgan.com', 'gs.com', 'goldmansachs.com', 'ubs.com', 'nb.com',
    'pimco.com', 'kkr.com', 'virtu.com', 'blackrock.com', 'morganstanley.com',
    'citadel.com', 'tdsecurities.com', 'ml.com', 'alliancebernstein.com',
    'fticonsulting.com', 'stblaw.com', 'sullcrom.com', 'troutman.com',
    'beckerglynn.com', 'dorflaw.com', 'willkie.com',
}

HOSPITAL_DOMAINS = {
    'northwell.edu', 'nyulangone.org', 'mountsinai.org',
}

HIGH_TITLE_TERMS = [
    'managing director', 'general partner', 'founding partner', 'managing partner', 'senior partner',
    'fund manager', 'portfolio manager',
    'chief executive', 'chief financial', 'chief operating', 'chief technology',
    'chief investment', 'chief information',
    'ceo', 'cfo', 'cto', 'coo', 'cio',
    'founder', 'co-founder',
    'head of',
]
# 'partner', 'president', 'principal' handled separately in has_high_title
# to avoid false positives (VP → president, "Account Partner" → partner, "Principal Engineer" → principal)
_PARTNER_EXCLUSIONS = {
    'account', 'channel', 'strategic', 'implementation', 'solutions',
    'solution', 'business', 'technology', 'alliance', 'referral', 'reseller',
}

# Company name signals for small local / lifestyle businesses.
# Founders/CEOs of these should be Medium (3), not High (5).
SMALL_BIZ_INDICATORS = [
    # Food & beverage
    'restaurant', 'cafe', 'coffee shop', 'coffee house', 'juice', 'smoothie',
    'bakery', 'pizza', 'deli', 'bagel', 'sandwich', 'burger', 'sushi', 'ramen',
    'steakhouse', 'bistro', 'tavern', 'eatery', 'food truck', 'catering',
    'ice cream', 'dessert', 'pastry', 'wine bar', 'cocktail bar', 'speakeasy',
    # Personal services
    'salon', 'barbershop', 'barber shop', 'nail salon', 'nail studio',
    'hair salon', 'beauty salon', 'day spa', 'lash studio', 'brow bar',
    'massage', 'med spa', 'medspa',
    # Retail / trades
    'boutique', 'flower shop', 'florist', 'dry clean', 'laundromat',
    'car wash', 'auto repair', 'auto body', 'pet grooming', 'dog grooming',
    # Fitness / wellness
    'gym', 'fitness studio', 'fitness center', 'yoga studio', 'pilates', 'crossfit',
]

def is_small_biz(company: str) -> bool:
    co = company.lower()
    return any(term in co for term in SMALL_BIZ_INDICATORS)

# These override HIGH signals — wealth advisors refer clients but don't invest personally
WEALTH_ADVISOR_TERMS = [
    'wealth advisor', 'wealth management advisor', 'wealth management',
    'private banker', 'private client', 'private wealth',
    'financial advisor', 'financial planner',
    'investment advisor', 'personal banking advisor',
]

# These are auto-Low or Low-Medium
DOWNGRADE_TERMS = [
    'real estate agent', 'realtor', 're agent', 're broker',
    'art broker', 'art dealer', 'fine art broker',
    'nft', 'crypto', 'web3',
    'intern', 'assistant', 'paralegal',
    'associate professor', 'clergy', 'pastor',
    # Fitness / personal training — low liquid wealth
    'personal trainer', 'fitness trainer', 'fitness instructor', 'fitness coach',
    'yoga instructor', 'pilates instructor', 'gym owner', 'fitness management',
]

FINANCE_COMPANIES = [
    'goldman sachs', 'morgan stanley', 'jp morgan', 'jpmorgan',
    'blackrock', 'blackstone', 'kkr', 'carlyle group', 'apollo global',
    'citadel', 'bridgewater', 'two sigma', 'renaissance technologies',
    'pimco', 'vanguard', 'fidelity investments', 'merrill lynch',
    'ubs', 'credit suisse', 'deutsche bank', 'barclays',
    'wells fargo', 'citigroup', 'citibank', 'bank of america',
    'neuberger berman', 'alliancebernstein', 'td securities',
    'lazard', 'evercore', 'jefferies', 'raymond james',
    'point72', 'millennium management', 'tiger global', 'coatue',
    'andreessen horowitz', 'sequoia capital', 'general atlantic',
    'warburg pincus', 'bain capital', 'cerberus capital', 'oaktree capital',
    'silver lake', 'skadden', 'kirkland & ellis', 'weil gotshal',
    'latham & watkins', 'sullivan & cromwell', 'debevoise & plimpton',
    'simpson thacher', 'cleary gottlieb', 'paul weiss', 'cravath',
    'white & case', 'proskauer', 'sidley austin', 'davis polk',
]

TRI_STATE = {'ny', 'nj', 'ct'}

# 5 = highest, 1 = lowest
SCORES = [5, 4, 3, 2, 1]
SCORE_LABELS = {5: 'High', 4: 'Medium-High', 3: 'Medium', 2: 'Low-Medium', 1: 'Low'}
SCORE_COLORS = {
    5: ('#1a7a45', '#eaf7f0'),   # green
    4: ('#1a5fa8', '#e8f0fb'),   # blue
    3: ('#8a6800', '#fdf6e3'),   # amber
    2: ('#b85a00', '#fdf0e8'),   # orange
    1: ('#a83030', '#fde8e8'),   # red
}

PERSONA_LIST = [
    'Finance Bro', 'Tech Wealth Builder', 'Business Owner', 'Corporate Climber',
    'Medical Pro', 'Young Diversifier', 'Everyday Investor', 'Cautious Retiree', 'Unknown',
]

# ─── HUBSPOT ──────────────────────────────────────────────────────────────────

def epoch_ms(d: date, end_of_day: bool = False) -> int:
    h, m, s = (23, 59, 59) if end_of_day else (0, 0, 0)
    return int(datetime(d.year, d.month, d.day, h, m, s, tzinfo=timezone.utc).timestamp() * 1000)

# ─── GOOGLE ENRICHMENT ───────────────────────────────────────────────────────

ENRICH_CACHE_FILE = Path('docs/enrich_cache.json')
_enrich_cache: dict = {}      # loaded from file at start of main()
_quota_exhausted: bool = False  # flip to True on first 429 — stops all further queries

def _parse_linkedin_result(title_tag: str, snippet: str) -> tuple:
    """Return (job_title, company) from a Google/LinkedIn search result.

    LinkedIn title tags look like:
      "Jane Smith - VP at Morgan Stanley | LinkedIn"
      "Jane Smith - Managing Director - Goldman Sachs | LinkedIn"
    Snippets look like:
      "VP at Morgan Stanley · New York · 500+ connections"
    """
    # Strip trailing "| LinkedIn" or "- LinkedIn"
    t = re.sub(r'\s*[|\-–]\s*LinkedIn\s*$', '', title_tag, flags=re.IGNORECASE).strip()
    # Strip leading "Name - " (everything up to first " - ")
    t = re.sub(r'^[^–\-]+ [-–] ', '', t, count=1).strip()

    # Pattern: "Title at Company"
    m = re.match(r'^(.+?)\s+at\s+(.+?)(?:\s*[·|\-–]|$)', t, re.IGNORECASE)
    if m:
        return m.group(1).strip(), m.group(2).strip()

    # Pattern: "Title - Company" or "Title · Company"
    m = re.match(r'^(.+?)\s*[–\-·]\s*(.+?)(?:\s*[–\-·]|$)', t)
    if m:
        return m.group(1).strip(), m.group(2).strip()

    # Fall back to snippet: "Title at Company · ..."
    m = re.match(r'^(.+?)\s+at\s+(.+?)(?:\s*·|\s*[-–]|\s*$)', snippet.strip(), re.IGNORECASE)
    if m:
        return m.group(1).strip(), m.group(2).strip()

    return t.strip(), ''


def _load_enrich_cache():
    global _enrich_cache
    if ENRICH_CACHE_FILE.exists():
        try:
            _enrich_cache = json.loads(ENRICH_CACHE_FILE.read_text(encoding='utf-8'))
            print(f'Loaded enrich cache: {len(_enrich_cache)} entries')
        except Exception:
            _enrich_cache = {}

def _save_enrich_cache():
    ENRICH_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    ENRICH_CACHE_FILE.write_text(json.dumps(_enrich_cache, indent=2), encoding='utf-8')


def google_enrich(name: str) -> dict:
    """Search LinkedIn for a person by name. Returns enriched property dict or {}.
    Returns None if quota is exhausted (caller should stop querying)."""
    global _quota_exhausted
    if _quota_exhausted or not GOOGLE_API_KEY or not GOOGLE_CSE_ID or not name.strip():
        return {}
    if name in _enrich_cache:
        return _enrich_cache[name]

    try:
        resp = requests.get(
            GOOGLE_SEARCH_URL,
            params={'key': GOOGLE_API_KEY, 'cx': GOOGLE_CSE_ID,
                    'q': f'"{name}" "New York" site:linkedin.com/in', 'num': 3},
            timeout=10,
        )
        if resp.status_code == 429:
            print('  Google quota exhausted for today — stopping enrichment', file=sys.stderr)
            _quota_exhausted = True
            return {}
        if not resp.ok:
            print(f'  Google search error {resp.status_code} for "{name}"', file=sys.stderr)
            _enrich_cache[name] = {}
            return {}

        items = resp.json().get('items', [])
        for item in items:
            inferred_title, inferred_company = _parse_linkedin_result(
                item.get('title', ''), item.get('snippet', '')
            )
            if inferred_title or inferred_company:
                result = {
                    'jobtitle':  inferred_title,
                    'company':   inferred_company,
                    '_enriched': True,
                }
                _enrich_cache[name] = result
                return result

        _enrich_cache[name] = {}
        return {}

    except Exception as e:
        print(f'  Google search exception for "{name}": {e}', file=sys.stderr)
        _enrich_cache[name] = {}
        return {}


def enrich_no_data_contacts(contacts: list) -> int:
    """For contacts with no title AND no company, try Google enrichment.
    Returns number of contacts enriched."""
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        return 0

    enriched = 0
    for c in contacts:
        if _quota_exhausted:
            break
        p    = c['properties']
        if p.get('jobtitle') or p.get('company'):
            continue   # already has data — skip

        fname = p.get('firstname') or ''
        lname = p.get('lastname')  or ''
        name  = f'{fname} {lname}'.strip()
        if not name:
            continue

        # Apply from cache if already looked up (hit or miss from a previous run)
        if name in _enrich_cache:
            cached = _enrich_cache[name]
            if cached:
                c['properties'] = {**p, **cached}
                enriched += 1
            continue   # either way, don't make an API call

        # New name — use a query against our daily quota
        if enriched >= ENRICH_LIMIT:
            break
        result = google_enrich(name)
        if result:
            c['properties'] = {**p, **result}
            enriched += 1
            print(f'  Enriched: {name} → {result.get("jobtitle", "")} @ {result.get("company", "")}')

    _save_enrich_cache()
    return enriched


def fetch_contacts(start: date, end: date) -> list:
    if not HUBSPOT_TOKEN:
        print('ERROR: HUBSPOT_API_KEY not set', file=sys.stderr)
        sys.exit(1)

    headers = {'Authorization': f'Bearer {HUBSPOT_TOKEN}', 'Content-Type': 'application/json'}
    contacts, after = [], None

    while True:
        payload = {
            'filterGroups': [{
                'filters': [{
                    'propertyName': 'outbound_rsvp_to_event',
                    'operator':     'BETWEEN',
                    'value':        str(epoch_ms(start)),
                    'highValue':    str(epoch_ms(end, end_of_day=True)),
                }]
            }],
            'properties': [
                'firstname', 'lastname', 'jobtitle', 'company',
                'email', 'phone', 'city', 'state', 'zip',
                'hubspot_owner_id', 'lifecyclestage', 'call_completed',
                'outbound_rsvp_to_event', 'attended_outbound_event',
                'admin_url', 'totalamountpurchased',
                'hs_v2_date_entered_current_stage',
            ],
            'limit': 200,
            'sorts': [{'propertyName': 'outbound_rsvp_to_event', 'direction': 'ASCENDING'}],
        }
        if after:
            payload['after'] = after

        resp = requests.post(SEARCH_URL, headers=headers, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        contacts.extend(data.get('results', []))
        after = data.get('paging', {}).get('next', {}).get('after')
        if not after:
            break

    return contacts

# ─── SCORING ──────────────────────────────────────────────────────────────────

def email_domain(email: str) -> str:
    return email.split('@')[-1].lower() if email and '@' in email else ''

def has_high_title(title: str) -> bool:
    tl = title.lower().strip()
    t  = ' ' + tl + ' '
    for term in HIGH_TITLE_TERMS:
        if (' ' + term + ' ') in t:
            return True
    # 'president' — only match when NOT preceded by 'vice'
    if ' president ' in t and 'vice president' not in tl:
        return True
    # 'partner' — only senior/equity partner contexts, not "Account/Channel Partner"
    if tl == 'partner' or tl.endswith(' partner'):
        prefix = tl[: -len(' partner')].strip() if ' partner' in tl else ''
        if prefix not in _PARTNER_EXCLUSIONS:
            return True
    # 'principal' — high only outside of clearly technical/analytical roles
    if tl == 'principal' or tl.startswith('principal ') or ' principal ' in t:
        tech_ctx = ['engineer', 'software', 'developer', 'analyst', 'scientist', 'researcher', 'architect']
        if not any(tc in tl for tc in tech_ctx):
            return True
    return False

def is_physician(title: str, email: str, company: str) -> bool:
    phys_terms = [
        'physician', 'surgeon', 'doctor', 'cardiologist', 'radiologist',
        'psychiatrist', 'dermatologist', 'neurologist', 'anesthesiologist',
        'ophthalmologist', 'dentist', 'medical director',
    ]
    if any(t in title for t in phys_terms):
        return True
    if email_domain(email) in HOSPITAL_DOMAINS:
        return True
    if any(t in company for t in ['hospital', 'medical center', 'health system', 'northwell', 'nyu langone', 'mount sinai']):
        return True
    return False

def score_contact(p: dict) -> tuple:
    """Returns (score: int 1-5, flags: list)."""
    title   = (p.get('jobtitle')       or '').lower()
    company = (p.get('company')        or '').lower()
    email   = (p.get('email')          or '').lower()
    lc      = (p.get('lifecyclestage') or '').lower()
    call    = (p.get('call_completed') or '').lower()
    state   = (p.get('state')          or '').lower().strip()

    flags = []
    if lc == 'customer' or call == 'order completed':
        flags.append('invested')
    if lc == 'opportunity':
        flags.append('opportunity')
    if call == 'no show':
        flags.append('no_show')
    if call == 'not interested':
        flags.append('not_interested')
    if state and state not in TRI_STATE:
        flags.append('non_tri_state')

    combined = title + ' ' + company
    no_data  = not title.strip() and not company.strip()

    # ── Already invested or warm pipeline contact ──────────────────────────────
    # Ignore if the lifecycle stage was set on the day of the event — that's
    # same-day HubSpot activity (e.g. someone logging an email confirmation)
    # not a genuine prior pipeline signal.
    rsvp_date  = (p.get('outbound_rsvp_to_event')              or '')[:10]
    stage_date = (p.get('hs_v2_date_entered_current_stage')    or '')[:10]
    same_day_stage = rsvp_date and stage_date and stage_date >= rsvp_date
    if same_day_stage and 'opportunity' in flags:
        flags.remove('opportunity')

    if 'invested' in flags or 'opportunity' in flags:
        return 5, flags

    # ── Hard disqualifiers: not interested or no show ─────────────────────────
    if 'not_interested' in flags:
        return 1, flags
    if 'no_show' in flags and any(t in combined for t in DOWNGRADE_TERMS):
        return 1, flags

    # ── Wealth advisors / financial advisors (refer clients, don't invest) ────
    is_wealth_adv = any(t in combined for t in WEALTH_ADVISOR_TERMS)
    if is_wealth_adv:
        return 1, flags

    # ── Real estate agents / brokers ──────────────────────────────────────────
    is_re_agent = any(t in combined for t in ['real estate agent', 'realtor', 're agent', 're broker'])
    if is_re_agent:
        return 1 if 'no_show' in flags else 2, flags

    # ── Art world — all art advisors, dealers, brokers, gallery staff → Low ────
    # These people think of art as a whole purchase, not fractional investment.
    # No exceptions for gallery owners/founders — they're all Low.
    is_art_world = any(t in combined for t in [
        'art dealer', 'fine art broker', 'art broker',
        'art advisor', 'art adviser', 'art consultant',
        'gallery',
    ])
    if is_art_world:
        return 1 if 'no_show' in flags else 2, flags

    # ── Music / entertainment industry workers → Low-Medium ───────────────────
    # Similar profile to art world — cultural workers without high liquid wealth.
    # Only fires on title, not company, to avoid catching execs at entertainment firms.
    is_music_ent = any(t in title for t in [
        'music producer', 'music programming', 'music curation', 'music curator',
        'music supervisor', 'film-maker', 'filmmaker', 'screenwriter',
        'cinematographer', 'fine art broker',
    ])
    if is_music_ent:
        return 1 if 'no_show' in flags else 2, flags

    # ── Other downgrade terms ─────────────────────────────────────────────────
    has_downgrade = any(t in combined for t in DOWNGRADE_TERMS)
    if 'broker' in combined and not any(fc in company for fc in FINANCE_COMPANIES):
        has_downgrade = True
    if has_downgrade:
        return 1 if 'no_show' in flags else 2, flags

    if 'no_show' in flags:
        return 2, flags

    if no_data:
        return 2, flags

    # ── HIGH signals ──────────────────────────────────────────────────────────
    if email_domain(email) in FINANCE_DOMAINS:
        sc = 5
    elif is_physician(title, email, company):
        sc = 5
    elif has_high_title(title):
        sc = 5
    elif any(fc in company for fc in FINANCE_COMPANIES):
        sc = 5
    else:
        # RE developers/executives (SVP at Extell etc.) → Medium-High
        re_exec_co    = any(t in company for t in ['real estate', 'realty', 'extell', 'related companies', 'tishman', 'sl green', 'brookfield'])
        re_exec_title = any(t in title   for t in ['vp', 'svp', 'evp', 'director', 'executive', 'president', 'ceo', 'coo', 'chief'])
        # ── MEDIUM-HIGH signals ───────────────────────────────────────────────
        medium_high = any(t in title for t in [
            'vice president', 'vp', 'director', 'senior director', 'svp', 'evp', 'avp',
            'senior manager', 'senior vice', 'associate director',
        ])
        if re_exec_co and re_exec_title:
            sc = 4
        elif medium_high:
            sc = 4
        else:
            sc = 3

    # ── Small biz cap — founders/CEOs of juice shops, salons, etc. → Medium ───
    if sc > 3 and is_small_biz(company) and 'invested' not in flags and 'opportunity' not in flags:
        sc = 3

    # ── Founder/co-founder with no company → can't verify scale → Medium-High ─
    if sc == 5 and not company.strip():
        tl = title.lower()
        if any(t in tl for t in ['founder', 'co-founder']) \
           and email_domain(email) not in FINANCE_DOMAINS \
           and not is_physician(title, email, company):
            sc = 4

    # ── NW cap — applied to all except finance-domain and physician hits ───────
    # Finance domain (@gs.com etc.) and physicians are reliable HIGH signals
    # regardless of estimated NW. Everything else is capped by wealth tier.
    _fin_domain = email_domain(email) in FINANCE_DOMAINS
    _physician  = is_physician(title, email, company)
    if 'invested' not in flags and 'opportunity' not in flags and not _fin_domain and not _physician:
        nw, _ = get_nw(p)
        if nw == '$50K–$200K':
            sc = min(sc, 2)
        elif nw == '$150K–$500K':
            sc = min(sc, 3)
        elif nw == '$500K–$2M':
            sc = min(sc, 4)   # straddles $1M — cap at Medium-High, not High

    return sc, flags

def get_persona(p: dict) -> str:
    title   = (p.get('jobtitle') or '').lower()
    company = (p.get('company')  or '').lower()
    email   = (p.get('email')    or '').lower()

    if not title.strip() and not company.strip():
        return 'Unknown'

    # Cautious Retiree — check first (explicit retirement signal overrides everything)
    if any(t in title for t in ['retired', 'retiree', 'retirement']):
        return 'Cautious Retiree'

    # Medical Pro — physicians, surgeons, dentists, anesthesiologists
    if is_physician(title, email_domain(email), company):
        return 'Medical Pro'

    # Finance Bro — Partners, MDs, PE/VC/hedge, attorneys, CPAs, consultants at finance firms
    _is_finance_co  = any(fc in company for fc in FINANCE_COMPANIES)
    _is_finance_dom = email_domain(email) in FINANCE_DOMAINS
    _finance_titles = any(t in title for t in [
        'managing director', 'general partner', 'founding partner', 'managing partner',
        'fund manager', 'portfolio manager', 'hedge fund',
        'investment banker', 'private equity', 'venture capital',
        'attorney', 'lawyer', 'counsel', 'solicitor',
        'cpa', 'accountant', 'certified public',
        'management consultant',
    ])
    if _is_finance_co or _is_finance_dom or _finance_titles:
        return 'Finance Bro'

    # Tech Wealth Builder — engineers, data scientists, devops at tech/fintech/defense
    _tech_titles = any(t in title for t in [
        'software engineer', 'software developer', 'data scientist', 'data engineer',
        'machine learning', 'ml engineer', 'ai engineer', 'devops', 'site reliability',
        'platform engineer', 'backend engineer', 'frontend engineer', 'full stack',
        'engineering manager', 'staff engineer', 'principal engineer', 'tech lead',
        'solutions architect', 'cloud architect', 'security engineer',
    ])
    _tech_cos = any(tc in company for tc in [
        'google', 'meta', 'apple', 'amazon', 'microsoft', 'netflix',
        'uber', 'airbnb', 'stripe', 'palantir', 'salesforce', 'oracle',
        'openai', 'anthropic', 'databricks', 'snowflake', 'figma', 'notion',
        'twilio', 'datadog', 'cloudflare', 'hashicorp', 'confluent',
    ])
    if _tech_titles or (_tech_cos and any(t in title for t in [
        'engineer', 'developer', 'scientist', 'architect', 'technical',
    ])):
        return 'Tech Wealth Builder'

    # Everyday Investor — blue-collar trades, service workers, technicians
    if any(t in title for t in [
        'electrician', 'hvac', 'plumber', 'carpenter', 'welder', 'machinist',
        'mechanic', 'technician', 'maintenance', 'installer', 'laborer',
        'truck driver', 'driver', 'operator', 'foreman', 'tradesman',
        'elevator', 'construction worker',
    ]):
        return 'Everyday Investor'

    # Business Owner — founders, CEOs, owners across varied industries
    if any(t in title for t in [
        'founder', 'co-founder', 'ceo', 'chief executive',
        'owner', 'proprietor', 'president',
    ]):
        return 'Business Owner'

    # Young Diversifier — early career, no seniority signals, product/account roles
    _senior_signals = any(t in title for t in [
        'senior', 'sr.', 'sr ', 'lead', 'head of', 'director', 'vp', 'vice president',
        'managing', 'principal', 'partner', 'chief', 'manager',
    ])
    _young_titles = any(t in title for t in [
        'analyst', 'associate', 'product manager', 'account executive',
        'account manager', 'coordinator', 'specialist', 'representative',
        'consultant', 'advisor', 'intern', 'assistant',
    ])
    if _young_titles and not _senior_signals:
        return 'Young Diversifier'

    # Corporate Climber — VPs, Directors, SVPs, Managers at large non-finance cos
    if any(t in title for t in [
        'vp', 'vice president', 'director', 'svp', 'evp', 'avp',
        'chief', 'cfo', 'coo', 'cio', 'cmo', 'cro', 'cto',
        'senior manager', 'senior director', 'senior vice',
        'executive director', 'managing director',
    ]):
        return 'Corporate Climber'

    # Everyday Investor fallback — general service roles
    if any(t in title for t in [
        'teacher', 'nurse', 'retail', 'customer service', 'social worker',
        'therapist', 'administrative', 'clerk', 'cashier',
    ]):
        return 'Everyday Investor'

    return 'Corporate Climber'


def get_nw(p: dict) -> tuple:
    title   = (p.get('jobtitle') or '').lower()
    company = (p.get('company')  or '').lower()

    if not title.strip() and not company.strip():
        return '—', 'No title or company data'

    # Physicians — before title tiers (title-only matching understimates their NW)
    if is_physician(title, email_domain(p.get('email', '')), company):
        return '$1M–$4M', 'Physician / medical professional'

    # Tier 1: PE/hedge fund partner/principal, elite law firm partner, bank MD → $3M–$10M
    elite_finance_title = any(t in title for t in [
        'managing director', 'general partner', 'founding partner',
        'fund manager', 'portfolio manager', 'principal',
    ])
    in_top_finance = any(fc in company for fc in FINANCE_COMPANIES)
    if elite_finance_title and in_top_finance:
        return '$3M–$10M', 'Senior role at top-tier finance firm'
    if 'partner' in title and any(lf in company for lf in [
        'simpson thacher', 'sullivan & cromwell', 'willkie', 'troutman', 'dorf',
        'skadden', 'kirkland', 'weil gotshal', 'latham', 'davis polk',
        'cleary', 'paul weiss', 'cravath', 'debevoise', 'proskauer',
    ]):
        return '$3M–$10M', 'Partner at elite law firm'

    # Tier 2: VP/Director at major finance, startup CEO, asset manager → $2M–$6M
    if any(t in title for t in ['vp', 'vice president', 'director', 'svp', 'evp', 'cfo', 'cto', 'coo', 'cio', 'ceo', 'chief']) \
            and in_top_finance:
        return '$2M–$6M', 'C-suite / VP at major finance firm'
    if any(t in title for t in ['ceo', 'chief executive', 'founder', 'co-founder']) and company:
        if is_small_biz(company):
            return '$500K–$2M', 'Small business CEO / founder'
        return '$2M–$6M', 'CEO / founder'

    # Attorney / CPA (non-finance firm) → $500K–$2M (associate) or $1M–$4M (partner)
    if any(t in title for t in ['attorney', 'lawyer', 'counsel', 'solicitor']):
        if any(t in title for t in ['partner', 'senior', 'managing']):
            return '$1M–$4M', 'Senior attorney'
        return '$500K–$2M', 'Attorney'
    if any(t in title for t in ['cpa', 'accountant', 'certified public']):
        if any(t in title for t in ['partner', 'senior', 'managing', 'principal']):
            return '$1M–$4M', 'Senior CPA / accounting partner'
        return '$500K–$2M', 'CPA / accountant'

    # Tier 3: C-suite mid-size, law firm associate, senior consultant → $1M–$4M
    if any(t in title for t in ['managing director', 'president', 'managing partner', 'cfo', 'cto', 'coo', 'cio', 'cmo', 'cro', 'cso', 'cdo', 'cpо']):
        return '$1M–$4M', 'C-suite / MD-level'
    if any(t in title for t in ['vp', 'vice president', 'director', 'svp', 'evp']):
        return '$1M–$4M', 'VP / Director-level'
    if any(t in title for t in ['owner', 'founder']) and company:
        if is_small_biz(company):
            return '$500K–$2M', 'Small business owner'
        return '$1M–$4M', 'Business owner'

    # Tier 4: Senior Manager, engineers at tech cos, consultants → $500K–$2M
    if any(t in title for t in ['senior manager', 'senior director', 'head of', 'lead', 'principal', 'senior']):
        return '$500K–$2M', 'Senior / mid-level professional'
    if any(t in title for t in ['manager', 'supervisor']):
        return '$500K–$2M', 'Manager-level'
    # Engineers and consultants — assume RSU/comp uplift at mid-career
    if any(t in title for t in ['engineer', 'developer', 'architect', 'scientist']):
        if any(t in title for t in ['senior', 'staff', 'principal', 'lead', 'sr.']):
            return '$500K–$2M', 'Senior engineer (RSU + salary)'
        return '$150K–$500K', 'Engineer / developer'
    if any(t in title for t in ['consultant', 'advisor']):
        return '$500K–$2M', 'Consultant / advisor'

    # Tier 5: Early / mid-career → $150K–$500K
    if any(t in title for t in ['analyst', 'associate', 'specialist', 'coordinator', 'representative', 'account executive', 'product manager']):
        return '$150K–$500K', 'Early / mid-career professional'

    # Tier 6: Trades, blue-collar, service → $50K–$200K
    if any(t in title for t in [
        'electrician', 'hvac', 'plumber', 'mechanic', 'technician',
        'driver', 'operator', 'laborer', 'maintenance', 'installer',
        'teacher', 'nurse', 'clerk', 'cashier',
    ]):
        return '$50K–$200K', 'Trade / service worker'

    # Tier 7: Intern/entry-level → $50K–$200K
    if any(t in title for t in ['intern', 'entry level', 'entry-level', 'student', 'junior', 'assistant']):
        return '$50K–$200K', 'Entry-level / intern'

    # Fallback: if title exists but doesn't match above, assume mid-career
    if title.strip():
        return '$150K–$500K', 'Assumed mid-career (title present, tier unmatched)'

    return '—', 'No title or company data'

# ─── EVENT STATS ──────────────────────────────────────────────────────────────

def compute_event_stats(contacts: list) -> dict:
    """Aggregate per-event metrics from a list of contacts."""
    attended = 0
    attended_score = 0
    account_created = 0
    invested = 0
    capital = 0.0

    for c in contacts:
        p = c['properties']
        if (p.get('attended_outbound_event') or '').strip().lower() != 'yes':
            continue
        sc, _ = score_contact(p)
        attended += 1
        attended_score += sc
        if (p.get('admin_url') or '').strip():
            account_created += 1
        try:
            amount = float(p.get('totalamountpurchased') or 0)
        except (ValueError, TypeError):
            amount = 0.0
        if amount > 0:
            invested += 1
            capital += amount

    return {
        'rsvps':          len(contacts),
        'attended':       attended,
        'attended_score': attended_score,
        'account_created': account_created,
        'invested':       invested,
        'capital':        round(capital),
    }

# ─── DATE HELPERS ─────────────────────────────────────────────────────────────

_MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
_DAYS   = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun']

def fmt_date(s: str) -> str:
    d = datetime.strptime(s, '%Y-%m-%d')
    return f"{_DAYS[d.weekday()]}, {_MONTHS[d.month-1]} {d.day}"

def is_past(s: str) -> bool:
    return s < date.today().isoformat()

# ─── HTML ─────────────────────────────────────────────────────────────────────

def score_badge_html(sc: int) -> str:
    fg, bg = SCORE_COLORS[sc]
    label  = SCORE_LABELS[sc]
    return (f'<span class="score-badge" data-score="{sc}" '
            f'style="background:{bg};color:{fg};border:1px solid {fg}55;'
            f'padding:4px 11px;border-radius:12px;font-size:0.78rem;font-weight:700;'
            f'letter-spacing:0.03em;white-space:nowrap;cursor:pointer;'
            f'display:inline-flex;align-items:center;gap:5px">'
            f'<span class="score-num">{sc}</span>'
            f'<span class="score-lbl" style="font-size:0.68rem;opacity:0.8">{label}</span>'
            f'</span>')

def li_url(name: str, company: str) -> str:
    return f'https://www.linkedin.com/search/results/people/?keywords={quote_plus((name + " " + company).strip())}'

def hs_url(cid: str) -> str:
    return f'https://app.hubspot.com/contacts/{PORTAL_ID}/record/0-1/{cid}'

_TITLE_ABBREVS = [
    ('chief executive officer',   'CEO'),
    ('chief technology officer',  'CTO'),
    ('chief operating officer',   'COO'),
    ('chief financial officer',   'CFO'),
    ('chief marketing officer',   'CMO'),
    ('chief revenue officer',     'CRO'),
    ('chief information officer', 'CIO'),
    ('chief product officer',     'CPO'),
    ('chief people officer',      'CPO'),
    ('chief data officer',        'CDO'),
    ('chief strategy officer',    'CSO'),
]

def shorten_title(title: str) -> str:
    for full, abbrev in _TITLE_ABBREVS:
        title = re.sub(re.escape(full), abbrev, title, flags=re.IGNORECASE)
    return title

def render_row(idx: int, c: dict) -> str:
    p        = c['properties']
    cid      = c['id']
    fname    = p.get('firstname') or ''
    lname    = p.get('lastname')  or ''
    name     = f'{fname} {lname}'.strip() or '(No name)'
    title    = shorten_title(p.get('jobtitle') or '')
    company  = p.get('company')  or ''
    owner_id = p.get('hubspot_owner_id') or ''
    city     = p.get('city')  or ''
    state    = p.get('state') or ''

    sc, flags = score_contact(p)
    per       = get_persona(p)
    nw, nw_r  = get_nw(p)

    owner_name = OWNERS.get(owner_id, owner_id or '—')
    if owner_id in INACTIVE_OWNERS:
        owner_cell = f'<span style="color:#c04040;font-weight:600">{escape(owner_name)} ⚠</span>'
    else:
        owner_cell = f'<span style="color:#8a9fc0">{escape(owner_name)}</span>'

    attended     = (p.get('attended_outbound_event') or '').strip().lower() == 'yes'
    attended_chk = 'checked' if attended else ''

    invested_badge = ('<span style="display:inline-block;background:#eaf7f0;color:#1a7a45;'
                      'border:1px solid #1a7a4555;border-radius:10px;font-size:0.62rem;'
                      'font-weight:700;padding:1px 7px;letter-spacing:0.04em;'
                      'vertical-align:middle;margin-right:5px">INV</span>'
                      if 'invested' in flags else '')
    opp_star = ''

    loc_html = ''

    ns_html = '<br><span style="font-size:0.7rem;color:#c94040">⚠ No Show</span>' if 'no_show' in flags else ''

    enriched_tag = (
        '<span title="Inferred via Google / LinkedIn search" '
        'style="font-size:0.6rem;color:#9aaac0;margin-left:4px;vertical-align:middle">🔍</span>'
        if p.get('_enriched') else ''
    )

    tc_parts = []
    if title:   tc_parts.append(escape(title) + enriched_tag)
    if company: tc_parts.append(f'<span style="color:#7a94b8;font-size:0.78rem">{escape(company)}</span>')
    tc_html = '<br>'.join(tc_parts) or '<span style="color:#c0ccd8">—</span>'

    nw_cell = (f'<strong style="font-size:0.85rem">{escape(nw)}</strong>'
               f'<br><span style="font-size:0.7rem;color:#8a9fc0">{escape(nw_r)}</span>')

    name_cell = (
        f'{opp_star}<strong>{escape(name)}</strong>{invested_badge}'
        f'{loc_html}{ns_html}'
    )

    return (
        f'<tr data-id="{escape(cid)}" data-auto="{sc}" '
        f'data-persona="{escape(per)}" data-score="{sc}">'
        f'<td style="color:#aabcd4;text-align:center">{idx}</td>'
        f'<td>{name_cell}</td>'
        f'<td>{tc_html}</td>'
        f'<td style="font-size:0.8rem">{escape(per)}</td>'
        f'<td>{nw_cell}</td>'
        f'<td style="text-align:center" class="score-cell">{score_badge_html(sc)}</td>'
        f'<td style="text-align:center">'
        f'<a href="{li_url(name, company)}" target="_blank" '
        f'style="color:#0a66c2;font-weight:700;text-decoration:none;font-size:0.8rem">LI↗</a></td>'
        f'<td style="text-align:center">'
        f'<a href="{hs_url(cid)}" target="_blank" '
        f'style="color:#ff7a59;font-weight:700;text-decoration:none;font-size:0.8rem">HS↗</a></td>'
        f'<td style="text-align:center">'
        f'<input type="checkbox" class="uninvite-chk" onchange="toggleUninvite(this)" '
        f'style="width:16px;height:16px;cursor:pointer;accent-color:#c94040" title="Uninvite"></td>'
        f'<td style="text-align:center">'
        f'<input type="checkbox" class="attended-chk" {attended_chk} '
        f'onchange="toggleAttended(this)" '
        f'style="width:16px;height:16px;cursor:pointer;accent-color:#1a7a45"></td>'
        f'</tr>\n'
    )

def render_panel(date_str: str, contacts: list, tab_id: str, active: bool) -> str:
    sorted_contacts = sorted(
        contacts,
        key=lambda c: score_contact(c['properties'])[0],
        reverse=True   # 5 first
    )

    rows_html = ''.join(render_row(i + 1, c) for i, c in enumerate(sorted_contacts))

    counts = defaultdict(int)
    for c in contacts:
        sc, _ = score_contact(c['properties'])
        counts[sc] += 1

    pills = []
    for sc in SCORES:
        if counts[sc]:
            fg, bg = SCORE_COLORS[sc]
            pills.append(
                f'<span style="background:{bg};color:{fg};padding:2px 9px;border-radius:10px;'
                f'font-size:0.68rem;font-weight:700;border:1px solid {fg}44">'
                f'{counts[sc]}× {SCORE_LABELS[sc]}</span>'
            )
    pills_html = ' '.join(pills)

    day_score = sum(score_contact(c['properties'])[0] for c in contacts)
    attended_sc = sum(
        score_contact(c['properties'])[0] for c in contacts
        if (c['properties'].get('attended_outbound_event') or '').strip().lower() == 'yes'
    )
    attended_score_html = (
        f'<span style="font-size:0.78rem;color:#1a7a45;font-weight:700" '
        f'id="attended-score-{tab_id}">Attended score: {attended_sc}</span>'
        if attended_sc > 0 else
        f'<span style="font-size:0.78rem;color:#1a7a45;font-weight:700;display:none" '
        f'id="attended-score-{tab_id}"></span>'
    )

    past_note = ' <span style="font-size:0.72rem;color:#9aaac0">(past)</span>' if is_past(date_str) else ''

    opts = '<option value="">All Scores</option>\n' + '\n'.join(
        f'<option value="{s}">{s} — {SCORE_LABELS[s]}</option>'
        for s in SCORES if counts[s]
    )

    display = 'block' if active else 'none'

    return f'''
<div id="tab-{tab_id}" class="tab-panel" style="display:{display}">
  <div class="panel-header">
    <div>
      <div style="display:flex;align-items:baseline;gap:12px;flex-wrap:wrap">
        <span class="rsvp-count">{len(contacts)} RSVPs{past_note}</span>
        <span style="font-size:0.78rem;color:#7a94b8">Day score: <strong style="color:#1b3c6e">{day_score}</strong></span>
        {attended_score_html}
      </div>
      <div class="score-pills" style="margin-top:6px">{pills_html}</div>
    </div>
    <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
      <select class="score-filter" data-tab="{tab_id}">
        {opts}
      </select>
      <button class="reset-overrides-btn" data-tab="{tab_id}"
              style="display:none;background:transparent;border:1px solid #dde3ee;
                     border-radius:8px;padding:8px 12px;font-size:0.78rem;color:#8a9fc0;
                     cursor:pointer;font-family:inherit"
              onclick="resetOverrides('{tab_id}')">
        Reset overrides
      </button>
    </div>
  </div>
  <div class="table-scroll">
    <table class="rsvp-table" id="tbl-{tab_id}">
      <thead><tr>
        <th style="width:34px">#</th>
        <th>Name</th>
        <th>Title / Company</th>
        <th>Persona</th>
        <th>Est. Net Worth</th>
        <th>Likelihood <span style="font-size:0.6rem;opacity:0.6">(click to override)</span></th>
        <th>LinkedIn</th>
        <th>HubSpot</th>
        <th>Uninvite</th>
        <th>Attended</th>
      </tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
  </div>
</div>'''

def build_html(by_date: dict, generated_at: str) -> str:
    today_str = date.today().isoformat()
    dates     = sorted(by_date.keys(), reverse=True)
    all_dates_json = '[' + ','.join(f'"{d}"' for d in dates) + ']'

    # Default tab: today if present, else nearest upcoming, else most recent past
    upcoming    = [d for d in dates if d >= today_str]  # dates is descending
    default_tab = (upcoming[-1] if upcoming else (dates[0] if dates else '')).replace('-', '')

    past_dates   = [d for d in dates if d < today_str]   # descending
    future_dates = [d for d in dates if d >= today_str]  # descending

    tab_btns = []
    for d in future_dates:
        tid    = d.replace('-', '')
        cnt    = len(by_date[d])
        label  = fmt_date(d)
        active = tid == default_tab
        tab_btns.append(
            f'<button class="tab-btn" data-tab="{tid}" data-date="{d}" '
            f'style="border-bottom:3px solid {"#c9a84c" if active else "transparent"}; '
            f'color:{"#1b3c6e" if active else "#7a94b8"};'
            f'font-weight:{"700" if active else "normal"};" '
            f'onclick="switchTab(\'{tid}\')">'
            f'{escape(label)}'
            f'<span class="tab-count">({cnt})</span></button>'
        )

    # Past events dropdown
    past_opts = ''.join(
        f'<button class="past-opt" data-tid="{d.replace("-","")}" '
        f'onclick="selectPast(\'{d.replace("-","")}\',\'{escape(fmt_date(d))}\')">'
        f'{escape(fmt_date(d))} <span style="opacity:0.6;font-size:0.7rem">({len(by_date[d])})</span>'
        f'</button>\n'
        for d in past_dates
    )

    # Determine if default tab is a past date (no upcoming events)
    default_is_past = default_tab in [d.replace('-', '') for d in past_dates]
    past_default_label = fmt_date(
        next(d for d in past_dates if d.replace('-', '') == default_tab)
    ) if default_is_past else ''

    past_btn_html = ''
    past_menu_html = ''
    if past_dates:
        init_label = escape(past_default_label) if default_is_past else 'Past Events'
        # Button stays in tab-bar; menu is top-level to avoid overflow-x clipping
        past_btn_html = f'''<button class="tab-btn past-dropdown-btn" id="pastDropBtn"
          style="{"border-bottom:3px solid #c9a84c;color:#1b3c6e;font-weight:700;" if default_is_past else "color:#9aaac0;"}"
          onclick="togglePastDropdown(event)">{init_label} ▾</button>'''
        past_menu_html = f'''<div class="past-dropdown-menu" id="pastDropMenu">
{past_opts}</div>'''

    panels = [
        render_panel(d, by_date[d], d.replace('-', ''), d.replace('-', '') == default_tab)
        for d in dates
    ]

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Masterworks — RSVP Dashboard</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#f0f3f8;color:#1b3c6e;font-family:-apple-system,'Segoe UI',Georgia,sans-serif;min-height:100vh}}

header{{background:#1b3c6e;padding:16px 28px;position:sticky;top:0;z-index:100;
        box-shadow:0 2px 10px rgba(0,0,0,0.2)}}
.header-row{{display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap}}
.brand{{font-size:0.62rem;letter-spacing:0.2em;text-transform:uppercase;color:#c9a84c;font-weight:700}}
.title{{font-size:0.98rem;font-weight:700;color:#fff;margin-top:2px}}
.meta {{font-size:0.65rem;color:rgba(255,255,255,0.4);margin-top:2px}}

.date-jump-wrap{{display:flex;align-items:center;gap:8px}}
.date-jump-label{{font-size:0.65rem;color:rgba(255,255,255,0.45);text-transform:uppercase;letter-spacing:0.1em}}
#dateJump{{background:rgba(255,255,255,0.12);border:1px solid rgba(255,255,255,0.2);
           border-radius:8px;padding:7px 10px;font-size:0.82rem;color:#fff;
           outline:none;cursor:pointer;font-family:inherit;color-scheme:dark}}
#dateJump:focus{{border-color:rgba(255,255,255,0.5)}}

.tab-bar{{background:#fff;border-bottom:1px solid #e2e8f4;padding:0 20px;
          display:flex;overflow-x:auto;box-shadow:0 1px 4px rgba(27,60,110,0.06)}}
.tab-bar::-webkit-scrollbar{{height:3px}}
.tab-bar::-webkit-scrollbar-thumb{{background:#c9a84c;border-radius:2px}}
.tab-btn{{padding:13px 18px;background:none;border:none;cursor:pointer;font-family:inherit;
          font-size:0.8rem;letter-spacing:0.03em;white-space:nowrap;transition:color 0.15s}}
.tab-btn:hover{{color:#1b3c6e}}
.tab-count{{font-size:0.68rem;opacity:0.6;margin-left:4px}}

.no-date-msg{{display:none;text-align:center;padding:60px 20px;color:#8a9fc0;font-size:0.88rem}}

.content{{padding:18px 20px 60px;max-width:1440px;margin:0 auto}}
.panel-header{{display:flex;align-items:flex-start;justify-content:space-between;
               flex-wrap:wrap;gap:10px;margin-bottom:14px}}
.rsvp-count{{font-size:0.95rem;font-weight:700;color:#1b3c6e}}
.score-pills{{display:flex;gap:6px;flex-wrap:wrap;margin-top:6px}}
.score-filter{{background:#f5f7fb;border:1px solid #dde3ee;border-radius:8px;
                 padding:8px 12px;font-size:0.8rem;color:#1b3c6e;outline:none;
                 cursor:pointer;font-family:inherit}}

.table-scroll{{overflow-x:auto}}
.rsvp-table{{width:100%;border-collapse:separate;border-spacing:0;background:#fff;
             border-radius:8px;overflow:hidden;box-shadow:0 1px 6px rgba(27,60,110,0.08)}}
.rsvp-table thead th{{background:#1b3c6e;color:#a8c8e8;font-size:0.68rem;
  text-transform:uppercase;letter-spacing:0.1em;font-weight:600;padding:10px 14px;
  text-align:center;white-space:nowrap}}
.rsvp-table td{{padding:11px 14px;font-size:0.83rem;color:#3a5070;vertical-align:middle;text-align:center}}
.rsvp-table tbody tr{{border-bottom:1px solid #eef1f7;transition:background 0.12s}}
.rsvp-table tbody tr:nth-child(even){{background:#f8fafd}}
.rsvp-table tbody tr:hover{{background:#edf3fb!important}}
.rsvp-table tbody tr.overridden td:first-child::after{{content:'✏';font-size:0.6rem;color:#c9a84c;margin-left:3px}}
.rsvp-table tbody tr:last-child{{border-bottom:none}}

/* Score override popover */
.score-popover{{position:absolute;background:#fff;border:1px solid #dde3ee;border-radius:10px;
                box-shadow:0 4px 20px rgba(0,0,0,0.15);padding:10px;z-index:500;
                display:none;flex-direction:column;gap:4px;min-width:140px}}
.score-popover.show{{display:flex}}
.score-opt{{display:flex;align-items:center;gap:8px;padding:7px 10px;border-radius:7px;
            cursor:pointer;border:none;background:none;font-family:inherit;
            font-size:0.82rem;width:100%;text-align:left;transition:background 0.1s}}
.score-opt:hover{{background:#f0f3f8}}
.score-opt-num{{width:22px;height:22px;border-radius:50%;display:flex;align-items:center;
                justify-content:center;font-weight:700;font-size:0.78rem;flex-shrink:0}}

.rsvp-table tbody tr.uninvited{{opacity:0.35;text-decoration:line-through}}

.nav-link{{font-size:0.72rem;color:#c9a84c;text-decoration:none;letter-spacing:0.08em;
           text-transform:uppercase;opacity:0.8;white-space:nowrap}}
.nav-link:hover{{opacity:1}}
.page-tabs{{display:flex;margin-top:2px;border-top:1px solid rgba(255,255,255,0.1)}}
.page-tab{{padding:10px 22px;font-size:0.7rem;letter-spacing:0.09em;text-transform:uppercase;
           text-decoration:none;color:rgba(255,255,255,0.42);border-bottom:2px solid transparent;
           transition:all 0.15s;white-space:nowrap}}
.page-tab:hover{{color:rgba(255,255,255,0.8);border-bottom-color:rgba(255,255,255,0.25)}}
.page-tab.active-tab{{color:#fff;border-bottom-color:#c9a84c;font-weight:700}}
.refresh-btn{{background:none;border:1px solid rgba(255,255,255,0.22);border-radius:6px;
              padding:5px 11px;color:rgba(255,255,255,0.65);font-size:0.72rem;cursor:pointer;
              font-family:inherit;letter-spacing:0.04em;transition:all 0.15s;white-space:nowrap}}
.refresh-btn:hover{{border-color:rgba(255,255,255,0.5);color:#fff}}
.refresh-btn:disabled{{opacity:0.45;cursor:default}}

.past-dropdown-btn{{font-style:italic}}
.past-dropdown-menu{{display:none;position:fixed;background:#fff;
  border:1px solid #dde3ee;border-radius:8px;
  box-shadow:0 4px 16px rgba(27,60,110,0.14);z-index:300;
  min-width:210px;max-height:300px;overflow-y:auto;padding:6px 0}}
.past-dropdown-menu.open{{display:block}}
.past-opt{{display:block;width:100%;text-align:left;padding:9px 16px;background:none;
  border:none;cursor:pointer;font-family:inherit;font-size:0.82rem;color:#3a5070;
  transition:background 0.1s;white-space:nowrap}}
.past-opt:hover{{background:#f0f3f8}}
.past-opt.active-past{{background:#edf3fb;color:#1b3c6e;font-weight:700}}
</style>
</head>
<body>

<header>
  <div class="header-row">
    <div>
      <div class="brand">Masterworks · Outbound</div>
      <div class="title">RSVP Lead Dashboard</div>
      <div class="meta">Updated {escape(generated_at)}</div>
    </div>
    <div style="display:flex;align-items:center;gap:14px;flex-wrap:wrap">
      <div class="date-jump-wrap">
        <span class="date-jump-label">Jump to date</span>
        <input type="date" id="dateJump" title="Jump to event date">
      </div>
      <button class="refresh-btn" id="refreshBtn" onclick="triggerRefresh()">↻ Refresh</button>
    </div>
  </div>
  <div class="page-tabs">
    <a href="index.html"   class="page-tab active-tab">RSVP Dashboard</a>
    <a href="events.html"  class="page-tab">Event Dashboard</a>
    <a href="scoring.html" class="page-tab">Scoring Logic</a>
  </div>
</header>

<div class="tab-bar" id="tabBar">
  {''.join(tab_btns)}
  {past_btn_html}
</div>
<div class="no-date-msg" id="noDateMsg">No RSVP data for this date in the current window.</div>

{past_menu_html}

<div class="content">
  {''.join(panels)}
</div>

<!-- Score override popover -->
<div class="score-popover" id="scorePopover">
  <div style="font-size:0.65rem;color:#8a9fc0;text-transform:uppercase;letter-spacing:0.1em;
              padding:2px 6px 6px;border-bottom:1px solid #eef1f7;margin-bottom:2px">
    Override score
  </div>
</div>

<script>
var GITHUB_REPO     = '{escape(GITHUB_REPO)}';
var GITHUB_WORKFLOW = '{GITHUB_WORKFLOW}';
var SHARED_GIST_ID  = '{SHARED_GIST_ID}';

function triggerRefresh() {{
  var tok = localStorage.getItem('gh_pat');
  if (!tok) {{
    tok = prompt('Enter your GitHub personal access token to trigger a refresh.\\n(Saved in your browser — you only need to do this once.)');
    if (!tok) return;
    localStorage.setItem('gh_pat', tok.trim());
    tok = tok.trim();
  }}
  var btn = document.getElementById('refreshBtn');
  btn.disabled = true;
  btn.textContent = 'Updating…';
  fetch('https://api.github.com/repos/' + GITHUB_REPO + '/actions/workflows/' + GITHUB_WORKFLOW + '/dispatches', {{
    method: 'POST',
    headers: {{
      'Authorization': 'token ' + tok,
      'Accept': 'application/vnd.github.v3+json',
      'Content-Type': 'application/json'
    }},
    body: JSON.stringify({{ref: 'main'}})
  }}).then(function(r) {{
    if (r.status === 204) {{
      btn.textContent = 'Updating… (~45s)';
      setTimeout(function(){{ location.reload(); }}, 45000);
    }} else if (r.status === 401) {{
      localStorage.removeItem('gh_pat');
      btn.disabled = false;
      btn.textContent = '↻ Refresh';
      alert('Token invalid or expired. Click Refresh to enter a new one.');
    }} else {{
      btn.disabled = false;
      btn.textContent = '↻ Refresh';
    }}
  }}).catch(function() {{
    btn.disabled = false;
    btn.textContent = '↻ Refresh';
  }});
}}

var TODAY      = '{today_str}';
var ALL_DATES  = {all_dates_json};
var SCORE_META = {{
  5: {{label:'High',       fg:'#1a7a45', bg:'#eaf7f0'}},
  4: {{label:'Medium-High',fg:'#1a5fa8', bg:'#e8f0fb'}},
  3: {{label:'Medium',     fg:'#8a6800', bg:'#fdf6e3'}},
  2: {{label:'Low-Medium', fg:'#b85a00', bg:'#fdf0e8'}},
  1: {{label:'Low',        fg:'#a83030', bg:'#fde8e8'}},
}};

// ── Shared state (GitHub Gist) ────────────────────────────────────────────────
var GIST_FILE   = 'mw_rsvp_state.json';
var _gistId     = null;
var _gistState  = {{}};
var _writeTimer = null;

function getSharedState(key) {{
  return (key in _gistState) ? String(_gistState[key]) : localStorage.getItem(key);
}}
function saveSharedState(key, val) {{
  _gistState[key] = val;
  localStorage.setItem(key, val);
  _writeGist();
}}
function removeSharedState(key) {{
  delete _gistState[key];
  localStorage.removeItem(key);
  _writeGist();
}}
function _writeGist() {{
  if (!_gistId) return;
  var tok = localStorage.getItem('gh_pat');
  if (!tok) return;
  clearTimeout(_writeTimer);
  _writeTimer = setTimeout(function() {{
    var files = {{}};
    files[GIST_FILE] = {{content: JSON.stringify(_gistState)}};
    fetch('https://api.github.com/gists/' + _gistId, {{
      method: 'PATCH',
      headers: {{'Authorization':'token '+tok,'Accept':'application/vnd.github.v3+json','Content-Type':'application/json'}},
      body: JSON.stringify({{files: files}})
    }});
  }}, 600);
}}
function _fetchGist(cb) {{
  fetch('https://api.github.com/gists/' + _gistId, {{
    headers: {{'Accept':'application/vnd.github.v3+json'}}, cache: 'no-store'
  }})
  .then(function(r) {{ return r.json(); }})
  .then(function(data) {{
    var f = data.files && data.files[GIST_FILE];
    if (f && f.content) {{ try {{ _gistState = JSON.parse(f.content); }} catch(e) {{}} }}
    if (cb) cb();
  }})
  .catch(function() {{ if (cb) cb(); }});
}}
function initSharedState(cb) {{
  _gistId = SHARED_GIST_ID;
  if (_gistId) {{ _fetchGist(cb); }} else {{ if (cb) cb(); }}
}}

// ── Tab switching ─────────────────────────────────────────────────────────────
function switchTab(id) {{
  document.querySelectorAll('.tab-panel').forEach(p => p.style.display = 'none');
  document.getElementById('noDateMsg').style.display = 'none';
  document.querySelectorAll('.tab-btn').forEach(b => {{
    var on = b.dataset.tab === id;
    b.style.borderBottom = on ? '3px solid #c9a84c' : '3px solid transparent';
    b.style.color        = on ? '#1b3c6e' : '#7a94b8';
    b.style.fontWeight   = on ? '700' : 'normal';
  }});
  var el = document.getElementById('tab-' + id);
  if (el) el.style.display = 'block';
  applyStoredOverrides(id);
  updateResetBtn(id);
}}

// ── Date jump ─────────────────────────────────────────────────────────────────
document.getElementById('dateJump').value = TODAY;
document.getElementById('dateJump').addEventListener('change', function() {{
  var val = this.value;  // YYYY-MM-DD
  var tid = val.replace(/-/g,'');
  var panel = document.getElementById('tab-' + tid);
  if (panel) {{
    switchTab(tid);
    // Scroll tab button into view
    var btn = document.querySelector('.tab-btn[data-tab="' + tid + '"]');
    if (btn) btn.scrollIntoView({{behavior:'smooth', block:'nearest', inline:'center'}});
  }} else {{
    document.querySelectorAll('.tab-panel').forEach(p => p.style.display = 'none');
    document.querySelectorAll('.tab-btn').forEach(b => {{
      b.style.borderBottom = '3px solid transparent';
      b.style.color        = '#7a94b8';
      b.style.fontWeight   = 'normal';
    }});
    document.getElementById('noDateMsg').style.display = 'block';
  }}
}});

// ── Score filter ──────────────────────────────────────────────────────────────
document.querySelectorAll('.score-filter').forEach(function(sel) {{
  sel.addEventListener('change', function() {{
    var val   = this.value;
    var tbody = document.querySelector('#tbl-' + this.dataset.tab + ' tbody');
    if (!tbody) return;
    Array.from(tbody.rows).forEach(function(row) {{
      row.style.display = (!val || row.dataset.score === val) ? '' : 'none';
    }});
  }});
}});

// ── Score override ────────────────────────────────────────────────────────────
var popover       = document.getElementById('scorePopover');
var activeCell    = null;

// Build popover options once
Object.keys(SCORE_META).sort(function(a,b){{return b-a;}}).forEach(function(sc) {{
  var m   = SCORE_META[sc];
  var btn = document.createElement('button');
  btn.className = 'score-opt';
  btn.innerHTML =
    '<span class="score-opt-num" style="background:' + m.bg + ';color:' + m.fg + '">' + sc + '</span>' +
    '<span>' + sc + ' — ' + m.label + '</span>';
  btn.addEventListener('click', function() {{
    if (!activeCell) return;
    var row = activeCell.closest('tr');
    var cid = row.dataset.id;
    var tab = row.closest('.tab-panel').id.replace('tab-','');
    setOverride(cid, parseInt(sc), tab);
    closePopover();
  }});
  popover.appendChild(btn);
}});

document.addEventListener('click', function(e) {{
  var badge = e.target.closest('.score-badge');
  if (badge) {{
    e.stopPropagation();
    activeCell = badge.closest('td');
    var rect   = badge.getBoundingClientRect();
    popover.style.top  = (rect.bottom + window.scrollY + 6) + 'px';
    popover.style.left = (rect.left  + window.scrollX) + 'px';
    popover.style.position = 'absolute';
    popover.classList.add('show');
    return;
  }}
  closePopover();
}});

function closePopover() {{
  popover.classList.remove('show');
  activeCell = null;
}}

function setOverride(cid, sc, tabId) {{
  var key = 'override_' + cid;
  if (sc === getAutoScore(cid, tabId)) {{
    removeSharedState(key);
  }} else {{
    saveSharedState(key, sc);
  }}
  applyOverride(cid, sc, tabId);
  updateResetBtn(tabId);
}}

function getAutoScore(cid, tabId) {{
  var row = document.querySelector('#tbl-' + tabId + ' tr[data-id="' + cid + '"]');
  return row ? parseInt(row.dataset.auto) : null;
}}

function applyOverride(cid, sc, tabId) {{
  var row    = document.querySelector('#tbl-' + tabId + ' tr[data-id="' + cid + '"]');
  if (!row) return;
  var cell   = row.querySelector('.score-cell');
  var m      = SCORE_META[sc];
  var auto   = parseInt(row.dataset.auto);
  var manual = getSharedState('override_' + cid);

  row.dataset.score = sc;
  cell.innerHTML =
    '<span class="score-badge" data-score="' + sc + '" ' +
    'style="background:' + m.bg + ';color:' + m.fg + ';border:1px solid ' + m.fg + '55;' +
    'padding:4px 11px;border-radius:12px;font-size:0.78rem;font-weight:700;' +
    'letter-spacing:0.03em;white-space:nowrap;cursor:pointer;' +
    'display:inline-flex;align-items:center;gap:5px">' +
    '<span class="score-num">' + sc + '</span>' +
    '<span class="score-lbl" style="font-size:0.68rem;opacity:0.8">' + m.label + '</span>' +
    (manual ? '<span style="font-size:0.62rem;opacity:0.6" title="Manually overridden">✏</span>' : '') +
    '</span>';

  if (manual && parseInt(manual) !== auto) {{
    row.classList.add('overridden');
  }} else {{
    row.classList.remove('overridden');
  }}

  // Re-sort tbody
  var tbody = row.closest('tbody');
  var rows  = Array.from(tbody.rows);
  rows.sort(function(a,b){{ return parseInt(b.dataset.score) - parseInt(a.dataset.score); }});
  rows.forEach(function(r, i) {{
    tbody.appendChild(r);
    r.cells[0].textContent = i + 1;
  }});
}}

function toggleUninvite(chk) {{
  var row = chk.closest('tr');
  var cid = row.dataset.id;
  var tid = row.closest('.tab-panel').id.replace('tab-','');
  if (chk.checked) {{
    saveSharedState('uninvite_' + cid, '1');
    row.classList.add('uninvited');
  }} else {{
    removeSharedState('uninvite_' + cid);
    row.classList.remove('uninvited');
  }}
  updateResetBtn(tid);
}}

function toggleAttended(chk) {{
  var row = chk.closest('tr');
  var cid = row.dataset.id;
  var tid = row.closest('.tab-panel').id.replace('tab-','');
  if (chk.checked) {{
    saveSharedState('attended_' + cid, '1');
  }} else {{
    removeSharedState('attended_' + cid);
  }}
  refreshHeader(tid);
  updateResetBtn(tid);
}}

function refreshHeader(tabId) {{
  var el = document.getElementById('attended-score-' + tabId);
  if (!el) return;
  var sc = 0;
  document.querySelectorAll('#tbl-' + tabId + ' .attended-chk:checked').forEach(function(chk) {{
    sc += parseInt(chk.closest('tr').dataset.score);
  }});
  el.textContent = sc > 0 ? 'Attended score: ' + sc : '';
  el.style.display = sc > 0 ? 'inline' : 'none';
}}

function applyStoredOverrides(tabId) {{
  var rows = document.querySelectorAll('#tbl-' + tabId + ' tbody tr');
  rows.forEach(function(row) {{
    var cid = row.dataset.id;
    var val = getSharedState('override_' + cid);
    if (val) applyOverride(cid, parseInt(val), tabId);
    if (getSharedState('uninvite_' + cid)) {{
      row.classList.add('uninvited');
      var uchk = row.querySelector('.uninvite-chk');
      if (uchk) uchk.checked = true;
    }}
    if (getSharedState('attended_' + cid)) {{
      var achk = row.querySelector('.attended-chk');
      if (achk) achk.checked = true;
    }}
  }});
  refreshHeader(tabId);
}}

function updateResetBtn(tabId) {{
  var rows   = document.querySelectorAll('#tbl-' + tabId + ' tbody tr');
  var hasAny = Array.from(rows).some(function(r) {{
    return getSharedState('override_'  + r.dataset.id) ||
           getSharedState('uninvite_'  + r.dataset.id) ||
           getSharedState('attended_'  + r.dataset.id);
  }});
  var btn = document.querySelector('.reset-overrides-btn[data-tab="' + tabId + '"]');
  if (btn) btn.style.display = hasAny ? 'block' : 'none';
}}

function resetOverrides(tabId) {{
  var rows = document.querySelectorAll('#tbl-' + tabId + ' tbody tr');
  rows.forEach(function(row) {{
    var cid  = row.dataset.id;
    var auto = parseInt(row.dataset.auto);
    removeSharedState('override_' + cid);
    removeSharedState('uninvite_' + cid);
    removeSharedState('attended_' + cid);
    applyOverride(cid, auto, tabId);
    row.classList.remove('uninvited');
    var uchk = row.querySelector('.uninvite-chk');
    if (uchk) uchk.checked = false;
    var achk = row.querySelector('.attended-chk');
    if (achk) achk.checked = false;
  }});
  refreshHeader(tabId);
  updateResetBtn(tabId);
}}

// ── Past events dropdown ──────────────────────────────────────────────────────
function togglePastDropdown(e) {{
  e.stopPropagation();
  var menu = document.getElementById('pastDropMenu');
  if (!menu) return;
  var isOpen = menu.classList.contains('open');
  menu.classList.remove('open');
  if (!isOpen) {{
    var rect = document.getElementById('pastDropBtn').getBoundingClientRect();
    menu.style.position = 'fixed';
    menu.style.top  = (rect.bottom + 4) + 'px';
    menu.style.left = rect.left + 'px';
    menu.classList.add('open');
  }}
}}

function selectPast(tid, label) {{
  var menu = document.getElementById('pastDropMenu');
  if (menu) menu.classList.remove('open');
  // Switch to the past panel
  switchTab(tid);
  // Re-style the past dropdown button (switchTab resets all .tab-btn styles)
  var btn = document.getElementById('pastDropBtn');
  if (btn) {{
    btn.textContent = label + ' ▾';
    btn.style.borderBottom = '3px solid #c9a84c';
    btn.style.color        = '#1b3c6e';
    btn.style.fontWeight   = '700';
  }}
  // Mark active option
  document.querySelectorAll('.past-opt').forEach(function(o) {{
    o.classList.toggle('active-past', o.dataset.tid === tid);
  }});
}}

document.addEventListener('click', function() {{
  var menu = document.getElementById('pastDropMenu');
  if (menu) menu.classList.remove('open');
}});

// ── Init ──────────────────────────────────────────────────────────────────────
(function() {{
  var defaultTab = '{default_tab}';

  // If the page was built on a different day, the baked-in defaultTab may be
  // stale. Always try to land on today's tab first.
  var todayTid = new Date().toLocaleDateString('en-CA').replace(/-/g, '');
  var todayPanel = document.getElementById('tab-' + todayTid);
  if (todayPanel && todayTid !== defaultTab) {{
    switchTab(todayTid);
    defaultTab = todayTid;
  }}

  initSharedState(function() {{
    if (defaultTab) {{
      applyStoredOverrides(defaultTab);
      updateResetBtn(defaultTab);
    }}
    if ('{past_default_label}' && defaultTab === '{default_tab}') {{
      document.querySelectorAll('.past-opt').forEach(function(o) {{
        if (o.dataset.tid === defaultTab) o.classList.add('active-past');
      }});
    }}
  }});
}})();
</script>
</body>
</html>'''

# ─── EVENTS PAGE ──────────────────────────────────────────────────────────────

def build_events_html(by_date: dict, generated_at: str) -> str:
    today_str = date.today().isoformat()

    events = []
    for d in sorted(by_date.keys(), reverse=True):
        s = compute_event_stats(by_date[d])
        events.append({
            'date':           d,
            'rsvps':          s['rsvps'],
            'attended':       s['attended'],
            'attended_score': s['attended_score'],
            'accountCreated': s['account_created'],
            'invested':       s['invested'],
            'capital':        s['capital'],
        })

    events_json = json.dumps(events)

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Masterworks — Event Dashboard</title>
<style>
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ background:#d8e2f0; color:#1e2a3a; font-family:'Georgia',serif; min-height:100vh; }}

  header {{ background:#1b3c6e; padding:16px 28px; position:sticky; top:0; z-index:100;
            box-shadow:0 2px 8px rgba(27,60,110,0.25); }}
  .header-row {{ display:flex; align-items:center; justify-content:space-between; gap:12px; flex-wrap:wrap; }}
  .brand {{ font-size:0.62rem; letter-spacing:0.2em; text-transform:uppercase; color:#c9a84c; font-weight:700; }}
  .title {{ font-size:0.98rem; font-weight:700; color:#fff; margin-top:2px; }}
  .meta  {{ font-size:0.65rem; color:rgba(255,255,255,0.4); margin-top:2px; }}
  .nav-link {{ color:#8eb4e0; text-decoration:none; font-size:0.72rem; letter-spacing:0.08em; white-space:nowrap; }}
  .nav-link:hover {{ color:#fff; }}
  .page-tabs {{ display:flex; margin-top:2px; border-top:1px solid rgba(255,255,255,0.1); }}
  .page-tab {{ padding:10px 22px; font-size:0.7rem; letter-spacing:0.09em; text-transform:uppercase;
               text-decoration:none; color:rgba(255,255,255,0.42); border-bottom:2px solid transparent;
               transition:all 0.15s; white-space:nowrap; }}
  .page-tab:hover {{ color:rgba(255,255,255,0.8); border-bottom-color:rgba(255,255,255,0.25); }}
  .page-tab.active-tab {{ color:#fff; border-bottom-color:#c9a84c; font-weight:700; }}
  .refresh-btn {{ background:none; border:1px solid rgba(255,255,255,0.22); border-radius:6px;
                  padding:5px 11px; color:rgba(255,255,255,0.65); font-size:0.72rem; cursor:pointer;
                  font-family:inherit; letter-spacing:0.04em; transition:all 0.15s; white-space:nowrap; }}
  .refresh-btn:hover {{ border-color:rgba(255,255,255,0.5); color:#fff; }}
  .refresh-btn:disabled {{ opacity:0.45; cursor:default; }}

  .stats-bar {{ background:#162f57; border-bottom:1px solid #1b3c6e;
               padding:0 40px; display:flex; align-items:stretch; overflow-x:auto; }}
  .stat-tile {{ padding:18px 28px; display:flex; flex-direction:column; align-items:center;
               gap:4px; border-right:1px solid rgba(255,255,255,0.07); flex-shrink:0; min-width:100px; }}
  .stat-tile:first-child {{ padding-left:0; }}
  .stat-tile:last-child  {{ border-right:none; }}
  .stat-value {{ font-size:1.65rem; color:#e8f0fc; letter-spacing:0.02em; line-height:1; }}
  .stat-label {{ font-size:0.65rem; color:#7aaace; text-transform:uppercase; letter-spacing:0.13em; white-space:nowrap; font-weight:600; }}
  .stat-tile.teal   .stat-value {{ color:#7dc4a8; }}
  .stat-tile.green  .stat-value {{ color:#6dbf82; }}
  .stat-tile.gold   .stat-value {{ color:#d4a96a; }}
  .stat-tile.purple .stat-value {{ color:#b49ee0; }}
  .stat-tile.amber  .stat-value {{ color:#c9a84c; }}
  .stat-tile.blue   .stat-value {{ color:#4a90d9; }}

  .funnel-wrap {{ background:#1a3660; border-bottom:2px solid #1b3c6e;
                 padding:16px 40px; display:flex; align-items:center; overflow-x:auto; }}
  .funnel-stage {{ display:flex; flex-direction:column; align-items:center; gap:5px; flex-shrink:0; min-width:140px; }}
  .funnel-row   {{ display:flex; align-items:center; gap:6px; }}
  .funnel-num   {{ font-size:1.3rem; color:#e8f0fc; line-height:1; font-weight:300; letter-spacing:0.02em; }}
  .funnel-sub   {{ font-size:0.72rem; color:#9aaac0; }}
  .funnel-bar-wrap {{ width:100px; height:6px; background:rgba(255,255,255,0.1); border-radius:3px; overflow:hidden; }}
  .funnel-bar-fill {{ height:100%; border-radius:3px; transition:width 0.4s ease; }}
  .fill-rsvp     {{ background:#4a90d9; }}
  .fill-attended {{ background:#7dc4a8; }}
  .fill-account  {{ background:#d4a96a; }}
  .funnel-label {{ font-size:0.66rem; color:#7aaace; text-transform:uppercase; letter-spacing:0.13em; font-weight:600; }}
  .funnel-arrow {{ font-size:1.1rem; color:#2f527a; padding:0 8px; align-self:center; margin-bottom:18px; flex-shrink:0; }}

  .controls {{ background:#22508a; padding:12px 40px; display:flex; align-items:center;
              gap:14px; flex-wrap:wrap; border-bottom:2px solid #1b3c6e; }}
  .search-input {{ background:#1b3c6e; border:1px solid #3a6aaa; border-radius:4px;
                  padding:8px 14px; color:#e8f0fc; font-size:0.88rem; font-family:'Georgia',serif;
                  outline:none; transition:border-color 0.2s; width:220px; }}
  .search-input:focus {{ border-color:#8eb4e0; }}
  .search-input::placeholder {{ color:#6a90be; }}
  .filter-btn {{ background:transparent; border:1px solid #4a78b8; color:#9bbfe0;
                padding:5px 13px; border-radius:20px; font-size:0.7rem;
                letter-spacing:0.08em; text-transform:uppercase; cursor:pointer;
                transition:all 0.2s; font-family:'Georgia',serif; }}
  .filter-btn:hover, .filter-btn.active {{ border-color:#fff; color:#fff; background:rgba(255,255,255,0.12); }}

  .count-bar {{ padding:9px 40px; font-size:0.68rem; color:#5a7a9e; letter-spacing:0.09em;
               text-transform:uppercase; background:#cfd9ec; border-bottom:1px solid #bfcce0; font-weight:600; }}

  .table-wrap {{ padding:0 16px 48px; overflow-x:auto; }}
  table {{ width:100%; border-collapse:separate; border-spacing:0; background:#fff;
          border-radius:4px; overflow:hidden; box-shadow:0 1px 4px rgba(27,60,110,0.08); margin-top:14px; }}
  thead th {{ background:#1b3c6e; color:#a8c8e8; font-size:0.72rem; text-transform:uppercase;
             letter-spacing:0.1em; font-weight:600; padding:11px 14px; text-align:center;
             white-space:nowrap; cursor:pointer; user-select:none; }}
  thead th:hover {{ color:#fff; }}
  thead th.sorted-asc::after  {{ content:' ↑'; color:#fff; }}
  thead th.sorted-desc::after {{ content:' ↓'; color:#fff; }}
  thead th.no-sort {{ cursor:default; }}
  thead th.no-sort:hover {{ color:#a8c8e8; }}
  thead th.group-rsvp     {{ color:#5fa8e8; border-bottom:3px solid #3a78c0; }}
  thead th.group-attended {{ color:#7dc4a8; border-bottom:3px solid #5aaa8a; }}
  thead th.group-score    {{ color:#4a90d9; border-bottom:3px solid #2a70b9; }}
  thead th.group-account  {{ color:#d4a96a; border-bottom:3px solid #b88840; }}
  thead th.group-invested {{ color:#6dbf82; border-bottom:3px solid #4a9f60; }}
  thead th.group-capital  {{ color:#b49ee0; border-bottom:3px solid #8060c0; }}
  tbody tr {{ border-bottom:1px solid #eef1f7; transition:background 0.15s; }}
  tbody tr:nth-child(even) {{ background:#f8fafd; }}
  tbody tr:hover {{ background:#edf3fb !important; }}
  tbody tr:last-child {{ border-bottom:none; }}
  tbody tr.pending-row td {{ opacity:0.6; }}
  td {{ padding:14px; font-size:0.85rem; color:#4a5f78; white-space:nowrap; text-align:center; }}
  td:first-child {{ font-size:0.68rem; color:#aabcd4; width:34px; }}
  .date-cell {{ color:#1b3c6e; font-size:0.88rem; font-weight:700; text-align:left; letter-spacing:0.01em; }}
  .num {{ font-variant-numeric:tabular-nums; }}
  .pending-badge {{ display:inline-block; padding:2px 7px; border-radius:10px; font-size:0.6rem;
                   letter-spacing:0.08em; text-transform:uppercase; background:#f4f6fa;
                   color:#8a9ab8; border:1px solid #dde4ef; }}
  .no-results {{ text-align:center; padding:60px; color:#9aaac0; font-size:0.9rem; letter-spacing:0.05em; }}

  footer {{ padding:24px 40px; text-align:center; font-size:0.7rem; color:#6a90be;
           letter-spacing:0.1em; text-transform:uppercase; background:#1b3c6e; }}
  .footer-note {{ padding:6px 40px 10px; font-size:0.62rem; color:#8aabcc; letter-spacing:0.06em; }}
</style>
</head>
<body>

<header>
  <div class="header-row">
    <div>
      <div class="brand">Masterworks · Outbound</div>
      <div class="title">Event Dashboard</div>
      <div class="meta">Updated {escape(generated_at)}</div>
    </div>
    <div style="display:flex;align-items:center;gap:14px;flex-wrap:wrap">
      <button class="refresh-btn" id="refreshBtn" onclick="triggerRefresh()">↻ Refresh</button>
    </div>
  </div>
  <div class="page-tabs">
    <a href="index.html"   class="page-tab">RSVP Dashboard</a>
    <a href="events.html"  class="page-tab active-tab">Event Dashboard</a>
    <a href="scoring.html" class="page-tab">Scoring Logic</a>
  </div>
</header>

<div class="stats-bar">
  <div class="stat-tile">        <span class="stat-value" id="sEvents">—</span>   <span class="stat-label">Events</span></div>
  <div class="stat-tile">        <span class="stat-value" id="sRSVPs">—</span>    <span class="stat-label">Total RSVPs</span></div>
  <div class="stat-tile teal">   <span class="stat-value" id="sAttended">—</span> <span class="stat-label">Attended</span></div>
  <div class="stat-tile">        <span class="stat-value" id="sAttRate">—</span>  <span class="stat-label">Att. Rate</span></div>
  <div class="stat-tile gold">   <span class="stat-value" id="sAccounts">—</span> <span class="stat-label">Accts Created</span></div>
  <div class="stat-tile">        <span class="stat-value" id="sAccRate">—</span>  <span class="stat-label">Acct Rate</span></div>
  <div class="stat-tile green">  <span class="stat-value" id="sInvested">—</span> <span class="stat-label">Investors</span></div>
  <div class="stat-tile purple"> <span class="stat-value" id="sCapital">—</span>  <span class="stat-label">Capital Raised</span></div>
  <div class="stat-tile blue">   <span class="stat-value" id="sAttScore">—</span> <span class="stat-label">Att. Score</span></div>
</div>

<div class="funnel-wrap">
  <div class="funnel-stage">
    <div class="funnel-row"><span class="funnel-num" id="fRSVP">—</span></div>
    <div class="funnel-bar-wrap"><div class="funnel-bar-fill fill-rsvp" style="width:100%"></div></div>
    <div class="funnel-label">RSVPs</div>
  </div>
  <div class="funnel-arrow">›</div>
  <div class="funnel-stage">
    <div class="funnel-row"><span class="funnel-num" id="fAtt">—</span><span class="funnel-sub" id="fAttPct"></span></div>
    <div class="funnel-bar-wrap"><div class="funnel-bar-fill fill-attended" id="fBarAtt" style="width:0%"></div></div>
    <div class="funnel-label">Attended</div>
  </div>
  <div class="funnel-arrow">›</div>
  <div class="funnel-stage">
    <div class="funnel-row"><span class="funnel-num" id="fAcc">—</span><span class="funnel-sub" id="fAccPct"></span></div>
    <div class="funnel-bar-wrap"><div class="funnel-bar-fill fill-account" id="fBarAcc" style="width:0%"></div></div>
    <div class="funnel-label">Accts Created</div>
  </div>
</div>

<div class="controls">
  <input type="text" class="search-input" id="searchInput" placeholder="Search date…" oninput="render()">
  <button class="filter-btn active" data-filter="all"     onclick="setFilter(this)">All</button>
  <button class="filter-btn"        data-filter="recent"  onclick="setFilter(this)">Last 30 Days</button>
  <button class="filter-btn"        data-filter="pending" onclick="setFilter(this)">Pending Attendance</button>
</div>

<div class="count-bar" id="countBar">Loading…</div>

<div class="table-wrap">
  <table id="mainTable">
    <thead>
      <tr id="headerRow">
        <th class="no-sort">#</th>
        <th onclick="setSort('date')"          style="text-align:left">Date</th>
        <th onclick="setSort('rsvps')"          class="group-rsvp">RSVPs</th>
        <th onclick="setSort('attended')"       class="group-attended">Attended</th>
        <th onclick="setSort('attended_score')" class="group-score">Att. Score</th>
        <th onclick="setSort('accRate')"        class="group-account">Acct Created</th>
        <th onclick="setSort('invested')"       class="group-invested">Investors</th>
        <th onclick="setSort('capital')"        class="group-capital">Capital Raised</th>
      </tr>
    </thead>
    <tbody id="tableBody"></tbody>
  </table>
</div>

<p class="footer-note">
  Data from HubSpot · Attended = <code>attended_outbound_event = Yes</code> · Acct Created = attended + <code>admin_url</code> populated · Invested = attended + <code>totalamountpurchased &gt; 0</code> · Att. Score = sum of lead scores for attended contacts
</p>
<footer>Masterworks Internal · Outbound Events · <span id="asOfDate"></span></footer>

<script>
var GITHUB_REPO     = '{escape(GITHUB_REPO)}';
var GITHUB_WORKFLOW = '{GITHUB_WORKFLOW}';

const eventData = {events_json};
const TODAY = '{today_str}';

function isPending(e) {{ return e.attended === 0 && e.date >= TODAY; }}
function isRecent(e)  {{
  var d = new Date(e.date), t = new Date(TODAY);
  return (t - d) / 86400000 <= 30;
}}
function fmt(n)    {{ return n == null ? '—' : n.toLocaleString(); }}
function fmtCap(n) {{
  if (!n) return '<span style="color:#b0c4d8">—</span>';
  if (n >= 1000000) return '$' + (n/1000000).toFixed(2) + 'M';
  if (n >= 1000)    return '$' + (n/1000).toFixed(1) + 'K';
  return '$' + n.toLocaleString();
}}
function pct(a, b) {{ if (!b) return null; return Math.round(a / b * 100); }}
function formatDate(d) {{
  var p = d.split('-');
  var months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  var days   = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
  return days[new Date(+p[0],+p[1]-1,+p[2]).getDay()] + ', ' + months[+p[1]-1] + ' ' + +p[2] + ', ' + p[0];
}}

var sortKey = 'date', sortDir = 'desc', activeFilter = 'all';

function setFilter(btn) {{
  document.querySelectorAll('.filter-btn').forEach(function(b){{ b.classList.remove('active'); }});
  btn.classList.add('active');
  activeFilter = btn.dataset.filter;
  render();
}}
function setSort(key) {{
  if (sortKey === key) sortDir = sortDir === 'asc' ? 'desc' : 'asc';
  else {{ sortKey = key; sortDir = 'desc'; }}
  document.querySelectorAll('#headerRow th:not(.no-sort)').forEach(function(th){{
    th.classList.remove('sorted-asc','sorted-desc');
  }});
  var cols = {{date:1,rsvps:2,attended:3,attended_score:4,accRate:5,invested:6,capital:7}};
  var th = document.querySelectorAll('#headerRow th')[cols[key]];
  if (th) th.classList.add(sortDir === 'asc' ? 'sorted-asc' : 'sorted-desc');
  render();
}}

function render() {{
  var q    = document.getElementById('searchInput').value.trim().toLowerCase();
  var body = document.getElementById('tableBody');
  body.innerHTML = '';

  var filtered = eventData.filter(function(e) {{
    if (activeFilter === 'recent'  && !isRecent(e))  return false;
    if (activeFilter === 'pending' && !isPending(e)) return false;
    if (q && !e.date.includes(q)) return false;
    return true;
  }});

  var sorted = filtered.slice().sort(function(a, b) {{
    var av, bv;
    if      (sortKey === 'date')          {{ av = a.date;                                   bv = b.date; }}
    else if (sortKey === 'rsvps')         {{ av = a.rsvps;                                  bv = b.rsvps; }}
    else if (sortKey === 'attended')      {{ av = a.attended;                               bv = b.attended; }}
    else if (sortKey === 'attended_score'){{ av = a.attended_score;                         bv = b.attended_score; }}
    else if (sortKey === 'accRate')       {{ av = pct(a.accountCreated,a.attended) || -1;   bv = pct(b.accountCreated,b.attended) || -1; }}
    else if (sortKey === 'invested')      {{ av = a.invested;                               bv = b.invested; }}
    else if (sortKey === 'capital')       {{ av = a.capital;                                bv = b.capital; }}
    else                                  {{ av = a.date; bv = b.date; }}
    var cmp = av > bv ? 1 : av < bv ? -1 : 0;
    return sortDir === 'asc' ? cmp : -cmp;
  }});

  sorted.forEach(function(e, i) {{
    var pending   = isPending(e);
    var accRate   = pct(e.accountCreated, e.attended);
    var attCell   = pending ? '<span class="pending-badge">Pending</span>' : fmt(e.attended);
    var scoreCell = (pending || !e.attended)
      ? '<span style="color:#b0c4d8">—</span>'
      : '<span style="color:#4a90d9;font-weight:600;font-variant-numeric:tabular-nums">' + e.attended_score + '</span>';
    var accCell   = (pending || !e.attended)
      ? '<span style="color:#b0c4d8">—</span>'
      : accRate != null
        ? '<span style="color:#d4a96a;font-weight:600">' + accRate + '%</span>'
        : '<span style="color:#b0c4d8">—</span>';
    var invCell   = e.invested > 0
      ? '<span style="color:#6dbf82;font-weight:600;font-variant-numeric:tabular-nums">' + e.invested + '</span>'
      : '<span style="color:#b0c4d8">—</span>';
    var tr = document.createElement('tr');
    if (pending) tr.classList.add('pending-row');
    tr.innerHTML =
      '<td>' + (i+1) + '</td>' +
      '<td class="date-cell">' + formatDate(e.date) + '</td>' +
      '<td class="num">' + fmt(e.rsvps) + '</td>' +
      '<td class="num">' + attCell + '</td>' +
      '<td class="num">' + scoreCell + '</td>' +
      '<td class="num">' + accCell + '</td>' +
      '<td class="num">' + invCell + '</td>' +
      '<td class="num">' + fmtCap(e.capital) + '</td>';
    body.appendChild(tr);
  }});

  if (sorted.length === 0) {{
    var tr = document.createElement('tr');
    tr.innerHTML = '<td colspan="8" class="no-results">No events match this filter.</td>';
    body.appendChild(tr);
  }}

  var total = eventData.reduce(function(s,e){{return s+e.rsvps;}},0);
  document.getElementById('countBar').textContent =
    'Showing ' + sorted.length + ' of ' + eventData.length + ' events · ' + total.toLocaleString() + ' total RSVPs';

  updateStats(filtered);
  updateFunnel(filtered);
}}

function updateStats(evts) {{
  var src     = evts || eventData;
  var tracked = src.filter(function(e){{return !isPending(e);}});
  var rsvpBase      = tracked.reduce(function(s,e){{return s+e.rsvps;}},0);
  var totalRSVPs    = src.reduce(function(s,e){{return s+e.rsvps;}},0);
  var totalAttended = tracked.reduce(function(s,e){{return s+e.attended;}},0);
  var totalAccounts = src.reduce(function(s,e){{return s+e.accountCreated;}},0);
  var totalInvested = src.reduce(function(s,e){{return s+e.invested;}},0);
  var totalCapital  = src.reduce(function(s,e){{return s+e.capital;}},0);
  var totalAttScore = tracked.reduce(function(s,e){{return s+e.attended_score;}},0);
  document.getElementById('sEvents').textContent   = src.length;
  document.getElementById('sRSVPs').textContent    = fmt(totalRSVPs);
  document.getElementById('sAttended').textContent = fmt(totalAttended);
  document.getElementById('sAttRate').textContent  = pct(totalAttended,rsvpBase) != null ? pct(totalAttended,rsvpBase)+'%' : '—';
  document.getElementById('sAccounts').textContent = fmt(totalAccounts);
  document.getElementById('sAccRate').textContent  = pct(totalAccounts,totalAttended) != null ? pct(totalAccounts,totalAttended)+'%' : '—';
  document.getElementById('sInvested').textContent = fmt(totalInvested);
  document.getElementById('sCapital').textContent  = fmtCap(totalCapital);
  document.getElementById('sAttScore').textContent = fmt(totalAttScore);
}}

function updateFunnel(evts) {{
  var src     = evts || eventData;
  var tracked = src.filter(function(e){{return !isPending(e);}});
  var r = src.reduce(function(s,e){{return s+e.rsvps;}},0);
  var a = tracked.reduce(function(s,e){{return s+e.attended;}},0);
  var c = src.reduce(function(s,e){{return s+e.accountCreated;}},0);
  var rsvpBase = tracked.reduce(function(s,e){{return s+e.rsvps;}},0);
  document.getElementById('fRSVP').textContent = fmt(r);
  document.getElementById('fAtt').textContent  = fmt(a);
  document.getElementById('fAcc').textContent  = fmt(c);
  var attP = pct(a, rsvpBase), accP = pct(c, r);
  document.getElementById('fAttPct').textContent  = attP != null ? attP+'%' : '';
  document.getElementById('fAccPct').textContent  = accP != null ? accP+'%' : '';
  document.getElementById('fBarAtt').style.width  = attP != null ? attP+'%' : '0%';
  document.getElementById('fBarAcc').style.width  = accP != null ? accP+'%' : '0%';
}}

function triggerRefresh() {{
  var tok = localStorage.getItem('gh_pat');
  if (!tok) {{
    tok = prompt('Enter your GitHub personal access token to trigger a refresh.\\n(Saved in your browser — you only need to do this once.)');
    if (!tok) return;
    localStorage.setItem('gh_pat', tok.trim());
    tok = tok.trim();
  }}
  var btn = document.getElementById('refreshBtn');
  btn.disabled = true;
  btn.textContent = 'Updating…';
  fetch('https://api.github.com/repos/' + GITHUB_REPO + '/actions/workflows/' + GITHUB_WORKFLOW + '/dispatches', {{
    method: 'POST',
    headers: {{
      'Authorization': 'token ' + tok,
      'Accept': 'application/vnd.github.v3+json',
      'Content-Type': 'application/json'
    }},
    body: JSON.stringify({{ref: 'main'}})
  }}).then(function(r) {{
    if (r.status === 204) {{
      btn.textContent = 'Updating… (~45s)';
      setTimeout(function(){{ location.reload(); }}, 45000);
    }} else if (r.status === 401) {{
      localStorage.removeItem('gh_pat');
      btn.disabled = false;
      btn.textContent = '↻ Refresh';
      alert('Token invalid or expired. Click Refresh to enter a new one.');
    }} else {{
      btn.disabled = false;
      btn.textContent = '↻ Refresh';
    }}
  }}).catch(function() {{
    btn.disabled = false;
    btn.textContent = '↻ Refresh';
  }});
}}

document.getElementById('asOfDate').textContent =
  'Data as of ' + new Date('{today_str}').toLocaleDateString('en-US', {{month:'long',day:'numeric',year:'numeric'}});
document.querySelectorAll('#headerRow th')[1].classList.add('sorted-desc');
render();
</script>
</body>
</html>'''

# ─── SCORING PAGE ─────────────────────────────────────────────────────────────

def build_scoring_html(generated_at: str) -> str:
    def chips(items, color, bg):
        return ''.join(
            '<span style="display:inline-block;background:' + bg + ';color:' + color + ';'
            'border-radius:12px;padding:3px 10px;font-size:0.75rem;margin:3px 3px 3px 0;'
            'font-family:inherit;white-space:nowrap">' + escape(i) + '</span>'
            for i in items
        )

    def tier_card(score, label, color_fg, color_bg, body):
        return (
            '<div style="background:' + color_bg + ';border-left:4px solid ' + color_fg + ';border-radius:8px;'
            'padding:18px 22px;margin-bottom:16px">'
            '<div style="display:flex;align-items:center;gap:12px;margin-bottom:10px">'
            '<span style="background:' + color_fg + ';color:#fff;border-radius:50%;width:28px;height:28px;'
            'display:flex;align-items:center;justify-content:center;'
            'font-weight:700;font-size:0.9rem;flex-shrink:0">' + str(score) + '</span>'
            '<span style="font-weight:700;font-size:1rem;color:' + color_fg + '">' + label + '</span>'
            '</div>' + body + '</div>'
        )

    def rule(text):
        return '<div style="font-size:0.83rem;color:#3a5070;margin:5px 0 5px 12px">&bull; ' + text + '</div>'

    def section(title):
        return '<div style="font-size:0.7rem;font-weight:700;letter-spacing:0.1em;text-transform:uppercase;color:#8a9ab8;margin:12px 0 5px">' + title + '</div>'

    def chip_row(items, color, bg):
        return '<div style="margin:5px 0 5px 12px">' + chips(items, color, bg) + '</div>'

    high_titles_fmt = [t.title() for t in HIGH_TITLE_TERMS] + [
        'President (not Vice President)', 'Partner (equity/law/PE only)', 'Principal (non-technical)']
    wealth_terms_fmt = [t.title() for t in WEALTH_ADVISOR_TERMS]
    finance_cos_sample = sorted(FINANCE_COMPANIES)[:24]

    # Pre-compute tier card bodies to avoid nested f-strings (Python < 3.12 limitation)
    card5 = tier_card(5, 'High', '#1a7a45', '#eaf7f0',
        section('Auto-High: lifecycle / call status') +
        rule('Already invested (Order Completed call outcome)') +
        rule('Warm pipeline (lifecyclestage = Opportunity)') +
        section('Auto-High: elite email domains') +
        chip_row(sorted(FINANCE_DOMAINS), '#1a7a45', '#d4f0e0') +
        section('Auto-High: title signals') +
        chip_row(high_titles_fmt, '#1a7a45', '#d4f0e0') +
        section('Auto-High: company signals (sample)') +
        chip_row(finance_cos_sample, '#1a7a45', '#d4f0e0') +
        rule('+ PE firms, hedge funds, major banks, top law firms, VC firms') +
        section('Auto-High: profession') +
        rule('Physicians, surgeons, MDs (pitched on MMFC K-1 angle)')
    )

    card4 = tier_card(4, 'Medium-High', '#1a5fa8', '#e8f0fb',
        rule('VP, Director, SVP, EVP, AVP, Senior Director, Associate Director at any company') +
        rule('Real estate executives (SVP at Extell, Related, Brookfield, etc.)') +
        rule('Senior engineers at FAANG (RSU hedge angle)') +
        rule('Principal Engineer / Analyst / Developer (not High &mdash; technical, not investment-focused)') +
        rule('UN / senior government &mdash; Senior Director level only') +
        rule('Founder / Co-Founder <em>with no company listed</em> &mdash; can\'t verify scale') +
        rule('CEO of small or unverifiable business (can\'t confirm funded / scale)')
    )

    card3 = tier_card(3, 'Medium', '#8a6800', '#fdf6e3',
        rule('Solo practitioners / small law firm attorneys') +
        rule('Senior Manager, small business owner') +
        rule('No data + NYC zip code (assume local)') +
        rule('CEO of a funded startup (Series A+) &mdash; High only if verifiable')
    )

    card2 = tier_card(2, 'Low-Medium', '#b85a00', '#fdf0e8',
        rule('Real estate agents / realtors (commission-based, low liquid wealth)') +
        rule('Art world: dealers, brokers, advisors, consultants, all gallery staff &mdash; no exceptions') +
        rule('Music / entertainment industry workers: producers, programmers, curators, filmmakers, screenwriters') +
        rule('NFT / crypto / web3 focused') +
        rule('No Show (prior call) without other downgrade signals') +
        rule('No data + no location') +
        rule('Interns, entry-level, paralegals, assistants')
    )

    card1 = tier_card(1, 'Low', '#a83030', '#fde8e8',
        rule('Previously said Not Interested') +
        rule('Wealth advisors / financial advisors / private bankers &mdash; they refer clients, they don\'t invest personally') +
        chip_row(wealth_terms_fmt, '#a83030', '#fde0e0') +
        rule('No Show + other disqualifying signals (low title, art world, etc.)')
    )

    caps_html = (
        rule('<strong>Estimated NW $150K&ndash;$500K</strong> &rarr; max score 3 (Medium), even if High title or finance company') +
        rule('<strong>Estimated NW $50K&ndash;$200K</strong> &rarr; max score 2 (Low-Medium)') +
        rule('<strong>Founder / Co-Founder with no company</strong> &rarr; max score 4 (Medium-High) &mdash; can\'t verify scale') +
        rule('<strong>Owner / CEO of a local lifestyle business</strong> (salon, restaurant, caf&eacute;, juice bar, etc.) &rarr; max score 3 (Medium)') +
        '<div style="margin-top:10px;font-size:0.75rem;color:#8a9ab8">'
        'NW cap does not apply to: elite email domains (gs.com, jpmorgan.com, etc.) or confirmed physicians.'
        '</div>'
    )

    gen = escape(generated_at)
    return '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Masterworks &mdash; Lead Scoring Logic</title>
<style>
  * { box-sizing:border-box; margin:0; padding:0; }
  body { font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
         background:#f4f6fa;color:#2a3a52;min-height:100vh; }
  header { background:linear-gradient(135deg,#1b3c6e 0%,#2a5298 100%);
           padding:20px 40px;color:#fff; }
  .header-row { display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px; }
  .brand { font-size:0.7rem;letter-spacing:0.18em;text-transform:uppercase;opacity:0.55;margin-bottom:2px; }
  .title { font-size:1.35rem;font-weight:700;letter-spacing:0.01em; }
  .meta  { font-size:0.65rem;color:rgba(255,255,255,0.4);margin-top:2px; }
  .page-tabs { display:flex; margin-top:2px; border-top:1px solid rgba(255,255,255,0.1); }
  .page-tab { padding:10px 22px; font-size:0.7rem; letter-spacing:0.09em; text-transform:uppercase;
              text-decoration:none; color:rgba(255,255,255,0.42); border-bottom:2px solid transparent;
              transition:all 0.15s; white-space:nowrap; }
  .page-tab:hover { color:rgba(255,255,255,0.8); border-bottom-color:rgba(255,255,255,0.25); }
  .page-tab.active-tab { color:#fff; border-bottom-color:#c9a84c; font-weight:700; }
  main { max-width:860px;margin:36px auto;padding:0 24px 60px; }
  h2 { font-size:0.75rem;letter-spacing:0.14em;text-transform:uppercase;color:#6a80a0;
       margin:32px 0 14px;border-bottom:1px solid #dde4ef;padding-bottom:6px; }
  .note { background:#fff;border:1px solid #dde4ef;border-radius:8px;padding:14px 18px;
          font-size:0.82rem;color:#5a7090;line-height:1.6;margin-bottom:20px; }
  .caps { background:#fff;border:1px solid #dde4ef;border-radius:8px;padding:18px 22px; }
  .nw-grid { background:#fff;border:1px solid #dde4ef;border-radius:8px;padding:18px 22px;
             font-size:0.83rem;color:#3a5070;line-height:2; }
  .nw-grid-inner { display:grid;grid-template-columns:1fr 1fr;gap:4px 24px; }
  footer { background:#1b3c6e;color:#6a90be;text-align:center;font-size:0.7rem;
           letter-spacing:0.1em;text-transform:uppercase;padding:20px 40px; }
</style>
</head>
<body>
<header>
  <div class="header-row">
    <div>
      <div class="brand">Masterworks &middot; Outbound</div>
      <div class="title">Lead Scoring Logic</div>
      <div class="meta">Updated ''' + gen + '''</div>
    </div>
  </div>
  <div class="page-tabs">
    <a href="index.html"   class="page-tab">RSVP Dashboard</a>
    <a href="events.html"  class="page-tab">Event Dashboard</a>
    <a href="scoring.html" class="page-tab active-tab">Scoring Logic</a>
  </div>
</header>

<main>

<div class="note">
  Scores run <strong>1 (Low) &rarr; 5 (High)</strong> and represent investment likelihood based on
  estimated net worth, title, company, and other signals. Computed automatically from HubSpot
  data each morning at 8am ET. Questions or disagreements? Tell Ani directly.
</div>

<h2>Score Tiers</h2>
''' + card5 + card4 + card3 + card2 + card1 + '''
<h2>Score Caps (override everything above)</h2>
<div class="caps">''' + caps_html + '''</div>

<h2>NW Estimation Tiers</h2>
<div class="nw-grid"><div class="nw-grid-inner">
  <div>PE/HF partner, elite law partner, bank MD</div><div style="color:#1a7a45;font-weight:600">$3M&ndash;$10M</div>
  <div>VP/Director at major bank, startup CEO</div><div style="color:#1a7a45;font-weight:600">$2M&ndash;$6M</div>
  <div>C-suite mid-size, law associate, senior consultant</div><div style="color:#1a5fa8;font-weight:600">$1M&ndash;$4M</div>
  <div>Senior Manager, small business owner</div><div style="color:#8a6800;font-weight:600">$500K&ndash;$2M</div>
  <div>Manager / associate / junior</div><div style="color:#b85a00;font-weight:600">$150K&ndash;$500K</div>
  <div>Intern / entry-level</div><div style="color:#a83030;font-weight:600">$50K&ndash;$200K</div>
</div></div>

<h2>Investor Personas</h2>
<div style="overflow-x:auto">
<table style="width:100%;border-collapse:collapse;background:#fff;border-radius:8px;overflow:hidden;border:1px solid #dde4ef;font-size:0.8rem">
  <thead>
    <tr>
      <th style="background:#1b3c6e;color:#a8c8e8;font-size:0.68rem;letter-spacing:0.08em;text-transform:uppercase;padding:10px 14px;text-align:left;white-space:nowrap">Persona</th>
      <th style="background:#1b3c6e;color:#a8c8e8;font-size:0.68rem;letter-spacing:0.08em;text-transform:uppercase;padding:10px 14px;text-align:left;white-space:nowrap">% of Base</th>
      <th style="background:#1b3c6e;color:#a8c8e8;font-size:0.68rem;letter-spacing:0.08em;text-transform:uppercase;padding:10px 14px;text-align:left;white-space:nowrap">Avg Portfolio</th>
      <th style="background:#1b3c6e;color:#a8c8e8;font-size:0.68rem;letter-spacing:0.08em;text-transform:uppercase;padding:10px 14px;text-align:left;white-space:nowrap">Age Range</th>
      <th style="background:#1b3c6e;color:#a8c8e8;font-size:0.68rem;letter-spacing:0.08em;text-transform:uppercase;padding:10px 14px;text-align:left;white-space:nowrap">1st Investment</th>
      <th style="background:#1b3c6e;color:#a8c8e8;font-size:0.68rem;letter-spacing:0.08em;text-transform:uppercase;padding:10px 14px;text-align:left;white-space:nowrap">Calls to Close</th>
      <th style="background:#1b3c6e;color:#a8c8e8;font-size:0.68rem;letter-spacing:0.08em;text-transform:uppercase;padding:10px 14px;text-align:left;white-space:nowrap">Product</th>
      <th style="background:#1b3c6e;color:#a8c8e8;font-size:0.68rem;letter-spacing:0.08em;text-transform:uppercase;padding:10px 14px;text-align:left;white-space:nowrap">How to Spot</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;font-weight:700;color:#1a5fa8;white-space:nowrap">Corporate Climber</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">25&ndash;28%</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">$1.8M</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">42&ndash;56</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">$200&ndash;$5K</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">3+</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070;font-size:0.75rem">Individual &rarr; MMFC at $50K+</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#5a7090;font-size:0.75rem">Uses corporate language; mentions spouse/CPA; tiny first investment; asks about recession performance</td>
    </tr>
    <tr style="background:#f8fafd">
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;font-weight:700;color:#1a7a45;white-space:nowrap">Finance Bro</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">18&ndash;20%</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">$5.2M</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">38&ndash;55</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">$40K&ndash;$100K</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">2</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070;font-size:0.75rem">MMFC (default)</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#5a7090;font-size:0.75rem">Uses IRR/NAV/carry fluently; challenges thesis not asks for education; compares fees to PE 2/20</td>
    </tr>
    <tr>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;font-weight:700;color:#6a3a8a;white-space:nowrap">Business Owner</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">12&ndash;15%</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">$3.5M</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">38&ndash;68</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">$10K&ndash;$25K</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">1&ndash;2</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070;font-size:0.75rem">Individual &rarr; MMFC at $5M+</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#5a7090;font-size:0.75rem">Leads with their company; asks about LLC/trust investing; compares art to real estate; decides fast or defers indefinitely</td>
    </tr>
    <tr style="background:#f8fafd">
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;font-weight:700;color:#0a7a8a;white-space:nowrap">Tech Wealth Builder</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">~12%</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">$1.2M</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">28&ndash;44</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">$10K&ndash;$50K</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">1</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070;font-size:0.75rem">Individual (10&ndash;20 paintings)</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#5a7090;font-size:0.75rem">Pre-researches before the call; asks technical questions immediately; mentions RSU concentration; wants data not narratives</td>
    </tr>
    <tr>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;font-weight:700;color:#7a5a00;white-space:nowrap">Cautious Retiree</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">8&ndash;10%</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">$3M</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">58&ndash;80</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">$500&ndash;$1K/quarter</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">2&ndash;3</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070;font-size:0.75rem">Individual (small)</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#5a7090;font-size:0.75rem">Mentions spouse immediately; describes pension/Social Security; states very small amounts; relaxed about hold period</td>
    </tr>
    <tr style="background:#f8fafd">
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;font-weight:700;color:#a85800;white-space:nowrap">Everyday Investor</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">8&ndash;10%</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">$180K</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">30&ndash;65</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">$1K&ndash;$5K</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">2&ndash;4</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070;font-size:0.75rem">Individual only</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#5a7090;font-size:0.75rem">States they know nothing about art; mentions blue-collar trade; available evenings only; plain language questions</td>
    </tr>
    <tr>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;font-weight:700;color:#a83030;white-space:nowrap">Medical Pro</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">7&ndash;8%</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">$3.2M</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">40&ndash;64</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">$25K&ndash;$50K</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070">2&ndash;3</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#3a5070;font-size:0.75rem">MMFC at $100K+ (K-1 consolidation)</td>
      <td style="padding:10px 14px;border-bottom:1px solid #eef1f7;color:#5a7090;font-size:0.75rem">Mentions being between patients; references Goldman/Morgan adviser; asks about K-1 burden; may already collect art</td>
    </tr>
    <tr style="background:#f8fafd">
      <td style="padding:10px 14px;font-weight:700;color:#1a5fa8;white-space:nowrap">Young Diversifier</td>
      <td style="padding:10px 14px;color:#3a5070">~7%</td>
      <td style="padding:10px 14px;color:#3a5070">$285K</td>
      <td style="padding:10px 14px;color:#3a5070">24&ndash;34</td>
      <td style="padding:10px 14px;color:#3a5070">$500&ndash;$2K</td>
      <td style="padding:10px 14px;color:#3a5070">1&ndash;3</td>
      <td style="padding:10px 14px;color:#3a5070;font-size:0.75rem">Individual; MMFC if compliance-restricted</td>
      <td style="padding:10px 14px;color:#5a7090;font-size:0.75rem">Says &ldquo;it seems cool&rdquo;; knows finance but not art; mentions compliance/trading restrictions; small first investment</td>
    </tr>
  </tbody>
</table>
</div>

</main>
<footer>Masterworks Outbound &middot; Scores auto-generated from HubSpot data &middot; Last updated ''' + gen + '''</footer>
</body>
</html>'''


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    today = date.today()
    start = today - timedelta(days=DAYS_BACK)
    end   = today + timedelta(days=DAYS_AHEAD)

    _load_enrich_cache()
    print(f'Fetching RSVPs {start} → {end}  (DAYS_BACK={DAYS_BACK}, DAYS_AHEAD={DAYS_AHEAD})')
    contacts = fetch_contacts(start, end)
    print(f'Got {len(contacts)} contacts')

    # Enrich only today + future events — skip past events entirely.
    # Today's contacts go first to max out the quota on what matters most.
    today_iso = today.isoformat()
    contacts_to_enrich = sorted(
        [c for c in contacts
         if (c['properties'].get('outbound_rsvp_to_event') or '')[:10] >= today_iso],
        key=lambda c: (c['properties'].get('outbound_rsvp_to_event') or ''),
    )

    n_enriched = enrich_no_data_contacts(contacts_to_enrich)
    if n_enriched:
        print(f'Enriched {n_enriched} no-data contacts via Google Search')

    by_date = defaultdict(list)
    for c in contacts:
        d = c['properties'].get('outbound_rsvp_to_event')
        if d:
            by_date[d].append(c)

    print(f'Dates: {sorted(by_date.keys())}')

    now_str = datetime.now(timezone.utc).strftime('%b %-d, %Y at %-I:%M %p UTC')

    docs = Path('docs')
    docs.mkdir(parents=True, exist_ok=True)

    rsvp_html = build_html(dict(by_date), now_str)
    (docs / 'index.html').write_text(rsvp_html, encoding='utf-8')
    print(f'Written → docs/index.html  ({len(rsvp_html):,} bytes)')

    events_html = build_events_html(dict(by_date), now_str)
    (docs / 'events.html').write_text(events_html, encoding='utf-8')
    print(f'Written → docs/events.html  ({len(events_html):,} bytes)')

    scoring_html = build_scoring_html(now_str)
    (docs / 'scoring.html').write_text(scoring_html, encoding='utf-8')
    print(f'Written → docs/scoring.html  ({len(scoring_html):,} bytes)')

if __name__ == '__main__':
    main()
