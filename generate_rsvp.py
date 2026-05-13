#!/usr/bin/env python3
"""
generate_rsvp.py
Masterworks Outbound Events — RSVP Dashboard Generator
Queries HubSpot for contacts RSVPed to events within DAYS_BACK..DAYS_AHEAD,
scores them 1–5, and writes docs/index.html.
"""

import base64
import hashlib
import json
import os
import re
import sys
import time
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
# RocketReach replaces NinjaPear (Apr 2026): higher photo coverage on
# professional contacts because RocketReach pulls from LinkedIn primarily,
# not Twitter/X.
ROCKETREACH_API_KEY = os.environ.get('ROCKETREACH_API_KEY', '').strip()
CENSUS_API_KEY      = os.environ.get('CENSUS_API_KEY', '').strip()
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
    'managing director', 'managing member',
    'general partner', 'founding partner', 'managing partner', 'senior partner',
    'fund manager', 'portfolio manager',
    'chief executive', 'chief financial', 'chief operating', 'chief technology',
    'chief investment', 'chief information',
    'ceo', 'cfo', 'cto', 'coo', 'cio',
    # 'founder', 'co-founder' removed — handled separately with conservative default (low unless proven)
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
    # Nonprofits / charities — CEOs of nonprofits have low liquid wealth
    'nonprofit', 'non-profit', 'foundation', 'charity', 'charities',
    '501(c)', 'ngo', 'social services', 'community organization',
]

def is_small_biz(company: str) -> bool:
    co = company.lower()
    return any(term in co for term in SMALL_BIZ_INDICATORS)

# These override HIGH signals — wealth advisors refer clients but don't invest personally
WEALTH_ADVISOR_TERMS = [
    'wealth advisor', 'wealth management advisor', 'wealth management', 'wealth manager',
    'private banker', 'private client', 'private wealth',
    'financial advisor', 'financial planner',
    'investment advisor', 'personal banking advisor',
]

# Wealth-management firms we actively want to recruit as channel partners.
# Wealth advisor / financial planner titles AT these firms flip from DQ to HIGH —
# we want them at events as referral sources, not as direct investors.
TARGET_WEALTH_FIRMS = [
    'lpl financial',
    'raymond james',
    'jp morgan', 'jpmorgan',
    'morgan stanley',
]
def is_target_wealth_firm(company: str) -> bool:
    c = (company or '').lower()
    if any(f in c for f in TARGET_WEALTH_FIRMS):
        return True
    return bool(re.search(r'\blpl\b', c))

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
    # Property / asset management (RE service, not ownership)
    'property manager', 'property management', 'asset manager', 'building manager',
    # Analyst roles — income without significant accumulated wealth
    'data analyst', 'business analyst', 'research analyst', 'marketing analyst',
    'financial analyst', 'credit analyst', 'junior analyst',
]

# Time-for-money service providers — income tied to hours worked, not scalable assets
SERVICE_PROVIDER_TERMS = [
    # Freelance / solo / independent
    'freelancer', 'freelance consultant', 'freelance designer', 'freelance writer',
    'freelance photographer', 'freelance videographer', 'freelance developer',
    'self-employed', 'independent contractor', 'sole proprietor', 'solopreneur',
    'independent consultant', 'independent advisor',
    # Coaching (non-corporate context)
    'life coach', 'business coach', 'executive coach', 'career coach',
    'health coach', 'wellness coach', 'dating coach', 'mindset coach',
    'leadership coach', 'performance coach',
    # Personal services
    'personal chef', 'private chef',
]

# Creators / influencers — income rarely translates to investable wealth at scale
CREATOR_TERMS = [
    'content creator', 'influencer', 'youtuber', 'tiktoker', 'tiktok creator',
    'blogger', 'podcaster', 'vlogger', 'brand ambassador',
    'social media creator', 'social media influencer',
]

FINANCE_COMPANIES = [
    'goldman sachs', 'morgan stanley', 'jp morgan', 'jpmorgan',
    'blackrock', 'blackstone', 'kkr', 'kohlberg kravis', 'carlyle group', 'apollo global',
    'citadel', 'bridgewater', 'two sigma', 'renaissance technologies',
    'pimco', 'vanguard', 'fidelity investments', 'merrill lynch',
    'ubs', 'credit suisse', 'deutsche bank', 'barclays',
    'wells fargo', 'citigroup', 'citibank', 'bank of america',
    'neuberger berman', 'alliancebernstein', 'td securities', 'scotiabank', 'royal bank of canada', 'rbc', 'cibc', 'bmo', 'bank of montreal',
    'lazard', 'evercore', 'jefferies', 'raymond james',
    'point72', 'millennium management', 'tiger global', 'coatue',
    'andreessen horowitz', 'sequoia capital', 'general atlantic',
    'warburg pincus', 'bain capital', 'cerberus capital', 'oaktree capital',
    'silver lake', 'skadden', 'kirkland & ellis', 'weil gotshal',
    'latham & watkins', 'sullivan & cromwell', 'debevoise & plimpton',
    'simpson thacher', 'cleary gottlieb', 'paul weiss', 'cravath',
    'white & case', 'proskauer', 'sidley austin', 'davis polk',
    # Major credit / multi-strat / quant funds
    'waterfall asset', 'schonfeld', 'sculptor capital', 'glenview capital',
    'canyon partners', 'king street capital', 'anchorage capital',
    'baupost', 'elliot management', 'paul singer',
]

TRI_STATE = {'ny', 'nj', 'ct'}

# Manual score overrides — map contact ID (string) → score (1–5).
# Applied at render time; data-auto still reflects the computed score.
SCORE_OVERRIDES: dict[str, int] = {
    '108755993716': 3,   # Anthony Rodriguez (ARC Excess & Surplus) — 4/15 override
    '129030882905': 3,   # Mark D'Alonzo — 4/15 override
}

# 5 = highest, 1 = lowest
SCORES = [5, 4, 3, 2, 1]
SCORE_LABELS = {5: 'High', 4: 'Medium-High', 3: 'Medium', 2: 'Low-Medium', 1: 'Low'}
SCORE_COLORS = {
    5: ('#ffffff', '#0a2f6b'),   # white on navy
    4: ('#ffffff', '#1a5fa8'),   # white on dark blue
    3: ('#1a5fa8', '#a3c9f0'),   # dark blue on sky
    2: ('#2b6cb0', '#d4e6f9'),   # blue on pale blue
    1: ('#6a9fd8', '#edf4fc'),   # light blue on near-white
}

# DQ / QP tag colors — applied to future-event RSVPs in addition to the score pill.
# QP = Qualified Person, DQ = should be Disqualified, UNCERTAIN = manual-review needed.
TAG_STYLES = {
    'QP':        ('#1a7a45', '#eaf7f0'),  # green
    'DQ':        ('#a83030', '#fde8e8'),  # red
    'UNCERTAIN': ('#8a6800', '#fdf6e3'),  # amber
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

ENRICH_CACHE_FILE = Path('enrich_cache.json')
_enrich_cache: dict = {}      # loaded from file at start of main()
_quota_exhausted: bool = False  # flip to True on first 429 — stops all further queries
_api_calls: int = 0           # total Google API calls this run; capped at ENRICH_LIMIT

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


def google_enrich(name: str, company_hint: str = '', domain: str = '',
                  email: str = '', linkedin_url: str = '') -> dict:
    """Search for a person's title/company using multiple strategies, stopping at
    the first successful result.  Query priority (most → least targeted):
      1. Specific LinkedIn profile URL from Pipl/HubSpot
      2. Name + company hint on LinkedIn (work email domain)
      3. Generic LinkedIn name search (num=5)
      4. Name on company's own website (work email domain)
      5. Raw email address search (last resort)
    """
    global _quota_exhausted, _api_calls
    if _quota_exhausted or not GOOGLE_API_KEY or not GOOGLE_CSE_ID or not name.strip():
        return {}
    if name in _enrich_cache:
        return _enrich_cache[name]

    queries = []

    # 1. Specific LinkedIn profile — most targeted, burns only 1 call
    if linkedin_url:
        li_path = re.search(r'linkedin\.com(/in/[^/?#\s]+)', linkedin_url)
        if li_path:
            queries.append(f'site:linkedin.com{li_path.group(1)}')

    # 2. Name + company on LinkedIn (work email gives us the company)
    if company_hint:
        queries.append(f'"{name}" "{company_hint}" site:linkedin.com')

    # 3. Generic LinkedIn search
    queries.append(f'"{name}" site:linkedin.com/in')

    # 4. Name on their company's own website
    if domain:
        queries.append(f'"{name}" site:{domain}')

    # 5. Email address — catches conference bios, company pages, etc.
    if email:
        queries.append(f'"{email}"')

    for q in queries:
        if _quota_exhausted or _api_calls >= ENRICH_LIMIT:
            _quota_exhausted = True
            break
        try:
            _api_calls += 1
            resp = requests.get(
                GOOGLE_SEARCH_URL,
                params={'key': GOOGLE_API_KEY, 'cx': GOOGLE_CSE_ID, 'q': q, 'num': 5},
                timeout=10,
            )
            if resp.status_code == 429:
                print('  Google quota exhausted for today — stopping enrichment', file=sys.stderr)
                _quota_exhausted = True
                break
            if not resp.ok:
                print(f'  Google search error {resp.status_code} for "{name}"', file=sys.stderr)
                continue

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

        except Exception as e:
            print(f'  Google search exception for "{name}": {e}', file=sys.stderr)

    _enrich_cache[name] = {}
    return {}


MANUAL_ENRICHMENTS_FILE = Path('manual_enrichments.json')


def _load_manual_enrichments() -> dict:
    """Load manual enrichments keyed by lowercase email."""
    if MANUAL_ENRICHMENTS_FILE.exists():
        try:
            entries = json.loads(MANUAL_ENRICHMENTS_FILE.read_text(encoding='utf-8'))
            return {e['email'].lower(): e for e in entries if e.get('email')}
        except Exception:
            return {}
    return {}


def _apply_manual_enrichments(contacts: list) -> int:
    """Apply manual_enrichments.json to contacts, write to HubSpot if needed."""
    manual = _load_manual_enrichments()
    if not manual:
        return 0
    applied = 0
    for c in contacts:
        p = c['properties']
        email = (p.get('email') or '').strip().lower()
        if email not in manual:
            continue
        entry = manual[email]
        patches = {}
        if entry.get('jobtitle') and not p.get('jobtitle'):
            patches['jobtitle'] = entry['jobtitle']
        if entry.get('company') and not p.get('company'):
            patches['company'] = entry['company']
        if patches:
            c['properties'] = {**p, **patches}
            applied += 1
            fname = (p.get('firstname') or '').strip()
            lname = (p.get('lastname') or '').strip()
            print(f'  Manual: {fname} {lname} → {patches}')
            _patch_hubspot_contact(c['id'], patches)
    return applied


def enrich_no_data_contacts(contacts: list) -> int:
    """For contacts missing title or company, try Google enrichment.
    Returns number of contacts enriched."""
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        return 0

    # Clear stale empty-miss cache entries for upcoming contacts so they're
    # retried daily until we find data. Successful enrichments stay cached forever.
    for c in contacts:
        p     = c['properties']
        fname = p.get('firstname') or ''
        lname = p.get('lastname')  or ''
        name  = f'{fname} {lname}'.strip()
        if name and _enrich_cache.get(name) == {}:
            del _enrich_cache[name]

    enriched = 0
    for c in contacts:
        if _quota_exhausted:
            break
        p    = c['properties']
        if p.get('jobtitle') and p.get('company'):
            continue   # has both — skip

        fname = p.get('firstname') or ''
        lname = p.get('lastname')  or ''
        name  = f'{fname} {lname}'.strip()
        if not name:
            continue

        # Apply from cache if already looked up successfully
        if name in _enrich_cache:
            cached = _enrich_cache[name]
            if cached:
                c['properties'] = {**p, **cached}
                enriched += 1
                # Write to HubSpot if fields are still blank (handles failed/missed prior writes)
                _patch_hubspot_contact(c['id'], {
                    k: v for k, v in cached.items()
                    if k in ('jobtitle', 'company') and v and not p.get(k)
                })
            continue   # either way, don't make an API call

        # New name — run multi-strategy enrichment (quota tracked inside google_enrich)
        email       = p.get('email', '')
        dom         = email_domain(email)
        personal    = dom in PERSONAL_DOMAINS
        co_hint     = domain_to_company(email) if not personal else ''
        li_url_hint = (
            p.get('pipl_linkedin') or p.get('hs_linkedin_url') or
            p.get('outbound_team___linkedin_url') or p.get('linkedin_personal_url') or ''
        ).strip()
        result = google_enrich(
            name,
            company_hint = co_hint,
            domain       = dom if not personal else '',
            email        = email if not personal else '',
            linkedin_url = li_url_hint,
        )
        if result:
            c['properties'] = {**p, **result}
            enriched += 1
            print(f'  Enriched: {name} → {result.get("jobtitle", "")} @ {result.get("company", "")}')
            # Write back to HubSpot — only fill blank fields, never overwrite
            _patch_hubspot_contact(c['id'], {
                k: v for k, v in result.items()
                if k in ('jobtitle', 'company') and v and not p.get(k)
            })

    _save_enrich_cache()
    return enriched


def _patch_hubspot_contact(contact_id: str, properties: dict):
    """PATCH a HubSpot contact with the given properties."""
    if not HUBSPOT_TOKEN or not properties:
        return
    try:
        resp = requests.patch(
            f'https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}',
            headers={'Authorization': f'Bearer {HUBSPOT_TOKEN}', 'Content-Type': 'application/json'},
            json={'properties': properties},
            timeout=10,
        )
        if resp.ok:
            print(f'  HubSpot updated contact {contact_id}: {properties}')
        else:
            print(f'  HubSpot patch failed for {contact_id}: HTTP {resp.status_code}', file=sys.stderr)
    except Exception as e:
        print(f'  HubSpot patch exception for {contact_id}: {e}', file=sys.stderr)


# ─── ROCKETREACH PHOTO ENRICHMENT ─────────────────────────────────────────────
# RocketReach replaces NinjaPear (which sourced photos from Twitter/X — too
# sparse on professional contacts). RocketReach pulls primarily from LinkedIn,
# giving much higher coverage.
#
# Inputs we can pass natively (no website-lookup detour):
#   1. linkedin_url (best — when HubSpot has hs_linkedin_url etc.)
#   2. name + current_employer
#   3. email
#
# Cost behavior: lookup credits are charged ONLY when a verified profile is
# returned. 404/failed lookups are free. We still cache misses to avoid
# re-attempting the same contact daily.
#
# Async behavior: /person/lookup may return status="searching"/"waiting"/
# "progress" if data needs a fresh crawl. We poll /person/checkStatus for
# up to ~30s before giving up.

ROCKETREACH_LOOKUP_ENDPOINT = 'https://api.rocketreach.co/api/v2/person/lookup'
ROCKETREACH_STATUS_ENDPOINT = 'https://api.rocketreach.co/api/v2/person/checkStatus'


def _clean_company_name(name: str) -> str:
    """Strip surrounding quotes/whitespace from a company name (HubSpot data is messy)."""
    return (name or '').strip().strip('"').strip("'").strip()


def _rocketreach_get_photo(*, linkedin_url: str = '', name: str = '',
                           current_employer: str = '', email: str = ''):
    """Call RocketReach /person/lookup, poll if needed, return profile_pic URL or None.

    Lookup credits are charged only on success (verified data). Failed lookups
    return None and cost nothing.
    """
    if not ROCKETREACH_API_KEY:
        return None

    params = {}
    if linkedin_url:
        params['linkedin_url'] = linkedin_url
    elif name and current_employer:
        params['name'] = name
        params['current_employer'] = current_employer
    elif email:
        params['email'] = email
    else:
        return None  # No valid input combination

    headers = {'Api-Key': ROCKETREACH_API_KEY}

    try:
        r = requests.get(ROCKETREACH_LOOKUP_ENDPOINT, headers=headers,
                         params=params, timeout=30)
        if r.status_code == 401:
            print('  RocketReach: invalid API key (401)', file=sys.stderr)
            return None
        if r.status_code == 404:
            return None
        if r.status_code == 429:
            print('  RocketReach: rate-limited (429), skipping', file=sys.stderr)
            return None
        if not r.ok:
            print(f'  RocketReach HTTP {r.status_code} for {params}', file=sys.stderr)
            return None

        data = r.json()
        # If data is already complete, return the photo immediately
        if data.get('profile_pic'):
            return data['profile_pic']

        status = (data.get('status') or '').lower()
        profile_id = data.get('id')
        if status == 'failed' or status == 'complete':
            # complete-without-photo means the person has no photo
            return None
        if not profile_id:
            return None  # nothing to poll

        # Poll /checkStatus until complete or we time out (~30s).
        for _attempt in range(6):
            time.sleep(5)
            try:
                pr = requests.get(
                    ROCKETREACH_STATUS_ENDPOINT,
                    headers=headers,
                    params={'ids': str(profile_id)},
                    timeout=15,
                )
                if not pr.ok:
                    continue
                results = pr.json()
                items = results if isinstance(results, list) else [results]
                for item in items:
                    if str(item.get('id')) != str(profile_id):
                        continue
                    s = (item.get('status') or '').lower()
                    if s == 'complete':
                        return item.get('profile_pic') or None
                    if s == 'failed':
                        return None
                    # else still pending — keep polling
            except Exception:
                continue

        print(f'  RocketReach: timed out polling profile {profile_id}', file=sys.stderr)
        return None
    except Exception as e:
        print(f'  RocketReach error for {params}: {e}', file=sys.stderr)
        return None


def proxycurl_enrich_photos(contacts: list) -> int:
    """Populate linkedin_image_url for contacts that lack it. Returns count enriched.

    Function name preserved for caller compatibility, but uses RocketReach
    under the hood. Successful lookups are written back to HubSpot so future
    daily runs read the photo from HubSpot for free.
    """
    if not ROCKETREACH_API_KEY:
        return 0

    enriched = 0
    for c in contacts:
        p = c['properties']
        # Skip if HubSpot already has a photo URL
        if (p.get('linkedin_image_url') or '').strip():
            continue

        # Cache by HubSpot contact ID — stable across input paths
        cache_key = f'rocketreach_photo:{c["id"]}'
        if cache_key in _enrich_cache:
            cached = _enrich_cache[cache_key]
            if cached:
                p['linkedin_image_url'] = cached
                enriched += 1
            continue   # cached miss → don't retry

        # Gather every input we have, in priority order
        first = (p.get('firstname') or '').strip()
        last  = (p.get('lastname')  or '').strip()
        if '@' in first:   # junk first-names that are actually emails
            first = ''
        full_name = f'{first} {last}'.strip()
        company   = _clean_company_name(p.get('company') or '')
        email     = (p.get('email') or '').strip().lower()
        is_personal = email_domain(email) in PERSONAL_DOMAINS
        li_url = (
            p.get('hs_linkedin_url') or p.get('linkedin_personal_url') or
            p.get('outbound_team___linkedin_url') or p.get('pipl_linkedin') or ''
        ).strip()

        # Choose the best path: LinkedIn URL > name+company > email
        photo_url = None
        if li_url:
            # Path A: linkedin_url — most accurate
            photo_url = _rocketreach_get_photo(linkedin_url=li_url)
        elif full_name and company:
            # Path B: name + current employer
            photo_url = _rocketreach_get_photo(name=full_name, current_employer=company)
        elif email and not is_personal:
            # Path C: work email
            photo_url = _rocketreach_get_photo(email=email)
        else:
            # No valid input → cache miss to skip future runs
            _enrich_cache[cache_key] = None
            continue

        _enrich_cache[cache_key] = photo_url
        if photo_url:
            p['linkedin_image_url'] = photo_url
            enriched += 1
            print(f'  RocketReach: photo found for contact {c["id"]} ({full_name})')
            # Write back to HubSpot so the next run gets it for free
            _patch_hubspot_contact(c['id'], {'linkedin_image_url': photo_url})

    _save_enrich_cache()
    return enriched


# Back-compat alias for any callers still importing the old name
_proxycurl_get_photo = _rocketreach_get_photo


# ─── NYC PLUTO PROPERTY LOOKUP ────────────────────────────────────────────────

PLUTO_URL = 'https://data.cityofnewyork.us/resource/64uk-42ks.json'

# Zip ranges that are definitively NYC boroughs
_NYC_ZIP_RANGES = [
    (10001, 10282, 1),  # Manhattan
    (10301, 10314, 5),  # Staten Island
    (10451, 10475, 2),  # Bronx
    (11004, 11109, 4),  # Queens (south/far)
    (11201, 11256, 3),  # Brooklyn
    (11351, 11697, 4),  # Queens (north/central)
]

_NYC_CITY_TO_BORO = {
    'new york': 1, 'manhattan': 1, 'new york city': 1, 'nyc': 1,
    'bronx': 2, 'the bronx': 2,
    'brooklyn': 3,
    'queens': 4, 'flushing': 4, 'astoria': 4, 'jamaica': 4, 'bayside': 4,
    'long island city': 4, 'lic': 4, 'jackson heights': 4, 'forest hills': 4,
    'ridgewood': 4, 'woodside': 4, 'sunnyside': 4, 'rego park': 4,
    'staten island': 5,
}

def _nyc_boro(city: str, zip_code: str) -> int | None:
    """Return NYC borough code (1–5) or None if not NYC."""
    city_l = city.lower().strip() if city else ''
    for name, code in _NYC_CITY_TO_BORO.items():
        if name in city_l:
            return code
    if zip_code and zip_code.isdigit():
        z = int(zip_code)
        for lo, hi, code in _NYC_ZIP_RANGES:
            if lo <= z <= hi:
                return code
    return None

# Addresses manually confirmed as commercial — checked before PLUTO/census.
# Normalized form (uppercase, unit stripped). Add new entries as needed.
COMMERCIAL_OVERRIDES = {
    '2 COLUMBUS CIRCLE',
    '150 E 42 ST',
    '62 CHELSEA PIERS',
    '437 MADISON AVE',
    '6 EAST 46 STREET',
    '6 E 46 ST',
}

_COMMERCIAL_KEYWORDS = re.compile(
    r'\b(ste|suite|floor|fl)\s+\S+', re.IGNORECASE)


def _pluto_strip_unit(address: str) -> str:
    """Strip apt/unit suffix and normalize address for PLUTO token matching."""
    addr = address.strip()
    # Strip trailing comma and anything after (city/state)
    addr = addr.split(',')[0].strip()
    # Remove named unit keywords with their value: "Apt 4B", "Suite 1605", "Fl 3", "#401"
    addr = re.sub(r'[\s,]+(apt|apartment|unit|ste|suite|fl|floor|ph|penthouse|rm|room|#)\s*\S*$',
                  '', addr, flags=re.IGNORECASE)
    # Remove trailing bare number that's a unit/suite (e.g. "41 Varick Ave 401" → "41 Varick Ave")
    # Only strip if there's already a house number at the start
    addr = re.sub(r'^(\d+\s+\S.+\s)\d+$', r'\1', addr.strip()).strip()
    # Strip letter suffix from house number: "11a Main St" → "11 Main St"
    addr = re.sub(r'^(\d+)[a-z]\s', lambda m: m.group(1) + ' ', addr, flags=re.IGNORECASE)
    # Strip ordinal suffixes: "90th" → "90", "1st" → "1"
    addr = re.sub(r'\b(\d+)(st|nd|rd|th)\b', r'\1', addr, flags=re.IGNORECASE)
    return addr.strip().upper()

def fetch_pluto_value(address: str, city: str, zip_code: str) -> str | None:
    """Query NYC Open Data PLUTO for a property value estimate.
    Returns display string like '$1.2M – $1.8M' or None if not NYC / not found."""
    boro = _nyc_boro(city, zip_code)
    if not boro:
        return None

    normalized = _pluto_strip_unit(address)
    if not normalized:
        return None

    if normalized in COMMERCIAL_OVERRIDES:
        return 'Commercial'

    cache_key = f'pluto:{normalized}:{zip_code or boro}'
    if cache_key in _enrich_cache:
        return _enrich_cache[cache_key]  # None = confirmed miss

    # Parse house number for prefix search
    m = re.match(r'^(\d+)', normalized)
    if not m:
        _enrich_cache[cache_key] = None
        return None
    house_num = m.group(1)

    try:
        where = f"zipcode='{zip_code}' AND address like '{house_num}%'" if zip_code \
                else f"borocode='{boro}' AND address like '{house_num}%'"
        resp = requests.get(PLUTO_URL, params={'$where': where, '$limit': 5}, timeout=10)
        if not resp.ok:
            print(f'  PLUTO {resp.status_code} for "{normalized}"', file=sys.stderr)
            _enrich_cache[cache_key] = None
            return None

        rows = resp.json()
        if not rows:
            _enrich_cache[cache_key] = None
            return None

        # Pick best match by token overlap (handles PLUTO full words vs. abbreviations)
        # e.g. normalized="215 W 90 ST" vs PLUTO "215 WEST 90 STREET" → tokens {"215","90"} match
        norm_tokens = set(normalized.split())

        def _addr_match_score(row_addr: str) -> int:
            row_tokens = set(re.sub(r'\b(\d+)(ST|ND|RD|TH)\b', r'\1',
                                    row_addr.upper().strip()).split())
            return len(norm_tokens & row_tokens)

        rows = sorted(rows, key=lambda r: _addr_match_score(r.get('address', '')), reverse=True)
        r = rows[0]

        assess_tot  = float(r.get('assesstot') or 0)
        bldg_class_full = (r.get('bldgclass') or '').upper().strip()
        bldg_class  = bldg_class_full[:1]
        units_total = max(int(r.get('unitstotal') or 1), 1)

        if assess_tot <= 0:
            _enrich_cache[cache_key] = None
            return None

        # Tag non-residential: first-letter check, plus full codes like O1-O9 (offices),
        # K1-K9 (stores), etc. that PLUTO sometimes miscategorizes
        if bldg_class not in ('A', 'B', 'C', 'D', 'R', 'S'):
            _enrich_cache[cache_key] = 'Commercial'
            return 'Commercial'

        # NYC property tax ratios by unit count:
        # Tax Class 1 (1–3 units): assessed ≈ 6% of market value
        # Tax Class 2 (4+ units): assessed ≈ 45% of market value (building total)
        if units_total <= 3:
            market = assess_tot / 0.06
        else:
            building_market = assess_tot / 0.45
            # Divide by units for a rough per-unit estimate
            market = building_market / units_total

        # Per-unit value >$6M is almost certainly a misclassified commercial building
        if market > 6_000_000:
            _enrich_cache[cache_key] = 'Commercial'
            print(f'  PLUTO {normalized}: Commercial (per-unit ${market:,.0f} > $6M threshold)')
            return 'Commercial'

        def _fmt(v: float) -> str:
            return f'${v / 1_000_000:.1f}M' if v >= 1_000_000 else f'${round(v / 1_000)}K'

        result = f'{_fmt(market * 0.8)} – {_fmt(market * 1.2)}'
        _enrich_cache[cache_key] = result
        print(f'  PLUTO {normalized}: {result} (class {bldg_class}, units {units_total}, assessed ${assess_tot:,.0f})')
        return result

    except Exception as e:
        print(f'  PLUTO exception for "{normalized}": {e}', file=sys.stderr)
        _enrich_cache[cache_key] = None
        return None


def fetch_census_value(zip_code: str) -> str | None:
    """Query Census ACS5 for median home value by zip. Returns e.g. '$580K median'."""
    if not zip_code or not CENSUS_API_KEY:
        return None
    cache_key = f'census:{zip_code}'
    if cache_key in _enrich_cache:
        return _enrich_cache[cache_key]
    try:
        r = requests.get(
            'https://api.census.gov/data/2022/acs/acs5',
            params={'get': 'B25077_001E', 'for': f'zip code tabulation area:{zip_code}',
                    'key': CENSUS_API_KEY},
            timeout=10)
        if not r.ok:
            _enrich_cache[cache_key] = None
            return None
        data = r.json()
        val = int(data[1][0])
        if val <= 0:
            _enrich_cache[cache_key] = None
            return None
        fmt = f'${val / 1_000_000:.1f}M' if val >= 1_000_000 else f'${round(val / 1_000)}K'
        result = f'{fmt} median'
        _enrich_cache[cache_key] = result
        print(f'  Census {zip_code}: {result}')
        return result
    except Exception as e:
        print(f'  Census exception for {zip_code}: {e}', file=sys.stderr)
        _enrich_cache[cache_key] = None
        return None


def pluto_enrich_contacts(contacts: list) -> int:
    """Look up property values: PLUTO for NYC, Census ACS fallback for others."""
    count = 0
    for c in contacts:
        p       = c['properties']
        address  = (p.get('address')  or '').strip()
        city     = (p.get('city')     or '').strip()
        zip_code = (p.get('zip')      or '').strip()
        if not address and not zip_code:
            continue
        if _COMMERCIAL_KEYWORDS.search(address):
            val = 'Commercial'
        elif address:
            val = fetch_pluto_value(address, city, zip_code)
        else:
            val = None
        if val is None and zip_code:
            val = fetch_census_value(zip_code)
        if val is not None:
            p['_pluto_val'] = val
            count += 1
    _save_enrich_cache()
    return count


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
                'outbound_event_attendee_disqualified', 'unknown_rsvp',
                'outbound_event_send_confirmation',
                'admin_url', 'totalamountpurchased', 'createdate',
                'hs_v2_date_entered_current_stage',
                'wealth_segment', 'inferred_income', 'address',
                'hs_linkedin_url', 'linkedin_personal_url', 'outbound_team___linkedin_url', 'pipl_linkedin',
                'linkedin_image_url',
                'hs_email_open', 'hs_email_delivered', 'hs_email_first_reply_date',
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

def domain_to_company(email: str) -> str:
    """Convert a work email domain to a readable company name.
    Returns '' for personal/generic domains or if no email provided."""
    dom = email_domain(email)
    if not dom or dom in PERSONAL_DOMAINS:
        return ''
    # Strip TLD(s) and any leading 'mail.' / 'em.' subdomains
    host = dom.split('.')[0]
    if host in ('mail', 'em', 'smtp', 'send', 'info'):
        parts = dom.split('.')
        host = parts[1] if len(parts) > 2 else parts[0]
    # Convert kebab/camel/run-together to title case words
    # e.g. redballoonsecurity → Red Balloon Security, tugboatusa → Tugboat Usa
    words = re.sub(r'([a-z])([A-Z])', r'\1 \2', host)   # camelCase split
    words = words.replace('-', ' ').replace('_', ' ')
    return words.title()

_DOMAIN_ALIASES = {
    'bofa': ['bank of america', 'bofa', 'merrill'],
    'baml': ['bank of america', 'merrill lynch'],
    'gs': ['goldman sachs', 'goldman'],
    'ms': ['morgan stanley'],
    'jpm': ['jpmorgan', 'jp morgan', 'chase'],
    'jpmc': ['jpmorgan', 'jp morgan', 'chase'],
    'citi': ['citigroup', 'citibank', 'citi'],
    'ubs': ['ubs'],
    'rbc': ['rbc', 'royal bank'],
    'db': ['deutsche bank'],
    'cs': ['credit suisse'],
    'hsbc': ['hsbc'],
    'bny': ['bny mellon', 'bank of new york'],
    'ml': ['merrill lynch', 'merrill'],
    'wf': ['wells fargo'],
    'wellsfargo': ['wells fargo'],
    'barclays': ['barclays'],
    'lazard': ['lazard'],
    'evercore': ['evercore'],
    'pjt': ['pjt partners'],
    'moelis': ['moelis'],
}

def _email_company_mismatch(email: str, company: str) -> bool:
    """True when a work-email domain doesn't match the HubSpot company name."""
    dom = email_domain(email)
    if not dom or not company or dom in PERSONAL_DOMAINS:
        return False
    host = dom.split('.')[0].lower()
    if host in ('mail', 'em', 'smtp', 'send', 'info'):
        parts = dom.split('.')
        host = parts[1].lower() if len(parts) > 2 else host
    co = re.sub(r'[^a-z0-9]', '', company.lower())
    if not co or not host:
        return False
    if host in co or co in host:
        return False
    # known abbreviation aliases
    co_lower = company.lower()
    if host in _DOMAIN_ALIASES:
        if any(a in co_lower for a in _DOMAIN_ALIASES[host]):
            return False
    # check if any company word (3+ chars) appears in the domain
    for w in re.split(r'[\s\-&,./]+', co_lower):
        if len(w) >= 3 and w in host:
            return False
    # check if domain parts appear in company
    for w in re.split(r'[\-_]+', host):
        if len(w) >= 3 and w in co:
            return False
    return True


def has_high_title(title: str) -> bool:
    tl = title.lower().strip()
    t  = ' ' + tl + ' '
    for term in HIGH_TITLE_TERMS:
        if (' ' + term + ' ') in t:
            return True
    # Any "Chief X Officer" title (Chief Insurance Officer, Chief People Officer, etc.)
    if re.search(r'\bchief\b.+\bofficer\b', tl):
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
    hosp_terms = ['medical center', 'health system', 'northwell', 'nyu langone', 'mount sinai']
    if any(t in company for t in hosp_terms):
        return True
    # 'hospital' check — must not match 'hospitality'
    co_words = company.replace(',', ' ').split()
    if any(w.startswith('hospital') and w not in ('hospitality', 'hospitalier') for w in co_words):
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
    # Exception: at target wealth-management firms (LPL, Raymond James, JPM, MS),
    # flip the DQ — we recruit these as channel partners.
    is_wealth_adv = any(t in combined for t in WEALTH_ADVISOR_TERMS)
    if is_wealth_adv:
        if is_target_wealth_firm(company):
            flags.append('target_wealth_firm')
            return 5, flags
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
    ]) or 'fine art' in company  # catches "Wellington Fine Art", "X Fine Art Gallery", etc.
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

    # ── Time-for-money service providers → Low-Medium ────────────────────────
    # Freelancers, solo coaches, independent contractors rarely have investable assets.
    is_service = any(t in combined for t in SERVICE_PROVIDER_TERMS)
    if is_service:
        return 1 if 'no_show' in flags else 2, flags

    # ── Creators / influencers → Low-Medium ──────────────────────────────────
    is_creator = any(t in combined for t in CREATOR_TERMS)
    if is_creator:
        return 1 if 'no_show' in flags else 2, flags

    # ── Generic consultant/coach/advisor with no company → Low-Medium ─────────
    # A named-firm consultant (Bain, Deloitte, etc.) stays in play; solo = service provider.
    if not company.strip() and email_domain(email) not in FINANCE_DOMAINS:
        if any(t in title for t in ['consultant', 'coach', 'advisor', 'adviser']):
            if not any(fc in title for fc in ['management consulting', 'strategy']):
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
    _at_finance_co = any(fc in company for fc in FINANCE_COMPANIES)
    if email_domain(email) in FINANCE_DOMAINS:
        sc = 5
    elif is_physician(title, email, company):
        sc = 5
    elif has_high_title(title):
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
        if _at_finance_co:
            sc = 4
        elif re_exec_co and re_exec_title:
            sc = 4
        elif medium_high:
            sc = 4
        else:
            sc = 3

    # ── Small biz cap — founders/CEOs of juice shops, salons, etc. → Medium ───
    if sc > 3 and is_small_biz(company) and 'invested' not in flags and 'opportunity' not in flags:
        sc = 3

    # ── Founder/co-founder — conservative default: Low-Medium unless proven ───
    # Most founders are solo, pre-revenue, or running lifestyle businesses.
    # Override exceptions: finance domain, physician, named top-tier finance firm,
    # OR an independently HIGH exec title (CEO, COO, etc.) alongside the founder signal.
    _is_founder_title = any(t in title for t in ['founder', 'co-founder', 'cofounder'])
    if _is_founder_title and 'invested' not in flags and 'opportunity' not in flags:
        _fin_dom   = email_domain(email) in FINANCE_DOMAINS
        _phys      = is_physician(title, email, company)
        _top_fin   = any(fc in company for fc in FINANCE_COMPANIES)
        _exec_title = (any(t in title for t in [
            'ceo', 'coo', 'cto', 'cfo', 'cio', 'cmo', 'cro',
            'chief executive', 'chief operating', 'chief technology',
            'chief financial', 'chief information', 'chief marketing',
            'managing director', 'managing member', 'managing partner',
            'general partner', 'president',
        ]) or bool(re.search(r'\bchief\b.+\bofficer\b', title)))
        if not (_fin_dom or _phys or _top_fin or _exec_title):
            sc = min(sc, 2)

    # ── Title-only HIGH → cap at Medium-High without a firm-quality signal ──────
    # A strong title (MD, President, CEO) at an unknown firm doesn't reliably mean
    # investable wealth. HIGH requires title + at least one firm-quality corroboration:
    # finance-domain email, confirmed physician, or named top-tier finance company.
    if sc == 5 and 'invested' not in flags and 'opportunity' not in flags:
        _firm_signal = (email_domain(email) in FINANCE_DOMAINS
                        or is_physician(title, email, company)
                        or any(fc in company for fc in FINANCE_COMPANIES))
        if not _firm_signal:
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


def explain_score(p: dict, sc: int, flags: list) -> str:
    """One-line reason explaining why this contact landed at `sc`."""
    title    = (p.get('jobtitle') or '').lower()
    company  = (p.get('company')  or '').lower()
    email    = (p.get('email')    or '').lower()
    dom      = email_domain(email)
    combined = title + ' ' + company

    if 'invested' in flags:           return 'Already invested'
    if 'opportunity' in flags:        return 'Warm pipeline — Opportunity stage'
    if 'not_interested' in flags:     return 'Said "not interested" on prior call'
    if 'target_wealth_firm' in flags: return 'Wealth advisor at target firm — channel partner'
    if 'under_30' in flags:           return 'Under 30 — recent grad cap'
    if 'tenure_10plus' in flags and sc == 3:
        return '10+ year tenure floor'

    if sc <= 2:
        if any(t in combined for t in WEALTH_ADVISOR_TERMS):
            return "Wealth advisor — refers clients, doesn't invest"
        if any(t in combined for t in ('art dealer', 'art advisor', 'art adviser', 'gallery')) or 'fine art' in company:
            return "Art world — fractional doesn't fit mental model"
        if any(t in combined for t in ('real estate agent', 'realtor', 're agent', 're broker')):
            return 'Real estate agent — commission income'
        if any(t in title for t in ('music producer', 'filmmaker', 'film-maker', 'screenwriter', 'cinematographer')):
            return 'Music / film — cultural sector'
        if any(t in combined for t in SERVICE_PROVIDER_TERMS):
            return 'Time-for-money service provider'
        if any(t in combined for t in CREATOR_TERMS):
            return 'Content creator / influencer'
        if 'no_show' in flags:
            return 'No-show on prior event + downgrade signal'
        if any(t in combined for t in DOWNGRADE_TERMS):
            return 'Junior or downgrade-term title'
        if any(t in title for t in ('founder', 'co-founder', 'cofounder')):
            return 'Founder — unverified scale'
        return 'Low-tier signals'

    if sc == 5:
        if dom in FINANCE_DOMAINS:
            return f'Finance domain ({dom})'
        if is_physician(title, email, company):
            return 'Physician / MD — pitched on K-1 angle'
        if any(fc in company for fc in FINANCE_COMPANIES):
            return 'Senior role at top-tier finance firm'
        scale = classify_company_scale(company, p.get('linkedin_company_size', ''))
        _t = title.strip()
        if (_t == 'partner' or _t.endswith(' partner')) and scale in ('large', 'mid'):
            return f'Partner at {scale}-scale firm'
        if scale in ('large', 'mid'):
            return f'Senior role at {scale}-scale firm'
        if has_high_title(title):
            return 'Senior title with firm corroboration'
        return 'Multiple HIGH signals'

    if sc == 4:
        if has_high_title(title):
            return 'Senior title — no firm-quality signal'
        if any(fc in company for fc in FINANCE_COMPANIES):
            return 'Mid-level role at top-tier finance firm'
        if any(t in title for t in ('senior director', 'associate director')) or \
           any(t in title for t in ('vp', 'vice president', 'svp', 'evp', 'avp')) or \
           ('director' in title and 'art director' not in title):
            return 'VP / Director-level title'
        if any(t in company for t in ('real estate', 'realty', 'extell', 'related companies', 'tishman', 'sl green', 'brookfield')):
            return 'Real-estate exec at major developer'
        return 'Medium-High — NW cap or mid-tier title'

    if sc == 3:
        if any(t in title for t in ('senior manager', 'lead', 'principal', 'head of')):
            return 'Senior manager / lead role'
        if any(t in title for t in ('attorney', 'lawyer', 'counsel')):
            return 'Solo / small-firm attorney'
        return 'Default mid-tier — no caps, no HIGH signals'

    return 'Standard scoring path'


# ─── DQ / QP TAG ──────────────────────────────────────────────────────────────
# Mirrors the disqualification framework used on the historical disqualified-
# attendee cohort. Applied to contacts RSVPed to FUTURE events.
# Returns 'QP' (qualified), 'DQ' (correctly disqualified), or 'UNCERTAIN'.

def _wealth_segment_low(ws: str):
    """Parse low end of a wealth-segment range like '$2M-$5M' or '$500K-$1M'."""
    if not ws:
        return None
    s = ws.upper().replace(' ', '').replace('$', '').replace('MM', 'M').replace('+TO', '-').replace('+', '')
    m = re.match(r'([\d.]+)([KM])?', s.split('-')[0])
    if not m:
        return None
    val = float(m.group(1))
    if m.group(2) == 'K':
        val *= 1_000
    elif m.group(2) == 'M':
        val *= 1_000_000
    return val


def _income_low(inc: str):
    """Parse low end of an inferred-income range — same format as wealth segment."""
    return _wealth_segment_low(inc)


def dq_qp_tag(p: dict) -> str:
    """Returns 'QP', 'DQ', or 'UNCERTAIN' for a contact's properties dict."""
    title   = (p.get('jobtitle') or '').lower()
    company = (p.get('company')  or '').lower()
    email   = (p.get('email')    or '').lower()
    ws_low  = _wealth_segment_low(p.get('wealth_segment') or '')
    inc_low = _income_low(p.get('inferred_income') or '')

    # QP triggers — any one is sufficient
    if ws_low and ws_low >= 1_000_000:
        return 'QP'
    if inc_low and inc_low >= 200_000:
        return 'QP'
    if email_domain(email) in FINANCE_DOMAINS:
        return 'QP'
    if company.strip() and any(t in title for t in [
        'ceo', 'chief executive', 'founder', 'co-founder', 'cofounder',
        'owner', 'president', 'managing director', 'managing partner',
        'general partner',
    ]) and 'vice president' not in title:
        return 'QP'

    # DQ — junk records or zero meaningful signal
    fname = (p.get('firstname') or '').strip()
    is_junk_name = '@' in fname or not fname
    has_nothing = (not title.strip() and not company.strip()
                   and not ws_low and not inc_low)
    if is_junk_name or has_nothing:
        return 'DQ'

    return 'UNCERTAIN'


def dq_qp_tag_html(p: dict) -> str:
    """Renders the DQ/QP pill — blank for UNCERTAIN. Guests can be QP but not DQ."""
    tag = dq_qp_tag(p)
    if not tag or tag == 'UNCERTAIN':
        return ''
    if tag == 'DQ' and (p.get('unknown_rsvp') or '').strip() == 'Guest':
        return ''
    fg, bg = TAG_STYLES[tag]
    return (
        f'<span class="dq-qp-tag" data-tag="{tag}" '
        f'style="display:inline-block;margin-left:6px;padding:2px 7px;'
        f'border-radius:10px;font-size:0.66rem;font-weight:700;'
        f'letter-spacing:0.04em;color:{fg};background:{bg};'
        f'border:1px solid {fg}55;vertical-align:middle">{tag}</span>'
    )


# ─── AVATAR (LinkedIn photo → Gravatar → initials cascade) ────────────────────

# Deterministic palette — same name always gets the same color.
_AVATAR_BG = ['#1a5fa8', '#1a7a45', '#8a6800', '#b85a00',
              '#7a3aa8', '#0a8a8a', '#c04040', '#4a4a8a']

def _initials(name: str) -> str:
    parts = [p for p in (name or '').strip().split() if p]
    if not parts:
        return '?'
    if len(parts) == 1:
        return parts[0][0].upper()
    return (parts[0][0] + parts[-1][0]).upper()


def _initials_avatar_html(name: str) -> str:
    initials = _initials(name)
    bg = _AVATAR_BG[sum(ord(c) for c in (name or '?')) % len(_AVATAR_BG)]
    return (
        f'<span style="display:inline-flex;align-items:center;justify-content:center;'
        f'width:30px;height:30px;border-radius:50%;background:{bg};color:#fff;'
        f'font-size:0.72rem;font-weight:700;flex-shrink:0;letter-spacing:0.02em">'
        f'{escape(initials)}</span>'
    )


def _gravatar_url(email: str, size: int = 60) -> str:
    """Return Gravatar URL with d=404 so onerror fires when no image is registered."""
    if not email or '@' not in email:
        return ''
    h = hashlib.md5(email.strip().lower().encode('utf-8')).hexdigest()
    return f'https://www.gravatar.com/avatar/{h}?s={size}&d=404'


def avatar_html(p: dict, name: str) -> str:
    """30px circular avatar — LinkedIn photo → Gravatar → initials cascade.

    The cascade is implemented client-side via the _avatarErr() JS helper:
      1. <img src> set to linkedin_image_url (if available)
      2. on error → switches to Gravatar (if email available)
      3. on error → swaps to initials avatar
    """
    li_img = (p.get('linkedin_image_url') or '').strip()
    if li_img and not li_img.startswith(('http://', 'https://')):
        li_img = ''
    grav = _gravatar_url(p.get('email') or '')
    fallback_html = _initials_avatar_html(name)

    style = ('width:30px;height:30px;border-radius:50%;object-fit:cover;'
             'flex-shrink:0;border:1px solid #e1e6ee;background:#f0f1f5;'
             'cursor:pointer')

    # Tier 1: have a LinkedIn photo URL — Gravatar is the next-step fallback
    if li_img:
        return (
            f'<img src="{escape(li_img)}" alt="" '
            f'data-fb="{escape(grav)}" '
            f'data-fb-html="{escape(fallback_html)}" '
            f'onerror="_avatarErr(this)" '
            f'onclick="openPhotoModal(event, this)" '
            f'title="Click to enlarge" '
            f'style="{style}">'
        )
    # Tier 2: no LinkedIn photo, but we have an email — try Gravatar directly
    if grav:
        return (
            f'<img src="{escape(grav)}" alt="" '
            f'data-fb-html="{escape(fallback_html)}" '
            f'onerror="_avatarErr(this)" '
            f'onclick="openPhotoModal(event, this)" '
            f'title="Click to enlarge" '
            f'style="{style}">'
        )
    # Tier 3: no email either — straight to initials (not clickable, no real photo to show)
    return fallback_html


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

    # Business Owner — founders, CEOs, owners of their own company
    # Exception: CEO/President at a known large/established company → Corporate Climber
    _is_owner_title = any(t in title for t in [
        'founder', 'co-founder', 'ceo', 'chief executive',
        'owner', 'proprietor', 'president',
    ])
    if _is_owner_title:
        _large_co = (any(fc in company for fc in FINANCE_COMPANIES) or
                     any(tc in company for tc in [
                         'google', 'meta', 'apple', 'amazon', 'microsoft', 'netflix',
                         'prudential', 'jpmorgan', 'chase', 'citibank', 'bank of america',
                         'wells fargo', 'morgan stanley', 'goldman', 'deloitte', 'mckinsey',
                         'bain ', 'bcg ', 'boston consulting', 'accenture', 'ibm', 'oracle',
                     ]))
        if _large_co:
            return 'Corporate Climber'
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
        'managing director', 'managing member', 'managing partner',
        'general partner', 'founding partner', 'senior partner',
        'fund manager', 'portfolio manager', 'principal',
    ])
    in_top_finance = any(fc in company for fc in FINANCE_COMPANIES)
    if elite_finance_title and in_top_finance:
        return '$3M–$10M', 'Senior role at top-tier finance firm'
    # Managing partner / managing member at any investment/finance context → $2M–$6M
    if any(t in title for t in ['managing partner', 'managing member', 'general partner']):
        return '$2M–$6M', 'Fund GP / Managing Partner (assumes fund economics)'
    if 'partner' in title and any(lf in company for lf in [
        'simpson thacher', 'sullivan & cromwell', 'willkie', 'troutman', 'dorf',
        'skadden', 'kirkland', 'weil gotshal', 'latham', 'davis polk',
        'cleary', 'paul weiss', 'cravath', 'debevoise', 'proskauer',
    ]):
        return '$3M–$10M', 'Partner at elite law firm'

    # Tier 2: VP/Director/C-suite at major finance → $2M–$6M
    if any(t in title for t in ['vp', 'vice president', 'director', 'svp', 'evp', 'cfo', 'cto', 'coo', 'cio', 'ceo', 'chief']) \
            and in_top_finance:
        return '$2M–$6M', 'C-suite / VP at major finance firm'
    # C-suite titles (any company, any chief* title) → at least $1M–$4M
    # Exception: CEO / Chief Executive at a small/lifestyle or nonprofit org → conservative
    _is_ceo = any(t in title for t in ['ceo', 'chief executive'])
    _org_email = email_domain(p.get('email', '')).endswith('.org')
    if _is_ceo and (is_small_biz(company) or _org_email):
        return '$150K–$500K', 'CEO of small / nonprofit organization'
    if any(t in title for t in ['chief investment', 'chief financial', 'chief operating',
                                 'chief technology', 'chief information', 'chief executive',
                                 'chief marketing', 'chief revenue', 'chief people',
                                 'chief product', 'chief data', 'chief strategy']):
        return '$1M–$4M', 'C-suite executive'
    # CEO without top-finance context → conservative NW estimate
    if _is_ceo and company:
        return '$500K–$2M', 'CEO (unverified scale)'
    # Founder/co-founder — assume low unless finance signal already triggered above
    if any(t in title for t in ['founder', 'co-founder', 'cofounder']):
        if not company.strip():
            return '$150K–$500K', 'Founder with no company listed'
        if is_small_biz(company):
            return '$150K–$500K', 'Founder of small / lifestyle business'
        return '$150K–$500K', 'Founder (unverified scale — assume conservative)'

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
    if 'owner' in title and company:
        if is_small_biz(company):
            return '$150K–$500K', 'Small business owner'
        return '$500K–$2M', 'Business owner'

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


# ─── PLUTO → NW ADJUSTMENT ────────────────────────────────────────────────────

_NW_TIER_ORDER = {
    '$3M–$10M':    6,
    '$2M–$6M':     5,
    '$1M–$4M':     4,
    '$500K–$2M':   3,
    '$150K–$500K': 2,
    '$50K–$200K':  1,
    '—':           0,
}

def _parse_pluto_low(pluto_val: str) -> float | None:
    """Parse the low end of a PLUTO range string e.g. '$1.4M – $2.1M' → 1_400_000."""
    m = re.match(r'\$([0-9.]+)(M|K)', pluto_val.strip())
    if not m:
        return None
    val  = float(m.group(1))
    mult = 1_000_000 if m.group(2) == 'M' else 1_000
    return val * mult

def pluto_nw_bump(current_nw: str, pluto_val: str) -> tuple | None:
    """Return (bumped_nw_str, reason) if PLUTO property estimate implies a higher NW
    tier than the current Claude estimate, else None.

    Logic: assume property is at most 50% of net worth (conservative).
    So implied NW floor = PLUTO low end ÷ 0.5.
    Only raises — never lowers — the estimate.
    PLUTO tends to underestimate for apartments, so this floor is already conservative.
    """
    if not pluto_val:
        return None
    low = _parse_pluto_low(pluto_val)
    if not low or low <= 0:
        return None

    # Property ≤ 50% of NW → NW ≥ 2 × property low end
    implied_floor = low * 2.0

    if implied_floor >= 3_000_000:
        implied_tier = '$3M–$10M'
    elif implied_floor >= 2_000_000:
        implied_tier = '$2M–$6M'
    elif implied_floor >= 1_000_000:
        implied_tier = '$1M–$4M'
    elif implied_floor >= 500_000:
        implied_tier = '$500K–$2M'
    elif implied_floor >= 150_000:
        implied_tier = '$150K–$500K'
    else:
        return None  # too low to be informative

    # Only bump up
    if _NW_TIER_ORDER.get(implied_tier, 0) <= _NW_TIER_ORDER.get(current_nw, 0):
        return None

    def _fmt(v: float) -> str:
        return f'${v / 1_000_000:.1f}M' if v >= 1_000_000 else f'${round(v / 1_000)}K'

    reason = f'PLUTO property {pluto_val} → implied NW floor {_fmt(implied_floor)} (property ≤ 50% of NW)'
    return implied_tier, reason


# ─── EVENT STATS ──────────────────────────────────────────────────────────────

def compute_event_stats(contacts: list) -> dict:
    """Aggregate per-event metrics from a list of contacts."""
    attended = 0
    attended_score = 0
    account_created = 0
    invested = 0
    capital = 0.0
    calls_booked = 0
    stale_45 = 0
    email_opens = 0
    email_delivered = 0
    replies = 0
    # per-tier breakdown of attendees
    tiers = {1:0, 2:0, 3:0, 4:0, 5:0}
    tier_calls    = {1:0, 2:0, 3:0, 4:0, 5:0}
    tier_accts    = {1:0, 2:0, 3:0, 4:0, 5:0}
    tier_invested = {1:0, 2:0, 3:0, 4:0, 5:0}

    for c in contacts:
        p = c['properties']
        if (p.get('attended_outbound_event') or '').strip().lower() != 'yes':
            continue
        sc, _ = score_contact(p)
        attended += 1
        attended_score += sc
        tiers[sc] = tiers.get(sc, 0) + 1

        # Call booked = any call_completed value (a call happened)
        has_call = bool((p.get('call_completed') or '').strip())
        if has_call:
            calls_booked += 1
            tier_calls[sc] = tier_calls.get(sc, 0) + 1
        elif sc >= 4:
            stale_45 += 1

        # Account created
        if (p.get('admin_url') or '').strip():
            account_created += 1
            tier_accts[sc] = tier_accts.get(sc, 0) + 1

        # Capital (only 2026+ conversions)
        acct_year = (p.get('createdate') or '')[:4]
        if acct_year >= '2026':
            try:
                amount = float(p.get('totalamountpurchased') or 0)
            except (ValueError, TypeError):
                amount = 0.0
            if amount > 0:
                invested += 1
                capital += amount
                tier_invested[sc] = tier_invested.get(sc, 0) + 1

        # Email engagement — marketing email properties
        try:
            email_opens += int(p.get('hs_email_open') or 0)
        except (ValueError, TypeError):
            pass
        try:
            email_delivered += int(p.get('hs_email_delivered') or 0)
        except (ValueError, TypeError):
            pass
        if (p.get('hs_email_first_reply_date') or '').strip():
            replies += 1

    return {
        'rsvps':          len(contacts),
        'attended':       attended,
        'attended_score': attended_score,
        'account_created': account_created,
        'invested':       invested,
        'capital':        round(capital),
        'calls_booked':   calls_booked,
        'stale_45':       stale_45,
        'email_opens':    email_opens,
        'email_delivered': email_delivered,
        'replies':        replies,
        'tiers':          tiers,
        'tier_calls':     tier_calls,
        'tier_accts':     tier_accts,
        'tier_invested':  tier_invested,
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
            f'</span>')

def li_url(name: str, company: str, p: dict | None = None) -> str:
    if p:
        direct = (
            p.get('hs_linkedin_url') or
            p.get('outbound_team___linkedin_url') or
            p.get('linkedin_personal_url') or
            p.get('pipl_linkedin') or ''
        ).strip()
        if direct:
            if not direct.startswith('http'):
                direct = 'https://' + direct
            return direct
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

def infer_nyc_neighborhood(address: str, city: str) -> str:
    """Return a neighborhood name for NYC addresses, or the city string otherwise."""
    city_l = (city or '').lower().strip()
    if not any(x in city_l for x in ('new york', 'nyc', 'manhattan')):
        return (city or '').strip()
    addr = (address or '').lower()
    # Named-street lookup
    named = [
        (['horatio', 'jane st', 'perry st', 'charles st', 'grove st', 'barrow',
          'bank st', 'bethune', 'leroy st', 'clarkson', 'morton st'], 'West Village'),
        (['gansevoort', 'little w 12th', 'meatpacking'], 'Meatpacking District'),
        (['bleecker', 'waverly', 'macdougal', 'washington sq', 'w 8th', 'w 9th',
          'w 10th', 'w 11th', 'w 12th', 'w 13th', 'west 8th', 'west 9th',
          'west 10th', 'west 11th', 'west 12th', 'west 13th'], 'Greenwich Village'),
        (['ave a', 'ave b', 'ave c', 'ave d', 'st marks', 'e 1st', 'e 2nd', 'e 3rd',
          'e 4th', 'e 5th', 'e 6th', 'e 7th', 'e 8th', 'e 9th', 'e 10th', 'e 11th',
          'e 12th', 'e 13th', 'east 1st', 'east 2nd', 'east 3rd'], 'East Village'),
        (['bond st', 'great jones', 'astor pl'], 'NoHo'),
        (['spring st', 'prince st', 'broome st', 'grand st', 'mercer', 'wooster',
          'greene st', 'west broadway', 'thompson st'], 'SoHo'),
        (['chambers', 'warren st', 'worth st', 'reade', 'murray st', 'barclay',
          'vesey', 'park pl'], 'TriBeCa'),
        (['wall st', 'water st', 'fulton st', 'liberty st', 'broad st',
          'maiden ln', 'rector', 'cedar st'], 'Financial District'),
        (['ludlow', 'orchard', 'rivington', 'stanton', 'delancey',
          'essex', 'allen st'], 'Lower East Side'),
        (['mulberry', 'mott st', 'bayard', 'canal st'], 'Chinatown/Little Italy'),
        (['w 14th', 'w 15th', 'w 16th', 'w 17th', 'w 18th', 'w 19th', 'w 20th',
          'w 21st', 'w 22nd', 'w 23rd', 'w 24th', 'w 25th', 'w 26th',
          'west 14th', 'west 15th', 'west 16th', 'west 17th', 'west 18th',
          'west 19th', 'west 20th', 'west 21st', 'west 22nd', 'west 23rd',
          'west 24th', 'west 25th', 'west 26th'], 'Chelsea'),
        (['e 14th', 'e 15th', 'e 16th', 'e 17th', 'e 18th', 'e 19th', 'e 20th',
          'e 21st', 'e 22nd', 'e 23rd', 'e 24th', 'e 25th', 'e 26th',
          'east 14th', 'east 15th', 'east 16th', 'east 17th', 'east 18th',
          'east 19th', 'east 20th', 'east 21st', 'east 22nd', 'east 23rd',
          'gramercy', 'irving pl', 'park ave s'], 'Gramercy/Flatiron'),
        (['w 27th', 'w 28th', 'w 29th', 'w 30th', 'w 31st', 'w 32nd', 'w 33rd',
          'west 27th', 'west 28th', 'west 29th', 'west 30th', 'west 31st',
          'west 32nd', 'west 33rd', 'hudson yards'], 'Chelsea/Hudson Yards'),
        (['e 27th', 'e 28th', 'e 29th', 'e 30th', 'e 31st', 'e 32nd', 'e 33rd',
          'east 27th', 'east 28th', 'east 29th', 'east 30th', 'east 31st',
          'lexington ave', 'kip', 'murray hill'], 'Murray Hill/Kips Bay'),
        (['riverside dr', 'central park w', 'riverside blvd',
          'w 60th', 'w 61st', 'w 62nd', 'w 63rd', 'w 64th', 'w 65th', 'w 66th',
          'w 67th', 'w 68th', 'w 69th', 'w 70th', 'w 71st', 'w 72nd', 'w 73rd',
          'w 74th', 'w 75th', 'w 76th', 'w 77th', 'w 78th', 'w 79th', 'w 80th',
          'w 81st', 'w 82nd', 'w 83rd', 'w 84th', 'w 85th', 'w 86th', 'w 87th',
          'w 88th', 'w 89th', 'w 90th', 'w 91st', 'w 92nd', 'w 93rd', 'w 94th',
          'w 95th', 'w 96th', 'west 60th', 'west 61st', 'west 72nd', 'west 79th',
          'west 86th', 'west 96th'], 'Upper West Side'),
        (['e 60th', 'e 61st', 'e 62nd', 'e 63rd', 'e 64th', 'e 65th', 'e 66th',
          'e 67th', 'e 68th', 'e 69th', 'e 70th', 'e 71st', 'e 72nd', 'e 73rd',
          'e 74th', 'e 75th', 'e 76th', 'e 77th', 'e 78th', 'e 79th', 'e 80th',
          'e 81st', 'e 82nd', 'e 83rd', 'e 84th', 'e 85th', 'e 86th', 'e 87th',
          'e 88th', 'e 89th', 'e 90th', 'e 91st', 'e 92nd', 'e 93rd', 'e 94th',
          'e 95th', 'e 96th', 'east 60th', 'east 72nd', 'east 79th', 'east 86th',
          'east 96th', 'park ave', 'madison ave', 'fifth ave', '5th ave'], 'Upper East Side'),
        (['morningside', 'cathedral pkwy', 'w 110th', 'w 111th', 'w 112th',
          'w 113th', 'west 110th', 'west 116th', 'claremont'], 'Morningside Heights'),
        (['w 125th', '125th', 'lenox', 'adam clayton', 'frederick douglass blvd',
          'malcolm x', 'harlem'], 'Harlem'),
    ]
    for keywords, hood in named:
        if any(kw in addr for kw in keywords):
            return hood
    # Broadway block-number inference
    if 'broadway' in addr:
        m = re.search(r'(\d+)\s+broadway', addr)
        if m:
            n = int(m.group(1))
            if n < 100:    return 'Financial District'
            elif n < 400:  return 'TriBeCa/SoHo'
            elif n < 900:  return 'Greenwich Village'
            elif n < 1500: return 'Flatiron/Union Square'
            elif n < 2000: return 'Midtown'
            elif n < 2800: return 'Upper West Side'
            else:          return 'Harlem'
    # Numbered cross-street inference
    m = re.search(r'\b(\d+)\w*\s+st(?:reet)?\b', addr)
    if m:
        n = int(m.group(1))
        is_west = bool(re.search(r'\bw(?:est)?\b', addr))
        if 34 <= n <= 59:  return 'Midtown West' if is_west else 'Midtown East'
        elif 60 <= n <= 96: return 'Upper West Side' if is_west else 'Upper East Side'
        elif n > 96:        return 'Morningside Heights' if is_west else 'East Harlem'
    return 'New York, NY'


def render_detail_row(p: dict, per: str, nw: str, sc: int = 0, flags: list = None) -> str:
    """Render the hidden dropdown detail row (today + future events only)."""
    hs_wealth    = (p.get('wealth_segment')  or '').strip() or '—'
    inferred_inc = (p.get('inferred_income') or '').strip() or '—'
    address      = (p.get('address') or '').strip()
    city         = (p.get('city')    or '').strip()
    zip_code     = (p.get('zip')     or '').strip()
    pluto_val    = (p.get('_pluto_val') or '').strip() or None
    state        = (p.get('state') or '').strip()

    # Neighborhood / location label
    neighborhood_raw = infer_nyc_neighborhood(address, city) if (address or city) else ''
    is_nyc_hood = neighborhood_raw and neighborhood_raw.lower() != city.lower()
    if is_nyc_hood:
        loc_label = neighborhood_raw  # e.g. "Upper East Side"
    else:
        # Title-case city + uppercase state abbreviation
        city_fmt  = ' '.join(w.capitalize() for w in city.split())  if city  else ''
        state_fmt = state.upper() if state else ''
        if city_fmt and state_fmt:
            loc_label = f'{city_fmt}, {state_fmt}'
        else:
            loc_label = city_fmt or '—'

    addr_display = escape(address) if address else '—'
    loc_display  = escape(loc_label)
    hood_label   = 'Nbhd' if is_nyc_hood else 'City'

    if pluto_val:
        prop_cell = (
            f'<div class="detail-cell">'
            f'<p class="detail-cell-label">Property</p>'
            f'<div class="seg-stack">'
            f'<div class="seg-row"><span class="seg-src">PLUTO</span>'
            f'<span class="seg-val">{escape(pluto_val)}</span></div>'
            f'<hr class="seg-divider">'
            f'<div class="seg-row"><span class="seg-src">Street</span>'
            f'<span class="seg-val">{addr_display}</span></div>'
            f'<hr class="seg-divider">'
            f'<div class="seg-row"><span class="seg-src">{hood_label}</span>'
            f'<span class="seg-val">{loc_display}</span></div>'
            f'</div>'
            f'</div>'
        )
    else:
        prop_cell = (
            f'<div class="detail-cell" data-zip="{escape(zip_code)}">'
            f'<p class="detail-cell-label">Property</p>'
            f'<div class="seg-stack">'
            f'<div class="seg-row"><span class="seg-src">Census</span>'
            f'<span class="census-value seg-val" style="color:#aabcd4">Loading…</span></div>'
            f'<hr class="seg-divider">'
            f'<div class="seg-row"><span class="seg-src">Street</span>'
            f'<span class="seg-val">{addr_display}</span></div>'
            f'<hr class="seg-divider">'
            f'<div class="seg-row"><span class="seg-src">{hood_label}</span>'
            f'<span class="seg-val">{loc_display}</span></div>'
            f'</div>'
            f'</div>'
        )

    return (
        f'<tr class="detail-row" style="display:none">'
        f'<td colspan="9" style="padding:0;border-bottom:1px solid #eef1f7;width:100%">'
        f'<div class="detail-inner">'
        f'<div class="detail-cell">'
        f'<p class="detail-cell-label">Persona</p>'
        f'<span class="persona-detail-pill">{escape(per)}</span>'
        + (
            f'<p class="detail-cell-label" style="margin-top:10px">Tier Logic</p>'
            f'<div style="font-size:0.78rem;color:#3a5070;line-height:1.4">{escape(explain_score(p, sc, flags or []))}</div>'
            if sc else ''
        )
        + f'</div>'
        f'<div class="detail-cell">'
        f'<p class="detail-cell-label">Wealth Segment</p>'
        f'<div class="seg-stack">'
        f'<div class="seg-row"><span class="seg-src">Claude</span>'
        f'<span class="seg-val">{escape(nw)}</span></div>'
        f'<hr class="seg-divider">'
        f'<div class="seg-row"><span class="seg-src">HS</span>'
        f'<span class="seg-val">{escape(hs_wealth)}</span></div>'
        f'<hr class="seg-divider">'
        f'<div class="seg-row"><span class="seg-src">Income</span>'
        f'<span class="seg-val">{escape(inferred_inc)}</span></div>'
        f'</div>'
        f'</div>'
        + prop_cell
        + f'</div></td></tr>\n'
    )


def render_row(idx: int, c: dict, show_dropdown: bool = False, show_unk: bool = False) -> str:
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
    if cid in SCORE_OVERRIDES:
        sc = SCORE_OVERRIDES[cid]
    per       = get_persona(p)
    nw, nw_r  = get_nw(p)

    # Bump Claude NW up if PLUTO property estimate implies a higher tier
    pluto_bump = pluto_nw_bump(nw, p.get('_pluto_val') or '')
    if pluto_bump:
        nw, nw_r = pluto_bump

    owner_name = OWNERS.get(owner_id, owner_id or '—')
    if owner_id in INACTIVE_OWNERS:
        owner_cell = f'<span style="color:#c04040;font-weight:600">{escape(owner_name)} ⚠</span>'
    else:
        owner_cell = f'<span style="color:#8a9fc0">{escape(owner_name)}</span>'

    attended       = (p.get('attended_outbound_event') or '').strip().lower() == 'yes'
    attended_chk   = 'checked' if attended else ''
    disqualified   = (p.get('outbound_event_attendee_disqualified') or '').strip().lower() == 'disqualified'
    uninvite_chk   = 'checked' if disqualified else ''
    uninvite_class = ' uninvited' if disqualified else ''
    send_conf      = (p.get('outbound_event_send_confirmation') or '').strip().lower() == 'yes'
    send_conf_chk  = 'checked' if send_conf else ''

    invested_badge = ('<span style="display:inline-block;background:#eaf7f0;color:#1a7a45;'
                      'border:1px solid #1a7a4555;border-radius:10px;font-size:0.62rem;'
                      'font-weight:700;padding:1px 7px;letter-spacing:0.04em;'
                      'vertical-align:middle;margin-right:5px">INV</span>'
                      if 'invested' in flags else '')
    rsvp_status = (p.get('unknown_rsvp') or '').strip().lower()
    if show_unk and rsvp_status == 'guest':
        unknown_badge = ('<span style="display:inline-block;background:#e8f0fe;color:#3b6bb0;'
                         'border:1px solid #3b6bb055;border-radius:10px;font-size:0.62rem;'
                         'font-weight:700;padding:1px 7px;letter-spacing:0.04em;'
                         'vertical-align:middle;margin-left:4px">+1</span>')
    elif show_unk and rsvp_status == 'unknown':
        unknown_badge = ('<span style="display:inline-block;background:#f0f1f5;color:#7a88a0;'
                         'border:1px solid #aab0c055;border-radius:10px;font-size:0.62rem;'
                         'font-weight:700;padding:1px 7px;letter-spacing:0.04em;'
                         'vertical-align:middle;margin-left:4px">?</span>')
    else:
        unknown_badge = ''
    opp_star = ''

    loc_html = ''

    ns_html = '<br><span style="font-size:0.7rem;color:#c94040">⚠ No Show</span>' if 'no_show' in flags else ''

    enriched_tag = (
        '<span title="Inferred via Google / LinkedIn search" '
        'style="font-size:0.6rem;color:#9aaac0;margin-left:4px;vertical-align:middle">🔍</span>'
        if p.get('_enriched') else ''
    )

    inferred_company = ''
    if not company:
        inferred_company = domain_to_company(p.get('email') or '')

    tc_parts = []
    if title:
        tc_parts.append(escape(title) + enriched_tag)
    _co_flag = ''
    if company and _email_company_mismatch(p.get('email') or '', company):
        _flag_dom = email_domain(p.get('email') or '')
        _co_flag = (f'<span title="Email domain ({escape(_flag_dom)}) doesn\'t match company" '
                    f'style="color:#c94040;font-size:0.65rem;margin-left:4px;cursor:help">⚑</span>')
    if company:
        tc_parts.append(f'<span style="color:#7a94b8;font-size:0.78rem">{escape(company)}{_co_flag}</span>')
    elif inferred_company:
        tc_parts.append(
            f'<span style="color:#9aaac0;font-size:0.75rem;font-style:italic" '
            f'title="Inferred from email domain">{escape(inferred_company)}</span>'
        )
    tc_html = '<br>'.join(tc_parts) or '<span style="color:#c0ccd8">—</span>'

    pluto_val = (p.get('_pluto_val') or '').strip()
    if pluto_val and pluto_val != 'Commercial':
        prop_html = f'<span style="font-size:0.75rem">{escape(pluto_val)}</span>'
    else:
        prop_html = '<span style="color:#c0ccd8">—</span>'

    nw_cell = f'<strong style="font-size:0.85rem">{escape(nw)}</strong>'

    name_cell = (
        f'<div style="display:flex;align-items:center;gap:10px">'
        f'{avatar_html(p, name)}'
        f'<div style="min-width:0">'
        f'{opp_star}<strong>{escape(name)}</strong>{invested_badge}{unknown_badge}'
        f'{loc_html}{ns_html}'
        f'</div>'
        f'</div>'
    )

    chevron_td = '<td style="text-align:center;padding:11px 4px"><span class="expand-chevron">▼</span></td>' if show_dropdown else ''
    tr_attrs   = (f'data-contact="{escape(cid)}" style="cursor:pointer" ' if show_dropdown else '')
    detail_row = render_detail_row(p, per, nw, sc, flags) if show_dropdown else ''

    return (
        f'<tr data-id="{escape(cid)}" data-auto="{sc}" '
        f'data-persona="{escape(per)}" data-score="{sc}" '
        f'data-disqualified="{"1" if disqualified else "0"}" '
        f'{tr_attrs}class="{uninvite_class.strip()}">'
        f'{chevron_td}'
        f'<td style="color:#aabcd4;text-align:center">{idx}</td>'
        f'<td>{name_cell}</td>'
        f'<td>{tc_html}</td>'
        f'<td style="text-align:center">{prop_html}</td>'
        f'<td style="text-align:center" class="score-cell">{score_badge_html(sc)}</td>'
        + (f'<td style="text-align:center">{dq_qp_tag_html(p)}</td>' if show_dropdown else '')
        + f'<td style="text-align:center;white-space:nowrap">'
        f'<a href="{li_url(name, company, p)}" target="_blank" '
        f'style="color:#0a66c2;font-weight:700;text-decoration:none;font-size:0.8rem;margin-right:8px">LI↗</a>'
        f'<a href="{hs_url(cid)}" target="_blank" '
        f'style="color:#ff7a59;font-weight:700;text-decoration:none;font-size:0.8rem">HS↗</a></td>'
        f'<td style="text-align:center">'
        f'<div class="status-slider" data-state="{"uninvite" if disqualified else "conf" if send_conf else "neutral"}">'
        f'<div class="slider-track"><div class="slider-thumb"></div></div>'
        f'<div class="slider-labels"><span class="sl-conf">✓</span><span class="sl-uninv">✕</span></div>'
        f'</div></td>'
        f'<td style="text-align:center">'
        f'<input type="checkbox" class="attended-chk" {attended_chk} '
        f'onchange="toggleAttended(this)" '
        f'style="width:16px;height:16px;cursor:pointer;accent-color:#1a7a45"></td>'
        f'</tr>\n'
        f'{detail_row}'
    )

NW_RANK = {'$3M–$10M': 6, '$2M–$6M': 5, '$1M–$4M': 4, '$500K–$2M': 3, '$150K–$500K': 2, '$50K–$200K': 1}

def likelihood_secondary(p: dict, flags: list) -> int:
    """Within a score tier: higher = more likely to invest."""
    s = 0
    if (p.get('attended_outbound_event') or '').lower() == 'yes':
        s += 10                                                  # attended — strongest signal
    if email_domain(p.get('email', '')) in FINANCE_DOMAINS:
        s += 4                                                   # finance domain email
    nw, _ = get_nw(p)
    s += NW_RANK.get(nw, 0)                                      # higher NW = more likely
    if 'no_show' in flags:
        s -= 5                                                   # penalise no-shows
    if (p.get('state') or '').lower().strip() in TRI_STATE:
        s += 1                                                   # local (NY/NJ/CT) slight edge
    return s

def render_panel(date_str: str, contacts: list, tab_id: str, active: bool, past: bool = False) -> str:
    def _sort_key(c):
        sc, flags = score_contact(c['properties'])
        return (
            -sc,                                                 # score DESC
            -likelihood_secondary(c['properties'], flags),       # likelihood DESC
            (c['properties'].get('lastname')  or '').lower(),   # last name ASC
            (c['properties'].get('firstname') or '').lower(),
        )
    sorted_contacts = sorted(contacts, key=_sort_key)

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

    day_score = sum(
        score_contact(c['properties'])[0] for c in contacts
        if (c['properties'].get('outbound_event_attendee_disqualified') or '').strip().lower() != 'disqualified'
    )
    attended_sc = sum(
        score_contact(c['properties'])[0] for c in contacts
        if (c['properties'].get('attended_outbound_event') or '').strip().lower() == 'yes'
    )
    attended_count  = sum(1 for c in contacts if (c['properties'].get('attended_outbound_event') or '').strip().lower() == 'yes')
    uninvite_count  = sum(1 for c in contacts if (c['properties'].get('outbound_event_attendee_disqualified') or '').strip().lower() == 'disqualified')
    send_conf_count = sum(1 for c in contacts if (c['properties'].get('outbound_event_send_confirmation') or '').strip().lower() == 'yes')
    attended_score_html = (
        f'<span style="font-size:0.78rem;color:#1a7a45;font-weight:700" '
        f'id="attended-score-{tab_id}">Attended score: {attended_sc}</span>'
        if attended_sc > 0 else
        f'<span style="font-size:0.78rem;color:#1a7a45;font-weight:700;display:none" '
        f'id="attended-score-{tab_id}"></span>'
    )

    past_note = ' <span style="font-size:0.72rem;color:#9aaac0">(past)</span>' if is_past(date_str) else ''
    display = 'block' if active else 'none'

    if past:
        # Lazy-load past events: render header + empty tbody; JS fills rows on first view
        return f'''
<div id="tab-{tab_id}" class="tab-panel" data-past="1" style="display:{display}">
  <div class="panel-header">
    <div>
      <div style="display:flex;align-items:baseline;gap:12px;flex-wrap:wrap">
        <span class="rsvp-count">{len(contacts)} RSVPs{past_note}</span>
        <span style="font-size:0.78rem;color:#7a94b8">Day score: <strong style="color:#1b3c6e">{day_score}</strong></span>
        {attended_score_html}
      </div>
      <div class="score-pills" style="margin-top:6px">{pills_html}</div>
    </div>
  </div>
  <div class="table-scroll">
    <table class="rsvp-table" id="tbl-{tab_id}">
      <thead><tr>
        <th style="width:34px">#</th>
        <th>Name</th>
        <th>Title / Company</th>
        <th style="width:120px">Property</th>
        <th style="width:120px">Likelihood</th>
        <th style="width:120px">Links</th>
      </tr></thead>
      <tbody id="tbody-{tab_id}"></tbody>
    </table>
  </div>
</div>'''

    # ── Future / today: full interactive panel ────────────────────────────────
    show_dropdown = not is_past(date_str)
    show_unk = date_str >= '2026-04-20'
    rows_html = ''.join(render_row(i + 1, c, show_dropdown, show_unk) for i, c in enumerate(sorted_contacts))

    opts = '<option value="">All Tiers</option>\n' + '\n'.join(
        f'<option value="{s}">{s} — {SCORE_LABELS[s]}</option>'
        for s in SCORES if counts[s]
    )

    return f'''
<div id="tab-{tab_id}" class="tab-panel" style="display:{display}">
  <div class="panel-header">
    <div>
      <div style="display:flex;align-items:baseline;gap:12px;flex-wrap:wrap">
        <span class="rsvp-count">{len(contacts)} RSVPs{past_note}</span>
        <span style="font-size:0.78rem;color:#7a94b8">Day score: <strong id="day-score-{tab_id}" style="color:#1b3c6e">{day_score}</strong></span>
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
        {'<th style="width:20px"></th>' if show_dropdown else ''}
        <th style="width:34px">#</th>
        <th>Name</th>
        <th>Title / Company</th>
        <th style="width:120px">Property</th>
        <th style="width:120px">Tier</th>
        <th style="width:120px">Threshold</th>
        <th style="width:120px">Links</th>
        <th style="width:120px">Email F/U</th>
        <th style="width:120px">Attended<br><span id="attended-count-{tab_id}" style="font-size:0.65rem;color:#1a7a45;font-weight:400">{attended_count if attended_count else ''}</span></th>
      </tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
  </div>
</div>'''

def build_html(by_date: dict, generated_at: str) -> str:
    _hs_tok_b64 = base64.b64encode(HUBSPOT_TOKEN.encode()).decode() if HUBSPOT_TOKEN else ''
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

    # Build JSON data for past events (used by lazy-load JS renderer)
    def _past_sort_key(c):
        sc, flags = score_contact(c['properties'])
        return (
            -sc,
            -likelihood_secondary(c['properties'], flags),
            (c['properties'].get('lastname')  or '').lower(),
            (c['properties'].get('firstname') or '').lower(),
        )

    past_events_data: dict = {}
    for d in past_dates:
        tid = d.replace('-', '')
        sorted_c = sorted(by_date[d], key=_past_sort_key)
        rows = []
        for c in sorted_c:
            p    = c['properties']
            name = f"{p.get('firstname', '')} {p.get('lastname', '')}".strip()
            sc, _ = score_contact(p)
            rows.append({
                'id':       c['id'],
                'name':     name,
                'jobtitle': p.get('jobtitle') or '',
                'company':  p.get('company')  or '',
                'pluto':    (p.get('_pluto_val') or '').strip(),
                'score':    sc,
                'li':       li_url(name, p.get('company') or '', p),
                'hs':       hs_url(c['id']),
                'coFlag':   _email_company_mismatch(p.get('email') or '', p.get('company') or ''),
                'emailDom': email_domain(p.get('email') or '') if _email_company_mismatch(p.get('email') or '', p.get('company') or '') else '',
            })
        past_events_data[tid] = rows
    past_events_json = json.dumps(past_events_data, ensure_ascii=False)

    panels = [
        render_panel(d, by_date[d], d.replace('-', ''), d.replace('-', '') == default_tab,
                     past=d in past_dates)
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

/* Three-state status slider */
.status-slider{{position:relative;width:48px;height:20px;display:inline-block;cursor:pointer;user-select:none}}
.slider-track{{position:absolute;top:3px;left:0;right:0;height:14px;background:#d0d5dd;border-radius:7px;transition:background 0.2s}}
.slider-thumb{{position:absolute;top:1px;left:16px;width:12px;height:12px;background:#fff;border-radius:50%;box-shadow:0 1px 3px rgba(0,0,0,0.3);transition:left 0.15s}}
.slider-labels{{position:absolute;top:2px;left:0;right:0;height:16px;display:flex;justify-content:space-between;align-items:center;padding:0 3px;pointer-events:none;font-size:0.55rem;font-weight:700}}
.sl-conf{{color:#1a7a45;opacity:0}} .sl-uninv{{color:#a83030;opacity:0}}
.status-slider[data-state="conf"] .slider-track{{background:#c0e8d0}}
.status-slider[data-state="conf"] .slider-thumb{{left:2px}}
.status-slider[data-state="conf"] .sl-conf{{opacity:1}}
.status-slider[data-state="uninvite"] .slider-track{{background:#f5c4c4}}
.status-slider[data-state="uninvite"] .slider-thumb{{left:33px}}
.status-slider[data-state="uninvite"] .sl-uninv{{opacity:1}}

/* ── Contact detail dropdown (today + future events only) ── */
.expand-chevron{{font-size:9px;color:#aaa;display:inline-block;transition:transform .15s;cursor:pointer}}
.expand-chevron.open{{transform:rotate(180deg)}}
.rsvp-table tbody tr.detail-row{{background:#f9f9f9!important;cursor:default}}
.rsvp-table tbody tr.detail-row:hover{{background:#f9f9f9!important}}
.detail-inner{{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;padding:10px 14px 10px 32px;width:100%;box-sizing:border-box}}
.detail-cell{{background:#fff;border:0.5px solid #e0e0e0;border-radius:8px;padding:10px 12px;
              display:flex;flex-direction:column;align-items:center;justify-content:flex-start;
              text-align:center;gap:6px}}
.detail-cell-label{{font-size:10px;color:#7a94b8;text-transform:uppercase;letter-spacing:.05em;font-weight:600}}
.seg-stack{{display:flex;flex-direction:column;gap:2px;width:100%}}
.seg-row{{display:flex;align-items:center;justify-content:center;gap:6px}}
.seg-src{{font-size:10px;color:#aaa;width:80px;text-align:right;flex-shrink:0}}
.seg-val{{font-size:13px;font-weight:500;color:#1b3c6e;width:120px;text-align:left;word-break:break-word}}
.seg-divider{{border:none;border-top:0.5px solid #e0e0e0;margin:3px 0;width:100%}}
.prop-value{{font-size:13px;font-weight:500;color:#1b3c6e}}
.prop-neighborhood{{font-size:12px;color:#888}}
.prop-own-tag{{font-size:11px;padding:2px 6px;border-radius:4px;background:#E6F1FB;color:#0C447C;font-weight:500}}
.prop-rent-tag{{font-size:11px;padding:2px 6px;border-radius:4px;background:#FAEEDA;color:#633806;font-weight:500}}
.persona-detail-pill{{display:inline-block;background:#f0f3f8;color:#1b3c6e;border:1px solid #c9d4e8;
                      border-radius:10px;font-size:0.75rem;font-weight:600;padding:3px 10px}}

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
  </div>
</header>

<div class="tab-bar" id="tabBar">
  {''.join(tab_btns)}
  {past_btn_html}
  <div style="flex:1"></div>
  <button class="tab-btn" data-tab="pipeline" onclick="switchTab('pipeline')"
    style="border-bottom:3px solid transparent;color:#7a94b8;font-weight:normal;border-left:1px solid #e2e8f4;padding-left:20px;">
    Pipeline Review</button>
</div>
<div class="no-date-msg" id="noDateMsg">No RSVP data for this date in the current window.</div>

{past_menu_html}

<div id="tab-pipeline" class="tab-panel" style="display:none">
  <iframe src="pipeline.html" style="width:100%;height:calc(100vh - 130px);border:none;display:block"></iframe>
</div>

<div class="content">
  {''.join(panels)}
</div>

<!-- Score override popover -->
<div class="score-popover" id="scorePopover">
  <div style="font-size:0.65rem;color:#8a9fc0;text-transform:uppercase;letter-spacing:0.1em;
              padding:2px 6px 6px;border-bottom:1px solid #eef1f7;margin-bottom:2px">
    Override tier
  </div>
</div>

<script>
var GITHUB_REPO       = '{escape(GITHUB_REPO)}';
var GITHUB_WORKFLOW   = '{GITHUB_WORKFLOW}';
var HS_TOKEN          = atob('{_hs_tok_b64}');
var PAST_EVENTS_DATA  = {past_events_json};
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
  _flushUninvitesAndRefresh(tok, btn);
}}

function _flushUninvitesAndRefresh(tok, btn) {{
  var state = {{}};
  for (var i = 0; i < localStorage.length; i++) {{
    var k = localStorage.key(i);
    if (k && (k.startsWith('uninvite_') || k.startsWith('sendconf_'))) {{
      state[k] = localStorage.getItem(k);
    }}
  }}
  var gistPromise = Object.keys(state).length
    ? fetch('https://api.github.com/gists/{SHARED_GIST_ID}', {{
        method: 'PATCH',
        headers: {{'Authorization': 'token ' + tok, 'Accept': 'application/vnd.github.v3+json', 'Content-Type': 'application/json'}},
        body: JSON.stringify({{ files: {{ '{GIST_STATE_FILE}': {{ content: JSON.stringify(state) }} }} }})
      }})
    : Promise.resolve();
  return gistPromise.then(function() {{
    return fetch('https://api.github.com/repos/' + GITHUB_REPO + '/actions/workflows/' + GITHUB_WORKFLOW + '/dispatches', {{
      method: 'POST',
      headers: {{'Authorization': 'token ' + tok, 'Accept': 'application/vnd.github.v3+json', 'Content-Type': 'application/json'}},
      body: JSON.stringify({{ref: 'main'}})
    }});
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
  5: {{label:'High',       fg:'#ffffff', bg:'#0a2f6b'}},
  4: {{label:'Medium-High',fg:'#ffffff', bg:'#1a5fa8'}},
  3: {{label:'Medium',     fg:'#1a5fa8', bg:'#a3c9f0'}},
  2: {{label:'Low-Medium', fg:'#2b6cb0', bg:'#d4e6f9'}},
  1: {{label:'Low',        fg:'#6a9fd8', bg:'#edf4fc'}},
}};

// ── Shared state (localStorage + direct HubSpot writes) ───────────────────────
function getSharedState(key) {{
  return localStorage.getItem(key);
}}
function saveSharedState(key, val) {{
  localStorage.setItem(key, val);
}}
function removeSharedState(key) {{
  localStorage.removeItem(key);
}}
function _patchHubSpot(cid, properties) {{
  // HubSpot private app tokens don't support CORS — browser fetch will be blocked.
  // Uninvites are synced via Gist → Python daily run instead (_syncUninvitesToGist).
}}
function _syncSharedStateToGist() {{
  var tok = localStorage.getItem('gh_pat');
  if (!tok) return;
  var state = {{}};
  for (var i = 0; i < localStorage.length; i++) {{
    var k = localStorage.key(i);
    if (k && (k.startsWith('uninvite_') || k.startsWith('sendconf_'))) {{
      state[k] = localStorage.getItem(k);
    }}
  }}
  fetch('https://api.github.com/gists/{SHARED_GIST_ID}', {{
    method: 'PATCH',
    headers: {{
      'Authorization': 'token ' + tok,
      'Accept': 'application/vnd.github.v3+json',
      'Content-Type': 'application/json'
    }},
    body: JSON.stringify({{ files: {{ '{GIST_STATE_FILE}': {{ content: JSON.stringify(state) }} }} }})
  }}).then(function(r) {{
    if (!r.ok) {{ console.error('Gist write returned', r.status); }}
  }}).catch(function(e) {{ console.error('Gist write failed:', e); }});
}}
// Back-compat alias — older inline callers may still reference this name.
function _syncUninvitesToGist() {{ _syncSharedStateToGist(); }}
function initSharedState(cb) {{
  if (cb) cb();
}}

// ── HTML escaping helper ──────────────────────────────────────────────────────
function escHtml(s) {{
  return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}}

// ── Avatar fallback cascade: LinkedIn → Gravatar → initials ──────────────────
function _avatarErr(img) {{
  var next = img.getAttribute('data-fb');
  if (next) {{
    img.removeAttribute('data-fb');  // prevent infinite loop
    img.src = next;
  }} else {{
    img.outerHTML = img.getAttribute('data-fb-html') || '';
  }}
}}

// ── Photo modal: click avatar → enlarge in popup ─────────────────────────────
function openPhotoModal(ev, img) {{
  if (ev) {{ ev.stopPropagation(); ev.preventDefault(); }}  // don't expand row
  var modal = document.getElementById('photo-modal');
  var modalImg = document.getElementById('photo-modal-img');
  if (!modal || !modalImg) return;
  modalImg.src = img.src;
  modalImg.alt = img.alt || '';
  modal.style.display = 'flex';
  document.body.style.overflow = 'hidden';  // freeze background scroll
}}
function closePhotoModal() {{
  var modal = document.getElementById('photo-modal');
  if (!modal) return;
  modal.style.display = 'none';
  document.body.style.overflow = '';
  var modalImg = document.getElementById('photo-modal-img');
  if (modalImg) modalImg.src = '';  // free memory
}}
// Close on Escape
document.addEventListener('keydown', function(e) {{
  if (e.key === 'Escape') closePhotoModal();
}});

// ── Lazy-load past event rows ─────────────────────────────────────────────────
var _renderedPast = {{}};
function renderPastTab(tabId) {{
  if (_renderedPast[tabId]) return;
  var contacts = PAST_EVENTS_DATA[tabId];
  if (!contacts) return;
  _renderedPast[tabId] = true;
  var tbody = document.getElementById('tbody-' + tabId);
  if (!tbody) return;
  var html = '';
  for (var i = 0; i < contacts.length; i++) {{
    var c = contacts[i];
    var m = SCORE_META[c.score] || {{label:'—', fg:'#666', bg:'#eee'}};
    var coFlagHtml = c.coFlag ? '<span title="Email domain (' + escHtml(c.emailDom) + ') does not match company" style="color:#c94040;font-size:0.65rem;margin-left:4px;cursor:help">⚑</span>' : '';
    var titleHtml = (c.jobtitle ? '<div style="font-size:0.82rem">' + escHtml(c.jobtitle) + '</div>' : '') +
                    (c.company  ? '<div style="font-size:0.75rem;color:#7a94b8">' + escHtml(c.company) + coFlagHtml + '</div>' : '');
    var propHtml = '';
    if (c.pluto && c.pluto !== 'Commercial') {{
      propHtml = '<span style="font-size:0.75rem">' + escHtml(c.pluto) + '</span>';
    }} else {{
      propHtml = '<span style="color:#c0ccd8">\u2014</span>';
    }}
    var badge = '<span style="background:' + m.bg + ';color:' + m.fg + ';border:1px solid ' + m.fg + '55;' +
                'padding:4px 11px;border-radius:12px;font-size:0.78rem;font-weight:700">' +
                c.score + '</span>';
    var liLink = c.li ? '<a href="' + escHtml(c.li) + '" target="_blank" rel="noopener" style="color:#0a66c2;font-weight:700;text-decoration:none;font-size:0.8rem;margin-right:8px">LI\u2197</a>' : '';
    var hsLink = '<a href="' + escHtml(c.hs) + '" target="_blank" rel="noopener" style="color:#ff7a59;font-weight:700;text-decoration:none;font-size:0.8rem">HS\u2197</a>';
    html += '<tr>' +
      '<td style="color:#9aaac0;font-size:0.78rem">' + (i + 1) + '</td>' +
      '<td style="font-weight:600">' + escHtml(c.name) + '</td>' +
      '<td>' + titleHtml + '</td>' +
      '<td style="text-align:center">' + propHtml + '</td>' +
      '<td>' + badge + '</td>' +
      '<td style="text-align:center;white-space:nowrap">' + liLink + hsLink + '</td>' +
    '</tr>';
  }}
  tbody.innerHTML = html;
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
  renderPastTab(id);
  applyStoredOverrides(id);
  initSliders();
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

function todayStr() {{
  var d = new Date();
  return d.getFullYear() + '-' + String(d.getMonth()+1).padStart(2,'0') + '-' + String(d.getDate()).padStart(2,'0');
}}

function setOverride(cid, sc, tabId) {{
  var key = 'override_' + cid;
  if (sc === getAutoScore(cid, tabId)) {{
    removeSharedState(key);
  }} else {{
    saveSharedState(key, sc + '|' + todayStr());
  }}
  applyOverride(cid, sc, tabId);
  updateResetBtn(tabId);
}}

function readOverride(cid) {{
  var raw = getSharedState('override_' + cid);
  if (!raw) return null;
  var parts = raw.split('|');
  var sc = parseInt(parts[0]);
  var dt = parts[1] || '';
  if (dt && dt !== todayStr()) {{
    removeSharedState('override_' + cid);
    return null;
  }}
  return sc;
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
  var manual = readOverride(cid);

  row.dataset.score = sc;
  cell.innerHTML =
    '<span class="score-badge" data-score="' + sc + '" ' +
    'style="background:' + m.bg + ';color:' + m.fg + ';border:1px solid ' + m.fg + '55;' +
    'padding:4px 11px;border-radius:12px;font-size:0.78rem;font-weight:700;' +
    'letter-spacing:0.03em;white-space:nowrap;cursor:pointer;' +
    'display:inline-flex;align-items:center;gap:5px">' +
    '<span class="score-num">' + sc + '</span>' +
    (manual !== null ? '<span style="font-size:0.62rem;opacity:0.6" title="Manually overridden">✏</span>' : '') +
    '</span>';

  if (manual !== null && manual !== auto) {{
    row.classList.add('overridden');
  }} else {{
    row.classList.remove('overridden');
  }}

  reorderTab(tabId);
}}

function reorderTab(tabId) {{
  var tbody = document.querySelector('#tbl-' + tabId + ' tbody');
  if (!tbody) return;

  // Collect [contactRow, detailRow|null] pairs — contact rows have data-id
  var pairs = [];
  var rows = Array.from(tbody.rows);
  var i = 0;
  while (i < rows.length) {{
    var r = rows[i];
    if (r.dataset.id) {{
      var next   = rows[i + 1];
      var detail = (next && !next.dataset.id) ? next : null;
      pairs.push([r, detail]);
      i += detail ? 2 : 1;
    }} else {{
      i++;
    }}
  }}

  // Sort: uninvited → bottom, then score descending
  pairs.sort(function(a, b) {{
    var aU = a[0].classList.contains('uninvited') ? 1 : 0;
    var bU = b[0].classList.contains('uninvited') ? 1 : 0;
    if (aU !== bU) return aU - bU;
    return parseInt(b[0].dataset.score) - parseInt(a[0].dataset.score);
  }});

  // Re-insert and renumber (# cell is cells[1] when chevron is present, else cells[0])
  pairs.forEach(function(pair, idx) {{
    var r      = pair[0];
    var hasChevron = r.cells[0] && r.cells[0].querySelector('.expand-chevron');
    var numCell    = r.cells[hasChevron ? 1 : 0];
    if (numCell) numCell.textContent = idx + 1;
    tbody.appendChild(r);
    if (pair[1]) tbody.appendChild(pair[1]);
  }});
}}

function initSliders() {{
  document.querySelectorAll('.status-slider').forEach(function(sl) {{
    sl.addEventListener('click', function(e) {{
      var rect = sl.getBoundingClientRect();
      var x = e.clientX - rect.left;
      var third = rect.width / 3;
      var newState;
      if (x < third) newState = 'conf';
      else if (x > third * 2) newState = 'uninvite';
      else newState = 'neutral';
      setSliderState(sl, newState);
    }});
  }});
}}

function setSliderState(sl, state) {{
  var row = sl.closest('tr');
  var cid = row.dataset.id;
  var tid = row.closest('.tab-panel').id.replace('tab-','');
  var prev = sl.getAttribute('data-state');
  sl.setAttribute('data-state', state);

  if (state === 'uninvite') {{
    saveSharedState('uninvite_' + cid, '1');
    removeSharedState('sendconf_' + cid);
    row.classList.add('uninvited');
  }} else if (state === 'conf') {{
    saveSharedState('sendconf_' + cid, '1');
    removeSharedState('uninvite_' + cid);
    row.classList.remove('uninvited');
  }} else {{
    removeSharedState('uninvite_' + cid);
    removeSharedState('sendconf_' + cid);
    row.classList.remove('uninvited');
  }}
  _syncUninvitesToGist();
  _syncSharedStateToGist();
  reorderTab(tid);
  refreshHeader(tid);
  updateResetBtn(tid);
}}

function toggleAttended(chk) {{
  var row = chk.closest('tr');
  var cid = row.dataset.id;
  var tid = row.closest('.tab-panel').id.replace('tab-','');
  if (chk.checked) {{
    saveSharedState('attended_' + cid, '1');
    _patchHubSpot(cid, {{attended_outbound_event: 'yes'}});
  }} else {{
    removeSharedState('attended_' + cid);
    _patchHubSpot(cid, {{attended_outbound_event: ''}});
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

  var attCount = document.querySelectorAll('#tbl-' + tabId + ' .attended-chk:checked').length;
  var attEl = document.getElementById('attended-count-' + tabId);
  if (attEl) attEl.textContent = attCount || '';

  var dayScore = 0;
  document.querySelectorAll('#tbl-' + tabId + ' tbody tr').forEach(function(row) {{
    if (!row.classList.contains('uninvited')) {{
      dayScore += parseInt(row.dataset.score) || 0;
    }}
  }});
  var dsEl = document.getElementById('day-score-' + tabId);
  if (dsEl) dsEl.textContent = dayScore;
}}

function applyStoredOverrides(tabId) {{
  var rows = document.querySelectorAll('#tbl-' + tabId + ' tbody tr');
  rows.forEach(function(row) {{
    var cid = row.dataset.id;
    var val = readOverride(cid);
    if (val !== null) applyOverride(cid, val, tabId);
    var sl = row.querySelector('.status-slider');
    if (sl) {{
      if (getSharedState('uninvite_' + cid)) {{
        sl.setAttribute('data-state', 'uninvite');
        row.classList.add('uninvited');
      }} else if (getSharedState('sendconf_' + cid)) {{
        sl.setAttribute('data-state', 'conf');
      }}
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
    return readOverride(r.dataset.id) !== null ||
           getSharedState('uninvite_'  + r.dataset.id) ||
           getSharedState('attended_'  + r.dataset.id) ||
           getSharedState('sendconf_'  + r.dataset.id);
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
    var wasUninvited = !!getSharedState('uninvite_' + cid);
    var wasAttended  = !!getSharedState('attended_'  + cid);
    var wasSendConf  = !!getSharedState('sendconf_'  + cid);
    removeSharedState('uninvite_' + cid);
    removeSharedState('attended_' + cid);
    removeSharedState('sendconf_' + cid);
    applyOverride(cid, auto, tabId);
    row.classList.remove('uninvited');
    var sl = row.querySelector('.status-slider');
    if (sl) sl.setAttribute('data-state', 'neutral');
    var achk = row.querySelector('.attended-chk');
    if (achk) achk.checked = false;
    if (wasUninvited) _patchHubSpot(cid, {{outbound_event_attendee_disqualified: ''}});
    if (wasAttended)  _patchHubSpot(cid, {{attended_outbound_event: ''}});
    if (wasSendConf)  _syncSharedStateToGist();
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

// ── Census ACS median home value (zip-level, no API key) ─────────────────────
var _censusCache = {{}};
async function _fetchCensusValue(zip) {{
  if (!zip) return null;
  if (_censusCache[zip] !== undefined) return _censusCache[zip];
  try {{
    var url = 'https://api.census.gov/data/2022/acs/acs5?get=B25077_001E&for=zip%20code%20tabulation%20area:' + zip + '&key={escape(CENSUS_API_KEY)}';
    var res = await fetch(url);
    var data = await res.json();
    var val = parseInt(data[1][0]);
    if (!val || val <= 0) {{ _censusCache[zip] = null; return null; }}
    var fmt = val >= 1000000
      ? '$' + (val / 1000000).toFixed(1) + 'M'
      : '$' + Math.round(val / 1000) + 'K';
    _censusCache[zip] = fmt + ' median';
    return _censusCache[zip];
  }} catch(e) {{ _censusCache[zip] = null; return null; }}
}}
(async function() {{
  var cells = document.querySelectorAll('.detail-cell[data-zip]');
  var zips = {{}};
  cells.forEach(function(c) {{ if (c.dataset.zip) zips[c.dataset.zip] = _fetchCensusValue(c.dataset.zip); }});
  for (var i = 0; i < cells.length; i++) {{
    var cell = cells[i];
    var zip  = cell.dataset.zip;
    if (!zip) continue;
    var result = await zips[zip];
    var span = cell.querySelector('.census-value');
    if (span) {{
      span.textContent = result || '—';
      span.style.color = result ? '#1b3c6e' : '#aabcd4';
      span.style.fontSize = result ? '13px' : '12px';
    }}
  }}
}})();

// ── Contact detail dropdown ───────────────────────────────────────────────────
document.querySelectorAll('tr[data-contact]').forEach(function(row) {{
  row.addEventListener('click', function(e) {{
    // Don't expand when clicking checkboxes, links, or score badge
    if (e.target.closest('input,a,.score-badge,.score-popover')) return;
    var detail  = row.nextElementSibling;
    var chevron = row.querySelector('.expand-chevron');
    if (!detail || !chevron) return;
    var isOpen = detail.style.display === 'table-row';
    detail.style.display = isOpen ? 'none' : 'table-row';
    chevron.classList.toggle('open', !isOpen);
  }});
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

  // Render past tab rows if default tab is a past event
  renderPastTab(defaultTab);

  initSharedState(function() {{
    // Re-apply overrides for every visible tab now that Gist is loaded
    document.querySelectorAll('.tab-panel').forEach(function(p) {{
      if (p.style.display !== 'none') {{
        var tid = p.id.replace('tab-', '');
        applyStoredOverrides(tid);
        updateResetBtn(tid);
      }}
    }});
    if (defaultTab) {{
      applyStoredOverrides(defaultTab);
      updateResetBtn(defaultTab);
    }}
    initSliders();
    if ('{past_default_label}' && defaultTab === '{default_tab}') {{
      document.querySelectorAll('.past-opt').forEach(function(o) {{
        if (o.dataset.tid === defaultTab) o.classList.add('active-past');
      }});
    }}
  }});
}})();
</script>

<!-- Photo modal — shown when an avatar is clicked -->
<div id="photo-modal"
     onclick="if (event.target === this) closePhotoModal()"
     style="display:none;position:fixed;inset:0;z-index:9999;
            background:rgba(8,16,32,0.78);align-items:center;justify-content:center;
            cursor:zoom-out">
  <div style="position:relative;max-width:90vw;max-height:90vh">
    <button onclick="closePhotoModal()"
            aria-label="Close"
            style="position:absolute;top:-12px;right:-12px;width:36px;height:36px;
                   border-radius:50%;border:none;background:#fff;color:#1b3c6e;
                   font-size:1.2rem;font-weight:700;cursor:pointer;
                   box-shadow:0 4px 14px rgba(0,0,0,0.35);line-height:1;
                   display:flex;align-items:center;justify-content:center">×</button>
    <img id="photo-modal-img" src="" alt=""
         style="display:block;max-width:90vw;max-height:90vh;border-radius:8px;
                box-shadow:0 12px 40px rgba(0,0,0,0.5);background:#1b3c6e;cursor:default"
         onclick="event.stopPropagation()">
  </div>
</div>
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
            'calls_booked':   s['calls_booked'],
            'stale_45':       s['stale_45'],
            'email_opens':    s['email_opens'],
            'email_delivered': s['email_delivered'],
            'replies':        s['replies'],
            'tiers':          s['tiers'],
            'tier_calls':     s['tier_calls'],
            'tier_accts':     s['tier_accts'],
            'tier_invested':  s['tier_invested'],
        })

    events_json = json.dumps(events)

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Masterworks — Event Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ background:#d8e2f0; color:#1e2a3a; font-family:'Georgia',serif; min-height:100vh; }}

  header {{ background:#1b3c6e; padding:16px 28px; position:sticky; top:0; z-index:100;
            box-shadow:0 2px 8px rgba(27,60,110,0.25); }}
  .header-row {{ display:flex; align-items:center; justify-content:space-between; gap:12px; flex-wrap:wrap; }}
  .brand {{ font-size:0.62rem; letter-spacing:0.2em; text-transform:uppercase; color:#c9a84c; font-weight:700; }}
  .title {{ font-size:0.98rem; font-weight:700; color:#fff; margin-top:2px; }}
  .meta  {{ font-size:0.65rem; color:rgba(255,255,255,0.4); margin-top:2px; }}
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

  /* Alert banner */
  .alert-banner {{ background:#fdecec; border-left:4px solid #a83030; border-bottom:1px solid #f2c6c6;
                  padding:12px 40px; display:flex; align-items:center; gap:14px; }}
  .alert-banner.hidden {{ display:none; }}
  .alert-icon {{ color:#a83030; font-size:1.15rem; font-weight:700; }}
  .alert-text {{ color:#6e2020; font-size:0.82rem; letter-spacing:0.02em; flex:1; }}

  /* Stat tiles (2 rows of 4) */
  .stats-bar {{ background:#162f57; border-bottom:1px solid #1b3c6e;
               padding:0 40px; display:grid; grid-template-columns:repeat(4, 1fr); align-items:stretch; }}
  .stat-tile {{ padding:18px 24px; display:flex; flex-direction:column; align-items:center;
               gap:4px; border-right:1px solid rgba(255,255,255,0.07); }}
  .stat-tile:last-child {{ border-right:none; }}
  .stat-value {{ font-size:1.65rem; color:#e8f0fc; letter-spacing:0.02em; line-height:1; }}
  .stat-label {{ font-size:0.65rem; color:#7aaace; text-transform:uppercase; letter-spacing:0.13em; white-space:nowrap; font-weight:600; }}
  .stat-sub   {{ font-size:0.66rem; color:#5a7a9e; letter-spacing:0.02em; margin-top:3px; font-variant-numeric:tabular-nums; }}
  .stat-tile.teal   .stat-value {{ color:#7dc4a8; }}
  .stat-tile.green  .stat-value {{ color:#6dbf82; }}
  .stat-tile.gold   .stat-value {{ color:#d4a96a; }}
  .stat-tile.purple .stat-value {{ color:#b49ee0; }}
  .stat-tile.amber  .stat-value {{ color:#c9a84c; }}
  .stat-tile.blue   .stat-value {{ color:#4a90d9; }}

  /* Section wraps */
  .section-wrap {{ padding:22px 40px 8px; }}
  .section-title {{ font-size:0.7rem; color:#5a7a9e; letter-spacing:0.14em;
                   text-transform:uppercase; font-weight:700; margin-bottom:10px; }}

  /* Charts */
  .chart-grid {{ display:grid; grid-template-columns:repeat(2, 1fr); gap:16px; }}
  .chart-card {{ background:#fff; border-radius:4px; box-shadow:0 1px 4px rgba(27,60,110,0.08); padding:16px 18px; }}
  .chart-header {{ display:flex; justify-content:space-between; align-items:baseline; margin-bottom:10px; }}
  .chart-title {{ font-size:0.78rem; color:#1b3c6e; font-weight:700; letter-spacing:0.04em; }}
  .chart-trend {{ font-size:0.74rem; letter-spacing:0.02em; font-weight:600; }}
  .chart-trend.up   {{ color:#1a7a45; }}
  .chart-trend.down {{ color:#a83030; }}
  .chart-trend.flat {{ color:#8a9ab8; }}
  .chart-canvas-wrap {{ height:160px; position:relative; }}

  /* Tier conversion table */
  .tier-table {{ width:100%; border-collapse:separate; border-spacing:0; background:#fff;
                border-radius:4px; overflow:hidden; box-shadow:0 1px 4px rgba(27,60,110,0.08); }}
  .tier-table thead th {{ background:#1b3c6e; color:#a8c8e8; font-size:0.72rem; text-transform:uppercase;
                        letter-spacing:0.1em; font-weight:600; padding:11px 14px; text-align:center; white-space:nowrap; }}
  .tier-table tbody tr:nth-child(even) {{ background:#f8fafd; }}
  .tier-table td {{ padding:12px 14px; font-size:0.85rem; color:#4a5f78; text-align:center; font-variant-numeric:tabular-nums; }}
  .tier-badge {{ display:inline-block; width:26px; height:26px; border-radius:50%;
                font-weight:700; line-height:26px; text-align:center; font-size:0.82rem; }}
  .tier-5 {{ background:#faf3e0; color:#8a6800; }}
  .tier-4 {{ background:#e8f0fb; color:#1a5fa8; }}
  .tier-3, .tier-2, .tier-1 {{ background:#f0f3f8; color:#6a7a92; }}
  .conv-high {{ color:#1a7a45; font-weight:700; }}
  .conv-mid  {{ color:#8a6800; font-weight:600; }}
  .conv-low  {{ color:#8a9ab8; }}
  .conv-none {{ color:#b0c4d8; }}

  /* Event table */
  .table-wrap {{ padding:0 40px 48px; overflow-x:auto; }}
  table.main {{ width:100%; border-collapse:separate; border-spacing:0; background:#fff;
               border-radius:4px; overflow:hidden; box-shadow:0 1px 4px rgba(27,60,110,0.08); margin-top:14px; }}
  table.main thead th {{ background:#1b3c6e; color:#a8c8e8; font-size:0.72rem; text-transform:uppercase;
                        letter-spacing:0.1em; font-weight:600; padding:11px 14px; text-align:center; white-space:nowrap; }}
  table.main thead th.group-rsvp     {{ color:#5fa8e8; border-bottom:3px solid #3a78c0; }}
  table.main thead th.group-attended {{ color:#7dc4a8; border-bottom:3px solid #5aaa8a; }}
  table.main thead th.group-score    {{ color:#c9a84c; border-bottom:3px solid #9a8030; }}
  table.main thead th.group-email    {{ color:#b49ee0; border-bottom:3px solid #8060c0; }}
  table.main thead th.group-call     {{ color:#e8a868; border-bottom:3px solid #c88040; }}
  table.main thead th.group-account  {{ color:#d4a96a; border-bottom:3px solid #b88840; }}
  table.main thead th.group-invested {{ color:#6dbf82; border-bottom:3px solid #4a9f60; }}
  table.main thead th.group-stale    {{ color:#d88080; border-bottom:3px solid #a83030; }}
  table.main tbody tr:nth-child(even) {{ background:#f8fafd; }}
  table.main tbody tr:hover {{ background:#edf3fb; }}
  table.main td {{ padding:14px; font-size:0.85rem; color:#4a5f78; white-space:nowrap; text-align:center;
                  font-variant-numeric:tabular-nums; }}
  .date-cell {{ color:#1b3c6e; font-size:0.88rem; font-weight:700; text-align:left; letter-spacing:0.01em; }}
  .tier-badges {{ display:inline-flex; gap:5px; }}
  .tier-mini {{ display:inline-flex; align-items:center; gap:3px; font-size:0.78rem; font-weight:600; }}
  .tier-mini .dot {{ width:18px; height:18px; border-radius:50%; line-height:18px; text-align:center;
                    font-size:0.65rem; font-weight:700; }}
  .tier-mini.t5 .dot {{ background:#faf3e0; color:#8a6800; }}
  .tier-mini.t4 .dot {{ background:#e8f0fb; color:#1a5fa8; }}
  .tier-mini.t3 .dot {{ background:#f0f3f8; color:#6a7a92; }}
  .muted-dash {{ color:#b0c4d8; }}
  .stale-warn {{ color:#a83030; font-weight:700; }}
  .stale-ok   {{ color:#b0c4d8; }}

  footer {{ padding:24px 40px; text-align:center; font-size:0.7rem; color:#6a90be;
           letter-spacing:0.1em; text-transform:uppercase; background:#1b3c6e; }}
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
  </div>
</header>

<!-- Alert banner (hidden when staleTotal === 0) -->
<div class="alert-banner hidden" id="alertBanner">
  <span class="alert-icon">!</span>
  <span class="alert-text"><strong id="staleCount">0</strong> Tier 4–5 leads across all events with no call booked</span>
</div>

<!-- Row 1: core counts -->
<div class="stats-bar">
  <div class="stat-tile teal">
    <span class="stat-value" id="sAttended">—</span>
    <span class="stat-label">Total Attended</span>
    <span class="stat-sub" id="sAttendedSub">—</span>
  </div>
  <div class="stat-tile">
    <span class="stat-value" id="sCalls">—</span>
    <span class="stat-label">Calls Booked</span>
    <span class="stat-sub" id="sCallsSub">—</span>
  </div>
  <div class="stat-tile gold">
    <span class="stat-value" id="sAccts">—</span>
    <span class="stat-label">Acct Creates</span>
    <span class="stat-sub" id="sAcctsSub">—</span>
  </div>
  <div class="stat-tile amber">
    <span class="stat-value" id="sInvested">—</span>
    <span class="stat-label">Investments</span>
    <span class="stat-sub" id="sInvestedSub">—</span>
  </div>
</div>

<!-- Row 2: rates -->
<div class="stats-bar">
  <div class="stat-tile blue">
    <span class="stat-value" id="sOpenRate">—</span>
    <span class="stat-label">Email Open Rate</span>
    <span class="stat-sub" id="sOpenRateSub">—</span>
  </div>
  <div class="stat-tile blue">
    <span class="stat-value" id="sReplyRate">—</span>
    <span class="stat-label">Reply Rate</span>
    <span class="stat-sub" id="sReplyRateSub">—</span>
  </div>
  <div class="stat-tile amber">
    <span class="stat-value" id="sT5Call">—</span>
    <span class="stat-label">Tier 5 → Call</span>
    <span class="stat-sub" id="sT5CallSub">—</span>
  </div>
  <div class="stat-tile gold">
    <span class="stat-value" id="sT5Inv">—</span>
    <span class="stat-label">Tier 5 → Invested</span>
    <span class="stat-sub" id="sT5InvSub">—</span>
  </div>
</div>

<!-- Trend charts -->
<div class="section-wrap">
  <div class="section-title">Trends Across Events</div>
  <div class="chart-grid">
    <div class="chart-card">
      <div class="chart-header"><span class="chart-title">Show Rate %</span><span class="chart-trend flat" id="trendShow">—</span></div>
      <div class="chart-canvas-wrap"><canvas id="c1"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="chart-header"><span class="chart-title">Email Open Rate %</span><span class="chart-trend flat" id="trendOpen">—</span></div>
      <div class="chart-canvas-wrap"><canvas id="c2"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="chart-header"><span class="chart-title">Call Booked Rate %</span><span class="chart-trend flat" id="trendCall">—</span></div>
      <div class="chart-canvas-wrap"><canvas id="c3"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="chart-header"><span class="chart-title">Investments (count)</span><span class="chart-trend flat" id="trendInv">—</span></div>
      <div class="chart-canvas-wrap"><canvas id="c4"></canvas></div>
    </div>
  </div>
</div>

<!-- Tier conversion table -->
<div class="section-wrap">
  <div class="section-title">Conversion by Tier · Aggregated All Events</div>
  <table class="tier-table">
    <thead>
      <tr>
        <th>Tier</th>
        <th>Leads</th>
        <th>Call Booked</th>
        <th>Acct Create</th>
        <th>Invested</th>
      </tr>
    </thead>
    <tbody id="tierTableBody"></tbody>
  </table>
</div>

<!-- Event table -->
<div class="section-wrap">
  <div class="section-title">Per-Event Breakdown</div>
</div>
<div class="table-wrap">
  <table class="main">
    <thead>
      <tr>
        <th style="text-align:left">Event</th>
        <th class="group-attended">Attended / RSVPs</th>
        <th class="group-score">Tiers</th>
        <th class="group-email">Open %</th>
        <th class="group-call">Calls Booked</th>
        <th class="group-account">Accts</th>
        <th class="group-invested">Invested</th>
        <th class="group-stale">Stale 4–5</th>
      </tr>
    </thead>
    <tbody id="eventTableBody"></tbody>
  </table>
</div>

<footer>Masterworks Internal · Outbound Events · Data as of {escape(generated_at)}</footer>

<script>
var GITHUB_REPO     = '{escape(GITHUB_REPO)}';
var GITHUB_WORKFLOW = '{GITHUB_WORKFLOW}';

const eventData = {events_json};
const TODAY = '{today_str}';

function isPending(e) {{ return e.attended === 0 && e.date >= TODAY; }}
function fmt(n)    {{ return n == null ? '—' : n.toLocaleString(); }}
function fmtCap(n) {{
  if (!n) return '—';
  if (n >= 1000000) return '$' + (n/1000000).toFixed(2) + 'M';
  if (n >= 1000)    return '$' + (n/1000).toFixed(1) + 'K';
  return '$' + n.toLocaleString();
}}
function pct(a, b) {{ if (!b) return null; return Math.round(a / b * 100); }}
function formatDateShort(d) {{
  var p = d.split('-');
  var months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  var days   = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
  return days[new Date(+p[0],+p[1]-1,+p[2]).getDay()] + ', ' + months[+p[1]-1] + ' ' + +p[2];
}}
function tracked() {{ return eventData.filter(function(e){{ return !isPending(e); }}); }}

function computeTotals() {{
  var t = tracked();
  var sum = function(arr, k) {{ return arr.reduce(function(s,e){{ return s + (e[k]||0); }}, 0); }};
  var tiers = {{1:0, 2:0, 3:0, 4:0, 5:0}};
  var tier_calls = {{1:0, 2:0, 3:0, 4:0, 5:0}};
  var tier_accts = {{1:0, 2:0, 3:0, 4:0, 5:0}};
  var tier_invested = {{1:0, 2:0, 3:0, 4:0, 5:0}};
  t.forEach(function(e) {{
    for (var k in (e.tiers||{{}}))          tiers[k]         += (e.tiers[k]||0);
    for (var k in (e.tier_calls||{{}}))     tier_calls[k]    += (e.tier_calls[k]||0);
    for (var k in (e.tier_accts||{{}}))     tier_accts[k]    += (e.tier_accts[k]||0);
    for (var k in (e.tier_invested||{{}}))  tier_invested[k] += (e.tier_invested[k]||0);
  }});
  return {{
    totRSVPs:     sum(t, 'rsvps'),
    totAttended:  sum(t, 'attended'),
    totCalls:     sum(t, 'calls_booked'),
    totAccts:     sum(t, 'accountCreated'),
    totInvested:  sum(t, 'invested'),
    totCapital:   sum(t, 'capital'),
    totOpens:     sum(t, 'email_opens'),
    totDelivered: sum(t, 'email_delivered'),
    totReplies:   sum(t, 'replies'),
    totStale45:   sum(t, 'stale_45'),
    tiers: tiers, tier_calls: tier_calls, tier_accts: tier_accts, tier_invested: tier_invested
  }};
}}

function convCell(n, leads) {{
  if (!leads) return '<span class="conv-none">—</span>';
  var p = Math.round(n / leads * 100);
  if (n === 0) return '<span class="conv-none">—</span>';
  var cls = p >= 25 ? 'conv-high' : p >= 8 ? 'conv-mid' : 'conv-low';
  return n + ' <span class="' + cls + '">(' + p + '%)</span>';
}}

function renderStats() {{
  var T = computeTotals();

  if (T.totStale45 > 0) {{
    document.getElementById('alertBanner').classList.remove('hidden');
    document.getElementById('staleCount').textContent = T.totStale45;
  }} else {{
    document.getElementById('alertBanner').classList.add('hidden');
  }}

  var showPct = pct(T.totAttended, T.totRSVPs);
  document.getElementById('sAttended').textContent    = fmt(T.totAttended);
  document.getElementById('sAttendedSub').textContent = fmt(T.totRSVPs) + ' RSVPs · ' + (showPct != null ? showPct : 0) + '% show';
  document.getElementById('sCalls').textContent       = fmt(T.totCalls);
  document.getElementById('sCallsSub').textContent    = (pct(T.totCalls, T.totAttended) || 0) + '% of attended';
  document.getElementById('sAccts').textContent       = fmt(T.totAccts);
  document.getElementById('sAcctsSub').textContent    = (pct(T.totAccts, T.totAttended) || 0) + '% of attended';
  document.getElementById('sInvested').textContent    = fmt(T.totInvested);
  document.getElementById('sInvestedSub').textContent = fmtCap(T.totCapital) + ' raised';

  var openRate = pct(T.totOpens, T.totDelivered);
  var replyRate = pct(T.totReplies, T.totAttended);
  var t5Call   = pct(T.tier_calls[5],    T.tiers[5]);
  var t5Inv    = pct(T.tier_invested[5], T.tiers[5]);
  document.getElementById('sOpenRate').textContent    = openRate != null ? openRate + '%' : '—';
  document.getElementById('sOpenRateSub').textContent = fmt(T.totOpens) + '/' + fmt(T.totDelivered) + ' opened';
  document.getElementById('sReplyRate').textContent   = replyRate != null ? replyRate + '%' : '—';
  document.getElementById('sReplyRateSub').textContent= fmt(T.totReplies) + ' replied';
  document.getElementById('sT5Call').textContent      = t5Call != null ? t5Call + '%' : '—';
  document.getElementById('sT5CallSub').textContent   = T.tier_calls[5] + ' of ' + T.tiers[5] + ' tier 5s';
  document.getElementById('sT5Inv').textContent       = t5Inv != null ? t5Inv + '%' : '—';
  document.getElementById('sT5InvSub').textContent    = T.tier_invested[5] + ' of ' + T.tiers[5] + ' converted';

  renderTierTable(T);
}}

function renderTierTable(T) {{
  var tbody = document.getElementById('tierTableBody');
  tbody.innerHTML = '';
  [5, 4, 3, 2, 1].forEach(function(tier) {{
    var leads = T.tiers[tier] || 0;
    var calls = T.tier_calls[tier] || 0;
    var accts = T.tier_accts[tier] || 0;
    var invs  = T.tier_invested[tier] || 0;
    var row = '<tr>' +
      '<td><span class="tier-badge tier-' + tier + '">' + tier + '</span></td>' +
      '<td>' + leads + '</td>' +
      '<td>' + convCell(calls, leads) + '</td>' +
      '<td>' + convCell(accts, leads) + '</td>' +
      '<td>' + convCell(invs, leads) + '</td>' +
      '</tr>';
    tbody.insertAdjacentHTML('beforeend', row);
  }});
}}

function renderEventTable() {{
  var tbody = document.getElementById('eventTableBody');
  tbody.innerHTML = '';
  var sorted = eventData.slice().sort(function(a,b){{ return a.date < b.date ? 1 : -1; }});
  sorted.forEach(function(e) {{
    if (isPending(e)) return;
    var t5 = (e.tiers||{{}})['5'] || 0;
    var t4 = (e.tiers||{{}})['4'] || 0;
    var t3 = (e.tiers||{{}})['3'] || 0;
    var openPct = pct(e.email_opens, e.email_delivered);
    var openCell = openPct != null ? openPct + '%' : '<span class="muted-dash">—</span>';
    var acctCell = e.accountCreated > 0 ? e.accountCreated : '<span class="muted-dash">—</span>';
    var invCell  = e.invested > 0 ? e.invested + ' · ' + fmtCap(e.capital) : '<span class="muted-dash">—</span>';
    var stale    = e.stale_45 || 0;
    var staleCell= stale > 2
      ? '<span class="stale-warn">' + stale + '</span>'
      : '<span class="stale-ok">' + stale + '</span>';
    var tiersCell= '<span class="tier-badges">' +
      '<span class="tier-mini t5"><span class="dot">5</span>' + t5 + '</span>' +
      '<span class="tier-mini t4"><span class="dot">4</span>' + t4 + '</span>' +
      '<span class="tier-mini t3"><span class="dot">3</span>' + t3 + '</span>' +
      '</span>';
    var row =
      '<tr>' +
      '<td class="date-cell">' + formatDateShort(e.date) + '</td>' +
      '<td>' + e.attended + ' / ' + e.rsvps + '</td>' +
      '<td>' + tiersCell + '</td>' +
      '<td>' + openCell + '</td>' +
      '<td>' + (e.calls_booked || 0) + '</td>' +
      '<td>' + acctCell + '</td>' +
      '<td>' + invCell + '</td>' +
      '<td>' + staleCell + '</td>' +
      '</tr>';
    tbody.insertAdjacentHTML('beforeend', row);
  }});
}}

function trendLabel(arr) {{
  var n = arr.length;
  if (n < 2) return {{label:'→ not enough data', cls:'flat'}};
  var w = Math.min(3, Math.floor(n/2));
  var first = 0, last = 0;
  for (var i=0; i<w; i++) first += arr[i];
  for (var i=n-w; i<n; i++) last += arr[i];
  first /= w; last /= w;
  if (last > first * 1.1 && last - first >= 1) return {{label:'↑ improving', cls:'up'}};
  if (last < first * 0.9 && first - last >= 1) return {{label:'↓ declining', cls:'down'}};
  return {{label:'→ holding steady', cls:'flat'}};
}}

function renderCharts() {{
  // chronological (oldest first) for trend lines
  var t = tracked().slice().sort(function(a,b){{ return a.date < b.date ? -1 : 1; }});
  var labels    = t.map(function(e){{ return formatDateShort(e.date); }});
  var showRates = t.map(function(e){{ return pct(e.attended, e.rsvps) || 0; }});
  var openRates = t.map(function(e){{ return pct(e.email_opens, e.email_delivered) || 0; }});
  var callRates = t.map(function(e){{ return pct(e.calls_booked, e.attended) || 0; }});
  var investCnt = t.map(function(e){{ return e.invested || 0; }});

  var tShow = trendLabel(showRates);
  var tOpen = trendLabel(openRates);
  var tCall = trendLabel(callRates);
  var tInv  = trendLabel(investCnt);
  [['trendShow',tShow], ['trendOpen',tOpen], ['trendCall',tCall], ['trendInv',tInv]].forEach(function(x){{
    var el = document.getElementById(x[0]);
    el.textContent = x[1].label;
    el.className = 'chart-trend ' + x[1].cls;
  }});

  var baseOpts = function(suggestedMax) {{
    return {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend:{{display:false}}, tooltip:{{enabled:true}} }},
      scales: {{
        x: {{ ticks:{{color:'#7a91ad', font:{{size:10, family:'Georgia'}}}}, grid:{{display:false}} }},
        y: {{ min:0, suggestedMax: suggestedMax, ticks:{{color:'#7a91ad', font:{{size:10, family:'Georgia'}}}}, grid:{{color:'#eef2f8'}} }}
      }}
    }};
  }};

  if (window._charts) window._charts.forEach(function(c){{ c.destroy(); }});
  window._charts = [];

  window._charts.push(new Chart(document.getElementById('c1'), {{
    type:'line',
    data:{{ labels:labels, datasets:[{{ data:showRates, borderColor:'#4a90d9', backgroundColor:'rgba(74,144,217,0.12)', tension:0.3, fill:true, pointRadius:3, pointBackgroundColor:'#4a90d9' }}] }},
    options: baseOpts(Math.max.apply(null, showRates.concat([30])))
  }}));
  window._charts.push(new Chart(document.getElementById('c2'), {{
    type:'line',
    data:{{ labels:labels, datasets:[{{ data:openRates, borderColor:'#b49ee0', backgroundColor:'rgba(180,158,224,0.12)', tension:0.3, fill:true, pointRadius:3, pointBackgroundColor:'#b49ee0' }}] }},
    options: baseOpts(Math.max.apply(null, openRates.concat([30])))
  }}));
  window._charts.push(new Chart(document.getElementById('c3'), {{
    type:'line',
    data:{{ labels:labels, datasets:[{{ data:callRates, borderColor:'#e8a868', backgroundColor:'rgba(232,168,104,0.12)', tension:0.3, fill:true, pointRadius:3, pointBackgroundColor:'#e8a868' }}] }},
    options: baseOpts(Math.max.apply(null, callRates.concat([20])))
  }}));
  window._charts.push(new Chart(document.getElementById('c4'), {{
    type:'bar',
    data:{{ labels:labels, datasets:[{{ data:investCnt, backgroundColor:'#c9a84c', borderRadius:3 }}] }},
    options: baseOpts(Math.max.apply(null, investCnt.concat([3])))
  }}));
}}

function render() {{
  renderStats();
  renderEventTable();
  renderCharts();
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
    method:'POST',
    headers:{{'Authorization':'token '+tok, 'Accept':'application/vnd.github.v3+json', 'Content-Type':'application/json'}},
    body: JSON.stringify({{ ref:'main' }})
  }}).then(function(r){{
    if (r.status === 204) {{
      btn.textContent = 'Updating… (~45s)';
      setTimeout(function(){{ location.reload(); }}, 45000);
    }} else if (r.status === 401) {{
      localStorage.removeItem('gh_pat');
      btn.disabled = false; btn.textContent = '↻ Refresh';
      alert('Token invalid. Click Refresh to enter a new one.');
    }} else {{
      btn.disabled = false; btn.textContent = '↻ Refresh';
    }}
  }}).catch(function(){{
    btn.disabled = false; btn.textContent = '↻ Refresh';
  }});
}}

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
    # Note: Founder / Co-Founder removed from High — defaulted to Low-Medium (see caps below)
    wealth_terms_fmt = [t.title() for t in WEALTH_ADVISOR_TERMS]
    finance_cos_sample = sorted(FINANCE_COMPANIES)[:24]

    # Pre-compute tier card bodies to avoid nested f-strings (Python < 3.12 limitation)
    target_wm_chips = ['LPL Financial', 'Raymond James', 'JP Morgan', 'Morgan Stanley']
    card5 = tier_card(5, 'High', '#1a7a45', '#eaf7f0',
        section('Auto-High: lifecycle / call status') +
        rule('Already invested (Order Completed call outcome)') +
        rule('Warm pipeline (lifecyclestage = Opportunity)') +
        section('Auto-High: target wealth-management firms (channel partners)') +
        chip_row(target_wm_chips, '#1a7a45', '#d4f0e0') +
        rule('Wealth advisor / financial planner titles at these firms &rarr; HIGH (flips the wealth-advisor DQ &mdash; we want them at events as referral sources)') +
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
        rule('UN / senior government &mdash; Senior Director level only')
    )

    card3 = tier_card(3, 'Medium', '#8a6800', '#fdf6e3',
        rule('Solo practitioners / small law firm attorneys') +
        rule('Senior Manager at non-finance company') +
        rule('No data + NYC zip code (assume local)')
    )

    card2 = tier_card(2, 'Low-Medium', '#b85a00', '#fdf0e8',
        rule('Founder / Co-Founder &mdash; <strong>default Low-Medium unless finance domain, physician, or named top-tier finance firm</strong>') +
        rule('CEO / Owner without verifiable scale (no press, no funding, no recognizable company)') +
        rule('Real estate agents / realtors (commission-based, low liquid wealth)') +
        rule('Art world: dealers, brokers, advisors, consultants, all gallery staff &mdash; no exceptions') +
        rule('Music / entertainment industry workers: producers, programmers, curators, filmmakers, screenwriters') +
        rule('Freelancers, independent contractors, sole proprietors, solopreneurs') +
        rule('Coaches: life, business, executive, career, health, wellness, mindset, etc.') +
        rule('Content creators / influencers / bloggers / podcasters') +
        rule('NFT / crypto / web3 focused') +
        rule('No Show (prior call) without other downgrade signals') +
        rule('No data + no location') +
        rule('Interns, entry-level, paralegals, assistants')
    )

    card1 = tier_card(1, 'Low', '#a83030', '#fde8e8',
        rule('Previously said Not Interested') +
        rule('Wealth advisors / financial advisors / private bankers &mdash; they refer clients, they don\'t invest personally <em style="color:#8a9ab8">(except at target firms above &mdash; LPL, Raymond James, JPM, MS &mdash; where they flip to HIGH)</em>') +
        chip_row(wealth_terms_fmt, '#a83030', '#fde0e0') +
        rule('Personal trainers, fitness coaches, yoga / pilates instructors, gym owners') +
        rule('No Show + other disqualifying signals (low title, art world, etc.)')
    )

    caps_html = (
        rule('<strong>Founder / Co-Founder (any context)</strong> &rarr; max score 2 (Low-Medium) unless: finance-domain email, confirmed physician, or company in top-tier finance list') +
        rule('<strong>Estimated NW $150K&ndash;$500K</strong> &rarr; max score 3 (Medium), even if High title or finance company') +
        rule('<strong>Estimated NW $50K&ndash;$200K</strong> &rarr; max score 2 (Low-Medium)') +
        rule('<strong>Owner / CEO of a local lifestyle business</strong> (salon, restaurant, caf&eacute;, gym, etc.) &rarr; max score 2 (Low-Medium)') +
        rule('<strong>Conservative principle</strong>: if income appears tied to time-for-money services (solo, no scale indicators) &rarr; default Low') +
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
  </div>
</header>

<main>

<div class="note">
  Tiers run <strong>1 (Low) &rarr; 5 (High)</strong> and represent investment likelihood based on
  estimated net worth, title, company, and other signals. Computed automatically from HubSpot
  data each morning at 8am ET. Questions or disagreements? Tell Ani directly.
</div>

<h2>Tiers</h2>
''' + card5 + card4 + card3 + card2 + card1 + '''
<h2>Tier Caps (override everything above)</h2>
<div class="caps">''' + caps_html + '''</div>

<h2>NW Estimation Tiers</h2>
<div class="nw-grid"><div class="nw-grid-inner">
  <div>PE/HF partner, elite law partner, bank MD</div><div style="color:#1a7a45;font-weight:600">$3M&ndash;$10M</div>
  <div>VP/Director at major bank, finance firm</div><div style="color:#1a7a45;font-weight:600">$2M&ndash;$6M</div>
  <div>C-suite mid-size, law associate, senior consultant</div><div style="color:#1a5fa8;font-weight:600">$1M&ndash;$4M</div>
  <div>Senior Manager, attorney (non-partner)</div><div style="color:#8a6800;font-weight:600">$500K&ndash;$2M</div>
  <div>Manager / associate / junior / founder (unverified)</div><div style="color:#b85a00;font-weight:600">$150K&ndash;$500K</div>
  <div>Intern / entry-level / trades / service worker</div><div style="color:#a83030;font-weight:600">$50K&ndash;$200K</div>
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


# ─── GIST → HUBSPOT UNINVITE SYNC ────────────────────────────────────────────

GIST_STATE_FILE = 'mw_rsvp_state.json'

def _read_gist_state() -> dict:
    """Read the shared-state Gist; return {} on any error."""
    try:
        r = requests.get(
            f'https://api.github.com/gists/{SHARED_GIST_ID}',
            headers={'Accept': 'application/vnd.github.v3+json'},
            timeout=10,
        )
        r.raise_for_status()
        content = r.json().get('files', {}).get(GIST_STATE_FILE, {}).get('content', '{}')
        return json.loads(content)
    except Exception as e:
        print(f'Gist read skipped: {e}', file=sys.stderr)
        return {}


def _write_gist_state(state: dict):
    """Write a (possibly empty) state dict back to the Gist."""
    gh_token = os.environ.get('GITHUB_TOKEN', '')
    if not gh_token:
        return
    try:
        requests.patch(
            f'https://api.github.com/gists/{SHARED_GIST_ID}',
            headers={'Authorization': f'token {gh_token}', 'Accept': 'application/vnd.github.v3+json'},
            json={'files': {GIST_STATE_FILE: {'content': json.dumps(state)}}},
            timeout=10,
        )
    except Exception as e:
        print(f'Gist write skipped: {e}', file=sys.stderr)


def sync_uninvites_from_gist(contacts: list):
    """Read the shared Gist state and PATCH HubSpot for any uninvited contacts."""
    state = _read_gist_state()
    if not state:
        return

    uninvite_ids = [k[len('uninvite_'):] for k, v in state.items()
                    if k.startswith('uninvite_') and v]
    if not uninvite_ids:
        return

    # Skip contacts already marked Disqualified in HubSpot — no need to re-patch
    already_done = {
        c['id'] for c in contacts
        if (c['properties'].get('outbound_event_attendee_disqualified') or '').strip().lower() == 'disqualified'
    }
    to_patch = [cid for cid in uninvite_ids if cid not in already_done]
    if not to_patch:
        print('  Uninvite sync: all already Disqualified in HubSpot, nothing to patch')
    else:
        headers = {'Authorization': f'Bearer {HUBSPOT_TOKEN}', 'Content-Type': 'application/json'}
        for cid in to_patch:
            try:
                resp = requests.patch(
                    f'https://api.hubapi.com/crm/v3/objects/contacts/{cid}',
                    headers=headers,
                    json={'properties': {'outbound_event_attendee_disqualified': 'Disqualified'}},
                    timeout=10,
                )
                status = 'OK' if resp.ok else f'HTTP {resp.status_code}'
                print(f'  Uninvite sync {cid}: {status}')
            except Exception as e:
                print(f'  Uninvite sync {cid}: error {e}', file=sys.stderr)

    # Clear ONLY uninvite_* keys — preserve sendconf_* (or other prefixes) for their own sync
    remaining = {k: v for k, v in state.items() if not k.startswith('uninvite_')}
    _write_gist_state(remaining)
    print('  Uninvite sync: uninvite_* keys cleared')


def sync_send_confirmations_from_gist(contacts: list):
    """Read the shared Gist state and PATCH HubSpot for any send-confirmation requests."""
    state = _read_gist_state()
    if not state:
        return

    sendconf_ids = [k[len('sendconf_'):] for k, v in state.items()
                    if k.startswith('sendconf_') and v]
    if not sendconf_ids:
        return

    # Skip contacts already set to "Yes" in HubSpot
    already_done = {
        c['id'] for c in contacts
        if (c['properties'].get('outbound_event_send_confirmation') or '').strip().lower() == 'yes'
    }
    to_patch = [cid for cid in sendconf_ids if cid not in already_done]
    if not to_patch:
        print('  Send-confirmation sync: all already set to Yes in HubSpot, nothing to patch')
    else:
        headers = {'Authorization': f'Bearer {HUBSPOT_TOKEN}', 'Content-Type': 'application/json'}
        for cid in to_patch:
            try:
                resp = requests.patch(
                    f'https://api.hubapi.com/crm/v3/objects/contacts/{cid}',
                    headers=headers,
                    json={'properties': {'outbound_event_send_confirmation': 'Yes'}},
                    timeout=10,
                )
                status = 'OK' if resp.ok else f'HTTP {resp.status_code}'
                print(f'  Send-confirmation sync {cid}: {status}')
            except Exception as e:
                print(f'  Send-confirmation sync {cid}: error {e}', file=sys.stderr)

    # Clear ONLY sendconf_* keys — preserve other prefixes
    remaining = {k: v for k, v in _read_gist_state().items() if not k.startswith('sendconf_')}
    _write_gist_state(remaining)
    print('  Send-confirmation sync: sendconf_* keys cleared')


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    today = date.today()
    start = today - timedelta(days=DAYS_BACK)
    end   = today + timedelta(days=DAYS_AHEAD)

    _load_enrich_cache()
    print(f'Fetching RSVPs {start} → {end}  (DAYS_BACK={DAYS_BACK}, DAYS_AHEAD={DAYS_AHEAD})')
    contacts = fetch_contacts(start, end)
    print(f'Got {len(contacts)} contacts')
    sync_uninvites_from_gist(contacts)
    sync_send_confirmations_from_gist(contacts)

    # Enrich only today + future events — skip past events entirely.
    # Today's contacts go first to max out the quota on what matters most.
    today_iso = today.isoformat()
    contacts_to_enrich = sorted(
        [c for c in contacts
         if (c['properties'].get('outbound_rsvp_to_event') or '')[:10] >= today_iso],
        key=lambda c: (c['properties'].get('outbound_rsvp_to_event') or ''),
    )

    n_manual = _apply_manual_enrichments(contacts_to_enrich)
    if n_manual:
        print(f'Applied {n_manual} manual enrichments')

    n_enriched = enrich_no_data_contacts(contacts_to_enrich)
    if n_enriched:
        print(f'Enriched {n_enriched} contacts via Google Search')

    n_pluto = pluto_enrich_contacts(contacts_to_enrich)
    if n_pluto:
        print(f'PLUTO property values fetched for {n_pluto} NYC contacts')

    n_proxycurl = proxycurl_enrich_photos(contacts_to_enrich)
    if n_proxycurl:
        print(f'RocketReach: fetched profile photos for {n_proxycurl} contacts')

    by_date = defaultdict(list)
    for c in contacts:
        d = c['properties'].get('outbound_rsvp_to_event')
        if d:
            by_date[d].append(c)

    print(f'Dates: {sorted(by_date.keys())}')

    _et = datetime.now(timezone(timedelta(hours=-4)))
    now_str = _et.strftime('%b %#d, %Y at %#I:%M %p ET') if sys.platform == 'win32' else _et.strftime('%b %-d, %Y at %-I:%M %p ET')

    docs = Path('docs')
    docs.mkdir(parents=True, exist_ok=True)

    rsvp_html = build_html(dict(by_date), now_str)
    (docs / 'index.html').write_text(rsvp_html, encoding='utf-8')
    print(f'Written → docs/index.html  ({len(rsvp_html):,} bytes)')


if __name__ == '__main__':
    main()
