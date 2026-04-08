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
    'jpmorgan.com', 'gs.com', 'ubs.com', 'nb.com', 'pimco.com', 'kkr.com',
    'virtu.com', 'blackrock.com', 'morganstanley.com', 'citi.com',
    'baml.com', 'wellsfargo.com', 'tdsecurities.com', 'ml.com',
    'alliancebernstein.com',
}

HIGH_TITLE_TERMS = [
    'managing director', 'vice president', 'general partner',
    'chief executive', 'chief financial', 'chief technology', 'chief operating',
    'ceo', 'cfo', 'cto', 'coo', 'vp', 'md',
    'founder', 'co-founder', 'president', 'principal', 'partner', 'chief',
]

DOWNGRADE_TERMS = [
    'real estate agent', 'realtor', 'art dealer', 'gallery', 'fine art',
    'nft', 'crypto', 'wealth advisor', 'private banker', 'financial advisor',
    'intern', 'assistant',
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
    t = ' ' + title.lower().strip() + ' '
    for term in HIGH_TITLE_TERMS:
        if (' ' + term + ' ') in t:
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
    if state and state not in TRI_STATE:
        flags.append('non_tri_state')

    combined      = title + ' ' + company
    has_downgrade = any(t in combined for t in DOWNGRADE_TERMS)
    if 'broker' in combined and not any(fc in company for fc in FINANCE_COMPANIES):
        has_downgrade = True
    no_data = not title.strip() and not company.strip()

    if has_downgrade and 'no_show' in flags:
        return 1, flags
    if has_downgrade or 'no_show' in flags:
        return 2, flags
    if no_data:
        return 2, flags

    if email_domain(email) in FINANCE_DOMAINS:
        return 5, flags
    if has_high_title(title):
        return 5, flags
    if any(fc in company for fc in FINANCE_COMPANIES):
        return 5, flags

    medium_high = any(t in title for t in [
        'director', 'senior director', 'head of', 'svp', 'evp', 'avp',
        'senior manager', 'senior vice', 'associate director',
    ])
    if medium_high:
        return 4, flags

    return 3, flags

def get_persona(p: dict) -> str:
    title   = (p.get('jobtitle') or '').lower()
    company = (p.get('company')  or '').lower()

    if not title.strip() and not company.strip():
        return 'Unknown'
    if any(t in title for t in ['retired', 'retiree']):
        return 'Cautious Retiree'
    if any(t in title for t in [
        'banker', 'trader', 'portfolio manager', 'fund manager', 'hedge fund',
        'private equity', 'attorney', 'lawyer', 'counsel', 'managing director', 'general partner',
    ]) or any(fc in company for fc in FINANCE_COMPANIES):
        return 'Finance Bro'
    if any(t in title for t in [
        'physician', 'doctor', 'surgeon', 'cardiologist', 'radiologist',
        'psychiatrist', 'dermatologist', 'neurologist', 'anesthesiologist',
        'medical director', 'dentist', 'ophthalmologist',
    ]) or any(t in company for t in ['hospital', 'medical center', 'health system', 'clinic']):
        return 'Medical Pro'
    if any(t in title for t in [
        'engineer', 'developer', 'software', 'data scientist', 'machine learning', 'tech lead', 'cto',
    ]) or any(tc in company for tc in [
        'google', 'meta', 'apple', 'amazon', 'microsoft', 'netflix',
        'uber', 'airbnb', 'stripe', 'palantir', 'salesforce', 'oracle',
    ]):
        return 'Tech Wealth Builder'
    if any(t in title for t in ['founder', 'co-founder', 'ceo', 'owner']):
        return 'Business Owner'
    if any(t in title for t in ['vp', 'vice president', 'director', 'chief', 'cfo', 'coo', 'svp', 'evp']):
        return 'Corporate Climber'
    if any(t in title for t in ['artist', 'designer', 'creative', 'photographer',
                                  'filmmaker', 'musician', 'writer', 'content creator']):
        return 'Young Diversifier'
    if any(t in title for t in ['teacher', 'nurse', 'sales representative', 'retail',
                                  'coordinator', 'administrative', 'customer service']):
        return 'Everyday Investor'
    return 'Corporate Climber'

def get_nw(p: dict) -> tuple:
    title   = (p.get('jobtitle') or '').lower()
    company = (p.get('company')  or '').lower()

    if not title.strip() and not company.strip():
        return '—', 'No title or company data'

    if any(t in title for t in ['managing director', 'general partner', 'partner', 'principal']) \
            and any(fc in company for fc in FINANCE_COMPANIES):
        return '$2M–$8M', 'Senior role at top-tier finance/law firm'
    if any(t in title for t in ['ceo', 'chief executive', 'founder', 'co-founder']):
        return ('$2M–$8M', 'C-suite at major institution') \
               if any(fc in company for fc in FINANCE_COMPANIES) \
               else ('$1M–$4M', 'CEO / founder')
    if any(t in title for t in ['managing director', 'president', 'managing partner']):
        return '$1M–$4M', 'MD / President-level'
    if any(t in title for t in ['vp', 'vice president', 'director', 'cfo', 'cto', 'coo', 'svp', 'evp']):
        return '$1M–$4M', 'VP / Director-level'
    if any(t in title for t in ['owner', 'founder']) and company:
        return '$1M–$4M', 'Business owner'
    if any(t in title for t in ['manager', 'senior', 'consultant', 'principal', 'lead', 'head of']):
        return '$500K–$2M', 'Mid-level professional'
    if any(t in title for t in ['analyst', 'associate', 'specialist', 'coordinator', 'engineer', 'developer']):
        return '$150K–$500K', 'Early / mid-career'
    return '$150K–$500K', 'Limited seniority data'

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

    tc_parts = []
    if title:   tc_parts.append(escape(title))
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
        <th>Score <span style="font-size:0.6rem;opacity:0.6">(click to override)</span></th>
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
      <a href="events.html" class="nav-link">Event Dashboard →</a>
      <div class="date-jump-wrap">
        <span class="date-jump-label">Jump to date</span>
        <input type="date" id="dateJump" title="Jump to event date">
      </div>
      <button class="refresh-btn" id="refreshBtn" onclick="triggerRefresh()">↻ Refresh</button>
    </div>
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
  initSharedState(function() {{
    if (defaultTab) {{
      applyStoredOverrides(defaultTab);
      updateResetBtn(defaultTab);
    }}
    if ('{past_default_label}') {{
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
      <a href="index.html" class="nav-link">← RSVP Dashboard</a>
      <button class="refresh-btn" id="refreshBtn" onclick="triggerRefresh()">↻ Refresh</button>
    </div>
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

# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    today = date.today()
    start = today - timedelta(days=DAYS_BACK)
    end   = today + timedelta(days=DAYS_AHEAD)

    print(f'Fetching RSVPs {start} → {end}  (DAYS_BACK={DAYS_BACK}, DAYS_AHEAD={DAYS_AHEAD})')
    contacts = fetch_contacts(start, end)
    print(f'Got {len(contacts)} contacts')

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

if __name__ == '__main__':
    main()
