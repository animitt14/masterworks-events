#!/usr/bin/env python3
"""
generate_pipeline.py
Generates docs/pipeline.html — Group 1 pipeline management table.
Group 1: RSVP known, owned by Ani, Attended Event = Yes,
         Call Completed != Order Completed, Not Disqualified.
"""

import json
import os
import re
import sys
import time
import requests
from datetime import datetime, timezone
from html import escape
from pathlib import Path

HUBSPOT_TOKEN = os.environ.get('HUBSPOT_API_KEY', '').strip()
PORTAL_ID = '5454671'
HEADERS = {'Authorization': f'Bearer {HUBSPOT_TOKEN}', 'Content-Type': 'application/json'}
SEARCH_URL = 'https://api.hubapi.com/crm/v3/objects/contacts/search'

DEAL_STAGES = {
    '1321369495': 'Event Attended',
    '1339121714': 'Attempted',
    '1321369496': 'Contacted',
    '1321369497': 'Meeting Scheduled',
    '1321369500': 'Nurture',
    '1321369502': 'Recommendation Made',
    '1321369499': 'Closed Won',
    '1321369501': 'Closed Lost',
    '1341309466': 'Disqualified',
}

STAGE_CSS = {
    '1321369495': 'stage-event',
    '1339121714': 'stage-attempted',
    '1321369496': 'stage-contacted',
    '1321369497': 'stage-meeting',
    '1321369500': 'stage-nurture',
    '1321369502': 'stage-rec',
    '1321369499': 'stage-won',
    '1321369501': 'stage-lost',
    '1341309466': 'stage-disq',
}

GALLERY_LEADS_PIPELINE = '880355706'
ANI_OWNER_ID = '77771452'


def fetch_all_contacts():
    filters = [
        {'propertyName': 'outbound_rsvp_to_event', 'operator': 'HAS_PROPERTY'},
        {'propertyName': 'hubspot_owner_id', 'operator': 'EQ', 'value': ANI_OWNER_ID},
        {'propertyName': 'attended_outbound_event', 'operator': 'EQ', 'value': 'Yes'},
        {'propertyName': 'call_completed', 'operator': 'NOT_IN', 'values': ['Order Completed']},
        {'propertyName': 'outbound_event_attendee_disqualified', 'operator': 'NOT_IN', 'values': ['Disqualified']},
    ]
    props = ['firstname', 'lastname', 'email', 'outbound_rsvp_to_event',
             'hs_last_sales_activity_timestamp', 'linkedin_url', 'hs_linkedinid']

    contacts = []
    after = None
    while True:
        body = {'filterGroups': [{'filters': filters}], 'properties': props, 'limit': 200}
        if after:
            body['after'] = after
        r = requests.post(SEARCH_URL, headers=HEADERS, json=body)
        r.raise_for_status()
        data = r.json()
        contacts.extend(data.get('results', []))
        after = data.get('paging', {}).get('next', {}).get('after')
        if not after:
            break
        time.sleep(0.2)

    print(f'Fetched {len(contacts)} contacts', flush=True)
    return contacts


def load_deals_from_cache():
    """Load deals from pipeline_deal_cache.json (populated via MCP each session)."""
    cache_path = Path(__file__).parent / 'pipeline_deal_cache.json'
    if not cache_path.exists():
        print('WARNING: pipeline_deal_cache.json not found — no deal data', flush=True)
        return []
    with open(cache_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    deals = data.get('results', [])
    fetched = data.get('fetched_at', 'unknown')
    print(f'Loaded {len(deals)} deals from cache (fetched {fetched})', flush=True)
    return deals


def normalize(s):
    return re.sub(r'[^a-z0-9]', '', (s or '').lower())


def build_deal_index(deals):
    """Pre-process deals for fast matching. Returns (records, by_name)."""
    records = []
    by_name = {}
    for deal in deals:
        p = deal.get('properties', {})
        raw = (p.get('dealname', '') or '').strip()
        record = {
            'stage': p.get('dealstage', ''),
            'amount': p.get('amount', ''),
            'times_contacted': p.get('num_contacted_notes', ''),
            'raw_lower': raw.lower(),
        }
        records.append(record)
        # Name index: strip email suffix, strip " - Placeholder..." suffixes
        name_part = re.sub(r'[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}', '', raw, flags=re.IGNORECASE).strip()
        name_part = re.split(r'\s+-\s+', name_part)[0].strip()
        key = normalize(name_part)
        if key and key not in by_name:
            by_name[key] = record
    return records, by_name


def match_deal(contact_props, records, by_name):
    email = (contact_props.get('email') or '').lower().strip()
    first = contact_props.get('firstname') or ''
    last = contact_props.get('lastname') or ''
    name_key = normalize(first + last)

    # Primary: contact email appears as a substring of the deal name
    # (handles "FirstLastemail@domain.com" format where email is embedded)
    if email:
        for r in records:
            if email in r['raw_lower']:
                return r

    # Fallback: normalized name match
    if name_key in by_name:
        return by_name[name_key]

    # Partial name match (catches "Justin Holder - Placeholder..." etc.)
    for dkey, dval in by_name.items():
        if dkey.startswith(name_key) and len(name_key) >= 4:
            return dval

    return None


def fmt_date(iso):
    if not iso:
        return ''
    try:
        dt = datetime.fromisoformat(iso.replace('Z', '+00:00'))
        return dt.strftime('%#m/%#d/%y') if sys.platform == 'win32' else dt.strftime('%-m/%-d/%y')
    except Exception:
        return iso[:10] if len(iso) >= 10 else iso


def fmt_amount(amount_str):
    if not amount_str:
        return ''
    try:
        return f'${float(amount_str):,.0f}'
    except Exception:
        return amount_str


def build_html(contacts, records, by_name):
    now = datetime.now(timezone.utc).strftime('%B %-d, %Y %H:%M UTC') if sys.platform != 'win32' \
        else datetime.now(timezone.utc).strftime('%B %#d, %Y %H:%M UTC')
    rows = []

    for c in sorted(contacts, key=lambda x: (x['properties'].get('outbound_rsvp_to_event') or ''), reverse=True):
        p = c.get('properties', {})
        cid = str(c['id'])
        first = p.get('firstname') or ''
        last = p.get('lastname') or ''
        name = escape(f'{first} {last}'.strip() or p.get('email') or cid)
        hs_url = f'https://app.hubspot.com/contacts/{PORTAL_ID}/record/0-1/{cid}'

        li_url = p.get('linkedin_url') or ''
        if not li_url and p.get('hs_linkedinid'):
            li_url = f'https://www.linkedin.com/in/{p["hs_linkedinid"]}'

        rsvp_date = fmt_date(p.get('outbound_rsvp_to_event', ''))
        last_contact = fmt_date(p.get('hs_last_sales_activity_timestamp', ''))

        deal = match_deal(p, records, by_name)
        stage_id = deal['stage'] if deal else ''
        stage_label = DEAL_STAGES.get(stage_id, stage_id) if stage_id else ''
        stage_css = STAGE_CSS.get(stage_id, '') if stage_id else ''
        amount = fmt_amount(deal['amount']) if deal else ''
        times_contacted = deal['times_contacted'] if deal else ''

        stage_cell = f'<span class="badge {stage_css}">{escape(stage_label)}</span>' if stage_label else '—'
        li_link = f'<a href="{escape(li_url)}" target="_blank">LI</a>' if li_url else '—'
        hs_link = f'<a href="{escape(hs_url)}" target="_blank">HS</a>'

        rows.append(f'    <tr>\n'
                    f'      <td><a href="{escape(hs_url)}" target="_blank">{name}</a></td>\n'
                    f'      <td>{rsvp_date}</td>\n'
                    f'      <td>{last_contact}</td>\n'
                    f'      <td>{stage_cell}</td>\n'
                    f'      <td>{escape(amount)}</td>\n'
                    f'      <td>{escape(times_contacted)}</td>\n'
                    f'      <td class="links">{hs_link} {li_link}</td>\n'
                    f'    </tr>')

    rows_html = '\n'.join(rows)
    count = len(contacts)

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pipeline — Group 1</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #0d0d0d; color: #e8e8e8; margin: 0; padding: 24px 28px; }}
  h1 {{ font-size: 1.3rem; font-weight: 600; margin: 0 0 4px; color: #f0f0f0; }}
  .meta {{ font-size: 0.78rem; color: #666; margin-bottom: 22px; }}
  .meta span {{ margin-right: 14px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.83rem; }}
  th {{ text-align: left; padding: 9px 12px; background: #141414; color: #777; font-weight: 500; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.04em; border-bottom: 1px solid #222; white-space: nowrap; position: sticky; top: 0; }}
  td {{ padding: 8px 12px; border-bottom: 1px solid #1a1a1a; vertical-align: middle; }}
  tr:hover td {{ background: #111; }}
  a {{ color: #c9a96e; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .links a {{ font-size: 0.72rem; font-weight: 600; color: #888; border: 1px solid #2a2a2a; border-radius: 3px; padding: 1px 5px; margin-right: 4px; }}
  .links a:hover {{ color: #c9a96e; border-color: #c9a96e; text-decoration: none; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 3px; font-size: 0.72rem; font-weight: 500; white-space: nowrap; }}
  .stage-event     {{ background: #0f2a0f; color: #6dbf6d; }}
  .stage-attempted {{ background: #2a2a10; color: #c9c96d; }}
  .stage-contacted {{ background: #0f1e2e; color: #6daacc; }}
  .stage-meeting   {{ background: #181828; color: #9d9ddd; }}
  .stage-nurture   {{ background: #2a1a10; color: #cc8a6d; }}
  .stage-rec       {{ background: #0f2222; color: #6dcccc; }}
  .stage-won       {{ background: #0a1f0a; color: #4dc94d; }}
  .stage-lost      {{ background: #220a0a; color: #cc5555; }}
  .stage-disq      {{ background: #1a1a1a; color: #555; }}
  td:nth-child(5)  {{ font-variant-numeric: tabular-nums; color: #c9a96e; }}
  td:nth-child(6)  {{ font-variant-numeric: tabular-nums; text-align: center; color: #888; }}
</style>
</head>
<body>
<h1>Pipeline &mdash; Group 1</h1>
<div class="meta">
  <span>{count} contacts</span>
  <span>Updated {now}</span>
</div>
<table>
  <thead>
    <tr>
      <th>Name</th>
      <th>Date Attended</th>
      <th>Last Contacted</th>
      <th>Deal Stage</th>
      <th>Deal Amount</th>
      <th># Contacted</th>
      <th>Links</th>
    </tr>
  </thead>
  <tbody>
{rows_html}
  </tbody>
</table>
</body>
</html>'''


def main():
    if not HUBSPOT_TOKEN:
        print('ERROR: HUBSPOT_API_KEY not set', file=sys.stderr)
        sys.exit(1)

    print('Fetching contacts...', flush=True)
    contacts = fetch_all_contacts()

    print('Loading deals from cache...', flush=True)
    deals = load_deals_from_cache()

    records, by_name = build_deal_index(deals)

    print('Building HTML...', flush=True)
    html = build_html(contacts, records, by_name)

    out = Path(__file__).parent / 'docs' / 'pipeline.html'
    out.write_text(html, encoding='utf-8')
    print(f'Written: {out}', flush=True)


if __name__ == '__main__':
    main()
