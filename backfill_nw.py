#!/usr/bin/env python3
"""
backfill_nw.py — Backfill claude_inferred_net_worth for all historical RSVPs.

For each contact with outbound_rsvp_to_event ≤ CUTOFF_DATE:
  1. Run PLUTO lookup on the HubSpot address (for display; uses cache)
  2. Run Whitepages reverse-phone lookup → owned_properties → PLUTO value
     (uses enrich_cache.json; only calls API for contacts not yet cached)
  3. Compute NW midpoint via get_nw() using all signals
  4. Write to claude_inferred_net_worth in HubSpot (skips unchanged values)

Usage:
    python backfill_nw.py             # cutoff = 2026-06-01
    python backfill_nw.py 2025-12-31  # custom cutoff date
"""

import os, sys, re, time
from datetime import date

# ── Load .env ─────────────────────────────────────────────────────────────────
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
if os.path.exists(_env_path):
    for _line in open(_env_path):
        _line = _line.strip()
        if _line and not _line.startswith('#') and '=' in _line:
            _k, _v = _line.split('=', 1)
            os.environ.setdefault(_k.strip(), _v.strip())

# ── Import machinery from main generator ─────────────────────────────────────
import generate_rsvp as g

CUTOFF_DATE   = sys.argv[1] if len(sys.argv) > 1 else '2026-06-01'
SEARCH_URL    = 'https://api.hubapi.com/crm/v3/objects/contacts/search'
CACHE_SAVE_N  = 25   # save cache + push NW to HubSpot after every N Whitepages API calls
PROGRESS_N    = 100  # print progress line every N contacts (cached or not)

# ─────────────────────────────────────────────────────────────────────────────

def fetch_historical(cutoff: str) -> list:
    """Fetch all contacts with outbound_rsvp_to_event ≤ cutoff (paginated).

    Uses the same GTE+LTE filter pattern as generate_rsvp.fetch_contacts —
    HubSpot's search requires a bounded range on date properties.
    """
    if not g.HUBSPOT_TOKEN:
        print('ERROR: HUBSPOT_API_KEY not set', file=sys.stderr)
        sys.exit(1)

    import requests
    headers  = {'Authorization': f'Bearer {g.HUBSPOT_TOKEN}', 'Content-Type': 'application/json'}
    cutoff_date = date.fromisoformat(cutoff)
    contacts, after = [], None
    page = 0

    while True:
        page += 1
        payload = {
            'filterGroups': [{'filters': [{
                # HubSpot Search API requires epoch ms for date properties
                'propertyName': 'outbound_rsvp_to_event',
                'operator':     'BETWEEN',
                'value':        str(g.epoch_ms(date(2020, 1, 1))),
                'highValue':    str(g.epoch_ms(cutoff_date, end_of_day=True)),
            }]}],
            'properties': [
                'firstname', 'lastname', 'jobtitle', 'company',
                'email', 'phone', 'address', 'city', 'state', 'zip',
                'wealth_segment', 'inferred_income', 'claude_inferred_net_worth',
                'outbound_rsvp_to_event',
            ],
            'limit': 200,
            'sorts': [{'propertyName': 'outbound_rsvp_to_event', 'direction': 'DESCENDING'}],
        }
        if after:
            payload['after'] = after

        resp = requests.post(SEARCH_URL, headers=headers, json=payload, timeout=30)
        if not resp.ok:
            print(f'  HubSpot error {resp.status_code}: {resp.text[:500]}', file=sys.stderr)
        resp.raise_for_status()
        data  = resp.json()
        batch = data.get('results', [])
        contacts.extend(batch)
        print(f'  Page {page}: {len(batch)} contacts → {len(contacts)} total', flush=True)
        after = data.get('paging', {}).get('next', {}).get('after')
        if not after:
            break

    return contacts


def wp_enrich_all(contacts: list) -> int:
    """Whitepages enrichment for all contacts — no date filter.

    Checks all three sub-cache keys (wp_hv, wp_owned, wp_age) before calling
    the API.  Saves the cache every CACHE_SAVE_N API calls.
    """
    import requests

    if not g.WHITEPAGES_API_KEY:
        print('  Whitepages: WHITEPAGES_API_KEY not set — skipping')
        return 0

    wp_headers = {'X-Api-Key': g.WHITEPAGES_API_KEY}
    count, n_no_phone, n_cached, api_calls = 0, 0, 0, 0
    nw_pushed_total = 0
    total = len(contacts)
    start_time = time.time()

    for idx, c in enumerate(contacts):
        p   = c['properties']
        cid = str(c['id'])

        hv_key    = f'wp_hv:{cid}'
        owned_key = f'wp_owned:{cid}'
        age_key   = f'wp_age:{cid}'

        # All three cached → restore and skip API call
        if (hv_key in g._enrich_cache and
                owned_key in g._enrich_cache and
                age_key   in g._enrich_cache):
            n_cached += 1
            if g._enrich_cache[hv_key]:
                p['_wp_home_value'] = g._enrich_cache[hv_key]
                count += 1
            if g._enrich_cache.get(owned_key):
                p['_wp_owned_prop'] = g._enrich_cache[owned_key]
            if g._enrich_cache.get(age_key):
                p['_wp_age_range'] = g._enrich_cache[age_key]

            # Periodic progress on cached contacts
            if (idx + 1) % PROGRESS_N == 0:
                elapsed = time.time() - start_time
                rate    = (idx + 1) / elapsed if elapsed > 0 else 0
                eta_s   = (total - idx - 1) / rate if rate > 0 else 0
                print(f'  [{idx+1}/{total} {(idx+1)/total*100:.0f}%] '
                      f'{n_cached} cached | {api_calls} API calls | '
                      f'{count} home values | ETA {eta_s/60:.0f}m', flush=True)
            continue

        phone = re.sub(r'\D', '', (p.get('phone') or ''))
        if not phone:
            n_no_phone += 1
            g._enrich_cache.setdefault(hv_key,    None)
            g._enrich_cache.setdefault(owned_key, None)
            g._enrich_cache.setdefault(age_key,   None)
            continue

        name = f"{p.get('firstname','')} {p.get('lastname','')}".strip()
        elapsed = time.time() - start_time
        rate    = (idx + 1) / elapsed if elapsed > 0 else 0
        eta_s   = (total - idx - 1) / rate if rate > 0 else 0
        print(f'  [{idx+1}/{total} {(idx+1)/total*100:.0f}%] WP → {name} '
              f'(ETA {eta_s/60:.0f}m)', flush=True)

        try:
            resp = requests.get(
                g.WHITEPAGES_PERSON_URL,
                params={'phone': phone},
                headers=wp_headers,
                timeout=15,
            )
            api_calls += 1

            if not resp.ok:
                print(f'    HTTP {resp.status_code}')
                g._enrich_cache[hv_key]    = None
                g._enrich_cache[owned_key] = None
                g._enrich_cache[age_key]   = None
                continue

            data    = resp.json()
            persons = data if isinstance(data, list) else (
                data.get('results') or data.get('persons') or [data]
            )

            age_ranges, owned_addrs, current_addrs = [], [], []
            for person in persons:
                ar = str(person.get('age_range') or person.get('age') or '').strip()
                if ar:
                    age_ranges.append(ar)
                for prop in (person.get('owned_properties') or []):
                    addr_str = (prop.get('address') or prop.get('full_address') or '').strip()
                    if addr_str:
                        parsed = g._wp_parse_wp_address(addr_str)
                        if parsed.get('street') and parsed.get('state_code'):
                            owned_addrs.append(parsed)
                for addr in (person.get('current_addresses') or []):
                    street = (addr.get('line1') or addr.get('street') or '').strip()
                    state  = (addr.get('state') or addr.get('state_code') or '').strip()
                    city_  = (addr.get('city')  or '').strip()
                    zip_   = (addr.get('zip')   or '').strip()
                    if street and state:
                        current_addrs.append({'street': street, 'city': city_,
                                              'state_code': state, 'zip': zip_})

            # Age
            best_age = age_ranges[0] if age_ranges else None
            g._enrich_cache[age_key] = best_age
            if best_age:
                p['_wp_age_range'] = best_age

            # Owned property → PLUTO (NW signal; only from owned_properties)
            owned_fmt = None
            for addr in owned_addrs:
                val = g.fetch_pluto_value(addr.get('street',''), addr.get('city',''), addr.get('zip',''))
                if val and val != 'Commercial':
                    owned_fmt = val
                    break
                if not owned_fmt and addr.get('zip'):
                    val = g.fetch_census_value(addr['zip'])
                    if val:
                        owned_fmt = val
                        break
            g._enrich_cache[owned_key] = owned_fmt
            if owned_fmt:
                p['_wp_owned_prop'] = owned_fmt
                print(f'    owned prop: {owned_fmt}')

            # Display value (owned first, then current address)
            display_fmt = owned_fmt
            if not display_fmt:
                for addr in current_addrs:
                    val = g.fetch_pluto_value(addr.get('street',''), addr.get('city',''), addr.get('zip',''))
                    if val and val != 'Commercial':
                        display_fmt = val
                        break
                    if not display_fmt and addr.get('zip'):
                        val = g.fetch_census_value(addr['zip'])
                        if val:
                            display_fmt = val
                            break

            g._enrich_cache[hv_key] = display_fmt
            if display_fmt:
                p['_wp_home_value'] = display_fmt
                count += 1

            # Periodic cache save + NW push (every CACHE_SAVE_N live API calls)
            if api_calls % CACHE_SAVE_N == 0:
                g._save_enrich_cache()
                n_pushed = push_nw(contacts[:idx + 1])
                nw_pushed_total += n_pushed
                print(f'  ↳ [{(idx+1)/total*100:.0f}%] checkpoint: cache saved, '
                      f'{n_pushed} NW written this batch ({nw_pushed_total} total), '
                      f'{api_calls} API calls', flush=True)

        except Exception as exc:
            print(f'  Exception for {name}: {exc}', file=sys.stderr)
            g._enrich_cache.setdefault(hv_key,    None)
            g._enrich_cache.setdefault(owned_key, None)
            g._enrich_cache.setdefault(age_key,   None)

    g._save_enrich_cache()
    elapsed_total = time.time() - start_time
    print(f'  Whitepages: {count} values found | {n_cached} from cache | '
          f'{n_no_phone} no phone | {api_calls} API calls | '
          f'{elapsed_total/60:.1f}m elapsed')
    return count


def push_nw(contacts: list) -> int:
    """Compute NW midpoint for each contact and write to HubSpot if changed."""
    updated, skipped_no_nw, skipped_same = 0, 0, 0

    for c in contacts:
        p   = c['properties']
        cid = str(c['id'])

        nw, nw_reason = g.get_nw(p)
        mid = g._NW_MIDPOINTS.get(nw)
        if mid is None:
            skipped_no_nw += 1
            continue

        existing_raw = (p.get('claude_inferred_net_worth') or '').strip()
        try:
            existing = int(float(existing_raw)) if existing_raw else None
        except (ValueError, TypeError):
            existing = None

        if existing == mid:
            skipped_same += 1
            continue

        name = f"{p.get('firstname','')} {p.get('lastname','')}".strip()
        print(f'  {name}: {g.nw_midpoint_fmt(nw)} ({nw_reason[:60]})')
        g._patch_hubspot_contact(cid, {'claude_inferred_net_worth': mid})
        updated += 1

    print(f'  Written: {updated}  |  unchanged: {skipped_same}  |  no NW signal: {skipped_no_nw}')
    return updated


def main():
    g._load_enrich_cache()

    print(f'Backfill NW — contacts with RSVP ≤ {CUTOFF_DATE}\n')

    # Step 1: Fetch
    print('─── Step 1: Fetch contacts ──────────────────────────────────────────────')
    contacts = fetch_historical(CUTOFF_DATE)
    print(f'Total: {len(contacts)} contacts\n')

    # Step 2: PLUTO from HubSpot address (display; no Whitepages needed)
    print('─── Step 2: PLUTO from HubSpot address ──────────────────────────────────')
    n_pluto = g.pluto_enrich_contacts(contacts)
    print(f'PLUTO: {n_pluto} values resolved\n')

    # Step 2b: Early NW push — captures wealth_segment / title / income signals
    # before Whitepages runs (guards against WP timeout losing all progress)
    print('─── Step 2b: Early NW push (pre-Whitepages signals) ─────────────────────')
    n_early = push_nw(contacts)
    print(f'Early push: {n_early} contacts written\n')

    # Step 3: Whitepages reverse-phone → owned property → PLUTO (NW signal)
    # Checkpoints every {CACHE_SAVE_N} API calls: saves cache + pushes updated NW
    print('─── Step 3: Whitepages enrichment ───────────────────────────────────────')
    n_wp = wp_enrich_all(contacts)
    print(f'Whitepages: {n_wp} display values resolved\n')

    # Step 4: Final NW push — picks up anything not caught at checkpoints
    print('─── Step 4: Final claude_inferred_net_worth push ────────────────────────')
    n_updated = push_nw(contacts)
    print(f'\nDone. {n_early} written early + {n_updated} updated after Whitepages = '
          f'{n_early + n_updated} total HubSpot writes.')


if __name__ == '__main__':
    main()
