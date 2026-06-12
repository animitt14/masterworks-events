# RSVP Dashboard

## Architecture (live, as of June 2026)
The dashboard is **rendered live from HubSpot on every request** by a Vercel
serverless function — no longer a 3×/day static snapshot.

- `api/index.py` — GET `/`: Basic-Auth gate (`DASHBOARD_PASSCODE`) → `render_live()` → HTML. Holds `HUBSPOT_API_KEY` server-side; 45s in-process + CDN cache.
- `api/action.py` — POST `/action`: write-back. `{contact_id, action, value}`, action ∈ `uninvite|sendconf|attended` → PATCHes HubSpot immediately. Replaces the old Gist relay.
- `generate_rsvp.py` — the renderer, reused as a library. `render_live()` sets nothing itself; the function sets `OFFLINE_ENRICH=1` so enrichment is **cache-only** (no external API calls, no HubSpot writes, no disk writes — Vercel FS is read-only).
- `vercel.json` — routes `/`→`api/index`, `/action`→`api/action`; `includeFiles` bundles `generate_rsvp.py` + the JSON caches.

### Cold vs hot data
- Live from HubSpot each request: RSVP, attended, # contacted, deal stage, DQ, send-confirmation — plus enriched fields that are patched back to HubSpot (jobtitle, company, `linkedin_image_url`).
- Cache-only on render (repopulated from `enrich_cache.json`, no network): `_pluto_val`, `_wp_home_value`.
- `confirmations.json`: email-reply confirmations — too costly to scan live (~2 HubSpot calls/contact), so the Action persists them and `render_live()` applies them.

## Project
- GitHub: animitt14/masterworks-events, branch main
- Live: Vercel deployment (the GitHub Pages URL is the static fallback only)
- Deploy: `vercel --prod` (or push to main — Vercel auto-deploys)
- Scheduled GitHub Action (`daily.yml`, 3×/day) warms `enrich_cache.json` + `confirmations.json` and writes the static `docs/index.html` fallback. It does NOT serve the live view.

## Vercel env vars
- `HUBSPOT_API_KEY` — HubSpot private-app token (used by both functions)
- `DASHBOARD_PASSCODE` — shared password for Basic-Auth access
- Enrichment keys (Google/RocketReach/etc.) are NOT needed by the function (cache-only); they live only in the GitHub Action secrets.

## Key Files
- `generate_rsvp.py` — main generator/renderer (ALWAYS read this before acting on dashboard data)
- `api/` — Vercel serverless functions (live render + write-back)
- `docs/` — static fallback HTML artifacts (never edit directly for contact data)
- `enrich_cache.json` / `confirmations.json` — caches read by the live function
- `.env` — HubSpot API token as `HUBSPOT_API_KEY` (gitignored, never commit)

## HubSpot Reference
- **Owner IDs:** Ani=77771452, Blake Martin=1433036370, Erik Bringsjord=73613833, Linna Henry=202057506, Michael DelPozzo=35397207
- **Gallery Leads pipeline:** 880355706
- **Deal stages:** Event Attended=1321369495, Attempted=1339121714, Contacted=1321369496, Meeting Scheduled=1321369497, Nurture=1321369500, Recommendation Made=1321369502, Closed Won=1321369499, Closed Lost=1321369501, Disqualified=1341309466
- **Key contact properties:** outbound_rsvp_to_event (date), attended_outbound_event (Yes/blank), outbound_event_attendee_disqualified, utm_source, haspurchased, call_completed, dnc___management, total_investment_portfolio
- **Contact deep link:** `https://app.hubspot.com/contacts/5454671/record/0-1/[contact-id]`
- Max 5-6 filters per group, batch contact IDs in groups of 7-8

## Workflow
1. The live page reflects HubSpot in real time — no rebuild needed to see data changes.
2. Code/cache changes deploy via `vercel --prod` or push to main (Vercel auto-deploys).
3. Never edit docs/*.html directly — overwritten by the Action's static fallback build.

## Write-back (uninvite / send-confirmation / attended)
Browser toggle → `postAction()` → POST `/action` → `api/action.py` PATCHes HubSpot
immediately (token is server-side). The old Gist relay and the GitHub-PAT prompt are
**gone**. The browser never talks to HubSpot directly (still CORS-blocked) — it talks to
our own function, which carries the dashboard's Basic-Auth credentials automatically.

## Enrichment Rules
- After first enrichment pass, always do a second pass on remaining blanks
- Multiple LinkedIn candidates: weight NYC/tri-state geography over email-handle match
- When ambiguous, flag to Ani instead of pushing bad data

## Python
- Use `py` launcher or `python` (3.14.4 installed)
- `py -m pip install ...` for packages

## Rules
- Always push after committing (don't ask)
- No recommendations or "want me to..." — just do the task and stop
- Default model: Sonnet. Switch to Opus only for novel architecture or subtle multi-system bugs.
