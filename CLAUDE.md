# RSVP Dashboard

## Project
- GitHub: animitt14/masterworks-events, branch main
- Published: https://animitt14.github.io/masterworks-events/
- Daily rebuild at 8am ET via GitHub Actions (push to main or workflow_dispatch)
- Manual rebuild: `git pull --rebase && git commit --allow-empty -m "trigger rebuild" && git push`

## Key Files
- `generate_rsvp.py` — main generator (ALWAYS read this before acting on dashboard data)
- `docs/` — generated HTML artifacts (never edit directly for contact data)
- `enrich_cache.json` — enrichment cache
- `.env` — HubSpot API token as `HUBSPOT_API_KEY` (gitignored, never commit)

## HubSpot Reference
- **Owner IDs:** Ani=77771452, Blake Martin=1433036370, Erik Bringsjord=73613833, Linna Henry=202057506, Michael DelPozzo=35397207
- **Gallery Leads pipeline:** 880355706
- **Deal stages:** Event Attended=1321369495, Attempted=1339121714, Contacted=1321369496, Meeting Scheduled=1321369497, Nurture=1321369500, Recommendation Made=1321369502, Closed Won=1321369499, Closed Lost=1321369501, Disqualified=1341309466
- **Key contact properties:** outbound_rsvp_to_event (date), attended_outbound_event (Yes/blank), outbound_event_attendee_disqualified, utm_source, haspurchased, call_completed, dnc___management, total_investment_portfolio
- **Contact deep link:** `https://app.hubspot.com/contacts/5454671/record/0-1/[contact-id]`
- Max 5-6 filters per group, batch contact IDs in groups of 7-8

## Workflow
1. Write updates to HubSpot via API
2. Trigger rebuild (empty commit + push)
3. Never edit docs/*.html directly — gets overwritten

## Uninvite Sync
Browser → Gist (44d6dd7bc96a5cbe2454b65ee55f8cdb) → Python sync → HubSpot PATCH. Gist clears after sync. HubSpot blocks CORS — never fetch HubSpot from browser JS.

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
