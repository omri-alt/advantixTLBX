# KLblend — context for AI assistants (Claude)

This file summarizes how the repo is built, what it automates, and what was added during recent work so you can navigate without re-discovering everything from scratch.

## What this project is

**KLblend** is a Python CLI toolkit (no long-running server required for core flows) that connects:

- **Kelkoo** — merchant feeds, PLA offers, aggregated reports, monetization checks.
- **Google Sheets** — daily tabs (`fixim`, `offers`, combined `offers_today`), monthly logs, Blend spreadsheet.
- **Keitaro** — campaigns, offers, flows; sync from sheets.
- **Zeropark / Yadore** — optional traffic and deeplink checks (see `README.md`).
- **SourceKnowledge (SK)** — affiliate DSP API for advertisers/campaigns; migration and bulk URL rewrites were added as auxiliary scripts.
- **Ecomnia** — advertiser DSP API; tracking URL migration tool lives in `tools/`.

Primary stack: Python 3, `requests`, `python-dotenv`, Google APIs where noted. Config is env-driven via `config.py` + `.env`.

## Repository layout

| Area | Role |
|------|------|
| Project root | Main entry scripts (`run_daily_workflow.py`, `update_offers_from_sheet.py`, `blend_sync_from_sheet.py`, SK migration scripts, etc.). |
| `integrations/` | API clients (Keitaro, Kelkoo helpers, Zeropark, Yadore). |
| `workflows/` | Kelkoo daily logic, monthly log, shared workflow code. |
| `cli/` | Thin wrappers that call root scripts with stable paths (`cli/run_daily_workflow.py`, etc.). |
| `services/` | Legacy/alternate client modules; some code paths still reference these. |
| `apps_script/` | Google Apps Script related assets. |
| `tools/` | One-off migration and utility scripts (e.g. Ecomnia tracking URL migration). |

**Convention:** Prefer matching existing style in a file (imports, logging, subprocess vs direct calls). Avoid drive-by refactors; keep changes scoped to the task.

## Configuration (`config.py`)

- Loads `.env` with `python-dotenv`.
- Includes `_read_env_fallback()` for malformed `.env` lines (extra spaces, `KEY = = value`).
- Important variables:
  - **Keitaro:** `KEITARO_BASE_URL`, `KEITARO_API_KEY`, optional campaign alias/id.
  - **Kelkoo / Sheets:** `FEED1_API_KEY`, `FEED2_API_KEY`, `KELKOO_SHEETS_SPREADSHEET_ID`, `BLEND_SHEETS_SPREADSHEET_ID`.
  - **SourceKnowledge:** `SOURCEKNOWLEDGE_API_KEY` from `KEYSK` or legacy `keySK` (see below).
  - **Zeropark / Yadore:** `KEYZP`, `YADORE_API_KEY`, etc.

## Daily Kelkoo → Keitaro workflow

**Script:** `run_daily_workflow.py` (wrapper: `cli/run_daily_workflow.py`).

Rough order: monthly log for yesterday → optional Blend potential refresh → delete previous day’s dated tabs → download feeds → reports/color fixim → pick merchants → PLA offers → combined sheet → Keitaro sync for both feeds.

**Flags (non-exhaustive):**

- `--skip-keitaro` — skips Nipuhim Keitaro sync and Blend Keitaro sync; still can run Blend sheet populate unless `--skip-blend`.
- `--skip-blend` — skips the whole Blend block.
- `--feed1-traffic-only` — feed1 Keitaro only, then Blend steps as configured.

**Blend block (step 7):**

- Refreshes **potentialKelkoo*** sheets per `BLEND_POTENTIAL_FEEDS` (currently `kelkoo1` only unless extended).
- Runs `populate_blend_from_potential.py` (monetized rows, dedupe, `clickCap` 50, `--max-add` daily cap).
- Runs `blend_sync_from_sheet.py` — prunes bad `auto='v'` rows, syncs Keitaro Blend campaign (alias documented in `README.md`).

Related: `blend_potential_merchants.py`, `populate_blend_from_potential.py`, `blend_sync_from_sheet.py` (uses `BLEND_SHEETS_SPREADSHEET_ID` from config).

## SourceKnowledge (SK) — what was added

### API key

- User stores the key in `.env` as **`KEYSK`**.
- `config.SOURCEKNOWLEDGE_API_KEY` resolves `KEYSK` / `keySK` with fallback parsing.
- Legacy copy of the original automation: `sk_legacy_snapshot.py` (do not treat as the single source of truth for new features; use dedicated scripts).

### Schemas / reference

- `sk_api_schemas.md` — inferred JSON shapes for advertisers, campaigns, stats endpoints (from legacy usage + docs). Update when live samples differ.

### Single-campaign tracking URL migration

- `migrate_sk_tracking_urls.py` — GET campaign → change `trackingUrl` (and optional modes) → PUT. Supports CSV or `--campaign-ids` with auto-built URLs from advertiser name `{brand}-{geo}-{prefix}` for KLWL/KLFIX-style prefixes.

### Bulk campaign rename + tracking URL

- `migrate_sk_campaigns_bulk.py` — lists all campaigns (paged) or reads `--ids-file`, builds:
  - **name:** `{brand}-{geo}-{prefix}-c{campaign_id}`
  - **trackingUrl:** Keitaro/shopli template with static `XgeoX` / `XbrandX` / `XhpX` replaced; dynamic `{clickid}`, `{oadest}`, etc. preserved.
- **Prefixes handled:** `KLWL*`, `KLFIX`, `KLFLEX`, `KLTESTED` (same template family as agreed).
- **PUT 400 `{oadest}` / deep link:** retries once with `sub_id_3={oadest}` removed if the API rejects deep-link macro.
- **403 Access Denied:** treated as **blocked** (SK “forever paused” / archived — not editable via API). Counted separately; IDs listed in blocked file.
- **Rate limits:** adaptive — on `429`, cooldown then retry; network errors retry after cooldown.
- **Resume:** default state file `sk_migration_state.json` tracks `done_ids`, `failed_ids`, `blocked_ids`, last index. **Skips IDs already in `done_ids`** unless `--no-resume`.
- **Outputs:** `sk_migration_failed_ids.txt`, `sk_migration_blocked_ids.txt`; override with `--failed-ids-file` / `--blocked-ids-file`.
- **`--only-active`:** when listing all campaigns, only include `active: true` from list response (reduces work on dead inventory).

CLI mirrors live under `cli/` where applicable (e.g. `cli/migrate_sk_campaigns_bulk.py`).

### Artifacts you may see on disk

After bulk runs, the repo may contain:

- `sk_migration_state.json`, `sk_migration_failed_ids.txt`, `sk_migration_blocked_ids.txt`
- Retry-specific files if custom `--state-file` / `--ids-file` were used (e.g. `sk_failed_ids_from_last_run.txt`, `sk_migration_retry_*.json`)

These are operational checkpoints; do not delete casually if a migration is mid-flight.

## How to run things safely

- Prefer **`--dry-run`** on migration scripts when available.
- For bulk SK: use default resume state so completed campaigns are not PUT again.
- Use **`--only-active`** for full-list passes if archived campaigns should be ignored at list time.
- Read **`README.md`** for full command examples (Kelkoo daily, Keitaro sync, Blend, monetization checker).

## What “success” looks like for assistants

- **Kelkoo/Keitaro:** Sheets updated, offers synced, flows weighted as documented.
- **Blend:** Sheet and Keitaro Blend campaign aligned; potential sheets refreshed per config.
- **SK migration:** Names and tracking URLs updated for editable campaigns; blocked IDs documented; failed IDs retried or handed off for manual/UI fixes.

## Ecomnia — tracking URL migration

### Tool

- `tools/update_tracking_urls.py` — fetches all Ecomnia advertiser campaigns via `GET /get-advertiser-campaigns`, rewrites tracking URLs from the old `dighlyconsive.com` format to the new `trck.shopli.city` format, then updates each campaign via `POST /update-advertiser-campaign`.

### Credentials

Hardcoded in the CONFIG block at the top of the script: `ADVERTISER_KEY`, `AUTH_KEY`, `SECRET_KEY`. Auth token is generated per-request as `MD5(timestamp + SECRET_KEY)`.

### URL conversion

- Outer shell (`https://shopli.city/raini?rain=...`) is preserved unchanged.
- Inner URL base switches from `https://dighlyconsive.com/<uuid>` → `https://trck.shopli.city/7FDKRK`.
- Parameter remapping:

  | Old param | New param |
  |-----------|-----------|
  | `click_id` | `external_id` |
  | `adv_price` | `cost` |
  | `sub_id` | `sub_id_5` |
  | `oadest` | `sub_id_3` |
  | `geo` | `sub_id_2` |
  | `brand` | `sub_id_6` |
  | `hp` | `sub_id_1` |
  | `ctrl_*` / `traffic_type` | DROPPED |

### Safe operation

- **`DRY_RUN = True`** (default) — prints all conversions, makes no API calls. Always run this first.
- Set `DRY_RUN = False` to apply changes.
- `REQUEST_DELAY = 0.5 s` polite delay between update calls; script has exponential backoff retry logic on transient failures.
- Campaigns that don't contain the old domain are skipped automatically.
- Audit CSV written to `update_results.csv` (columns: `campaign_id`, `campaign_name`, `old_url`, `new_url`, `geo`, `brand`, `hp`, `api_response`, `status`).

### How to run

```bash
cd tools
python update_tracking_urls.py          # dry run (default)
# edit DRY_RUN = False, then:
python update_tracking_urls.py          # live run
```

## Editing guidelines (project norms)

- Small, focused diffs; match surrounding code style.
- Do not commit or rewrite `.env` secrets; reference variable names only.
- Prefer `config` imports for shared IDs and API keys over hardcoding spreadsheet IDs in new code.

---

*Last aligned with repo state: daily workflow + Blend integration, SK migration tooling, checkpoint/blocked handling, and `KEYSK` configuration.*
