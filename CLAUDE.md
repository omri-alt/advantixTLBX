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


| Area                      | Role                                                                                                                                 |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| Project root              | Main entry scripts (`run_daily_workflow.py`, `update_offers_from_sheet.py`, `blend_sync_from_sheet.py`, SK migration scripts, etc.). |
| `integrations/`           | API clients (Keitaro, Kelkoo helpers, Zeropark, Yadore, **autoserver** SK/ZP/EC helpers).                                            |
| `automations/autoserver/` | Merged AutoServer jobs (hourly + manual); see AutoServer section below.                                                              |
| `scheduler/`              | In-process schedulers (`autoserver_scheduler.py` APScheduler wiring).                                                                |
| `workflows/`              | Kelkoo daily logic, monthly log, shared workflow code.                                                                               |
| `cli/`                    | Thin wrappers that call root scripts with stable paths (`cli/run_daily_workflow.py`, etc.).                                          |
| `services/`               | Legacy/alternate client modules; some code paths still reference these.                                                              |
| `apps_script/`            | Google Apps Script related assets.                                                                                                   |
| `templates/help.html`     | Help Center (`/help`): flags, tables, and examples moved off tool pages.                                                             |
| `static/`                 | Shared UI assets (e.g. `ui_shared.css` for compact headers + Help layout).                                                           |
| `tools/`                  | One-off migration and utility scripts (e.g. Ecomnia tracking URL migration).                                                         |


**Convention:** Prefer matching existing style in a file (imports, logging, subprocess vs direct calls). Avoid drive-by refactors; keep changes scoped to the task.

## Configuration (`config.py`)

- Loads `.env` with `python-dotenv`.
- Includes `_read_env_fallback()` for malformed `.env` lines (extra spaces, `KEY = = value`).
- Important variables:
  - **Keitaro:** `KEITARO_BASE_URL`, `KEITARO_API_KEY`, optional campaign alias/id.
  - **Kelkoo / Sheets:** `FEED1_API_KEY`, `FEED2_API_KEY`, optional `FEED3_API_KEY`, … (any `FEEDn_API_KEY` is picked up by `discover_kelkoo_feed_api_keys()` for sales-report-only flows), optional `FEED2_MERCHANTS_GEOS` (comma 2-letter geos for feed2 merchants API only—skips markets you have on feed1 but not on feed2), optional per-feed raw geos `FEEDn_RAW_REPORT_GEOS` (else `KELKOO_RAW_REPORT_GEOS`), legacy publisher key `KEY_KL` / `keyKL` if no `FEEDn_API_KEY` is set, `KELKOO_SHEETS_SPREADSHEET_ID`, `BLEND_SHEETS_SPREADSHEET_ID`, `KELKOO_LATE_SALES_SPREADSHEET_ID`.
  - **Adexa (Blend sync):** `ADEXA_SITE_ID` (required for `feed=adexa` rows when building Keitaro offer URLs in `blend_sync_from_sheet.py`).
  - **SourceKnowledge:** `SOURCEKNOWLEDGE_API_KEY` from `KEYSK` or legacy `keySK` (see below).
  - **Zeropark / Yadore:** `KEYZP`, `YADORE_API_KEY`, etc.
  - **AutoServer:** `AUTOSERVER_SCHEDULER_ENABLED` (default on), `AUTOSERVER_RUN_LOG_PATH`, `AUTOSERVER_RUN_LOG_MAX`, `SK_TOOLS_SPREADSHEET_ID` (QualityWL gspread workbook), **`SK_OPTIMIZER_SHEET_ID`** (SK exploration / WL tracking tabs; defaults to `SK_TOOLS_SPREADSHEET_ID`). Legacy names `keyZP`, `keySK`, `ECadvKey` / `ECauthKey` / `ECsecretKey`, `keyKL` are populated from KLblend keys when missing—see `integrations/autoserver/env.py`.

## Daily Kelkoo → Keitaro workflow

**Script:** `run_daily_workflow.py` (wrapper: `cli/run_daily_workflow.py`).

Rough order: monthly log for yesterday → optional Blend potential refresh → delete previous day’s dated tabs → download feeds → reports/color fixim → pick merchants (default **top 3** per geo, rank-weighted PLA interleave) → PLA offers → combined sheet → Keitaro sync for both feeds → Blend block → **yesterday sales report** tabs on the late-sales workbook → **Kelkoo late-sales** diff (dry-run unless `--late-sales-apply`).

**Flags (non-exhaustive):**

- `--skip-keitaro` — skips Nipuhim Keitaro sync and Blend Keitaro sync; still can run Blend sheet populate unless `--skip-blend`.
- `--skip-blend` — skips the whole Blend block.
- `--feed1-traffic-only` — feed1 Keitaro only, then Blend steps as configured.
- `--single-merchant-per-geo` — legacy: only one merchant per geo for PLA (default is three).
- `--skip-sales-report` — skip Kelkoo yesterday sales export (`workflows/kelkoo_sales_report.py`) before late-sales.
- `--skip-late-sales` — skip the Kelkoo late-sales step after the sales report.
- `--late-sales-apply` — late-sales step sends GET postbacks when combined with a successful diff (still suppressed if global `--dry-run` is set).
- `--dry-run` — workflow: sales report does not write Sheets; late-sales never applies GETs.
- `--skip-blend-prune` — skip step 7a½ (Keitaro detach for offers not monetized in potential sheets).

**Nipuhim Keitaro sync:** `update_offers_from_sheet.py` only uploads the **first N** store-link rows per geo from the offers tab (default in script is 10). The daily workflow passes `--max-offers 60` so it matches the PLA generator cap (`run_daily_workflow.KEITARO_SYNC_MAX_OFFERS_PER_GEO`, aligned with up to 20 product rows per merchant × up to 3 merchants per geo); otherwise multi-merchant PLA rows beyond that cap never reach Keitaro.

- `--geo uk,fr,de` — comma-separated 2-letter geos: only those countries get fresh PLA + Keitaro sync; **existing rows for other geos stay** in `{date}_offers_*` (merge/replace per geo, not full tab wipe).
- `--merchant-override 1:uk=15248713` — repeatable; forces feed `1` or `2`, geo `uk`, merchant id list (comma = fallback order). Manual wins over auto-pick if both are set.
- `--merchant-auto-override 1:uk` — platform picks rank **2** for that feed+geo from fixim-ranked candidates; `1:uk:3` picks rank 3, etc.
- `--offers-and-keitaro-only` — skips monthly log 0a, Blend potential 0b, tab delete 0, and **does not rewrite fixim from a fresh feed download**; still downloads feeds for PLA id alternates, refetches reports + recolors existing `{date}_fixim_*`, then steps 3–6 only (no step 4b monthly log “today”, no Blend step 7).

**Control Center (Flask):** `/workflows/daily` exposes geo + merchant-mode dropdowns; they normalize args so users do not have to type raw flags. The homepage loads `GET /api/postback-status` (UTC “today” rollup from `runtime/daily_postbacks_last_run.json`) for the slim postback banner above the overview. **Long-form UI copy** (flags, caveats, examples) lives in `**/help`** (Help Center); tool pages keep a one-line subtitle + `?` link to the matching anchor.

**AutoServer (merged):** Legacy AutoServer **libz** clients live under `integrations/autoserver/` (`sk.py`, `zp.py`, `ec.py`, `kl_as.py`, `skunmon.py`, `gdocs_as.py`, **`sk_optimizer.py`**). Hourly automations are in `automations/autoserver/` (`MehilotAuto`, `KLFIXoptimize`, `PauseUnmonSK`, **`SKExplorationOptimizer`**, `KLWL`, `QualityWL`, `CloseNipuhimAuto`). `integrations/autoserver/env.py` maps KLblend `config` values into AutoServer-style env names (`keyZP`, `keySK`, `ECadvKey`, …) before those modules import. **Scheduler:** `scheduler/autoserver_scheduler.py` starts a `BackgroundScheduler` job at **minute 0 every hour** (same cadence as the old AutoServer `app.py`); `start_autoserver_scheduler()` is invoked from `app.py` at import time (works with Gunicorn). Disable on extra workers with `AUTOSERVER_SCHEDULER_ENABLED=0`. **Run log:** append-only JSON list at `data/autoserver_run_log.json` (max 500 entries, `AUTOSERVER_RUN_LOG_MAX`), written by `BaseAutomation._wrap_run`. **API:** `GET /api/automations` (status + last run per job), `GET /api/automations/log?limit=20`, `GET /api/automations/trigger/all` and `GET /api/automations/trigger/<ClassName>` return **202** and queue work on the scheduler (or a daemon thread if the scheduler is stopped). **UI:** `/automations` (linked from the homepage Tools grid).

**SK exploration optimizer:** Workbook `SK_OPTIMIZER_SHEET_ID` (defaults to the same id as `SK_TOOLS_SPREADSHEET_ID`). Tabs **`SKtrackExploration`** and **`SKtrackWL`** — headers are ensured on each hourly run; to bootstrap columns without waiting for the job, run **`python cli/setup_optimizer_sheets.py`** (also appends **`budgetReachedYesterday`** to EC **`trackExploration`** / **`trackWL`** when missing). Hourly job **`SKExplorationOptimizer`** runs `checkUnmonExploration_SK()` then `checkUnmonWL_SK()`. Exploration sheet: for each active row, aggregate **today** (UTC) clicks per `subId` from `GET .../stats/campaigns/{id}/by-publisher`; subs with **≥30 clicks** and not in the JSON **`wl`** list get **`POST .../campaigns/{id}/bid-factor` with `bidFactor: 0`** (SK has no separate blacklist array on campaign GET). Monetization: column **`monNetwork`** — `kl` (Kelkoo link API via `keyKL`), `feed1` / `feed2` (`FEED1_API_KEY` / `FEED2_API_KEY`), `feed3` / `yadore` (Yadore deeplink), `feed4` / `adexa` (Adexa link monetizer; needs `ADEXA_SITE_ID`). Column **`monUrl`** is the merchant URL to probe (optional if parsable `hp=` exists on the SK campaign `trackingUrl`). Unmonetized → `pause_campaign` + sheet status **`paused-unmon`**. WL sheet skips blacklisting. **Budget reached yesterday:** SK uses campaign **`dailyBudget`** vs sum of **`spend`** on by-publisher stats for **yesterday (UTC)**; EC uses **`daily_budget`** / **`dailybudget`** vs **`adv-stats-by-date`** `spend` — both write **`budgetReachedYesterday`** as `Yes` / `No` / `No limit` (EC columns added in `update_track_sheet` / `update_trackWLsheet`). **UI:** Flask **`/sk`** hub (tiles + links to sheets, QualityWL, tools sheet); **`/sk/bulk-open`** runs `sk_bulk_open_from_sheet.py`. With **`--apply --register-exploration`** (checkbox on the bulk form), each created campaign is appended to **`SKtrackExploration`** via `append_sk_exploration_tracking_rows` (dedupes by `campaignId`). Optional **`--mon-network`** (default `kl`) sets the new row’s monetization column.

**Blend block (step 7):**

- Refreshes **potentialKelkoo*** sheets per `BLEND_POTENTIAL_FEEDS` in `.env` (default `kelkoo1,kelkoo2`; feeds without an API key are skipped).
- Runs `populate_blend_from_potential.py` (monetized rows from each potential sheet, dedupe by geo+merchantId+`feed`, `clickCap` 50). Daily cap **`BLEND_POPULATE_MAX_ADD`** (default **5000**, max 20000) is only a safety limit — intent is that merchants who already passed conversion rules on a given feed’s potential sheet can be appended to the Blend tab for that same `feed` (kelkoo1 vs kelkoo2 vs adexa vs yadore are separate columns/tabs). Optional CLI `--prioritize-brand` / `--prioritize-merchant-id` reorders one-off runs. Diagnose: `python tools/diagnose_blend_potential_merchant.py --feed kelkoo2 --brand …`.
- **7a½ — Blend prune (Keitaro):** after potential refresh, detaches Blend-campaign offers whose names no longer match a **monetized** row on the corresponding `potentialKelkoo*` / `potentialAdexa` / `potentialYadore` sheet (same `kelkoo_monetization` column as populate). If a potential sheet **fails to load**, that feed is skipped (no removals from missing data). Implemented in `blend_sync_from_sheet.py` (`run_blend_prune_unmonetized_keitaro`, also run at the start of `blend_sync_from_sheet.py`). Use `--skip-blend-prune` on the daily workflow to bypass. `blend_sync_from_sheet.py --dry-run` logs Keitaro detachments only (no stream updates from the prune step).
- Runs `blend_sync_from_sheet.py` — prunes bad `auto='v'` rows, syncs Keitaro Blend campaign (alias documented in `README.md`).

Related: `blend_potential_merchants.py`, `populate_blend_from_potential.py`, `blend_sync_from_sheet.py` (uses `BLEND_SHEETS_SPREADSHEET_ID` from config).

## Blend → Keitaro (`blend_sync_from_sheet.py`)

- Reads tab `**Blend`** on `BLEND_SHEETS_SPREADSHEET_ID`; columns include `brandName`, `offerUrl`, `clickCap`, `geo`, `feed` (`kelkoo1` / `kelkoo2` / `adexa` / `yadore`), `auto`, optional `merchantId`.
- **Kelkoo** rows: `offerUrl` is wrapped like Nipuhim via `assistance.build_offer_action_payload` (per-feed account ids).
- **Adexa** rows: outer shell `https://shopli.city/raino?rain=` then inner `https://api.adexad.com/LinksMerchant.php?siteID=…&country=<geo>&merchantUrl=<encoded>&clickid={subid}`. The `rain` value is quoted so `=`, `&`, `?`, and Keitaro macros stay readable (only `merchantUrl` is heavily encoded), matching the proven **feed4** style more closely than a fully percent-encoded inner URL.
- **Yadore** rows: outer `https://shopli.city/rainotest?rain=` then inner `api.yadore.com/v2/d` with encoded merchant URL, market, `placementId={subid}`, `projectId` from `YADORE_PROJECT_ID` (with a documented fallback in the script).

## SourceKnowledge (SK) — what was added

### API key

- User stores the key in `.env` as `**KEYSK`**.
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
- `**--only-active`:** when listing all campaigns, only include `active: true` from list response (reduces work on dead inventory).

CLI mirrors live under `cli/` where applicable (e.g. `cli/migrate_sk_campaigns_bulk.py`).

### Artifacts you may see on disk

After bulk runs, the repo may contain:

- `sk_migration_state.json`, `sk_migration_failed_ids.txt`, `sk_migration_blocked_ids.txt`
- Retry-specific files if custom `--state-file` / `--ids-file` were used (e.g. `sk_failed_ids_from_last_run.txt`, `sk_migration_retry_*.json`)

These are operational checkpoints; do not delete casually if a migration is mid-flight.

## How to run things safely

- Prefer `**--dry-run`** on migration scripts when available.
- For bulk SK: use default resume state so completed campaigns are not PUT again.
- Use `**--only-active`** for full-list passes if archived campaigns should be ignored at list time.
- Read `**README.md**` for full command examples (Kelkoo daily, Keitaro sync, Blend, monetization checker).

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

  | Old param                 | New param     |
  | ------------------------- | ------------- |
  | `click_id`                | `external_id` |
  | `adv_price`               | `cost`        |
  | `sub_id`                  | `sub_id_5`    |
  | `oadest`                  | `sub_id_3`    |
  | `geo`                     | `sub_id_2`    |
  | `brand`                   | `sub_id_6`    |
  | `hp`                      | `sub_id_1`    |
  | `ctrl_`* / `traffic_type` | DROPPED       |


### Safe operation

- `**DRY_RUN = True`** (default) — prints all conversions, makes no API calls. Always run this first.
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

*Last aligned with repo state: daily workflow merchant rerun/override UI and flags, Blend Adexa offer URL shape, SK migration tooling, checkpoint/blocked handling, and `KEYSK` configuration.*