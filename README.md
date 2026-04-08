## KLblend

Automation around **Kelkoo** merchants feeds and **Keitaro** offers/flows.

- **Stack:** Python 3 CLI tools (no server)
- **Main scripts:** `run_daily_workflow.py`, `build_monthly_merchant_log.py`, `enrich_monthly_log_monetization.py`, `update_offers_from_sheet.py`, `run_keitaro_sync.py`, `run_blend_workflow.py`, `monetization_check.py`
- **New structure (recommended):**
  - `integrations/` – API clients (Keitaro, Zeropark, Yadore)
  - `cli/` – stable CLI entrypoints (wrappers around the root scripts)

### 1. Setup

- **Python + venv (recommended):**

  ```powershell
  cd c:\Users\Acer\KLblend
  python -m venv .venv
  .venv\Scripts\activate
  pip install -r requirements.txt
  ```

- **Environment:** copy `.env.example` → `.env` and fill:

  - `KEITARO_BASE_URL`, `KEITARO_API_KEY`, `KEITARO_CAMPAIGN_ALIAS`
  - `FEED1_API_KEY`, `FEED2_API_KEY` (Kelkoo)
  - `KELKOO_ACCOUNT_ID`, `KELKOO_ACCOUNT_ID_2`, `FEED1_KELKOO_ACCOUNT_ID` as needed
  - `KEYZP` (Zeropark API token, for resuming campaigns after offer sync)
  - `YADORE_API_KEY` (Yadore feed3 token for deeplink checks)

- **Google service account:**

  - Put `credentials.json` in the project root.
  - Share the spreadsheet with the service account email.

Spreadsheet ID: set `KELKOO_SHEETS_SPREADSHEET_ID` in `.env`, or it defaults to the project notebook ID.

### 2. Daily workflow (end‑to‑end)

Script: `run_daily_workflow.py`

```powershell
cd c:\Users\Acer\KLblend
python run_daily_workflow.py              # today
python run_daily_workflow.py --date 2026-03-11
python run_daily_workflow.py --skip-keitaro   # only sheets, no Keitaro
```

Preferred (same behavior, via `cli/`):

```powershell
python cli\run_daily_workflow.py
```

**Monthly merchant log** (from all `YYYY-MM-DD_offers_1` / `_offers_2` tabs in a month):

```powershell
python build_monthly_merchant_log.py                 # current UTC month → e.g. march_log_1, march_log_2
python build_monthly_merchant_log.py --year-month 2026-03 --dry-run
```

Log columns include **Merchant name**: same-day fixim tab if present, otherwise Kelkoo **merchants feed** plus aggregated **report** for that calendar month (1st through yesterday, capped to month end), per feed API key. Column **E** is **Kelkoo monetization** (`monetized`, `not_monetized`, `no_merchant_url`, `error (http)`, etc.) — filled by the daily workflow for **yesterday’s** rows only, or by:

```powershell
python enrich_monthly_log_monetization.py --year-month 2026-03
python enrich_monthly_log_monetization.py --year-month 2026-03 --force
```

High‑level steps:

0a. **Monthly log (before delete):** merge merchants from **yesterday’s** `YYYY-MM-DD_offers_1` / `_offers_2` into `{month}_log_*` and run Kelkoo **search/link** monetization for those rows only (column E).

0. **Delete the previous day’s daily tabs** (if they exist):  
   `YYYY-MM-DD_fixim_1`, `_fixim_2`, `_offers_1`, `_offers_2`, `_offers_today` for *yesterday* relative to the run date.
1. **Download merchants feeds** for both Kelkoo accounts:
   - Writes `YYYY-MM-DD_fixim_1` and `YYYY-MM-DD_fixim_2` sheets.
2. **Fetch Kelkoo reports** (month‑to‑date, or previous month on the 1st):
   - Colors fixim sheets (red / yellow / green) based on performance and a CPC floor.
3. **Pick green merchants**:
   - One green merchant per geo per feed.
4. **Generate offers** from Kelkoo PLA feed:
   - Writes `YYYY-MM-DD_offers_1` and `YYYY-MM-DD_offers_2`.
5. **Create combined offers sheet**:
   - `YYYY-MM-DD_offers_today` with a `Feed` column (1 or 2).
6. **Sync both feeds to Keitaro** (unless `--skip-keitaro`):
   - Internally runs `update_offers_from_sheet.py` for each feed.

### 3. Keitaro sync from a sheet

Script: `update_offers_from_sheet.py`

Purpose: read an offers sheet (one per feed) and make Keitaro offers/flows match it.

Usage examples:

```powershell
python update_offers_from_sheet.py
python update_offers_from_sheet.py --sheet "2026-03-11_offers_1"
python update_offers_from_sheet.py --sheet "2026-03-11_offers_2" --account 2
python update_offers_from_sheet.py --sheet "2026-03-11_offers_1" --max-offers 5
```

Key behavior:

- Reads columns:
  - **A** = country (geo), **D** = Store Link.
- Per geo:
  - Takes up to `--max-offers` rows (default 10).
  - Ensures that many Keitaro offers exist for that geo and feed:
    - Offer names: `feed1_uk_productN` for account 1, `feed2_uk_productN` for account 2.
  - Archives extra offers beyond `max-offers`.
  - Updates `action_payload` with the store URL (Kelkoo click‑out URL template).
- **Flows / traffic:**
  - Resolves streams/flows for the campaign alias (e.g. `HrQBXp`).
  - For each geo, sets the flow’s offers to **both feed1 and feed2** offers:
    - Combined list: `feed1_<geo>_product*` + `feed2_<geo>_product*`.
    - `set_flow_offers` assigns equal shares so traffic is split evenly.
- Writes back to the sheet:
  - Columns **G** and **H** = `live` and `offerUpload timestamp` for each updated row.

### 4. Keitaro sync only (no feeds/reports)

Script: `run_keitaro_sync.py`

Use this when offers sheets are already created, but only the Keitaro upload needs to be (re)run.

```powershell
python run_keitaro_sync.py                  # today’s YYYY-MM-DD_offers_1/_2
python run_keitaro_sync.py --date 2026-03-11
```

Preferred:

```powershell
python cli\run_keitaro_sync.py --date 2026-03-11
```

Behavior:

- Runs `update_offers_from_sheet.py --sheet YYYY-MM-DD_offers_1` for **feed1**.
- Runs `update_offers_from_sheet.py --sheet YYYY-MM-DD_offers_2 --account 2` for **feed2**.
- Flows end up with combined feed1+feed2 offers per geo.

### 5. Resume Zeropark campaigns (traffic source)

Script: `resume_zeropark_campaigns.py`

After offers are updated in Keitaro, resume Zeropark campaigns for the same countries so traffic can run.

- **Zeropark API:** Uses `KEYZP` from `.env` (API token from Zeropark Dashboard → My account → Security).
- **Sheet "Zeropark Campaigns":** In the same Google spreadsheet. Column **A** = Country (geo code, e.g. `fr`, `uk`, `de`), column **B** = Zeropark Campaign ID (UUID). The sheet is created automatically with headers if it does not exist; you fill in the campaign IDs per country.
- **Which countries:** The script reads the list of countries from the day’s offers sheets (`YYYY-MM-DD_offers_1` and `_offers_2`, column A). For each of those geos, if a Campaign ID is set in "Zeropark Campaigns", it calls `POST https://panel.zeropark.com/api/campaign/{campaignId}/resume`.

```powershell
python resume_zeropark_campaigns.py
python resume_zeropark_campaigns.py --date 2026-03-11
```

Run this after `run_keitaro_sync.py` (or after the full daily workflow) so only countries that received updated offers are resumed.

### 6. Blend workflow (sheet-driven, clickCap weighted)

This workflow syncs Keitaro campaign **Blend** (alias `9Xq9dSMh`) from a dedicated Google Sheet.

- **Spreadsheet:** `1h9lBPTREEJO9VVvj6wctCgCOn3YcwJBGIk_MBwXw-xY`
- **Tab:** `Blend`
- **Columns (header row):**
  - `brandName` (stable key; used in offer naming)
  - `offerUrl` (destination URL)
  - `clickCap` (weight; higher = more traffic share)
  - `geo` (country code like `fr`, `de`, `uk`)
  - `merchantId` (manual; used for Kelkoo report matching to build `potentialBlends`)

What it does:

1. **Ensures geo flows exist** in the Blend campaign (flow name = geo; country filter applied).
2. **Creates/updates offers** per row with naming: `blend_{geo}_{slug(brandName)}`.
3. **Attaches offers to the geo flow** and sets **weighted shares** based on `clickCap` (shares sum to 100).
4. **Generates `potentialBlends`** sheet: uses Kelkoo aggregated reports (**feed1**) and lists merchants with \(sales/leads > 0.01\).

Run it:

```powershell
cd c:\Users\Acer\KLblend
python run_blend_workflow.py
python run_blend_workflow.py --geo fr
python run_blend_workflow.py --skip-potential
python run_blend_workflow.py --only-potential
python run_blend_workflow.py --start 2026-03-01 --end 2026-03-10
```

Preferred:

```powershell
python cli\run_blend_workflow.py --geo fr
```

Scripts:

- `blend_sync_from_sheet.py`: reads the `Blend` tab and syncs offers/flows to Keitaro.
- `blend_potential_merchants.py`: fetches Kelkoo aggregated reports (feed1) month → yesterday and writes `potentialBlends` for merchants with \(sales/leads > 0.01\). Can enrich merchant name/domain/geo from the Kelkoo merchants feed; `--min-sales` can be used to require at least 1 sale (default).

### 7. Monetization checker (Kelkoo feed1 + Yadore feed3)

Script: `monetization_check.py`

Purpose: given a list of `url` + `geo` pairs in a Google Sheet, check whether the merchant URL is monetizable via:

- **Kelkoo (feed1)**: `GET /publisher/shopping/v2/search/link`
- **Yadore (feed3)**: `POST https://api.yadore.com/v2/deeplink`

Google Sheet:

- **Spreadsheet**: `1z1Y-vPuqk6zI673ytgBQvoQNnqMosFeZkdAiOMMPgM0`
- **Input tab**: `sourceToCheck` (columns: `url`, `geo`)
- **Output tab**: `Matches` (overwritten each run)

Yadore details:

- Auth uses the `API-Key` header (not Bearer).
- Request body uses:
  - `market` = geo (lowercase)
  - `placementId` = `WAF4IibbRqGG`
  - `isCouponing` = `false` (by default)
- Response can be either top-level or under a `result` wrapper; we parse both and write:
  - `yadore_root_found`, `yadore_root_total`
  - per-url `yadore_found`, `yadore_echo_url`, `yadore_clickUrl`
  - `yadore_estimatedCpc_amount`, `yadore_estimatedCpc_currency`, `yadore_logoUrl`
  - `yadore_error` if the API returns an error (e.g. invalid key)

Run it:

```powershell
cd c:\Users\Acer\KLblend
python monetization_check.py
```

Preferred:

```powershell
python cli\monetization_check.py --max-rows 20
```

Important note about `.env` formatting:

- Keep secrets in `.env` in the form `KEY=VALUE` (no extra spaces, no `= =`), otherwise tools like `python-dotenv` may not load them and the API will return “API-Key is invalid”.

### 8. URL patch helper

Script: `replace_offer_url_part.py`

Used to bulk‑replace a substring in all Keitaro offer URLs, e.g. changing a parameter name.

```powershell
python replace_offer_url_part.py "publisherClickId={clickid}" "publisherClickId={subid}" --dry-run
python replace_offer_url_part.py "publisherClickId={clickid}" "publisherClickId={subid}"
```

- `--dry-run` prints which offers would change.
- Without `--dry-run` it updates the offers in Keitaro.

### 9. Initial flow setup

Script: `ensure_geo_offers.py`

One‑time helper to create a baseline set of offers (non‑feed‑prefixed) and attach 3 offers per geo to flows.

Typically you only need this when bootstrapping the system; the daily workflow and sheet sync now work with the feed‑prefixed offers (`feed1_...`, `feed2_...`).

### 10. Where to tweak logic

- Kelkoo daily workflow (feeds, reports, coloring, green selection, offer generation):  
  `workflows/kelkoo_daily.py`
- Keitaro offer/flow helpers and payload construction:  
  `assistance.py`
- Sheet → Keitaro sync logic (per‑geo max offers, flow updates, writeback):  
  `update_offers_from_sheet.py`
- Blend (sheet-driven, clickCap weighted) workflow:  
  `run_blend_workflow.py`, `blend_sync_from_sheet.py`, `blend_potential_merchants.py`
- Monetization checker (Kelkoo + Yadore):  
  `monetization_check.py`, `services/yadore_client.py`

When coming back to the project, start by:

1. Checking `.env` and `credentials.json` still match your Keitaro and Google accounts.
2. Running `python run_daily_workflow.py --skip-keitaro` to verify Kelkoo + Sheets.
3. Running `python run_keitaro_sync.py --date <date>` to sync offers/flows from a known good offers sheet.
#   a d v a n t i x T L B X  
 