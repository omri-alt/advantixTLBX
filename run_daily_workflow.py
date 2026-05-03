#!/usr/bin/env python3
"""
Full daily workflow: merchants feed → reports & color segmentation →
pick merchants → generate PLA offers → combined sheet → sync Keitaro.

0a. Before deleting tabs: merge yesterday's offers into {month}_log_1 / _log_2 and set column E (Kelkoo monetization).
0b. Refresh Blend potentialKelkoo* sheets (same report window as this run).
0. Delete the previous calendar day's YYYY-MM-DD_fixim_* / _offers_* / _offers_today tabs if present.
1. Download merchants feed for feed1 and feed2 → write YYYY-MM-DD_fixim_1, _fixim_2.
2. Fetch Kelkoo reports (month-to-date or previous month on 1st); color fixim sheets.
3. Choose top-N merchants per geo (CPC + leads rules in workflows.kelkoo_daily); default up to **3**
   merchants per geo with rank-weighted PLA interleave (use ``--single-merchant-per-geo`` for legacy single-merchant).
4. Generate PLA offers → write YYYY-MM-DD_offers_1, _offers_2.
5. Create YYYY-MM-DD_offers_today (combined view with Feed column).
6. Sync both feeds to Keitaro (unless --skip-keitaro or no offers).
7. Blend: refresh potential → populate Blend → blend_sync_from_sheet (unless --skip-blend).

Optional: ``--run-daily-conversion-postbacks`` runs ``run_daily_conversion_postbacks.py`` after a successful
workflow (Kelkoo per-geo + Adexa + Yadore → Keitaro GET postbacks). Use ``--postback-report-date YYYY-MM-DD`` to
override the stats date (default: yesterday UTC).

After PLA / Keitaro / Blend (or when skipped): **8)** yesterday Kelkoo sales report tabs on the late-sales workbook
(``workflows.kelkoo_sales_report``), then **9)** Kelkoo late-sales diff/dry-run (use ``--late-sales-apply`` to send
GET postbacks). Use ``--skip-sales-report`` / ``--skip-late-sales`` to bypass. Global ``--dry-run`` skips sheet writes
for the sales report and forces late-sales dry-run.

Requires .env: KEITARO_BASE_URL, KEITARO_API_KEY, FEED1_API_KEY, FEED2_API_KEY; credentials.json.
Optional: ``BLEND_POTENTIAL_FEEDS`` (comma list, default ``kelkoo1,kelkoo2``) for step 0b/7a; feeds without an API key are skipped.

  python run_daily_workflow.py
  python run_daily_workflow.py --date 2026-04-03
  python run_daily_workflow.py --skip-keitaro
  python run_daily_workflow.py --feed1-traffic-only
  python run_daily_workflow.py --single-merchant-per-geo
  python run_daily_workflow.py --skip-blend
  python run_daily_workflow.py --skip-blend-sync
  python run_daily_workflow.py --geo uk,fr
  python run_daily_workflow.py --geo uk --merchant-override 1:uk=15248713
  python run_daily_workflow.py --geo uk --merchant-auto-override 1:uk
  python run_daily_workflow.py --geo de --merchant-auto-override 2:de:3
  python run_daily_workflow.py --offers-and-keitaro-only --geo es --merchant-skip-replace 1:es:111111=222222
  python run_daily_workflow.py --offers-and-keitaro-only --date 2026-04-15 --geo de
  python run_daily_workflow.py --include-flex
  python run_daily_workflow.py --run-daily-conversion-postbacks
  python run_daily_workflow.py --run-daily-conversion-postbacks --postback-report-date 2026-04-08
  python run_daily_workflow.py --skip-sales-report
  python run_daily_workflow.py --skip-late-sales
  python run_daily_workflow.py --late-sales-apply
  python run_daily_workflow.py --dry-run
  python run_daily_workflow.py --skip-blend-prune
"""
from __future__ import annotations

import logging
import re
import subprocess
from collections import Counter
from typing import Any, Dict, List, Optional, Set, Tuple
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv

load_dotenv()

from config import (
    ADEXA_API_KEY,
    ADEXA_SITE_ID,
    BLEND_POTENTIAL_FEEDS,
    FEED1_API_KEY,
    FEED2_API_KEY,
    FEED2_MERCHANTS_GEOS,
    KELKOO_SHEETS_SPREADSHEET_ID,
    YADORE_API_KEY,
)

from workflows.kelkoo_daily import (
    _normalize_merchant_id_from_sheet,
    apply_fixim_colors,
    build_pla_id_alternates_for_feed,
    download_merchants_feed,
    fetch_reports,
    generate_offers_rank_weighted_interleave,
    get_top_merchants_per_geo,
    merge_offers_replace_geos,
    read_offers_sheet_rows,
    write_fixim_sheet,
    write_offers_sheet,
)
from workflows.monthly_log_monetization import (
    upsert_run_merchants_into_monthly_log,
    upsert_yesterday_merchants_into_monthly_log,
)

SPREADSHEET_ID = KELKOO_SHEETS_SPREADSHEET_ID

# Keitaro Nipuhim sync reads only the first N rows per geo from the offers sheet (``--max-offers``).
# Keep in sync with ``generate_offers_rank_weighted_interleave`` default combined per-geo cap (60).
KEITARO_SYNC_MAX_OFFERS_PER_GEO = 60

_DAILY_SHEET_SUFFIXES = ("_fixim_1", "_fixim_2", "_offers_1", "_offers_2", "_offers_today")

_MERCHANT_OVERRIDE_RE = re.compile(r"^([12]):([a-z]{2})=(.+)$", re.I)
_MERCHANT_AUTO_OVERRIDE_RE = re.compile(r"^([12]):([a-z]{2})(?::(\d+))?$", re.I)
_MERCHANT_SKIP_REPLACE_RE = re.compile(r"^([12]):([a-z]{2}):(\d+)=(\d+)$", re.I)


def _parse_geo_list_csv(s: str) -> Set[str]:
    out: Set[str] = set()
    for part in s.split(","):
        g = part.strip().lower()[:2]
        if len(g) == 2:
            out.add(g)
    return out


def _parse_merchant_override_arg(spec: str) -> Optional[Tuple[int, str, List[str]]]:
    """
    One override: ``1:uk=15248713`` or ``2:de=111,222`` (feed 1 or 2, geo, merchant id list).
    """
    m = _MERCHANT_OVERRIDE_RE.match((spec or "").strip())
    if not m:
        return None
    feed = int(m.group(1))
    geo = m.group(2).lower()[:2]
    ids_raw = m.group(3).strip()
    ids: List[str] = []
    for piece in ids_raw.split(","):
        nid = _normalize_merchant_id_from_sheet(piece.strip())
        if nid:
            ids.append(nid)
    if not ids:
        return None
    return feed, geo, ids


def _collect_merchant_overrides(
    argv: List[str],
) -> Tuple[Dict[int, Dict[str, List[str]]], Set[str]]:
    """Returns (feed -> geo -> merchant ids) and implied geos from overrides."""
    out: Dict[int, Dict[str, List[str]]] = {1: {}, 2: {}}
    implied_geos: Set[str] = set()
    i = 0
    while i < len(argv):
        if argv[i] == "--merchant-override" and i + 1 < len(argv):
            parsed = _parse_merchant_override_arg(argv[i + 1])
            if parsed:
                feed, geo, ids = parsed
                out[feed][geo] = ids
                implied_geos.add(geo)
            i += 2
            continue
        i += 1
    return out, implied_geos


def _parse_merchant_auto_override_arg(spec: str) -> Optional[Tuple[int, str, int]]:
    """
    Platform-driven override:
      - ``1:uk``    -> choose rank 2 for feed1/uk (next best)
      - ``2:de:3``  -> choose rank 3 for feed2/de
    """
    m = _MERCHANT_AUTO_OVERRIDE_RE.match((spec or "").strip())
    if not m:
        return None
    feed = int(m.group(1))
    geo = m.group(2).lower()[:2]
    rank = int(m.group(3) or "2")
    if rank < 1:
        rank = 1
    return feed, geo, rank


def _parse_merchant_skip_replace_arg(spec: str) -> Optional[Tuple[int, str, str, str]]:
    """
    One swap: ``1:es:15248713=99887766`` → feed 1, geo es, replace merchant id 15248713 with 99887766
    in the chosen merchant list (same slot / list order; dedupe if the substitute already appears).
    """
    m = _MERCHANT_SKIP_REPLACE_RE.match((spec or "").strip())
    if not m:
        return None
    feed = int(m.group(1))
    geo = m.group(2).lower()[:2]
    old_id = _normalize_merchant_id_from_sheet(m.group(3))
    new_id = _normalize_merchant_id_from_sheet(m.group(4))
    if len(geo) != 2 or not old_id or not new_id or old_id == new_id:
        return None
    return feed, geo, old_id, new_id


def _collect_merchant_skip_replaces(argv: List[str]) -> Dict[int, List[Tuple[str, str, str]]]:
    """feed -> list of (geo, old_merchant_id, new_merchant_id), in argv order."""
    out: Dict[int, List[Tuple[str, str, str]]] = {1: [], 2: []}
    i = 0
    while i < len(argv):
        if argv[i] == "--merchant-skip-replace" and i + 1 < len(argv):
            parsed = _parse_merchant_skip_replace_arg(argv[i + 1])
            if parsed:
                feed, geo, old_id, new_id = parsed
                out[feed].append((geo, old_id, new_id))
            i += 2
            continue
        i += 1
    return out


def _collect_merchant_auto_overrides(
    argv: List[str],
) -> Tuple[Dict[int, Dict[str, int]], Set[str]]:
    """Returns (feed -> geo -> rank) and implied geos from auto-overrides."""
    out: Dict[int, Dict[str, int]] = {1: {}, 2: {}}
    implied_geos: Set[str] = set()
    i = 0
    while i < len(argv):
        if argv[i] == "--merchant-auto-override" and i + 1 < len(argv):
            parsed = _parse_merchant_auto_override_arg(argv[i + 1])
            if parsed:
                feed, geo, rank = parsed
                out[feed][geo] = rank
                implied_geos.add(geo)
            i += 2
            continue
        i += 1
    return out, implied_geos


def _parse_daily_workflow_argv(argv: List[str]) -> dict:
    """Subset of flags parsed from argv (manual loop, matches existing style)."""
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    skip_keitaro = "--skip-keitaro" in argv
    skip_blend = "--skip-blend" in argv
    skip_blend_sync = "--skip-blend-sync" in argv
    skip_blend_prune = "--skip-blend-prune" in argv
    feed1_traffic_only = "--feed1-traffic-only" in argv
    merchants_per_geo = 1 if "--single-merchant-per-geo" in argv else 3
    include_flex_merchants = "--include-flex" in argv
    static_only = not include_flex_merchants
    only_geos: Set[str] | None = None
    run_daily_conversion_postbacks = "--run-daily-conversion-postbacks" in argv
    offers_and_keitaro_only = "--offers-and-keitaro-only" in argv
    postback_report_date = (datetime.now(timezone.utc).date() - timedelta(days=1)).strftime("%Y-%m-%d")
    skip_sales_report = "--skip-sales-report" in argv
    skip_late_sales = "--skip-late-sales" in argv
    late_sales_apply = "--late-sales-apply" in argv
    workflow_dry_run = "--dry-run" in argv
    merchant_overrides, implied_geos_manual = _collect_merchant_overrides(argv)
    merchant_auto_overrides, implied_geos_auto = _collect_merchant_auto_overrides(argv)
    merchant_skip_replaces = _collect_merchant_skip_replaces(argv)

    i = 0
    while i < len(argv):
        if argv[i] == "--date" and i + 1 < len(argv):
            date_str = argv[i + 1].strip()
            i += 2
            continue
        if argv[i] == "--postback-report-date" and i + 1 < len(argv):
            postback_report_date = argv[i + 1].strip()
            i += 2
            continue
        if argv[i] == "--geo" and i + 1 < len(argv):
            gset = _parse_geo_list_csv(argv[i + 1])
            if gset:
                only_geos = gset
            i += 2
            continue
        i += 1

    partial_geos: Set[str] = set()
    if only_geos:
        partial_geos |= only_geos
    partial_geos |= implied_geos_manual
    partial_geos |= implied_geos_auto
    # Fast rerun: swap applies to named geos; merge those tabs only (no need to also pass --geo).
    if offers_and_keitaro_only:
        for flist in merchant_skip_replaces.values():
            for geo, _old, _new in flist:
                g = (geo or "").strip().lower()[:2]
                if len(g) == 2:
                    partial_geos.add(g)

    return {
        "date_str": date_str,
        "skip_keitaro": skip_keitaro,
        "skip_blend": skip_blend,
        "skip_blend_sync": skip_blend_sync,
        "skip_blend_prune": skip_blend_prune,
        "feed1_traffic_only": feed1_traffic_only,
        "merchants_per_geo": merchants_per_geo,
        "static_only": static_only,
        "only_geos": only_geos,
        "partial_geos": frozenset(partial_geos),
        "merchant_overrides": merchant_overrides,
        "merchant_auto_overrides": merchant_auto_overrides,
        "merchant_skip_replaces": merchant_skip_replaces,
        "run_daily_conversion_postbacks": run_daily_conversion_postbacks,
        "offers_and_keitaro_only": offers_and_keitaro_only,
        "postback_report_date": postback_report_date,
        "skip_sales_report": skip_sales_report,
        "skip_late_sales": skip_late_sales,
        "late_sales_apply": late_sales_apply,
        "workflow_dry_run": workflow_dry_run,
    }

BLEND_DAILY_MAX_NEW_ROWS = 50


def _blend_potential_feeds_for_run() -> tuple[str, ...]:
    """Feeds from config that have the required credentials (otherwise skip with a note)."""
    out: list[str] = []
    for f in BLEND_POTENTIAL_FEEDS:
        if f == "kelkoo1" and (FEED1_API_KEY or "").strip():
            out.append(f)
        elif f == "kelkoo2" and (FEED2_API_KEY or "").strip():
            out.append(f)
        elif f == "adexa" and (ADEXA_SITE_ID or "").strip() and (ADEXA_API_KEY or "").strip():
            out.append(f)
        elif f == "yadore" and (YADORE_API_KEY or "").strip():
            out.append(f)
        elif f == "kelkoo1":
            print("   Note: Blend potential 'kelkoo1' skipped (missing FEED1_API_KEY).")
        elif f == "kelkoo2":
            print("   Note: Blend potential 'kelkoo2' skipped (missing FEED2_API_KEY).")
        elif f == "adexa":
            print("   Note: Blend potential 'adexa' skipped (missing ADEXA_SITE_ID / ADEXA_API_KEY).")
        elif f == "yadore":
            print("   Note: Blend potential 'yadore' skipped (missing YADORE_API_KEY).")
    return tuple(out)


def run_blend_potential_sheets(
    start_str: str,
    end_str: str,
    feeds: tuple[str, ...] | None = None,
) -> bool:
    """Refresh potentialKelkoo* sheets in the Blend spreadsheet."""
    script = Path(__file__).resolve().parent / "blend_potential_merchants.py"
    ok = True
    for feed in feeds or _blend_potential_feeds_for_run():
        cmd = [sys.executable, str(script), "--feed", feed, "--start", start_str, "--end", end_str]
        r = subprocess.run(cmd)
        ok = ok and (r.returncode == 0)
    return ok


def run_populate_blend_from_potential(
    feed: str = "kelkoo1",
    max_add: int = BLEND_DAILY_MAX_NEW_ROWS,
) -> bool:
    script = Path(__file__).resolve().parent / "populate_blend_from_potential.py"
    cmd = [
        sys.executable,
        str(script),
        "--feed",
        feed,
        "--max-add",
        str(max_add),
    ]
    return subprocess.run(cmd).returncode == 0


def run_optional_daily_conversion_postbacks(report_date: str) -> None:
    """Subprocess: Kelkoo (per geo) + Adexa + Yadore postbacks; has its own resume state on disk."""
    script = Path(__file__).resolve().parent / "run_daily_conversion_postbacks.py"
    cmd = [sys.executable, str(script), "--report-date", report_date]
    print()
    print("Daily conversion postbacks (Kelkoo + Adexa + Yadore) ...")
    r = subprocess.run(cmd)
    if r.returncode != 0:
        print(f"   Warning: run_daily_conversion_postbacks.py exited with code {r.returncode}.")


def run_blend_sync_from_sheet(extra_args: list[str] | None = None) -> bool:
    script = Path(__file__).resolve().parent / "blend_sync_from_sheet.py"
    cmd = [sys.executable, str(script)]
    if extra_args:
        cmd.extend(extra_args)
    return subprocess.run(cmd).returncode == 0


def run_blend_daily_steps(
    *,
    skip_keitaro: bool,
    skip_blend: bool,
    skip_blend_sync: bool,
    skip_blend_prune: bool = False,
) -> None:
    if skip_blend:
        return
    print("7. Blend workflow (spreadsheet + Keitaro campaign) ...")
    for feed in _blend_potential_feeds_for_run():
        print(f"   7a. Populate Blend from potential ({feed}, max {BLEND_DAILY_MAX_NEW_ROWS} new rows) ...")
        if not run_populate_blend_from_potential(feed=feed, max_add=BLEND_DAILY_MAX_NEW_ROWS):
            print(f"   Warning: populate_blend_from_potential ({feed}) exited non-zero.")
    if not skip_blend_prune:
        print("   7a½. Blend prune: detach Keitaro Blend offers not monetized in potential sheets ...")
        try:
            from blend_sync_from_sheet import run_blend_prune_unmonetized_keitaro

            blend_svc = get_sheets_service()
            res = run_blend_prune_unmonetized_keitaro(blend_svc, only_geo=None, dry_run=False)
            removed = res.get("removed") or []
            geo_set = {str(x[1]) for x in removed if len(x) > 1}
            print(
                f"   Removed {len(removed)} offers from Keitaro Blend flows across "
                f"{len(geo_set)} geo(s)."
            )
        except Exception as e:
            print(f"   Warning: Blend prune step failed: {e}")
    else:
        print("   7a½. Skipping Blend prune (--skip-blend-prune).")
    if skip_keitaro:
        print("   7b. Skipping Blend Keitaro sync (--skip-keitaro).")
        print()
        return
    if skip_blend_sync:
        print("   7b. Skipping Blend Keitaro sync (--skip-blend-sync).")
        print()
        return
    print("   7b. blend_sync_from_sheet (prune auto='v' non-monetized + Keitaro) ...")
    if not run_blend_sync_from_sheet():
        print("   Blend Keitaro sync failed.")
        sys.exit(1)
    print()


def get_credentials_path() -> str:
    p = Path(__file__).resolve().parent / "credentials.json"
    if not p.exists():
        raise FileNotFoundError(f"credentials.json not found at {p}")
    return str(p)


def get_sheets_service():
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds = service_account.Credentials.from_service_account_file(get_credentials_path())
    return build("sheets", "v4", credentials=creds).spreadsheets()


def delete_dated_daily_sheets(service, spreadsheet_id: str, day_str: str) -> list[str]:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day_str):
        return []
    titles_to_remove = [f"{day_str}{suf}" for suf in _DAILY_SHEET_SUFFIXES]
    meta = service.get(
        spreadsheetId=spreadsheet_id,
        fields="sheets(properties(sheetId,title))",
    ).execute()
    title_to_id = {
        s["properties"]["title"]: s["properties"]["sheetId"]
        for s in meta.get("sheets", [])
    }
    requests = []
    deleted: list[str] = []
    for t in titles_to_remove:
        sid = title_to_id.get(t)
        if sid is not None:
            requests.append({"deleteSheet": {"sheetId": sid}})
            deleted.append(t)
    if requests:
        service.batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": requests}).execute()
    return deleted


def read_sheet_values(service, sheet_name: str, range_a1: str = "A:H"):
    quoted = sheet_name.replace("'", "''")
    try:
        result = service.values().get(
            spreadsheetId=SPREADSHEET_ID, range=f"'{quoted}'!{range_a1}"
        ).execute()
        return result.get("values") or []
    except Exception:
        return None


def create_combined_offers_sheet(service, date_str: str) -> bool:
    sheet1_name = f"{date_str}_offers_1"
    sheet2_name = f"{date_str}_offers_2"
    combined_name = f"{date_str}_offers_today"

    rows1 = read_sheet_values(service, sheet1_name)
    rows2 = read_sheet_values(service, sheet2_name)

    if not rows1 and not rows2:
        return False

    header_with_feed = ["Feed", "Country", "Merchant ID", "Product Title", "Store Link", "Audit Status", "Timestamp"]
    combined_rows: list[list] = []
    if rows1:
        for i, row in enumerate(rows1):
            if i == 0:
                combined_rows.append(header_with_feed)
            else:
                cells = (row + [""] * 6)[:6]
                combined_rows.append(["1"] + cells)
    if rows2:
        for i, row in enumerate(rows2):
            if i == 0 and not combined_rows:
                combined_rows.append(header_with_feed)
            if i == 0:
                continue
            cells = (row + [""] * 6)[:6]
            combined_rows.append(["2"] + cells)

    if not combined_rows:
        return False

    meta = service.get(spreadsheetId=SPREADSHEET_ID, fields="sheets(properties(title))").execute()
    titles = [s.get("properties", {}).get("title") for s in meta.get("sheets", [])]
    if combined_name not in titles:
        service.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": combined_name}}}]},
        ).execute()

    quoted = combined_name.replace("'", "''")
    try:
        service.values().clear(spreadsheetId=SPREADSHEET_ID, range=f"'{quoted}'!A1:Z1000").execute()
    except Exception:
        pass
    service.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{quoted}'!A1",
        valueInputOption="USER_ENTERED",
        body={"values": combined_rows},
    ).execute()
    print(f"  Created {combined_name} with {len(combined_rows)} rows.")
    return True


def run_update_offers_from_sheet(
    sheet_name: str,
    account: int,
    extra_args: list[str] | None = None,
) -> bool:
    script = Path(__file__).resolve().parent / "update_offers_from_sheet.py"
    cmd = [sys.executable, str(script), "--sheet", sheet_name]
    if account == 2:
        cmd.extend(["--account", "2"])
    merged = list(extra_args or [])
    if "--max-offers" not in merged:
        merged.extend(["--max-offers", str(KEITARO_SYNC_MAX_OFFERS_PER_GEO)])
    if merged:
        cmd.extend(merged)
    return subprocess.run(cmd).returncode == 0


def _log_pla_merchant_distribution(label: str, rows: List[Dict[str, Any]]) -> None:
    """Log offer counts per (Country, Merchant ID) before Keitaro sheet sync."""
    c = Counter()
    for r in rows:
        geo = str(r.get("Country") or "").strip().upper()
        mid = str(r.get("Merchant ID") or "").strip()
        if geo and mid:
            c[(geo, mid)] += 1
    print(f"   PLA rows for {label}: total={len(rows)} distinct (geo,merchant)={len(c)}")
    shown = 0
    for (geo, mid), n in sorted(c.items(), key=lambda x: (-x[1], x[0][0], x[0][1])):
        print(f"      {geo} merchant_id={mid}: {n} offers")
        shown += 1
        if shown >= 48:
            if len(c) > shown:
                print(f"      ... ({len(c) - shown} more (geo,merchant) pairs omitted)")
            break


def _apply_merchant_overrides_to_chosen(
    chosen: Dict[str, List[str]],
    feed_num: int,
    merchant_overrides: Dict[int, Dict[str, List[str]]],
) -> Dict[str, List[str]]:
    ch = dict(chosen)
    for geo, ids in merchant_overrides.get(feed_num, {}).items():
        g = geo.strip().lower()[:2]
        if len(g) == 2 and ids:
            ch[g] = list(ids)
    return ch


def _apply_platform_auto_overrides_to_chosen(
    chosen: Dict[str, List[str]],
    *,
    ranked_per_geo: Dict[str, List[str]],
    feed_num: int,
    merchant_auto_overrides: Dict[int, Dict[str, int]],
) -> Dict[str, List[str]]:
    """
    Replace a geo with a platform-picked rank from the ranked candidates list.
    Manual ``--merchant-override`` can still override this afterwards.
    """
    ch = dict(chosen)
    for geo, rank in merchant_auto_overrides.get(feed_num, {}).items():
        g = geo.strip().lower()[:2]
        if len(g) != 2:
            continue
        candidates = list(ranked_per_geo.get(g) or [])
        if not candidates:
            print(f"   Feed{feed_num} auto-override {g}: no ranked candidates on fixim; keeping current selection.")
            continue
        idx = max(0, int(rank) - 1)
        if idx >= len(candidates):
            print(
                f"   Feed{feed_num} auto-override {g}: requested rank {rank} "
                f"but only {len(candidates)} candidates available; keeping current selection."
            )
            continue
        selected = candidates[idx]
        ch[g] = [selected]
        print(f"   Feed{feed_num} auto-override {g}: selected rank {rank} merchant {selected}.")
    return ch


def _apply_merchant_skip_replaces(
    chosen: Dict[str, List[str]],
    feed_num: int,
    swaps_by_feed: Dict[int, List[Tuple[str, str, str]]],
) -> Dict[str, List[str]]:
    """
    Replace one merchant id with another in the chosen list for a geo (PLA source list).
    Runs after manual/auto overrides. If ``old`` is not in the list for that geo, logs and skips.
    """
    swaps = swaps_by_feed.get(feed_num) or []
    if not swaps:
        return chosen
    ch = dict(chosen)
    for geo, old_id, new_id in swaps:
        g = (geo or "").strip().lower()[:2]
        if len(g) != 2:
            continue
        cur = [str(x).strip() for x in (ch.get(g) or []) if str(x).strip()]
        if not cur:
            print(
                f"   Feed{feed_num} merchant-skip-replace {g}: no merchants in selection; "
                f"cannot apply {old_id}→{new_id}"
            )
            continue
        norm_cur = [_normalize_merchant_id_from_sheet(x) for x in cur]
        if old_id not in norm_cur:
            print(
                f"   Feed{feed_num} merchant-skip-replace {g}: merchant {old_id} not in chosen {cur!r}; "
                f"skipping {old_id}→{new_id}"
            )
            continue
        replaced = [_normalize_merchant_id_from_sheet(x) for x in cur]
        replaced = [new_id if x == old_id else x for x in replaced]
        # Drop duplicate ids while keeping order (e.g. swap onto an id already in the list).
        seen: Set[str] = set()
        deduped: List[str] = []
        for x in replaced:
            if not x:
                continue
            if x in seen:
                continue
            seen.add(x)
            deduped.append(x)
        ch[g] = deduped
        print(f"   Feed{feed_num} merchant-skip-replace {g}: {old_id}→{new_id} merchants now {deduped!r} (was {cur!r})")
    return ch


def _run_post_pla_automation_tail(service: Any, pa: dict) -> None:
    """Sales report (late-sales workbook) then Kelkoo late-sales flow; errors are logged, non-fatal."""
    dry = bool(pa.get("workflow_dry_run"))
    if dry:
        print("   (--dry-run: sales report will not write sheets; late-sales will not apply GETs.)")

    print()
    print("8. Kelkoo yesterday sales report (late-sales workbook) ...")
    if pa.get("skip_sales_report"):
        print("   Skipped (--skip-sales-report).")
    else:
        try:
            from workflows.kelkoo_sales_report import run_yesterday_sales_reports

            run_yesterday_sales_reports(service, dry_run=dry)
            print("   Done.")
        except Exception as e:
            print(f"   Sales report step error (non-fatal): {e}")

    print()
    print("9. Kelkoo late-sales detection ...")
    if pa.get("skip_late_sales"):
        print("   Skipped (--skip-late-sales).")
        return
    try:
        from kelkoo_late_sales import run_late_sales_flow

        from config import KELKOO_LATE_SALES_SPREADSHEET_ID, LATE_SALES_POSTBACK_BASE

        root = Path(__file__).resolve().parent
        cred = root / "credentials.json"
        apply_ls = bool(pa.get("late_sales_apply")) and not dry
        res = run_late_sales_flow(
            credentials_path=cred,
            spreadsheet_id=KELKOO_LATE_SALES_SPREADSHEET_ID,
            postback_base=LATE_SALES_POSTBACK_BASE,
            as_of_str="",
            apply=apply_ls,
        )
        if res.get("ok"):
            mode = "apply" if apply_ls else "dry-run"
            print(f"   Late-sales ({mode}): ok.")
        else:
            print(f"   Late-sales: {res.get('error', 'failed')}")
    except Exception as e:
        print(f"   Late-sales step error (non-fatal): {e}")


def run_pla_offers_keitaro_blend_tail(
    service: Any,
    *,
    date_str: str,
    merchants1: list,
    merchants2: list,
    perf1: dict,
    perf2: dict,
    fixim_1: str,
    fixim_2: str,
    merchants_per_geo: int,
    partial_geos: frozenset[str],
    merchant_overrides: Dict[int, Dict[str, List[str]]],
    merchant_auto_overrides: Dict[int, Dict[str, int]],
    merchant_skip_replaces: Dict[int, List[Tuple[str, str, str]]],
    merge_offers_tabs: bool,
    run_monthly_log_today: bool,
    skip_keitaro: bool,
    feed1_traffic_only: bool,
    skip_blend: bool,
    skip_blend_sync: bool,
    run_blend_steps: bool,
    run_daily_conversion_postbacks: bool,
    postback_report_date: str,
    skip_blend_prune: bool = False,
) -> None:
    """Steps 3–6 (optional 4b) and optional 7: merchant selection → PLA → Keitaro → Blend."""
    offers_1 = f"{date_str}_offers_1"
    offers_2 = f"{date_str}_offers_2"

    base_top_n = max(1, int(merchants_per_geo))
    max_auto_rank = 1
    for feed_data in merchant_auto_overrides.values():
        for rank in feed_data.values():
            if rank > max_auto_rank:
                max_auto_rank = rank
    rank_depth = max(base_top_n, max_auto_rank)
    print(
        f"3. Choosing merchants (top-{rank_depth} scan; up to {base_top_n} per geo; "
        "report rules + CPC floor) ..."
    )
    print(
        f"   Config: merchants_per_geo={merchants_per_geo} "
        "(use --single-merchant-per-geo to force 1 merchant per geo)."
    )
    ranked1 = get_top_merchants_per_geo(
        service, SPREADSHEET_ID, fixim_1, perf1, top_n=rank_depth
    )
    ranked2 = get_top_merchants_per_geo(
        service, SPREADSHEET_ID, fixim_2, perf2, top_n=rank_depth
    )
    chosen1 = {geo: mids[:base_top_n] for geo, mids in ranked1.items()}
    chosen2 = {geo: mids[:base_top_n] for geo, mids in ranked2.items()}
    chosen1 = _apply_platform_auto_overrides_to_chosen(
        chosen1,
        ranked_per_geo=ranked1,
        feed_num=1,
        merchant_auto_overrides=merchant_auto_overrides,
    )
    chosen2 = _apply_platform_auto_overrides_to_chosen(
        chosen2,
        ranked_per_geo=ranked2,
        feed_num=2,
        merchant_auto_overrides=merchant_auto_overrides,
    )
    chosen1 = _apply_merchant_overrides_to_chosen(chosen1, 1, merchant_overrides)
    chosen2 = _apply_merchant_overrides_to_chosen(chosen2, 2, merchant_overrides)
    chosen1 = _apply_merchant_skip_replaces(chosen1, 1, merchant_skip_replaces)
    chosen2 = _apply_merchant_skip_replaces(chosen2, 2, merchant_skip_replaces)
    if partial_geos:
        chosen1 = {k: v for k, v in chosen1.items() if k in partial_geos}
        chosen2 = {k: v for k, v in chosen2.items() if k in partial_geos}
    print(f"   Feed1: {len(chosen1)} geos")
    print(f"   Feed2: {len(chosen2)} geos")
    print("   Merchant selection detail (ranked list length vs chosen count per geo):")
    for label, ranked, chosen in (
        ("Feed1", ranked1, chosen1),
        ("Feed2", ranked2, chosen2),
    ):
        for g in sorted(chosen.keys()):
            rlist = ranked.get(g) or []
            clist = chosen.get(g) or []
            print(
                f"   {label} geo={g}: ranked_merchants={len(rlist)} chosen_merchants={len(clist)} "
                f"ids={clist!r} top_of_ranking={rlist[: max(5, len(clist))]!r}"
            )
    for label, ch in (("Feed1", chosen1), ("Feed2", chosen2)):
        if "it" in ch:
            print(f"   {label} Italy: PLA will use merchant id(s) {ch['it']!r} (country=it)")
        else:
            print(
                f"   {label} Italy: not in selection — **no PLA requests** for IT "
                "(merchant failed CPC/leads/visible rules or no IT row on fixim)."
            )
    print()

    print("4. Generating offers from PLA feed ...")
    pla_alt1 = build_pla_id_alternates_for_feed(merchants1)
    pla_alt2 = build_pla_id_alternates_for_feed(merchants2)
    rows1_new = generate_offers_rank_weighted_interleave(
        FEED1_API_KEY, chosen1, pla_id_alternates=pla_alt1
    )
    rows2_new = generate_offers_rank_weighted_interleave(
        FEED2_API_KEY, chosen2, pla_id_alternates=pla_alt2
    )

    if merge_offers_tabs and partial_geos:
        existing1 = read_offers_sheet_rows(service, SPREADSHEET_ID, offers_1)
        existing2 = read_offers_sheet_rows(service, SPREADSHEET_ID, offers_2)
        rows1 = merge_offers_replace_geos(existing1, rows1_new, set(partial_geos))
        rows2 = merge_offers_replace_geos(existing2, rows2_new, set(partial_geos))
    else:
        rows1 = rows1_new
        rows2 = rows2_new

    _log_pla_merchant_distribution("feed1 (final rows before sheet + Keitaro)", rows1)
    _log_pla_merchant_distribution("feed2 (final rows before sheet + Keitaro)", rows2)

    write_offers_sheet(service, SPREADSHEET_ID, offers_1, rows1)
    it1 = sum(1 for r in rows1 if str(r.get("Country", "")).strip().upper() == "IT")
    print(f"   {offers_1}: {len(rows1)} offers ({it1} for IT)")
    write_offers_sheet(service, SPREADSHEET_ID, offers_2, rows2)
    it2 = sum(1 for r in rows2 if str(r.get("Country", "")).strip().upper() == "IT")
    print(f"   {offers_2}: {len(rows2)} offers ({it2} for IT)")
    print()

    if run_monthly_log_today:
        print("4b. Monthly log: upserting today's merchants (no monetization checks) ...")
        try:
            upsert_run_merchants_into_monthly_log(
                service,
                SPREADSHEET_ID,
                date_str,
                1,
                api_key=FEED1_API_KEY,
                check_monetization=False,
            )
            upsert_run_merchants_into_monthly_log(
                service,
                SPREADSHEET_ID,
                date_str,
                2,
                api_key=FEED2_API_KEY,
                check_monetization=False,
            )
            print("   Done.")
        except Exception as e:
            print(f"   Monthly log upsert (today) skipped: {e}")
        print()

    print("5. Creating combined offers sheet ...")
    create_combined_offers_sheet(service, date_str)
    print()

    if skip_keitaro:
        print("Skipping Keitaro sync (--skip-keitaro).")
        if run_blend_steps:
            run_blend_daily_steps(
                skip_keitaro=True,
                skip_blend=skip_blend,
                skip_blend_sync=skip_blend_sync,
                skip_blend_prune=skip_blend_prune,
            )
        if run_daily_conversion_postbacks:
            run_optional_daily_conversion_postbacks(postback_report_date)
        return

    if not rows1 and not rows2:
        print("6. Syncing to Keitaro ...")
        print("   No offers generated for either feed today; skipping Keitaro sync.")
        print()
        if run_blend_steps:
            run_blend_daily_steps(
                skip_keitaro=False,
                skip_blend=skip_blend,
                skip_blend_sync=skip_blend_sync,
                skip_blend_prune=skip_blend_prune,
            )
        print("Done. No offers to sync.")
        if run_daily_conversion_postbacks:
            run_optional_daily_conversion_postbacks(postback_report_date)
        return

    print(
        f"6. Syncing feed1 to Keitaro (up to {KEITARO_SYNC_MAX_OFFERS_PER_GEO} offers per geo from sheet) ..."
    )
    feed1_extra_args = ["--traffic-feed1-only"] if feed1_traffic_only else None

    if not rows1:
        print("   No feed1 offers generated; skipping feed1 sync.")
    elif not run_update_offers_from_sheet(offers_1, 1, extra_args=feed1_extra_args):
        print("   Feed1 sync failed.")
        sys.exit(1)

    if feed1_traffic_only:
        print("   Feed2 traffic disabled (skipping feed2 sync).")
        print()
        if run_blend_steps:
            run_blend_daily_steps(
                skip_keitaro=False,
                skip_blend=skip_blend,
                skip_blend_sync=skip_blend_sync,
                skip_blend_prune=skip_blend_prune,
            )
        print("Done. Feed1 traffic only synced to Keitaro.")
        if run_daily_conversion_postbacks:
            run_optional_daily_conversion_postbacks(postback_report_date)
        return

    print("   Syncing feed2 to Keitaro ...")
    if not rows2:
        print("   No feed2 offers generated; skipping feed2 sync.")
    elif not run_update_offers_from_sheet(offers_2, 2):
        print("   Feed2 sync failed.")
        sys.exit(1)
    print()
    if run_blend_steps:
        run_blend_daily_steps(
            skip_keitaro=False,
            skip_blend=skip_blend,
            skip_blend_sync=skip_blend_sync,
            skip_blend_prune=skip_blend_prune,
        )
    print("Done. Both feeds synced to Keitaro.")
    if run_daily_conversion_postbacks:
        run_optional_daily_conversion_postbacks(postback_report_date)


def main() -> None:
    argv = sys.argv[1:]
    pa = _parse_daily_workflow_argv(argv)
    date_str = pa["date_str"]
    skip_keitaro = pa["skip_keitaro"]
    skip_blend = pa["skip_blend"]
    skip_blend_sync = pa["skip_blend_sync"]
    feed1_traffic_only = pa["feed1_traffic_only"]
    merchants_per_geo = int(pa["merchants_per_geo"])
    static_only = pa["static_only"]
    partial_geos: frozenset[str] = pa["partial_geos"]
    merchant_overrides: Dict[int, Dict[str, List[str]]] = pa["merchant_overrides"]
    merchant_auto_overrides: Dict[int, Dict[str, int]] = pa["merchant_auto_overrides"]
    merchant_skip_replaces: Dict[int, List[Tuple[str, str, str]]] = pa["merchant_skip_replaces"]
    run_daily_conversion_postbacks = pa["run_daily_conversion_postbacks"]
    offers_and_keitaro_only = pa["offers_and_keitaro_only"]
    postback_report_date = pa["postback_report_date"]
    skip_blend_prune = bool(pa.get("skip_blend_prune"))

    merge_offers_tabs = bool(partial_geos)

    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    if today.day == 1:
        start_of_period = yesterday.replace(day=1)
        end_of_period = yesterday
    else:
        start_of_period = today.replace(day=1)
        end_of_period = yesterday
    start_str = start_of_period.strftime("%Y-%m-%d")
    end_str = end_of_period.strftime("%Y-%m-%d")
    yesterday_str = (datetime.strptime(date_str, "%Y-%m-%d").date() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"Daily workflow for {date_str}")
    if merchants_per_geo == 1:
        print("Merchants per geo: 1 (--single-merchant-per-geo).")
    if offers_and_keitaro_only:
        print("Mode: --offers-and-keitaro-only (skips monthly log, Blend potential, tab delete, feed→fixim download; re-colors fixim from reports)")
    if partial_geos:
        print(f"Partial geo scope (merge into existing offers tabs): {', '.join(sorted(partial_geos))}")
    if merchant_overrides.get(1) or merchant_overrides.get(2):
        print(f"Merchant overrides: feed1={merchant_overrides.get(1)!r} feed2={merchant_overrides.get(2)!r}")
    if merchant_auto_overrides.get(1) or merchant_auto_overrides.get(2):
        print(
            f"Platform auto-overrides: feed1={merchant_auto_overrides.get(1)!r} "
            f"feed2={merchant_auto_overrides.get(2)!r}"
        )
    if merchant_skip_replaces.get(1) or merchant_skip_replaces.get(2):
        print(
            f"Merchant skip→replace (PLA): feed1={merchant_skip_replaces.get(1)!r} "
            f"feed2={merchant_skip_replaces.get(2)!r}"
        )
    print(f"Reports: {start_str} to {end_str} (month to yesterday)")
    _lk = logging.getLogger("workflows.kelkoo_daily")
    _lk.setLevel(logging.INFO)
    if not _lk.handlers:
        _h = logging.StreamHandler(sys.stdout)
        _h.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        _lk.addHandler(_h)
        _lk.propagate = False
    print()

    service = get_sheets_service()

    fixim_1 = f"{date_str}_fixim_1"
    fixim_2 = f"{date_str}_fixim_2"

    if offers_and_keitaro_only:
        print(
            "Skipping 0a/0b/0 (monthly log monetization, Blend potential, delete dated tabs); "
            "fixim tabs are not re-downloaded — only re-colored from reports."
        )
        print()
        print("1. Downloading merchants feeds (for PLA id alternates; fixim sheets unchanged) ...")
        merchants1 = download_merchants_feed(FEED1_API_KEY, static_only=static_only)
        print(f"   feed1 merchant objects: {len(merchants1)}")
        merchants2 = download_merchants_feed(
            FEED2_API_KEY,
            list(FEED2_MERCHANTS_GEOS) if FEED2_MERCHANTS_GEOS else None,
            static_only=static_only,
        )
        print(f"   feed2 merchant objects: {len(merchants2)}")
        print()

        print("2. Fetching Kelkoo reports and coloring fixim sheets ...")
        perf1: dict = {}
        perf2: dict = {}
        try:
            perf1 = fetch_reports(FEED1_API_KEY, start_str, end_str)
            apply_fixim_colors(service, SPREADSHEET_ID, fixim_1, perf1)
            print(f"   {fixim_1}: colored by performance ({len(perf1)} merchants in report)")
        except Exception as e:
            print(f"   Feed1 reports/color: {e}")
        try:
            perf2 = fetch_reports(FEED2_API_KEY, start_str, end_str)
            apply_fixim_colors(service, SPREADSHEET_ID, fixim_2, perf2)
            print(f"   {fixim_2}: colored by performance ({len(perf2)} merchants in report)")
        except Exception as e:
            print(f"   Feed2 reports/color: {e}")
        print()

        run_pla_offers_keitaro_blend_tail(
            service,
            date_str=date_str,
            merchants1=merchants1,
            merchants2=merchants2,
            perf1=perf1,
            perf2=perf2,
            fixim_1=fixim_1,
            fixim_2=fixim_2,
            merchants_per_geo=merchants_per_geo,
            partial_geos=partial_geos,
            merchant_overrides=merchant_overrides,
            merchant_auto_overrides=merchant_auto_overrides,
            merchant_skip_replaces=merchant_skip_replaces,
            merge_offers_tabs=merge_offers_tabs,
            run_monthly_log_today=False,
            skip_keitaro=skip_keitaro,
            feed1_traffic_only=feed1_traffic_only,
            skip_blend=skip_blend,
            skip_blend_sync=skip_blend_sync,
            run_blend_steps=False,
            run_daily_conversion_postbacks=run_daily_conversion_postbacks,
            postback_report_date=postback_report_date,
            skip_blend_prune=skip_blend_prune,
        )
        _run_post_pla_automation_tail(service, pa)
        return

    print("0a. Monthly log: yesterday's merchants + Kelkoo monetization (column E) ...")
    try:
        n1 = upsert_yesterday_merchants_into_monthly_log(
            service, SPREADSHEET_ID, yesterday_str, 1, FEED1_API_KEY
        )
        n2 = upsert_yesterday_merchants_into_monthly_log(
            service, SPREADSHEET_ID, yesterday_str, 2, FEED2_API_KEY
        )
        print(f"   Kelkoo link checks: feed1={n1}, feed2={n2} (yesterday={yesterday_str})")
        if n1 == 0 and n2 == 0:
            print("   Note: no rows imported from yesterday offers sheets (or sheets were missing).")
    except Exception as e:
        print(f"   Monthly log monetization skipped: {e}")
    print()

    _pot_feeds = _blend_potential_feeds_for_run()
    _pot_tabs = (
        ", ".join("potential" + f[0].upper() + f[1:] for f in _pot_feeds)
        if _pot_feeds
        else "(none — no BLEND_POTENTIAL_FEEDS with API keys)"
    )
    print(f"0b. Updating Blend potential sheets ({_pot_tabs}) ...")
    try:
        if not _pot_feeds:
            print("   Skipped (no feeds to refresh).")
        elif run_blend_potential_sheets(start_str, end_str, feeds=_pot_feeds):
            print("   Done.")
        else:
            print("   Warning: one or more potential sheets failed to update.")
    except Exception as e:
        print(f"   Warning: could not update potential sheets: {e}")
    print()

    print("0. Removing previous day's daily sheets (if any) ...")
    removed = delete_dated_daily_sheets(service, SPREADSHEET_ID, yesterday_str)
    if removed:
        for t in removed:
            print(f"   Deleted: {t}")
    else:
        print(f"   Nothing to remove for {yesterday_str}")
    print()

    print("1. Downloading merchants feed (feed1) ...")
    merchants1 = download_merchants_feed(FEED1_API_KEY, static_only=static_only)
    write_fixim_sheet(service, SPREADSHEET_ID, fixim_1, merchants1)
    print(f"   {fixim_1}: {len(merchants1)} merchants")
    print("   Downloading merchants feed (feed2) ...")
    merchants2 = download_merchants_feed(
        FEED2_API_KEY,
        list(FEED2_MERCHANTS_GEOS) if FEED2_MERCHANTS_GEOS else None,
        static_only=static_only,
    )
    write_fixim_sheet(service, SPREADSHEET_ID, fixim_2, merchants2)
    print(f"   {fixim_2}: {len(merchants2)} merchants")
    print()

    print("2. Fetching Kelkoo reports and coloring fixim sheets ...")
    perf1: dict = {}
    perf2: dict = {}
    try:
        perf1 = fetch_reports(FEED1_API_KEY, start_str, end_str)
        apply_fixim_colors(service, SPREADSHEET_ID, fixim_1, perf1)
        print(f"   {fixim_1}: colored by performance ({len(perf1)} merchants in report)")
    except Exception as e:
        print(f"   Feed1 reports/color: {e}")
    try:
        perf2 = fetch_reports(FEED2_API_KEY, start_str, end_str)
        apply_fixim_colors(service, SPREADSHEET_ID, fixim_2, perf2)
        print(f"   {fixim_2}: colored by performance ({len(perf2)} merchants in report)")
    except Exception as e:
        print(f"   Feed2 reports/color: {e}")
    print()

    run_pla_offers_keitaro_blend_tail(
        service,
        date_str=date_str,
        merchants1=merchants1,
        merchants2=merchants2,
        perf1=perf1,
        perf2=perf2,
        fixim_1=fixim_1,
        fixim_2=fixim_2,
        merchants_per_geo=merchants_per_geo,
        partial_geos=partial_geos,
        merchant_overrides=merchant_overrides,
        merchant_auto_overrides=merchant_auto_overrides,
        merchant_skip_replaces=merchant_skip_replaces,
        merge_offers_tabs=merge_offers_tabs,
        run_monthly_log_today=True,
        skip_keitaro=skip_keitaro,
        feed1_traffic_only=feed1_traffic_only,
        skip_blend=skip_blend,
        skip_blend_sync=skip_blend_sync,
        run_blend_steps=True,
        run_daily_conversion_postbacks=run_daily_conversion_postbacks,
        postback_report_date=postback_report_date,
        skip_blend_prune=skip_blend_prune,
    )
    _run_post_pla_automation_tail(service, pa)


if __name__ == "__main__":
    main()
