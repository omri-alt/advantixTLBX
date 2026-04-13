#!/usr/bin/env python3
"""
Full daily workflow: merchants feed → reports & color segmentation →
pick merchants → generate PLA offers → combined sheet → sync Keitaro.

0a. Before deleting tabs: merge yesterday's offers into {month}_log_1 / _log_2 and set column E (Kelkoo monetization).
0b. Refresh Blend potentialKelkoo* sheets (same report window as this run).
0. Delete the previous calendar day's YYYY-MM-DD_fixim_* / _offers_* / _offers_today tabs if present.
1. Download merchants feed for feed1 and feed2 → write YYYY-MM-DD_fixim_1, _fixim_2.
2. Fetch Kelkoo reports (month-to-date or previous month on 1st); color fixim sheets.
3. Choose top-N merchants per geo (CPC + leads rules in workflows.kelkoo_daily).
4. Generate PLA offers → write YYYY-MM-DD_offers_1, _offers_2.
5. Create YYYY-MM-DD_offers_today (combined view with Feed column).
6. Sync both feeds to Keitaro (unless --skip-keitaro or no offers).
7. Blend: refresh potential → populate Blend → blend_sync_from_sheet (unless --skip-blend).

Optional: ``--run-daily-conversion-postbacks`` runs ``run_daily_conversion_postbacks.py`` after a successful
workflow (Kelkoo per-geo + Adexa + Yadore → Keitaro GET postbacks). Use ``--postback-report-date YYYY-MM-DD`` to
override the stats date (default: yesterday UTC).

Requires .env: KEITARO_BASE_URL, KEITARO_API_KEY, FEED1_API_KEY, FEED2_API_KEY; credentials.json.
Optional: ``BLEND_POTENTIAL_FEEDS`` (comma list, default ``kelkoo1,kelkoo2``) for step 0b/7a; feeds without an API key are skipped.

  python run_daily_workflow.py
  python run_daily_workflow.py --date 2026-04-03
  python run_daily_workflow.py --skip-keitaro
  python run_daily_workflow.py --feed1-traffic-only
  python run_daily_workflow.py --multi-merchant-fallback
  python run_daily_workflow.py --skip-blend
  python run_daily_workflow.py --skip-blend-sync
  python run_daily_workflow.py --geo uk
  python run_daily_workflow.py --include-flex
  python run_daily_workflow.py --run-daily-conversion-postbacks
  python run_daily_workflow.py --run-daily-conversion-postbacks --postback-report-date 2026-04-08
"""
from __future__ import annotations

import logging
import re
import subprocess
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
    apply_fixim_colors,
    build_pla_id_alternates_for_feed,
    download_merchants_feed,
    fetch_reports,
    generate_offers_with_fallback,
    get_top_merchants_per_geo,
    write_fixim_sheet,
    write_offers_sheet,
)
from workflows.monthly_log_monetization import (
    upsert_run_merchants_into_monthly_log,
    upsert_yesterday_merchants_into_monthly_log,
)

SPREADSHEET_ID = KELKOO_SHEETS_SPREADSHEET_ID

_DAILY_SHEET_SUFFIXES = ("_fixim_1", "_fixim_2", "_offers_1", "_offers_2", "_offers_today")

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


def run_blend_daily_steps(*, skip_keitaro: bool, skip_blend: bool, skip_blend_sync: bool) -> None:
    if skip_blend:
        return
    print("7. Blend workflow (spreadsheet + Keitaro campaign) ...")
    for feed in _blend_potential_feeds_for_run():
        print(f"   7a. Populate Blend from potential ({feed}, max {BLEND_DAILY_MAX_NEW_ROWS} new rows) ...")
        if not run_populate_blend_from_potential(feed=feed, max_add=BLEND_DAILY_MAX_NEW_ROWS):
            print(f"   Warning: populate_blend_from_potential ({feed}) exited non-zero.")
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
    if extra_args:
        cmd.extend(extra_args)
    return subprocess.run(cmd).returncode == 0


def main() -> None:
    argv = sys.argv[1:]
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    skip_keitaro = "--skip-keitaro" in argv
    skip_blend = "--skip-blend" in argv
    skip_blend_sync = "--skip-blend-sync" in argv
    feed1_traffic_only = "--feed1-traffic-only" in argv
    blend_multi_merchant_fallback = "--multi-merchant-fallback" in argv
    include_flex_merchants = "--include-flex" in argv
    static_only = not include_flex_merchants
    only_geo: str | None = None
    run_daily_conversion_postbacks = "--run-daily-conversion-postbacks" in argv
    postback_report_date = (datetime.now(timezone.utc).date() - timedelta(days=1)).strftime("%Y-%m-%d")

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
            g = argv[i + 1].strip().lower()[:2]
            if len(g) == 2:
                only_geo = g
            i += 2
            continue
        i += 1

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
    if only_geo:
        print(f"Country override: {only_geo} only")
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

    fixim_1 = f"{date_str}_fixim_1"
    fixim_2 = f"{date_str}_fixim_2"
    offers_1 = f"{date_str}_offers_1"
    offers_2 = f"{date_str}_offers_2"

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

    TOP_MERCHANTS_PER_GEO = 3 if blend_multi_merchant_fallback else 1
    print(f"3. Choosing merchants (top-{TOP_MERCHANTS_PER_GEO} per geo; report rules + CPC floor) ...")
    chosen1 = get_top_merchants_per_geo(
        service, SPREADSHEET_ID, fixim_1, perf1, top_n=TOP_MERCHANTS_PER_GEO
    )
    chosen2 = get_top_merchants_per_geo(
        service, SPREADSHEET_ID, fixim_2, perf2, top_n=TOP_MERCHANTS_PER_GEO
    )
    if only_geo:
        # Country rerun mode: keep full fixim + colors for all geos, but only refresh
        # offers/sync for the selected geo.
        chosen1 = {k: v for k, v in chosen1.items() if k == only_geo}
        chosen2 = {k: v for k, v in chosen2.items() if k == only_geo}
    print(f"   Feed1: {len(chosen1)} geos")
    print(f"   Feed2: {len(chosen2)} geos")
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
    rows1 = generate_offers_with_fallback(
        FEED1_API_KEY, chosen1, pla_id_alternates=pla_alt1
    )
    write_offers_sheet(service, SPREADSHEET_ID, offers_1, rows1)
    it1 = sum(1 for r in rows1 if str(r.get("Country", "")).strip().upper() == "IT")
    print(f"   {offers_1}: {len(rows1)} offers ({it1} for IT)")
    rows2 = generate_offers_with_fallback(
        FEED2_API_KEY, chosen2, pla_id_alternates=pla_alt2
    )
    write_offers_sheet(service, SPREADSHEET_ID, offers_2, rows2)
    it2 = sum(1 for r in rows2 if str(r.get("Country", "")).strip().upper() == "IT")
    print(f"   {offers_2}: {len(rows2)} offers ({it2} for IT)")
    print()

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
        run_blend_daily_steps(skip_keitaro=True, skip_blend=skip_blend, skip_blend_sync=skip_blend_sync)
        if run_daily_conversion_postbacks:
            run_optional_daily_conversion_postbacks(postback_report_date)
        return

    if not rows1 and not rows2:
        print("6. Syncing to Keitaro ...")
        print("   No offers generated for either feed today; skipping Keitaro sync.")
        print()
        run_blend_daily_steps(skip_keitaro=False, skip_blend=skip_blend, skip_blend_sync=skip_blend_sync)
        print("Done. No offers to sync.")
        if run_daily_conversion_postbacks:
            run_optional_daily_conversion_postbacks(postback_report_date)
        return

    print("6. Syncing feed1 to Keitaro ...")
    feed1_extra_args = ["--traffic-feed1-only"] if feed1_traffic_only else None

    if not rows1:
        print("   No feed1 offers generated; skipping feed1 sync.")
    elif not run_update_offers_from_sheet(offers_1, 1, extra_args=feed1_extra_args):
        print("   Feed1 sync failed.")
        sys.exit(1)

    if feed1_traffic_only:
        print("   Feed2 traffic disabled (skipping feed2 sync).")
        print()
        run_blend_daily_steps(skip_keitaro=False, skip_blend=skip_blend, skip_blend_sync=skip_blend_sync)
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
    run_blend_daily_steps(skip_keitaro=False, skip_blend=skip_blend, skip_blend_sync=skip_blend_sync)
    print("Done. Both feeds synced to Keitaro.")
    if run_daily_conversion_postbacks:
        run_optional_daily_conversion_postbacks(postback_report_date)


if __name__ == "__main__":
    main()
