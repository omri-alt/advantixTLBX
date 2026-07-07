"""Stage implementations for daily workflow v2 (each runs in its own process)."""
from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List

# Repo root on path when launched as subprocess
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from workflows.daily_v2.context import RunContext


def _setup_kelkoo_logging() -> None:
    _lk = logging.getLogger("workflows.kelkoo_daily")
    _lk.setLevel(logging.INFO)
    if not _lk.handlers:
        _h = logging.StreamHandler(sys.stdout)
        _h.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        _lk.addHandler(_h)
        _lk.propagate = False


def _import_daily():
    import run_daily_workflow as rdw

    return rdw


def stage_monthly_log(ctx: RunContext) -> int:
    rdw = _import_daily()
    from workflows.monthly_log_monetization import upsert_yesterday_merchants_into_monthly_log

    print(f"0a. Monthly log: yesterday's merchants + Kelkoo monetization (column E) ...")
    service = rdw.get_sheets_service()
    try:
        n1 = upsert_yesterday_merchants_into_monthly_log(
            service, rdw.SPREADSHEET_ID, ctx.yesterday_str, 1, rdw.FEED1_API_KEY
        )
        n2 = upsert_yesterday_merchants_into_monthly_log(
            service, rdw.SPREADSHEET_ID, ctx.yesterday_str, 2, rdw.FEED2_API_KEY
        )
        n5 = 0
        if rdw.nipuhim_feed5_enabled():
            n5 = upsert_yesterday_merchants_into_monthly_log(
                service, rdw.SPREADSHEET_ID, ctx.yesterday_str, 5, rdw.FEED5_API_KEY
            )
        msg = f"   Kelkoo link checks: feed1={n1}, feed2={n2}"
        if rdw.nipuhim_feed5_enabled():
            msg += f", feed5={n5}"
        print(msg + f" (yesterday={ctx.yesterday_str})")
        if n1 == 0 and n2 == 0:
            print("   Note: no rows imported from yesterday offers sheets (or sheets were missing).")
    except Exception as e:
        print(f"   Monthly log monetization skipped: {e}")
        return 1
    return 0


def stage_blend_potential(ctx: RunContext) -> int:
    rdw = _import_daily()
    feeds = rdw._blend_potential_feeds_for_run()
    tabs = (
        ", ".join("potential" + f[0].upper() + f[1:] for f in feeds)
        if feeds
        else "(none — no BLEND_POTENTIAL_FEEDS with API keys)"
    )
    print(f"0b. Updating Blend potential sheets ({tabs}) ...")
    try:
        if not feeds:
            print("   Skipped (no feeds to refresh).")
            return 0
        if rdw.run_blend_potential_sheets(ctx.start_str, ctx.end_str, feeds=feeds):
            print("   Done.")
            return 0
        print("   Warning: one or more potential sheets failed to update.")
        return 1
    except Exception as e:
        print(f"   Warning: could not update potential sheets: {e}")
        return 1


def stage_delete_prev_tabs(ctx: RunContext) -> int:
    rdw = _import_daily()
    print("0. Removing previous day's daily sheets (if any) ...")
    service = rdw.get_sheets_service()
    removed = rdw.delete_dated_daily_sheets(service, rdw.SPREADSHEET_ID, ctx.yesterday_str)
    if removed:
        for t in removed:
            print(f"   Deleted: {t}")
    else:
        print(f"   Nothing to remove for {ctx.yesterday_str}")
    return 0


def stage_download_fixim(ctx: RunContext) -> int:
    rdw = _import_daily()
    from workflows.kelkoo_daily import download_merchants_feed, write_fixim_sheet

    static_only = bool(ctx.pa.get("static_only", True))
    service = rdw.get_sheets_service()
    print("1. Downloading merchants feed (feed1) ...")
    merchants1 = download_merchants_feed(rdw.FEED1_API_KEY, static_only=static_only)
    write_fixim_sheet(service, rdw.SPREADSHEET_ID, ctx.fixim_1, merchants1)
    print(f"   {ctx.fixim_1}: {len(merchants1)} merchants")
    print("   Downloading merchants feed (feed2) ...")
    merchants2 = download_merchants_feed(
        rdw.FEED2_API_KEY,
        list(rdw.FEED2_MERCHANTS_GEOS) if rdw.FEED2_MERCHANTS_GEOS else None,
        static_only=static_only,
    )
    write_fixim_sheet(service, rdw.SPREADSHEET_ID, ctx.fixim_2, merchants2)
    print(f"   {ctx.fixim_2}: {len(merchants2)} merchants")
    if rdw.nipuhim_feed5_enabled():
        fixim_5 = f"{ctx.date_str}_fixim_5"
        print("   Downloading merchants feed (feed5) ...")
        merchants5 = download_merchants_feed(
            rdw.FEED5_API_KEY,
            list(rdw.FEED5_MERCHANTS_GEOS) if rdw.FEED5_MERCHANTS_GEOS else None,
            static_only=static_only,
        )
        write_fixim_sheet(service, rdw.SPREADSHEET_ID, fixim_5, merchants5)
        print(f"   {fixim_5}: {len(merchants5)} merchants")
    return 0


def stage_merchants_pla_alt(ctx: RunContext) -> int:
    rdw = _import_daily()
    from workflows.kelkoo_daily import download_merchants_feed

    static_only = bool(ctx.pa.get("static_only", True))
    print("1. Downloading merchants feeds (for PLA id alternates; fixim sheets unchanged) ...")
    merchants1 = download_merchants_feed(rdw.FEED1_API_KEY, static_only=static_only)
    print(f"   feed1 merchant objects: {len(merchants1)}")
    merchants2 = download_merchants_feed(
        rdw.FEED2_API_KEY,
        list(rdw.FEED2_MERCHANTS_GEOS) if rdw.FEED2_MERCHANTS_GEOS else None,
        static_only=static_only,
    )
    print(f"   feed2 merchant objects: {len(merchants2)}")
    if rdw.nipuhim_feed5_enabled():
        merchants5 = download_merchants_feed(
            rdw.FEED5_API_KEY,
            list(rdw.FEED5_MERCHANTS_GEOS) if rdw.FEED5_MERCHANTS_GEOS else None,
            static_only=static_only,
        )
        print(f"   feed5 merchant objects: {len(merchants5)}")
        ctx.write_json_artifact("merchants5_count.json", len(merchants5))
    ctx.write_json_artifact("merchants1_count.json", len(merchants1))
    ctx.write_json_artifact("merchants2_count.json", len(merchants2))
    # PLA stage re-downloads; counts are enough to verify this stage ran.
    return 0


def stage_reports_color(ctx: RunContext) -> int:
    rdw = _import_daily()
    from workflows.kelkoo_daily import apply_fixim_colors, fetch_reports

    _setup_kelkoo_logging()
    service = rdw.get_sheets_service()
    print("2. Fetching Kelkoo reports and coloring fixim sheets ...")
    perf1: dict = {}
    perf2: dict = {}
    rc = 0
    try:
        perf1 = fetch_reports(rdw.FEED1_API_KEY, ctx.start_str, ctx.end_str)
        apply_fixim_colors(service, rdw.SPREADSHEET_ID, ctx.fixim_1, perf1)
        print(f"   {ctx.fixim_1}: colored by performance ({len(perf1)} merchants in report)")
    except Exception as e:
        print(f"   Feed1 reports/color: {e}")
        rc = 1
    try:
        perf2 = fetch_reports(rdw.FEED2_API_KEY, ctx.start_str, ctx.end_str)
        apply_fixim_colors(service, rdw.SPREADSHEET_ID, ctx.fixim_2, perf2)
        print(f"   {ctx.fixim_2}: colored by performance ({len(perf2)} merchants in report)")
    except Exception as e:
        print(f"   Feed2 reports/color: {e}")
        rc = 1
    perf5: dict = {}
    if rdw.nipuhim_feed5_enabled():
        fixim_5 = f"{ctx.date_str}_fixim_5"
        try:
            perf5 = fetch_reports(rdw.FEED5_API_KEY, ctx.start_str, ctx.end_str)
            apply_fixim_colors(service, rdw.SPREADSHEET_ID, fixim_5, perf5)
            print(f"   {fixim_5}: colored by performance ({len(perf5)} merchants in report)")
        except Exception as e:
            print(f"   Feed5 reports/color: {e}")
            rc = 1
    ctx.write_json_artifact("perf1.json", perf1)
    ctx.write_json_artifact("perf2.json", perf2)
    ctx.write_json_artifact("perf5.json", perf5)
    return rc


def _merchant_selection(ctx: RunContext) -> tuple[Dict[str, List[str]], Dict[str, List[str]], int]:
    rdw = _import_daily()
    from workflows.kelkoo_daily import get_top_merchants_per_geo

    pa = ctx.pa
    merchants_per_geo = int(pa.get("merchants_per_geo") or 3)
    merchant_overrides = pa.get("merchant_overrides") or {}
    merchant_auto_overrides = pa.get("merchant_auto_overrides") or {}
    merchant_skip_replaces = pa.get("merchant_skip_replaces") or {}
    partial_geos = ctx.partial_geos()

    perf1 = ctx.read_json_artifact("perf1.json")
    perf2 = ctx.read_json_artifact("perf2.json")
    perf5 = ctx.read_json_artifact("perf5.json") if rdw.nipuhim_feed5_enabled() else {}
    fixim_5 = f"{ctx.date_str}_fixim_5"
    service = rdw.get_sheets_service()

    base_top_n = max(1, merchants_per_geo)
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
    ranked1 = get_top_merchants_per_geo(
        service, rdw.SPREADSHEET_ID, ctx.fixim_1, perf1, top_n=rank_depth
    )
    ranked2 = get_top_merchants_per_geo(
        service, rdw.SPREADSHEET_ID, ctx.fixim_2, perf2, top_n=rank_depth
    )
    ranked5: dict = {}
    if rdw.nipuhim_feed5_enabled():
        ranked5 = get_top_merchants_per_geo(
            service, rdw.SPREADSHEET_ID, fixim_5, perf5, top_n=rank_depth
        )
    chosen1 = {geo: mids[:base_top_n] for geo, mids in ranked1.items()}
    chosen2 = {geo: mids[:base_top_n] for geo, mids in ranked2.items()}
    chosen1 = rdw._apply_platform_auto_overrides_to_chosen(
        chosen1,
        ranked_per_geo=ranked1,
        feed_num=1,
        merchant_auto_overrides=merchant_auto_overrides,
    )
    chosen2 = rdw._apply_platform_auto_overrides_to_chosen(
        chosen2,
        ranked_per_geo=ranked2,
        feed_num=2,
        merchant_auto_overrides=merchant_auto_overrides,
    )
    chosen1 = rdw._apply_merchant_overrides_to_chosen(chosen1, 1, merchant_overrides)
    chosen2 = rdw._apply_merchant_overrides_to_chosen(chosen2, 2, merchant_overrides)
    chosen1 = rdw._apply_merchant_skip_replaces(chosen1, 1, merchant_skip_replaces)
    chosen2 = rdw._apply_merchant_skip_replaces(chosen2, 2, merchant_skip_replaces)
    chosen5: dict = {}
    if rdw.nipuhim_feed5_enabled():
        chosen5 = {geo: mids[:base_top_n] for geo, mids in ranked5.items()}
        chosen5 = rdw._apply_platform_auto_overrides_to_chosen(
            chosen5,
            ranked_per_geo=ranked5,
            feed_num=5,
            merchant_auto_overrides=merchant_auto_overrides,
        )
        chosen5 = rdw._apply_merchant_overrides_to_chosen(chosen5, 5, merchant_overrides)
        chosen5 = rdw._apply_merchant_skip_replaces(chosen5, 5, merchant_skip_replaces)
    if partial_geos:
        chosen1 = {k: v for k, v in chosen1.items() if k in partial_geos}
        chosen2 = {k: v for k, v in chosen2.items() if k in partial_geos}
        if rdw.nipuhim_feed5_enabled():
            chosen5 = {k: v for k, v in chosen5.items() if k in partial_geos}

    print(f"   Feed1: {len(chosen1)} geos")
    print(f"   Feed2: {len(chosen2)} geos")
    if rdw.nipuhim_feed5_enabled():
        print(f"   Feed5: {len(chosen5)} geos")
    ctx.write_json_artifact("ranked1.json", ranked1)
    ctx.write_json_artifact("ranked2.json", ranked2)
    ctx.write_json_artifact("chosen1.json", chosen1)
    ctx.write_json_artifact("chosen2.json", chosen2)
    if rdw.nipuhim_feed5_enabled():
        ctx.write_json_artifact("ranked5.json", ranked5)
        ctx.write_json_artifact("chosen5.json", chosen5)
    return chosen1, chosen2, 0


def stage_merchant_pick(ctx: RunContext) -> int:
    _, _, rc = _merchant_selection(ctx)
    return rc


def stage_pla_offers(ctx: RunContext) -> int:
    rdw = _import_daily()
    from workflows.kelkoo_daily import (
        build_pla_id_alternates_for_feed,
        download_merchants_feed,
        generate_offers_rank_weighted_interleave,
        merge_offers_replace_geos,
        read_offers_sheet_rows,
        write_offers_sheet,
    )
    from workflows.monthly_log_monetization import upsert_run_merchants_into_monthly_log

    pa = ctx.pa
    static_only = bool(pa.get("static_only", True))
    partial_geos = ctx.partial_geos()
    merge_offers_tabs = bool(partial_geos)
    offers_and_keitaro_only = bool(pa.get("offers_and_keitaro_only"))

    chosen1 = ctx.read_json_artifact("chosen1.json")
    chosen2 = ctx.read_json_artifact("chosen2.json")
    chosen5 = (
        ctx.read_json_artifact("chosen5.json") if rdw.nipuhim_feed5_enabled() else {}
    )
    offers_1 = f"{ctx.date_str}_offers_1"
    offers_2 = f"{ctx.date_str}_offers_2"
    offers_5 = f"{ctx.date_str}_offers_5"

    print("4. Generating offers from PLA feed ...")
    merchants1 = download_merchants_feed(rdw.FEED1_API_KEY, static_only=static_only)
    merchants2 = download_merchants_feed(
        rdw.FEED2_API_KEY,
        list(rdw.FEED2_MERCHANTS_GEOS) if rdw.FEED2_MERCHANTS_GEOS else None,
        static_only=static_only,
    )
    pla_alt1 = build_pla_id_alternates_for_feed(merchants1)
    pla_alt2 = build_pla_id_alternates_for_feed(merchants2)
    rows1_new = generate_offers_rank_weighted_interleave(
        rdw.FEED1_API_KEY, chosen1, pla_id_alternates=pla_alt1
    )
    rows2_new = generate_offers_rank_weighted_interleave(
        rdw.FEED2_API_KEY, chosen2, pla_id_alternates=pla_alt2
    )
    rows5: list = []
    rows5_new: list = []
    if rdw.nipuhim_feed5_enabled():
        merchants5 = download_merchants_feed(
            rdw.FEED5_API_KEY,
            list(rdw.FEED5_MERCHANTS_GEOS) if rdw.FEED5_MERCHANTS_GEOS else None,
            static_only=static_only,
        )
        pla_alt5 = build_pla_id_alternates_for_feed(merchants5)
        rows5_new = generate_offers_rank_weighted_interleave(
            rdw.FEED5_API_KEY, chosen5, pla_id_alternates=pla_alt5
        )

    service = rdw.get_sheets_service()
    if merge_offers_tabs and partial_geos:
        existing1 = read_offers_sheet_rows(service, rdw.SPREADSHEET_ID, offers_1)
        existing2 = read_offers_sheet_rows(service, rdw.SPREADSHEET_ID, offers_2)
        rows1 = merge_offers_replace_geos(existing1, rows1_new, set(partial_geos))
        rows2 = merge_offers_replace_geos(existing2, rows2_new, set(partial_geos))
        if rdw.nipuhim_feed5_enabled():
            existing5 = read_offers_sheet_rows(service, rdw.SPREADSHEET_ID, offers_5)
            rows5 = merge_offers_replace_geos(existing5, rows5_new, set(partial_geos))
        else:
            rows5 = rows5_new
    else:
        rows1 = rows1_new
        rows2 = rows2_new
        rows5 = rows5_new

    rdw._log_pla_merchant_distribution("feed1 (final rows before sheet + Keitaro)", rows1)
    rdw._log_pla_merchant_distribution("feed2 (final rows before sheet + Keitaro)", rows2)
    if rdw.nipuhim_feed5_enabled():
        rdw._log_pla_merchant_distribution("feed5 (final rows before sheet + Keitaro)", rows5)

    write_offers_sheet(service, rdw.SPREADSHEET_ID, offers_1, rows1)
    it1 = sum(1 for r in rows1 if str(r.get("Country", "")).strip().upper() == "IT")
    print(f"   {offers_1}: {len(rows1)} offers ({it1} for IT)")
    write_offers_sheet(service, rdw.SPREADSHEET_ID, offers_2, rows2)
    it2 = sum(1 for r in rows2 if str(r.get("Country", "")).strip().upper() == "IT")
    print(f"   {offers_2}: {len(rows2)} offers ({it2} for IT)")
    if rdw.nipuhim_feed5_enabled():
        write_offers_sheet(service, rdw.SPREADSHEET_ID, offers_5, rows5)
        it5 = sum(1 for r in rows5 if str(r.get("Country", "")).strip().upper() == "IT")
        print(f"   {offers_5}: {len(rows5)} offers ({it5} for IT)")

    run_monthly_log_today = not offers_and_keitaro_only
    if run_monthly_log_today:
        print("4b. Monthly log: upserting today's merchants (no monetization checks) ...")
        try:
            upsert_run_merchants_into_monthly_log(
                service,
                rdw.SPREADSHEET_ID,
                ctx.date_str,
                1,
                api_key=rdw.FEED1_API_KEY,
                check_monetization=False,
            )
            upsert_run_merchants_into_monthly_log(
                service,
                rdw.SPREADSHEET_ID,
                ctx.date_str,
                2,
                api_key=rdw.FEED2_API_KEY,
                check_monetization=False,
            )
            if rdw.nipuhim_feed5_enabled():
                upsert_run_merchants_into_monthly_log(
                    service,
                    rdw.SPREADSHEET_ID,
                    ctx.date_str,
                    5,
                    api_key=rdw.FEED5_API_KEY,
                    check_monetization=False,
                )
            print("   Done.")
        except Exception as e:
            print(f"   Monthly log upsert (today) skipped: {e}")

    meta = {
        "offers_1": offers_1,
        "offers_2": offers_2,
        "rows1": len(rows1),
        "rows2": len(rows2),
    }
    if rdw.nipuhim_feed5_enabled():
        meta.update({"offers_5": offers_5, "rows5": len(rows5)})
    ctx.write_json_artifact("offers_meta.json", meta)
    return 0


def stage_combined_offers(ctx: RunContext) -> int:
    rdw = _import_daily()
    print("5. Creating combined offers sheet ...")
    service = rdw.get_sheets_service()
    if not rdw.create_combined_offers_sheet(service, ctx.date_str):
        print("   Warning: combined sheet not created (no offer rows?).")
    try:
        from integrations.nipuhim_zp_blacklist_review import snapshot_offer_slots_for_day

        snap = snapshot_offer_slots_for_day(datetime.strptime(ctx.date_str, "%Y-%m-%d").date())
        print(f"   Saved Nipuhim offer-slot snapshot ({snap.get('offer_slots', 0)} slots).")
    except Exception as e:
        print(f"   Nipuhim offer-slot snapshot skipped: {e}")
    return 0


def stage_keitaro_sync(ctx: RunContext) -> int:
    rdw = _import_daily()
    pa = ctx.pa
    feed1_traffic_only = bool(pa.get("feed1_traffic_only"))
    offers_1 = f"{ctx.date_str}_offers_1"
    offers_2 = f"{ctx.date_str}_offers_2"

    meta = ctx.read_json_artifact("offers_meta.json")
    rows1 = int(meta.get("rows1") or 0)
    rows2 = int(meta.get("rows2") or 0)
    rows5 = int(meta.get("rows5") or 0)
    use_feed5 = rdw.nipuhim_feed5_enabled()

    if not rows1 and not rows2 and (not use_feed5 or not rows5):
        print("6. Syncing to Keitaro ...")
        print("   No offers generated for any feed today; skipping Keitaro sync.")
        return 0

    print(
        f"6. Syncing feed1 to Keitaro (up to {rdw.KEITARO_SYNC_MAX_OFFERS_PER_GEO} offers per geo from sheet) ..."
    )
    feed1_extra_args = ["--traffic-feed1-only"] if feed1_traffic_only else None
    if not rows1:
        print("   No feed1 offers generated; skipping feed1 sync.")
    elif not rdw.run_update_offers_from_sheet(offers_1, 1, extra_args=feed1_extra_args):
        print("   Feed1 sync failed.")
        return 1

    if feed1_traffic_only:
        print("   Feed2 traffic disabled (skipping feed2 sync).")
        return 0

    print("   Syncing feed2 to Keitaro ...")
    if not rows2:
        print("   No feed2 offers generated; skipping feed2 sync.")
    elif not rdw.run_update_offers_from_sheet(offers_2, 2):
        print("   Feed2 sync failed.")
        return 1

    if use_feed5:
        offers_5 = f"{ctx.date_str}_offers_5"
        print("   Syncing feed5 to Keitaro ...")
        if not rows5:
            print("   No feed5 offers generated; skipping feed5 sync.")
        elif not rdw.run_update_offers_from_sheet(offers_5, 5):
            print("   Feed5 sync failed.")
            return 1
    return 0


def stage_keitaro_sync_nipuhim_v2(ctx: RunContext) -> int:
    rdw = _import_daily()
    pa = ctx.pa
    meta = ctx.read_json_artifact("offers_meta.json")
    rows1 = int(meta.get("rows1") or 0)
    rows2 = int(meta.get("rows2") or 0)
    rows5 = int(meta.get("rows5") or 0)
    if not rows1 and not rows2 and (not rdw.nipuhim_feed5_enabled() or not rows5):
        print("6b. Nipuhim v2 sync ...")
        print("   No offers generated for any feed today; skipping.")
        return 0
    if not rdw.run_nipuhim_v2_keitaro_sync(
        ctx.date_str,
        feed1_traffic_only=bool(pa.get("feed1_traffic_only")),
    ):
        return 1
    return 0


def stage_blend(ctx: RunContext) -> int:
    rdw = _import_daily()
    pa = ctx.pa
    only_geo = None
    og = pa.get("only_geos")
    if og and len(og) == 1:
        only_geo = next(iter(og))
    else:
        pg = pa.get("partial_geos")
        if pg and len(pg) == 1:
            only_geo = next(iter(pg))
    rdw.run_blend_daily_steps(
        skip_keitaro=bool(pa.get("skip_keitaro")),
        skip_blend=False,
        skip_blend_sync=bool(pa.get("skip_blend_sync")),
        skip_blend_prune=bool(pa.get("skip_blend_prune")),
        blend_v2_enabled=False,
        only_geo=only_geo,
    )
    return 0


def stage_blend_v2(ctx: RunContext) -> int:
    rdw = _import_daily()
    pa = ctx.pa
    only_geo = None
    og = pa.get("only_geos")
    if og and len(og) == 1:
        only_geo = next(iter(og))
    else:
        pg = pa.get("partial_geos")
        if pg and len(pg) == 1:
            only_geo = next(iter(pg))
    if not rdw.run_blend_v2_keitaro_sync(only_geo=only_geo):
        return 1
    return 0


def stage_hub_rewire(ctx: RunContext) -> int:
    rdw = _import_daily()
    if not rdw.run_hub_rewire_daily_step(ctx.date_str):
        return 1
    return 0


def stage_late_sales(ctx: RunContext) -> int:
    rdw = _import_daily()
    service = rdw.get_sheets_service()
    try:
        rdw._run_post_pla_automation_tail(service, ctx.pa)
    except Exception as e:
        print(f"   Late conversion step error: {e}")
        return 1
    return 0


def stage_conversion_postbacks(ctx: RunContext) -> int:
    rdw = _import_daily()
    report_date = str(ctx.pa.get("postback_report_date") or "")
    rdw.run_optional_daily_conversion_postbacks(report_date)
    return 0


STAGE_HANDLERS: Dict[str, Callable[[RunContext], int]] = {
    "monthly_log": stage_monthly_log,
    "blend_potential": stage_blend_potential,
    "delete_prev_tabs": stage_delete_prev_tabs,
    "download_fixim": stage_download_fixim,
    "merchants_pla_alt": stage_merchants_pla_alt,
    "reports_color": stage_reports_color,
    "merchant_pick": stage_merchant_pick,
    "pla_offers": stage_pla_offers,
    "combined_offers": stage_combined_offers,
    "keitaro_sync": stage_keitaro_sync,
    "keitaro_sync_nipuhim_v2": stage_keitaro_sync_nipuhim_v2,
    "blend": stage_blend,
    "blend_v2": stage_blend_v2,
    "hub_rewire": stage_hub_rewire,
    "late_sales": stage_late_sales,
    "conversion_postbacks": stage_conversion_postbacks,
}


def run_stage(ctx: RunContext, stage_id: str) -> int:
    handler = STAGE_HANDLERS.get(stage_id)
    if not handler:
        print(f"Unknown stage: {stage_id}")
        return 2
    print(f"=== Stage: {stage_id} (run {ctx.run_id}) ===")
    print(f"Daily workflow for {ctx.date_str}")
    print(f"Reports: {ctx.start_str} to {ctx.end_str} (month to yesterday)")
    print()
    return int(handler(ctx) or 0)
