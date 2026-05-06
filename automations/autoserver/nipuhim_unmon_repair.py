from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Sequence, Tuple

from automations.autoserver.base_automation import BaseAutomation

logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _credentials_path() -> Path:
    p = _REPO_ROOT / "credentials.json"
    if not p.exists():
        raise FileNotFoundError(f"credentials.json not found at {p}")
    return p


def _get_sheets_service() -> Any:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds = service_account.Credentials.from_service_account_file(str(_credentials_path()))
    return build("sheets", "v4", credentials=creds).spreadsheets()


def _month_to_yesterday_range() -> Tuple[str, str]:
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    if today.day == 1:
        start = yesterday.replace(day=1)
        end = yesterday
    else:
        start = today.replace(day=1)
        end = yesterday
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def _geo_from_country_cell(raw: Any) -> str:
    s = str(raw or "").strip().upper()
    if len(s) < 2:
        return ""
    g = s[:2].lower()
    if g == "gb":
        return "uk"
    return g


def _parse_int_env(name: str, default: int) -> int:
    try:
        return int((os.getenv(name) or str(default)).strip())
    except ValueError:
        return default


def _bool_env(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if not v:
        return default
    return v not in ("0", "false", "no", "off")


def _probe_row_unmonetized(store_link: str, geo: str, api_key: str) -> bool:
    """
    True if this product URL no longer monetizes on Kelkoo (404 from search/link).
    Invalid / missing URLs count as unmonetized. Transient HTTP errors → False (do not swap).
    """
    link = (store_link or "").strip()
    if not link.startswith("http"):
        return True
    from integrations.kelkoo_search import kelkoo_merchant_link_check

    res = kelkoo_merchant_link_check(link, geo, api_key)
    if res.get("status") == "not_found":
        return True
    if res.get("status") == "ok":
        return not bool(res.get("found"))
    return False


def _geo_merchant_order_and_links(
    rows: List[Dict[str, Any]],
) -> Tuple[Dict[str, List[str]], Dict[Tuple[str, str], str]]:
    """Per geo: merchants in sheet row order; first HTTP store link per (geo, merchant)."""
    order: Dict[str, List[str]] = {}
    seen_mid_per_geo: Dict[str, set[str]] = defaultdict(set)
    links: Dict[Tuple[str, str], str] = {}

    from workflows.kelkoo_daily import _normalize_merchant_id_from_sheet

    for r in rows:
        geo = _geo_from_country_cell(r.get("Country"))
        mid = _normalize_merchant_id_from_sheet(r.get("Merchant ID"))
        link = str(r.get("Store Link") or "").strip()
        if not geo or not mid:
            continue
        if (geo, mid) not in links and link.startswith("http"):
            links[(geo, mid)] = link
        if mid not in seen_mid_per_geo[geo]:
            seen_mid_per_geo[geo].add(mid)
            order.setdefault(geo, []).append(mid)
    return order, links


def _rebuild_merchants_for_geo(
    geo: str,
    current_ordered: Sequence[str],
    failed_mids: set[str],
    ranked: Sequence[str],
    *,
    target_n: int,
) -> List[str]:
    kept = [m for m in current_ordered if m and m not in failed_mids]
    seen = set(kept)
    out = list(kept)
    for c in ranked:
        if c in seen:
            continue
        out.append(c)
        seen.add(c)
        if len(out) >= target_n:
            break
    return out[:target_n]


class NipuhimUnmonRepair(BaseAutomation):
    """
    Scheduled check: today's Nipuhim offer sheets (feed1 + feed2). For each (geo, merchant),
    probes one product Store Link via Kelkoo ``search/link``. If the merchant is no longer
    monetized (HTTP 404), rebuilds merchant list from the current fixim tab + MTD reports,
    regenerates PLA for affected geos, merges into the dated offers tab, re-syncs Keitaro
    via ``update_offers_from_sheet.py``, and refreshes ``{date}_offers_today``.

    Optional: ``NIPUHIM_UNMON_REPAIR_BLEND_SYNC=1`` runs ``blend_sync_from_sheet.py`` afterward.

    Cadence: default every 2 hours at odd hours (1,3,5,…) to alternate with BlendSync2h.
    Override with ``NIPUHIM_UNMON_REPAIR_HOUR_MOD`` and ``NIPUHIM_UNMON_REPAIR_HOUR_PHASE``.
    """

    def on_hourly_signal(self, hour: int) -> None:
        mod = max(1, _parse_int_env("NIPUHIM_UNMON_REPAIR_HOUR_MOD", 2))
        phase = _parse_int_env("NIPUHIM_UNMON_REPAIR_HOUR_PHASE", 1) % mod
        if hour % mod != phase:
            return
        if not _bool_env("NIPUHIM_UNMON_REPAIR_ENABLED", True):
            logger.info("NipuhimUnmonRepair skipped (NIPUHIM_UNMON_REPAIR_ENABLED=0)")
            return
        logger.info("NipuhimUnmonRepair hourly tick hour=%s", hour)
        self._wrap_run("scheduler", self._execute)

    def run_manually(self) -> dict[str, Any]:
        logger.info("NipuhimUnmonRepair manual trigger")
        out = self._wrap_run("manual", self._execute)
        out["timestamp"] = datetime.now(timezone.utc).isoformat()
        return out

    def _execute(self) -> None:
        from config import (
            FEED1_API_KEY,
            FEED2_API_KEY,
            FEED2_MERCHANTS_GEOS,
            KELKOO_SHEETS_SPREADSHEET_ID,
        )
        from run_daily_workflow import create_combined_offers_sheet
        from workflows.kelkoo_daily import (
            build_pla_id_alternates_for_feed,
            download_merchants_feed,
            fetch_reports,
            generate_offers_rank_weighted_interleave,
            get_top_merchants_per_geo,
            merge_offers_replace_geos,
            read_offers_sheet_rows,
            write_offers_sheet,
        )

        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        offers_1 = f"{date_str}_offers_1"
        offers_2 = f"{date_str}_offers_2"
        fixim_1 = f"{date_str}_fixim_1"
        fixim_2 = f"{date_str}_fixim_2"
        spreadsheet_id = (KELKOO_SHEETS_SPREADSHEET_ID or "").strip()
        if not spreadsheet_id:
            raise RuntimeError("KELKOO_SHEETS_SPREADSHEET_ID is not configured")

        merchants_per_geo = max(1, _parse_int_env("NIPUHIM_UNMON_REPAIR_MERCHANTS_PER_GEO", 3))
        rank_scan = max(merchants_per_geo, _parse_int_env("NIPUHIM_UNMON_REPAIR_RANK_SCAN", 28))
        max_offers_sync = max(1, _parse_int_env("NIPUHIM_UNMON_REPAIR_MAX_OFFERS", 60))
        check_delay_s = float((os.getenv("NIPUHIM_UNMON_LINK_CHECK_DELAY_S") or "0.12").strip() or "0.12")

        service = _get_sheets_service()

        start_s, end_s = _month_to_yesterday_range()
        failures_by_feed: Dict[int, Dict[str, set[str]]] = {1: defaultdict(set), 2: defaultdict(set)}

        for feed_num, api_key, offers_sheet in (
            (1, FEED1_API_KEY, offers_1),
            (2, FEED2_API_KEY, offers_2),
        ):
            if not (api_key or "").strip():
                logger.info("NipuhimUnmonRepair: skip feed%s (missing API key)", feed_num)
                continue
            rows = read_offers_sheet_rows(service, spreadsheet_id, offers_sheet)
            if not rows:
                logger.info("NipuhimUnmonRepair: no rows in %r — skip feed%s", offers_sheet, feed_num)
                continue
            order_map, links_map = _geo_merchant_order_and_links(rows)
            for geo, mids in sorted(order_map.items()):
                for mid in mids:
                    link = links_map.get((geo, mid), "")
                    if _probe_row_unmonetized(link, geo, api_key):
                        failures_by_feed[feed_num][geo].add(mid)
                        logger.warning(
                            "NipuhimUnmonRepair: feed%s geo=%s merchant=%s unmonetized (probe link present=%s)",
                            feed_num,
                            geo,
                            mid,
                            bool(link),
                        )
                    time.sleep(max(0.0, check_delay_s))

        any_fail = any(any(d.values()) for d in failures_by_feed.values())
        if not any_fail:
            logger.info("NipuhimUnmonRepair: all probed merchants still monetized — nothing to do")
            return

        logger.info("NipuhimUnmonRepair: rebuilding PLA for failures=%r", failures_by_feed)

        for feed_num, api_key, offers_sheet, fixim_sheet in (
            (1, FEED1_API_KEY, offers_1, fixim_1),
            (2, FEED2_API_KEY, offers_2, fixim_2),
        ):
            bad_geos = failures_by_feed.get(feed_num) or {}
            touch_geos = {g for g, s in bad_geos.items() if s}
            if not touch_geos or not (api_key or "").strip():
                continue

            perf = fetch_reports(api_key, start_s, end_s)
            ranked_all = get_top_merchants_per_geo(
                service,
                spreadsheet_id,
                fixim_sheet,
                perf,
                top_n=rank_scan,
            )
            existing = read_offers_sheet_rows(service, spreadsheet_id, offers_sheet)
            if not existing:
                logger.warning(
                    "NipuhimUnmonRepair: cannot merge feed%s — missing offers sheet %r",
                    feed_num,
                    offers_sheet,
                )
                continue

            order_map, _ = _geo_merchant_order_and_links(existing)
            chosen: Dict[str, List[str]] = {}
            for geo in sorted(touch_geos):
                failed_here = set(bad_geos.get(geo) or ())
                ranked = ranked_all.get(geo) or []
                cur = order_map.get(geo) or []
                chosen[geo] = _rebuild_merchants_for_geo(
                    geo,
                    cur,
                    failed_here,
                    ranked,
                    target_n=merchants_per_geo,
                )
                logger.info(
                    "NipuhimUnmonRepair: feed%s geo=%s old=%r failed=%r new=%r",
                    feed_num,
                    geo,
                    cur,
                    sorted(failed_here),
                    chosen[geo],
                )

            merchants_dl = download_merchants_feed(
                api_key,
                list(FEED2_MERCHANTS_GEOS) if feed_num == 2 and FEED2_MERCHANTS_GEOS else None,
                static_only=not _bool_env("NIPUHIM_UNMON_REPAIR_INCLUDE_FLEX", False),
            )
            pla_alt = build_pla_id_alternates_for_feed(merchants_dl)
            new_rows: List[Dict[str, Any]] = []
            for geo in geo_list:
                new_rows.extend(
                    generate_offers_rank_weighted_interleave(
                        api_key,
                        {geo: chosen[geo]},
                        pla_id_alternates=pla_alt,
                    )
                )
            merged = merge_offers_replace_geos(existing, new_rows, set(geo_list))
            write_offers_sheet(service, spreadsheet_id, offers_sheet, merged)
            logger.info(
                "NipuhimUnmonRepair: feed%s wrote %s offers to %r (replaced geos=%s)",
                feed_num,
                len(merged),
                offers_sheet,
                ",".join(sorted(geo_list)),
            )

            cmd = [
                sys.executable,
                str(_REPO_ROOT / "update_offers_from_sheet.py"),
                "--sheet",
                offers_sheet,
                "--max-offers",
                str(max_offers_sync),
            ]
            if feed_num == 2:
                cmd.extend(["--account", "2"])
            logger.info("NipuhimUnmonRepair: syncing Keitaro feed%s: %s", feed_num, " ".join(cmd))
            r = subprocess.run(cmd, cwd=str(_REPO_ROOT))
            if r.returncode != 0:
                raise RuntimeError(
                    f"update_offers_from_sheet.py feed{feed_num} exited with code {r.returncode}"
                )

        create_combined_offers_sheet(service, date_str)

        if _bool_env("NIPUHIM_UNMON_REPAIR_BLEND_SYNC", False):
            bcmd = [sys.executable, str(_REPO_ROOT / "blend_sync_from_sheet.py")]
            logger.info("NipuhimUnmonRepair: running %s", " ".join(bcmd))
            br = subprocess.run(bcmd, cwd=str(_REPO_ROOT))
            if br.returncode != 0:
                raise RuntimeError(f"blend_sync_from_sheet.py exited with code {br.returncode}")

        logger.info("NipuhimUnmonRepair: completed for %s", date_str)
