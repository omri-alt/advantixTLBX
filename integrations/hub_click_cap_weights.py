"""
Hub campaign stream weights from Blend clickCap totals and Nipuhim geo/merchant coverage.

Blend: sum ``clickCap`` per ``feed`` on the Blend sheet (rows with cap > 0).
Nipuhim: inspect today's ``{date}_offers_*`` tabs to see which geos are active and how
many distinct merchants are selected per geo.
"""
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, FrozenSet, List, Optional, Tuple


def _utc_today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _nipuhim_sheet_for_feed(date_str: str, feed_key: str) -> Optional[str]:
    fk = (feed_key or "").strip().lower()
    if fk == "kelkoo1":
        return f"{date_str}_offers_1"
    if fk == "kelkoo2":
        return f"{date_str}_offers_2"
    if fk == "kelkoo5":
        return f"{date_str}_offers_5"
    return None


def _latest_daily_run_dir() -> Optional[Path]:
    meta = Path(__file__).resolve().parents[1] / "runtime" / "workflow_runs" / "daily.json"
    if not meta.exists():
        return None
    try:
        payload = json.loads(meta.read_text(encoding="utf-8"))
    except Exception:
        return None
    raw = str(payload.get("run_dir") or "").strip()
    if not raw:
        return None
    p = Path(raw)
    return p if p.exists() else None


def _merchant_counts_from_chosen_artifacts(
    *,
    date_str: str,
    feed_keys: Tuple[str, ...],
) -> Tuple[Dict[str, Dict[str, int]], List[str]]:
    run_dir = _latest_daily_run_dir()
    if not run_dir:
        return {}, ["Nipuhim merchant counts: no daily run metadata/artifacts fallback"]

    feed_to_artifact = {
        "kelkoo1": "chosen1.json",
        "kelkoo2": "chosen2.json",
        "kelkoo5": "chosen5.json",
    }
    out: Dict[str, Dict[str, int]] = {}
    logs: List[str] = []
    artifacts_dir = run_dir / "artifacts"
    for fk in feed_keys:
        name = feed_to_artifact.get(fk)
        if not name:
            continue
        path = artifacts_dir / name
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            logs.append(f"Nipuhim {fk}: could not read artifact {name}: {e}")
            continue
        counts = {
            str(geo).strip().lower()[:2]: len([m for m in (mids or []) if str(m).strip()])
            for geo, mids in (payload or {}).items()
            if str(geo).strip()
        }
        if counts:
            out[fk] = counts
            parts = ", ".join(f"{geo}={count}" for geo, count in sorted(counts.items()))
            logs.append(
                f"Nipuhim {fk}: merchant counts from {name} for {date_str} ({parts})"
            )
    return out, logs


def nipuhim_feed_geo_merchant_counts(
    *,
    date_str: Optional[str] = None,
    feed_keys: Tuple[str, ...] = ("kelkoo1", "kelkoo2", "kelkoo5"),
) -> Tuple[Dict[str, Dict[str, int]], List[str]]:
    """
    Distinct merchant count per feed+geo from today's Nipuhim offers tabs.

    Uses the generated offers sheets because they are the post-selection source of truth:
    one geo can have 1-3 merchants selected, each repeated across many product rows.
    """
    from update_offers_from_sheet import SPREADSHEET_ID, get_credentials_path
    from geos import normalize_geo
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError

    day = (date_str or _utc_today_str()).strip()
    logs: List[str] = []
    out: Dict[str, Dict[str, int]] = {}

    try:
        creds = service_account.Credentials.from_service_account_file(get_credentials_path())
        service = build("sheets", "v4", credentials=creds).spreadsheets()
    except Exception as e:
        fallback, fb_logs = _merchant_counts_from_chosen_artifacts(date_str=day, feed_keys=feed_keys)
        return fallback, [f"Nipuhim merchant counts: sheets client unavailable: {e}"] + fb_logs

    for fk in feed_keys:
        sheet = _nipuhim_sheet_for_feed(day, fk)
        if not sheet:
            continue
        quoted = sheet.replace("'", "''")
        try:
            rows = (
                service.values()
                .get(spreadsheetId=SPREADSHEET_ID, range=f"'{quoted}'!A:B")
                .execute()
                .get("values")
                or []
            )
        except HttpError as e:
            msg = str(e)
            if "Unable to parse range" in msg:
                logs.append(f"Nipuhim {fk}: missing offers sheet {sheet!r}")
                continue
            logs.append(f"Nipuhim {fk}: could not read {sheet!r}: {e}")
            continue
        by_geo: Dict[str, set[str]] = defaultdict(set)
        for row in rows:
            if len(row) < 2:
                continue
            geo = normalize_geo(str(row[0] or "").strip())
            mid = str(row[1] or "").strip().split(".")[0]
            if not geo or not mid or geo in ("country", "geo"):
                continue
            by_geo[geo].add(mid)
        counts = {geo: len(mids) for geo, mids in by_geo.items() if mids}
        out[fk] = counts
        if counts:
            parts = ", ".join(f"{geo}={count}" for geo, count in sorted(counts.items()))
            logs.append(
                f"Nipuhim {fk}: {len(counts)} geo(s) with merchant counts from {sheet!r} ({parts})"
            )
        else:
            logs.append(f"Nipuhim {fk}: 0 geos with merchants from {sheet!r}")
    if any(out.get(fk) for fk in feed_keys):
        return out, logs

    fallback, fb_logs = _merchant_counts_from_chosen_artifacts(date_str=day, feed_keys=feed_keys)
    if fallback:
        logs.extend(fb_logs)
        return fallback, logs
    return out, logs + fb_logs


def blend_feed_click_caps(*, sheets_service: Any = None) -> Tuple[Dict[str, float], List[str]]:
    """Sum Blend sheet ``clickCap`` per feed tag (only rows with cap > 0)."""
    logs: List[str] = []
    caps: Dict[str, float] = defaultdict(float)
    try:
        from blend_sync_from_sheet import get_sheets_service, read_blend_rows
    except Exception as e:
        return {}, [f"Blend clickCap: import failed: {e}"]

    try:
        service = sheets_service or get_sheets_service()
        rows = read_blend_rows(service)
    except Exception as e:
        return {}, [f"Blend clickCap: sheet read failed: {e}"]

    for row in rows:
        cap = float(row.click_cap or 0)
        if cap <= 0:
            continue
        fk = (row.feed_tag or "kelkoo1").strip().lower()
        caps[fk] += cap

    if caps:
        parts = [f"{fk}={int(v) if v == int(v) else v}" for fk, v in sorted(caps.items())]
        logs.append(f"Blend clickCap totals: {', '.join(parts)}")
    else:
        logs.append("Blend clickCap totals: none (no rows with clickCap > 0)")
    return dict(caps), logs


def nipuhim_feed_offer_slots(
    *,
    date_str: Optional[str] = None,
    max_offers_per_geo: int = 60,
) -> Tuple[Dict[str, float], List[str]]:
    """
    Count Nipuhim offer slots per Kelkoo feed from today's offers tabs.

    Each store-link row (per geo, capped at ``max_offers_per_geo``) counts as one slot.
    """
    feed_geos, logs = nipuhim_feed_active_geos(
        date_str=date_str,
        max_offers_per_geo=max_offers_per_geo,
    )
    caps = {fk: float(len(geos)) for fk, geos in feed_geos.items()}
    if not caps:
        logs.append(f"Nipuhim offer slots: no data for {(date_str or _utc_today_str()).strip()} offers tabs")
    return caps, logs


def nipuhim_feed_active_geos(
    *,
    date_str: Optional[str] = None,
    max_offers_per_geo: int = 60,
    feed_keys: Tuple[str, ...] = ("kelkoo1", "kelkoo2", "kelkoo5"),
) -> Tuple[Dict[str, FrozenSet[str]], List[str]]:
    """
    Geos with at least one store-link row on today's ``{date}_offers_*`` tab per feed.
    """
    from geos import normalize_geo
    from update_offers_from_sheet import read_sheet_today_offers

    day = (date_str or _utc_today_str()).strip()
    logs: List[str] = []
    out: Dict[str, frozenset[str]] = {}

    for fk in feed_keys:
        sheet = _nipuhim_sheet_for_feed(day, fk)
        if not sheet:
            continue
        try:
            by_geo, _ = read_sheet_today_offers(sheet, max_per_geo=max_offers_per_geo)
        except Exception as e:
            logs.append(f"Nipuhim {fk}: could not read {sheet!r}: {e}")
            continue
        geos = frozenset(
            normalize_geo(g)
            for g, links in (by_geo or {}).items()
            if links
        )
        out[fk] = geos
        if geos:
            logs.append(
                f"Nipuhim {fk}: {len(geos)} geo(s) with offers from {sheet!r} "
                f"({', '.join(sorted(geos))})"
            )
        else:
            logs.append(f"Nipuhim {fk}: 0 geos with offers from {sheet!r}")

    return out, logs


def hub_nipuhim_equal_weights_per_geo(
    feed_geos: Dict[str, FrozenSet[str]],
    *,
    active_feeds: frozenset[str],
) -> Tuple[Dict[str, Dict[str, float]], List[str]]:
    """
    Per-geo hub weights: equal split across feeds that have offers for that geo.

    Example: if kelkoo1+kelkoo2+kelkoo5 all have ``de`` offers → 33.33% each on ``de_*`` streams.
    If only kelkoo1 has ``au`` offers → 100% to ``hub_nipuhim_kelkoo1`` on ``au_*`` streams.
    """
    all_geos: set[str] = set()
    for fk in active_feeds:
        all_geos |= set(feed_geos.get(fk, frozenset()))

    weights_by_geo: Dict[str, Dict[str, float]] = {}
    logs: List[str] = []
    for geo in sorted(all_geos):
        feeds_with_geo = sorted(fk for fk in active_feeds if geo in feed_geos.get(fk, frozenset()))
        if not feeds_with_geo:
            continue
        share = 100.0 / len(feeds_with_geo)
        weights_by_geo[geo] = {f"hub_nipuhim_{fk}": share for fk in feeds_with_geo}
        logs.append(
            f"Hub {geo}: {len(feeds_with_geo)} feed(s) @ {share:.2f}% each "
            f"({', '.join(feeds_with_geo)})"
        )
    return weights_by_geo, logs


def hub_offer_weights_from_caps(
    blend_feed_caps: Dict[str, float],
    nipuhim_feed_caps: Dict[str, float],
    *,
    active_feeds: frozenset[str],
    hub_types: Tuple[str, ...] = ("blend", "nipuhim"),
) -> Dict[str, float]:
    """Convert per-feed capacity numbers into hub offer weight percentages."""
    use_blend = "blend" in hub_types
    use_nipuhim = "nipuhim" in hub_types
    blend_total = (
        sum(max(0.0, float(blend_feed_caps.get(fk, 0))) for fk in active_feeds)
        if use_blend
        else 0.0
    )
    nipuhim_total = (
        sum(max(0.0, float(nipuhim_feed_caps.get(fk, 0))) for fk in active_feeds)
        if use_nipuhim
        else 0.0
    )
    type_total = blend_total + nipuhim_total
    if type_total <= 0:
        return {}

    out: Dict[str, float] = {}
    for hub_type, feed_caps, type_sum in (
        ("blend", blend_feed_caps, blend_total),
        ("nipuhim", nipuhim_feed_caps, nipuhim_total),
    ):
        if hub_type not in hub_types:
            continue
        type_frac = type_sum / type_total
        feed_total = sum(max(0.0, float(feed_caps.get(fk, 0))) for fk in active_feeds)
        for feed_key in active_feeds:
            name = f"hub_{hub_type}_{feed_key}"
            if feed_total <= 0:
                out[name] = 0.0
                continue
            cap = max(0.0, float(feed_caps.get(feed_key, 0)))
            if cap <= 0:
                out[name] = 0.0
                continue
            out[name] = type_frac * (cap / feed_total) * 100.0
    return out
