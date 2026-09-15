"""
Kelkoo publisher MTD revenue (Feed API) for the Control Center overview tile.

Uses the fast aggregated report (``groupBy=day``), then applies the feed's
postback net share (Feed 2 = 0.7). Independent of Keitaro.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from config import (
    kelkoo_api_key_for_postback_tag,
    kelkoo_postback_revenue_share,
)

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_FEED_TAG = "kelkoo2"
AGGREGATED_URL = "https://api.kelkoogroup.net/publisher/reports/v1/aggregated"
_REFRESH_LOCK = threading.Lock()
_REFRESH_IN_FLIGHT: Dict[str, bool] = {}


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def cache_path(feed_tag: str = DEFAULT_FEED_TAG) -> Path:
    tag = (feed_tag or DEFAULT_FEED_TAG).strip().lower() or DEFAULT_FEED_TAG
    return ROOT / "runtime" / f"{tag}_api_revenue.json"


def running_marker_path(feed_tag: str = DEFAULT_FEED_TAG) -> Path:
    tag = (feed_tag or DEFAULT_FEED_TAG).strip().lower() or DEFAULT_FEED_TAG
    return ROOT / "runtime" / f"{tag}_api_revenue.running"


def _mark_refresh_running(feed_tag: str, *, running: bool) -> None:
    path = running_marker_path(feed_tag)
    try:
        if running:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(_utc_now(), encoding="utf-8")
        elif path.is_file():
            path.unlink()
    except OSError:
        pass


def _refresh_marker_active(feed_tag: str, *, max_age_sec: float = 10 * 60) -> bool:
    path = running_marker_path(feed_tag)
    if not path.is_file():
        return False
    try:
        age = time.time() - path.stat().st_mtime
        if age > max_age_sec:
            try:
                path.unlink()
            except OSError:
                pass
            return False
        return True
    except OSError:
        return False


def _f_money(v: Any) -> float:
    try:
        return float(str(v if v is not None else "0").replace(",", ".").strip() or 0)
    except (TypeError, ValueError):
        return 0.0


def _empty_payload(
    *,
    tag: str,
    share: float,
    yesterday: date,
    mtd_start: date,
    mtd_end: date,
) -> Dict[str, Any]:
    return {
        "feed": tag,
        "label": "Kelkoo 2 (Feed API)" if tag == "kelkoo2" else f"{tag} (Feed API)",
        "source": "kelkoo_aggregated",
        "metric": "lead_estimated_revenue_usd",
        "share": share,
        "yesterday": None,
        "mtd": None,
        "yesterday_gross": None,
        "mtd_gross": None,
        "cpc_yesterday": None,
        "cpc_mtd": None,
        "sale_yesterday": 0.0,
        "sale_mtd": 0.0,
        "error": None,
        "coverage_pct": 100.0,
        "incomplete": False,
        "ranges": {
            "yesterday": yesterday.isoformat(),
            "mtd_from": mtd_start.isoformat(),
            "mtd_to": mtd_end.isoformat(),
        },
        "as_of_utc": _utc_now(),
    }


def fetch_kelkoo_feed_api_revenue(
    *,
    feed_tag: str = DEFAULT_FEED_TAG,
    yesterday: date,
    mtd_start: date,
    mtd_end: date,
    geos: Optional[Any] = None,
    max_workers: int = 8,
) -> Dict[str, Any]:
    """
    MTD + yesterday lead estimated revenue from Kelkoo aggregated reports.

    One ``groupBy=day`` call for the MTD window; amounts are net after
    ``kelkoo_postback_revenue_share`` (Feed 2 = 0.7). Sale GMV is ignored.
    """
    del geos, max_workers  # legacy kwargs kept for callers
    tag = (feed_tag or DEFAULT_FEED_TAG).strip().lower() or DEFAULT_FEED_TAG
    api_key = kelkoo_api_key_for_postback_tag(tag)
    share = float(kelkoo_postback_revenue_share(feed_tag=tag) or 1.0)
    base = _empty_payload(
        tag=tag,
        share=share,
        yesterday=yesterday,
        mtd_start=mtd_start,
        mtd_end=mtd_end,
    )

    if not api_key:
        base["error"] = f"Missing API key for {tag}"
        return base
    if mtd_start > mtd_end:
        base.update(
            {
                "yesterday": 0.0,
                "mtd": 0.0,
                "yesterday_gross": 0.0,
                "mtd_gross": 0.0,
                "cpc_yesterday": 0.0,
                "cpc_mtd": 0.0,
            }
        )
        return base

    headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}
    params = {
        "start": mtd_start.isoformat(),
        "end": mtd_end.isoformat(),
        "groupBy": "day",
        "format": "JSON",
    }
    try:
        r = requests.get(AGGREGATED_URL, headers=headers, params=params, timeout=60)
    except requests.RequestException as e:
        base["error"] = f"Aggregated report request failed: {e}"
        return base
    if r.status_code != 200:
        base["error"] = f"Aggregated report HTTP {r.status_code}: {(r.text or '')[:240]}"
        return base
    try:
        rows = r.json()
    except ValueError:
        base["error"] = "Aggregated report returned invalid JSON"
        return base
    if not isinstance(rows, list):
        base["error"] = "Aggregated report unexpected shape"
        return base

    y_key = yesterday.isoformat()
    cpc_by_day: Dict[str, float] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        day = str(row.get("day") or "")[:10]
        if not day:
            continue
        # Prefer USD; fall back to EUR only if USD missing (should not happen with groupBy=day).
        usd = row.get("leadEstimatedRevenueInUsd")
        if usd is None or str(usd).strip() == "":
            usd = row.get("leadEstimatedRevenueInEur")
        cpc_by_day[day] = _f_money(usd)

    cpc_mtd = sum(cpc_by_day.values())
    cpc_y = float(cpc_by_day.get(y_key) or 0.0)

    base.update(
        {
            "yesterday_gross": round(cpc_y, 4),
            "mtd_gross": round(cpc_mtd, 4),
            "cpc_yesterday": round(cpc_y * share, 4),
            "cpc_mtd": round(cpc_mtd * share, 4),
            "yesterday": round(cpc_y * share, 4),
            "mtd": round(cpc_mtd * share, 4),
            "day_count": len(cpc_by_day),
            "error": None,
            "as_of_utc": _utc_now(),
        }
    )
    return base


def read_cached_kelkoo_feed_api_revenue(
    feed_tag: str = DEFAULT_FEED_TAG,
    *,
    mtd_from: Optional[str] = None,
    mtd_to: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    path = cache_path(feed_tag)
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    if mtd_from or mtd_to:
        ranges = data.get("ranges") or {}
        if mtd_from and str(ranges.get("mtd_from") or "") != str(mtd_from):
            return None
        if mtd_to and str(ranges.get("mtd_to") or "") != str(mtd_to):
            return None
    return data


def write_cached_kelkoo_feed_api_revenue(data: Dict[str, Any], feed_tag: str = DEFAULT_FEED_TAG) -> Path:
    path = cache_path(feed_tag)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    payload = dict(data or {})
    payload["cached_utc"] = _utc_now()
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    tmp.replace(path)
    return path


def refresh_kelkoo_feed_api_revenue(
    *,
    feed_tag: str = DEFAULT_FEED_TAG,
    yesterday: Optional[date] = None,
    mtd_start: Optional[date] = None,
    mtd_end: Optional[date] = None,
) -> Dict[str, Any]:
    """Compute and persist Feed API revenue for the overview window (fast aggregated)."""
    from integrations.overview import overview_period

    y, m0, m1 = overview_period()
    if yesterday is None:
        yesterday = y
    if mtd_start is None:
        mtd_start = m0
    if mtd_end is None:
        mtd_end = m1
    tag = (feed_tag or DEFAULT_FEED_TAG).strip().lower() or DEFAULT_FEED_TAG
    with _REFRESH_LOCK:
        if _REFRESH_IN_FLIGHT.get(tag) or _refresh_marker_active(tag):
            cached = read_cached_kelkoo_feed_api_revenue(tag)
            if cached and cached.get("mtd") is not None:
                out = dict(cached)
                out["refresh_status"] = "running"
                return out
            stub = missing_kelkoo_feed_api_revenue(tag)
            stub["refresh_status"] = "running"
            return stub
        _REFRESH_IN_FLIGHT[tag] = True
        _mark_refresh_running(tag, running=True)
    try:
        data = fetch_kelkoo_feed_api_revenue(
            feed_tag=tag,
            yesterday=yesterday,
            mtd_start=mtd_start,
            mtd_end=mtd_end,
        )
        data["refresh_status"] = "ok" if not data.get("error") else "error"
        write_cached_kelkoo_feed_api_revenue(data, tag)
        return data
    except Exception as e:
        err = missing_kelkoo_feed_api_revenue(tag)
        err["error"] = str(e)
        err["refresh_status"] = "error"
        write_cached_kelkoo_feed_api_revenue(err, tag)
        raise
    finally:
        with _REFRESH_LOCK:
            _REFRESH_IN_FLIGHT[tag] = False
        _mark_refresh_running(tag, running=False)


def missing_kelkoo_feed_api_revenue(feed_tag: str = DEFAULT_FEED_TAG) -> Dict[str, Any]:
    from integrations.overview import overview_period

    yesterday, mtd_start, mtd_end = overview_period()
    tag = (feed_tag or DEFAULT_FEED_TAG).strip().lower() or DEFAULT_FEED_TAG
    share = float(kelkoo_postback_revenue_share(feed_tag=tag) or 1.0)
    out = _empty_payload(
        tag=tag,
        share=share,
        yesterday=yesterday,
        mtd_start=mtd_start,
        mtd_end=mtd_end,
    )
    out["refresh_status"] = "missing"
    return out


def get_kelkoo_feed_api_revenue(
    *,
    feed_tag: str = DEFAULT_FEED_TAG,
    refresh: bool = False,
) -> Dict[str, Any]:
    """
    Return Feed API revenue for the current MTD window.

    Cache hit is instant. On miss (or ``refresh=True``), runs the fast aggregated
    fetch synchronously (~5–15s) so the homepage tile can show numbers without a
    long background poll.
    """
    from integrations.overview import overview_period, ranges_dict

    yesterday, mtd_start, mtd_end = overview_period()
    ranges = ranges_dict(yesterday, mtd_start, mtd_end)
    tag = (feed_tag or DEFAULT_FEED_TAG).strip().lower() or DEFAULT_FEED_TAG

    if refresh:
        return refresh_kelkoo_feed_api_revenue(
            feed_tag=tag,
            yesterday=yesterday,
            mtd_start=mtd_start,
            mtd_end=mtd_end,
        )

    with _REFRESH_LOCK:
        running = bool(_REFRESH_IN_FLIGHT.get(tag)) or _refresh_marker_active(tag)

    cached = read_cached_kelkoo_feed_api_revenue(
        tag,
        mtd_from=ranges["mtd_from"],
        mtd_to=ranges["mtd_to"],
    )
    if cached is not None:
        out = dict(cached)
        st = str(out.get("refresh_status") or "")
        if out.get("mtd") is not None and st in ("ok", "cached", ""):
            out["refresh_status"] = "cached" if st != "ok" else "ok"
            if running:
                # Keep showing last good numbers while a refresh is in flight.
                out["refresh_status"] = "running"
            return out
        if running:
            out["refresh_status"] = "running"
            return out
        if st == "error" or out.get("mtd") is None:
            # Fall through to sync rebuild.
            pass
        else:
            out.setdefault("refresh_status", "cached")
            return out

    if running:
        stub = missing_kelkoo_feed_api_revenue(tag)
        stub["refresh_status"] = "running"
        return stub

    # Fast enough to compute inline on first load / miss.
    return refresh_kelkoo_feed_api_revenue(
        feed_tag=tag,
        yesterday=yesterday,
        mtd_start=mtd_start,
        mtd_end=mtd_end,
    )


def queue_kelkoo_feed_api_revenue_refresh(feed_tag: str = DEFAULT_FEED_TAG) -> Dict[str, Any]:
    """
    Rebuild Feed API revenue.

    Aggregated MTD is fast, so this runs synchronously and returns the final
    payload (UI can still treat 202 + poll as best-effort).
    """
    tag = (feed_tag or DEFAULT_FEED_TAG).strip().lower() or DEFAULT_FEED_TAG
    data = refresh_kelkoo_feed_api_revenue(feed_tag=tag)
    out = dict(data)
    out["queued"] = False
    out["sync"] = True
    return out
