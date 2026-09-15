"""
Kelkoo publisher raw-report revenue (Feed API), independent of Keitaro postbacks.

Used by the Control Center overview so operators can compare Feed 2 API CPC/sale
totals to the Keitaro affiliation row.
"""
from __future__ import annotations

import csv
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests

from config import (
    kelkoo_api_key_for_postback_tag,
    kelkoo_postback_revenue_share,
    raw_report_geos_for_postback_tag,
)
from integrations.daily_conversion_postbacks import fetch_kelkoo_raw_tsv

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_FEED_TAG = "kelkoo2"
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


def _refresh_marker_active(feed_tag: str, *, max_age_sec: float = 45 * 60) -> bool:
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


def _daterange(start: date, end: date) -> List[date]:
    if start > end:
        return []
    out: List[date] = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def _sum_geo_day(
    *,
    geo: str,
    day: str,
    api_key: str,
    session: Optional[requests.Session] = None,
    retries: int = 3,
) -> Dict[str, Any]:
    """Sum leadValid CPC for one geo/day. Amounts are gross (pre-share).

    Note: ``saleValueInUsd`` is order GMV, not publisher commission — do not treat
    it as revenue for the overview tile.
    """
    out: Dict[str, Any] = {
        "geo": geo,
        "day": day,
        "http": 0,
        "ready": False,
        "unsupported": False,
        "cpc_gross": 0.0,
        "sale_gmv_gross": 0.0,
        "n_cpc": 0,
        "n_sale": 0,
    }
    own_session = session is None
    sess = session or requests.Session()
    try:
        status = 0
        body = ""
        for attempt in range(max(1, int(retries or 1))):
            status, body = fetch_kelkoo_raw_tsv(geo, day, api_key, sess)
            out["http"] = int(status)
            if status == 400:
                # Country not enabled for this publisher key — not a transient miss.
                out["unsupported"] = True
                return out
            if status == 200 and (body or "").strip():
                break
            if attempt + 1 < max(1, int(retries or 1)):
                time.sleep(0.35 * (attempt + 1))
        else:
            return out

        out["ready"] = True
        cpc_g = 0.0
        sale_gmv = 0.0
        n_cpc = 0
        n_sale = 0
        for row in csv.DictReader(StringIO(body), delimiter="\t"):
            if (row.get("leadValid") or "").strip().lower() == "true":
                cpc_g += _f_money(row.get("leadEstimatedRevenueInUsd"))
                n_cpc += 1
            if (row.get("sale") or "").strip().lower() == "true":
                sale_gmv += _f_money(row.get("saleValueInUsd"))
                n_sale += 1
        out["cpc_gross"] = cpc_g
        out["sale_gmv_gross"] = sale_gmv
        out["n_cpc"] = n_cpc
        out["n_sale"] = n_sale
        return out
    finally:
        if own_session:
            sess.close()


def fetch_kelkoo_feed_api_revenue(
    *,
    feed_tag: str = DEFAULT_FEED_TAG,
    yesterday: date,
    mtd_start: date,
    mtd_end: date,
    geos: Optional[Sequence[str]] = None,
    max_workers: int = 8,
) -> Dict[str, Any]:
    """
    Sum Kelkoo raw-report **leadValid CPC** for the overview window.

    Returns net amounts (after ``kelkoo_postback_revenue_share``) as primary
    ``yesterday`` / ``mtd``. Sale order GMV is reported separately and is **not**
    included in revenue (``saleValueInUsd`` is basket value, not commission).
    """
    tag = (feed_tag or DEFAULT_FEED_TAG).strip().lower() or DEFAULT_FEED_TAG
    api_key = kelkoo_api_key_for_postback_tag(tag)
    share = float(kelkoo_postback_revenue_share(feed_tag=tag) or 1.0)
    geo_list = [g.strip().lower() for g in (geos or raw_report_geos_for_postback_tag(tag)) if g and str(g).strip()]
    days = _daterange(mtd_start, mtd_end)
    y_key = yesterday.isoformat()

    base: Dict[str, Any] = {
        "feed": tag,
        "label": "Kelkoo 2 (Feed API)" if tag == "kelkoo2" else f"{tag} (Feed API)",
        "source": "kelkoo_raw_report",
        "metric": "leadValid_cpc",
        "share": share,
        "yesterday": None,
        "mtd": None,
        "yesterday_gross": None,
        "mtd_gross": None,
        "cpc_yesterday": None,
        "cpc_mtd": None,
        "sale_gmv_yesterday": None,
        "sale_gmv_mtd": None,
        # Legacy aliases kept for older UI; always 0 (GMV is not revenue).
        "sale_yesterday": 0.0,
        "sale_mtd": 0.0,
        "error": None,
        "ready_geo_days": 0,
        "failed_geo_days": 0,
        "unsupported_geo_days": 0,
        "geo_count": len(geo_list),
        "day_count": len(days),
        "coverage_pct": None,
        "incomplete": False,
        "ranges": {
            "yesterday": y_key,
            "mtd_from": mtd_start.isoformat(),
            "mtd_to": mtd_end.isoformat(),
        },
        "as_of_utc": _utc_now(),
    }

    if not api_key:
        base["error"] = f"Missing API key for {tag}"
        return base
    if not geo_list:
        base["error"] = f"No raw-report geos configured for {tag}"
        return base
    if not days:
        base.update(
            {
                "yesterday": 0.0,
                "mtd": 0.0,
                "yesterday_gross": 0.0,
                "mtd_gross": 0.0,
                "cpc_yesterday": 0.0,
                "cpc_mtd": 0.0,
                "sale_gmv_yesterday": 0.0,
                "sale_gmv_mtd": 0.0,
                "coverage_pct": 100.0,
            }
        )
        return base

    cpc_by_day: Dict[str, float] = {d.isoformat(): 0.0 for d in days}
    gmv_by_day: Dict[str, float] = {d.isoformat(): 0.0 for d in days}
    ready = 0
    failed = 0
    unsupported = 0
    jobs: List[Tuple[str, str]] = [(d.isoformat(), g) for d in days for g in geo_list]
    # Geos that returned HTTP 400 — skip remaining days for that geo in this run.
    skip_geos: set[str] = set()

    workers = max(1, min(int(max_workers or 8), 12))

    def _run_jobs(job_list: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
        """Fetch jobs; return list of (day, geo) that still need a retry."""
        nonlocal ready, unsupported
        need_retry: List[Tuple[str, str]] = []
        work = [(day, geo) for day, geo in job_list if geo not in skip_geos]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {
                pool.submit(
                    _sum_geo_day,
                    geo=geo,
                    day=day,
                    api_key=api_key,
                    session=None,
                    retries=3,
                ): (day, geo)
                for day, geo in work
            }
            for fut in as_completed(futs):
                day, geo = futs[fut]
                try:
                    row = fut.result()
                except Exception as e:
                    need_retry.append((day, geo))
                    logger.info("Kelkoo API revenue %s %s/%s failed: %s", tag, day, geo, e)
                    continue
                if row.get("unsupported"):
                    if geo not in skip_geos:
                        skip_geos.add(geo)
                    unsupported += 1
                    continue
                if not row.get("ready"):
                    need_retry.append((day, geo))
                    continue
                ready += 1
                cpc_by_day[day] = float(cpc_by_day.get(day) or 0.0) + float(row.get("cpc_gross") or 0.0)
                gmv_by_day[day] = float(gmv_by_day.get(day) or 0.0) + float(row.get("sale_gmv_gross") or 0.0)
        return need_retry

    pending = _run_jobs(jobs)
    if pending:
        time.sleep(1.0)
        still = _run_jobs(pending)
        failed = len(still)
    else:
        failed = 0

    # Expected coverage excludes permanently unsupported geos discovered this run.
    supported_geos = max(0, len(geo_list) - len(skip_geos))
    expected = supported_geos * len(days)
    coverage = (100.0 * ready / expected) if expected else 100.0
    incomplete = bool(expected and ready < expected)

    cpc_mtd = sum(cpc_by_day.values())
    gmv_mtd = sum(gmv_by_day.values())
    cpc_y = float(cpc_by_day.get(y_key) or 0.0) if y_key in cpc_by_day else 0.0
    gmv_y = float(gmv_by_day.get(y_key) or 0.0) if y_key in gmv_by_day else 0.0

    err = None
    if incomplete and coverage < 85.0:
        err = f"Incomplete raw-report coverage ({coverage:.0f}% of supported geo-days)"

    base.update(
        {
            "yesterday_gross": round(cpc_y, 4),
            "mtd_gross": round(cpc_mtd, 4),
            "cpc_yesterday": round(cpc_y * share, 4),
            "cpc_mtd": round(cpc_mtd * share, 4),
            "sale_gmv_yesterday": round(gmv_y, 4),
            "sale_gmv_mtd": round(gmv_mtd, 4),
            "sale_yesterday": 0.0,
            "sale_mtd": 0.0,
            "yesterday": round(cpc_y * share, 4),
            "mtd": round(cpc_mtd * share, 4),
            "ready_geo_days": ready,
            "failed_geo_days": failed,
            "unsupported_geo_days": unsupported,
            "unsupported_geos": sorted(skip_geos),
            "supported_geo_count": supported_geos,
            "coverage_pct": round(coverage, 1),
            "incomplete": incomplete,
            "error": err,
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
    """Compute and persist Feed API revenue for the overview window."""
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
            if cached:
                out = dict(cached)
                out["refresh_status"] = "running"
                return out
            stub = missing_kelkoo_feed_api_revenue(tag)
            stub["refresh_status"] = "running"
            stub["error"] = None
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
        data["refresh_status"] = "ok"
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
    from integrations.overview import overview_period, ranges_dict

    yesterday, mtd_start, mtd_end = overview_period()
    tag = (feed_tag or DEFAULT_FEED_TAG).strip().lower() or DEFAULT_FEED_TAG
    return {
        "feed": tag,
        "label": "Kelkoo 2 (Feed API)" if tag == "kelkoo2" else f"{tag} (Feed API)",
        "source": "kelkoo_raw_report",
        "share": float(kelkoo_postback_revenue_share(feed_tag=tag) or 1.0),
        "yesterday": None,
        "mtd": None,
        "error": None,
        "refresh_status": "missing",
        "ranges": ranges_dict(yesterday, mtd_start, mtd_end),
        "as_of_utc": None,
    }


def get_kelkoo_feed_api_revenue(
    *,
    feed_tag: str = DEFAULT_FEED_TAG,
    refresh: bool = False,
) -> Dict[str, Any]:
    """
    Return cached Feed API revenue for the current MTD window.

    Does not auto-compute on miss (raw multi-geo MTD is slow). Pass ``refresh=True``
    or call :func:`queue_kelkoo_feed_api_revenue_refresh` from the UI.
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
        if running and st not in ("ok", "cached"):
            out["refresh_status"] = "running"
        elif not running and st == "running":
            # Stale in-progress stub (worker died / marker expired) — treat as miss.
            out["refresh_status"] = "missing"
            out["mtd"] = None
            out["yesterday"] = None
        else:
            out.setdefault("refresh_status", "cached")
        return out

    stub = missing_kelkoo_feed_api_revenue(tag)
    if running:
        stub["refresh_status"] = "running"
    return stub


def queue_kelkoo_feed_api_revenue_refresh(feed_tag: str = DEFAULT_FEED_TAG) -> Dict[str, Any]:
    """Start a background refresh; return immediately with ``refresh_status``."""
    from integrations.overview import overview_period

    tag = (feed_tag or DEFAULT_FEED_TAG).strip().lower() or DEFAULT_FEED_TAG
    with _REFRESH_LOCK:
        already = bool(_REFRESH_IN_FLIGHT.get(tag)) or _refresh_marker_active(tag)
        if already:
            cached = read_cached_kelkoo_feed_api_revenue(tag) or missing_kelkoo_feed_api_revenue(tag)
            out = dict(cached)
            out["refresh_status"] = "running"
            out["queued"] = False
            return out
        _REFRESH_IN_FLIGHT[tag] = True
        _mark_refresh_running(tag, running=True)

    # Persist a running stub so other workers / GET polls see progress immediately.
    stub = missing_kelkoo_feed_api_revenue(tag)
    stub["refresh_status"] = "running"
    stub["error"] = None
    try:
        write_cached_kelkoo_feed_api_revenue(stub, tag)
    except Exception:
        pass

    def _run() -> None:
        yesterday, mtd_start, mtd_end = overview_period()
        try:
            data = fetch_kelkoo_feed_api_revenue(
                feed_tag=tag,
                yesterday=yesterday,
                mtd_start=mtd_start,
                mtd_end=mtd_end,
            )
            data["refresh_status"] = "ok"
            write_cached_kelkoo_feed_api_revenue(data, tag)
        except Exception:
            logger.exception("Kelkoo Feed API revenue refresh failed for %s", tag)
            try:
                err = missing_kelkoo_feed_api_revenue(tag)
                err["error"] = "Feed API revenue refresh failed"
                err["refresh_status"] = "error"
                write_cached_kelkoo_feed_api_revenue(err, tag)
            except Exception:
                pass
        finally:
            with _REFRESH_LOCK:
                _REFRESH_IN_FLIGHT[tag] = False
            _mark_refresh_running(tag, running=False)

    threading.Thread(target=_run, name=f"kelkoo-api-rev-{tag}", daemon=True).start()
    out = dict(stub)
    out["queued"] = True
    return out
