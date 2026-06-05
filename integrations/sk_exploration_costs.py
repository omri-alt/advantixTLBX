"""
SKtrackExploration campaign spend for the SK console cost widget.

Uses ``GET /affiliate/v2/stats/by-campaign`` (one paginated request per UTC day),
filters to campaign IDs on ``SKtrackExploration``, and persists a snapshot for the UI.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

from config import SK_OPTIMIZER_SHEET_ID, SOURCEKNOWLEDGE_API_KEY
from integrations.overview_costs import SK_API_BASE, _sk_headers

logger = logging.getLogger(__name__)

TAB_EXPLORATION = "SKtrackExploration"
_ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT_PATH = _ROOT / "runtime" / "sk_exploration_cost_snapshot.json"

_REFRESH_LOCK = threading.Lock()
_REFRESH_RUNNING = False

_HTTP_TIMEOUT = max(15, int((os.getenv("SK_EXPLORATION_COST_TIMEOUT") or "45").strip() or "45"))
# All ``SKtrackExploration`` rows with a campaign ID (any status). Set
# ``SK_EXPLORATION_COST_ALL_STATUSES=0`` or ``SK_EXPLORATION_COST_ACTIVE_ONLY=1`` for active rows only.
_ALL_STATUSES = str(os.getenv("SK_EXPLORATION_COST_ALL_STATUSES", "1")).strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)
if str(os.getenv("SK_EXPLORATION_COST_ACTIVE_ONLY") or "").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
):
    _ALL_STATUSES = False


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def _parse_campaign_id(raw: Any) -> Optional[int]:
    s = str(raw or "").strip()
    if not s:
        return None
    try:
        cid = int(s)
        return cid if cid > 0 else None
    except (TypeError, ValueError):
        return None


def _row_included(row: dict) -> bool:
    if _ALL_STATUSES:
        return True
    st = str(row.get("status") or "").strip().lower()
    return st in ("", "active", "[]")


def fetch_exploration_campaign_ids() -> Tuple[Set[int], Optional[str]]:
    """Distinct campaign IDs from ``SKtrackExploration``."""
    sheet_id = (SK_OPTIMIZER_SHEET_ID or "").strip()
    if not sheet_id:
        return set(), "SK_OPTIMIZER_SHEET_ID is not set"
    try:
        from integrations.autoserver import gdocs_as as gd

        rows = gd.read_sheet_withID(sheet_id, TAB_EXPLORATION)
    except Exception as e:
        logger.warning("SK exploration cost: sheet read failed: %s", e)
        return set(), f"Could not read {TAB_EXPLORATION}: {e}"

    out: Set[int] = set()
    for row in rows or []:
        if not isinstance(row, dict) or not _row_included(row):
            continue
        cid = _parse_campaign_id(row.get("campaignId") or row.get("campId"))
        if cid is not None:
            out.add(cid)
    return out, None


def _fetch_by_campaign_page(
    api_key: str,
    day: date,
    page: int,
) -> Tuple[Optional[dict], Optional[str]]:
    """One page of ``GET .../stats/by-campaign`` for a single UTC day."""
    headers = _sk_headers(api_key)
    ds = day.isoformat()
    url = f"{SK_API_BASE}/stats/by-campaign"
    try:
        r = requests.get(
            url,
            headers=headers,
            params={"from": ds, "to": ds, "page": page},
            timeout=_HTTP_TIMEOUT,
        )
    except requests.RequestException as e:
        return None, str(e)
    if r.status_code == 429:
        time.sleep(2.0)
        try:
            r = requests.get(
                url,
                headers=headers,
                params={"from": ds, "to": ds, "page": page},
                timeout=_HTTP_TIMEOUT,
            )
        except requests.RequestException as e:
            return None, str(e)
    if r.status_code != 200:
        return None, f"HTTP {r.status_code}"
    try:
        data = r.json()
    except Exception as e:
        return None, str(e)
    if isinstance(data, dict) and data.get("error"):
        return None, str(data.get("error"))
    return data if isinstance(data, dict) else None, None


def _exploration_spend_for_day(api_key: str, campaign_ids: Set[int], day: date) -> Tuple[float, Optional[str]]:
    """
    Sum ``spend`` for exploration campaign IDs from ``by-campaign`` stats (paginated).

    Typically 1–N pages per day (1000 items/page) instead of one call per campaign.
    """
    if not campaign_ids:
        return 0.0, None
    total = 0.0
    page = 1
    last_err: Optional[str] = None
    while page <= 500:
        data, err = _fetch_by_campaign_page(api_key, day, page)
        if err:
            last_err = err
            break
        if not data:
            break
        items = data.get("items")
        if isinstance(items, list):
            for it in items:
                if not isinstance(it, dict):
                    continue
                cid = _parse_campaign_id(it.get("id") or it.get("campaignId"))
                if cid is None or cid not in campaign_ids:
                    continue
                spend = it.get("spend")
                if spend is None:
                    continue
                try:
                    total += float(spend)
                except (TypeError, ValueError):
                    pass
        if not data.get("hasMore"):
            break
        page += 1
        time.sleep(0.05)
    return round(total, 4), last_err


def fetch_exploration_cost_widget(*, days: int = 7) -> Dict[str, Any]:
    """Live SK API fetch via ``by-campaign`` (≈1 paginated call per UTC day)."""
    days = max(1, min(14, int(days)))
    api_key = (SOURCEKNOWLEDGE_API_KEY or "").strip()
    if not api_key:
        return _empty_payload(error="SOURCEKNOWLEDGE_API_KEY / KEYSK not set")

    cids, sheet_err = fetch_exploration_campaign_ids()
    if sheet_err:
        return _empty_payload(error=sheet_err)

    yesterday = _utc_today() - timedelta(days=1)
    day_list = [yesterday - timedelta(days=offset) for offset in range(days - 1, -1, -1)]

    started = time.time()
    daily: List[Dict[str, Any]] = []
    partial_errors = 0

    # One by-campaign request series per day (parallel over days).
    with ThreadPoolExecutor(max_workers=min(7, len(day_list))) as pool:
        futures = {pool.submit(_exploration_spend_for_day, api_key, cids, day): day for day in day_list}
        results: Dict[str, Tuple[float, Optional[str]]] = {}
        for fut in as_completed(futures):
            day = futures[fut]
            try:
                spend, err = fut.result()
            except Exception as e:
                spend, err = 0.0, str(e)
            if err:
                partial_errors += 1
                logger.warning("SK by-campaign %s: %s", day.isoformat(), err)
            results[day.isoformat()] = (spend, err)

    for day in day_list:
        spend, _err = results.get(day.isoformat(), (0.0, None))
        daily.append(
            {
                "date": day.isoformat(),
                "label": day.strftime("%a %d"),
                "spend": spend,
            }
        )

    y_spend = daily[-1]["spend"] if daily else 0.0
    total = round(sum(float(d.get("spend") or 0) for d in daily), 4)

    err_msg: Optional[str] = None
    if not cids:
        err_msg = "No campaign IDs on SKtrackExploration"
    elif partial_errors >= len(day_list):
        err_msg = "SK by-campaign stats unavailable for all days"

    return {
        "status": "ready",
        "yesterday": round(float(y_spend), 4),
        "total_7d": total,
        "daily": daily,
        "campaign_count": len(cids),
        "yesterday_date": yesterday.isoformat(),
        "range_start": day_list[0].isoformat() if day_list else "",
        "range_end": yesterday.isoformat(),
        "as_of_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "fetch_seconds": round(time.time() - started, 2),
        "error": err_msg,
        "partial_errors": partial_errors,
        "active_only": not _ALL_STATUSES,
        "source": "by-campaign",
    }


def _empty_payload(*, error: str) -> Dict[str, Any]:
    return {
        "status": "error",
        "yesterday": None,
        "total_7d": None,
        "daily": [],
        "campaign_count": 0,
        "yesterday_date": "",
        "range_start": "",
        "range_end": "",
        "as_of_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "fetch_seconds": 0,
        "error": error,
        "partial_errors": 0,
        "active_only": not _ALL_STATUSES,
        "source": "by-campaign",
    }


def load_snapshot() -> Optional[Dict[str, Any]]:
    if not SNAPSHOT_PATH.exists():
        return None
    try:
        data = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except Exception as e:
        logger.warning("SK exploration cost snapshot read failed: %s", e)
        return None


def save_snapshot(data: Dict[str, Any]) -> None:
    SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = SNAPSHOT_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=True, indent=2), encoding="utf-8")
    tmp.replace(SNAPSHOT_PATH)


def refresh_running() -> bool:
    with _REFRESH_LOCK:
        return _REFRESH_RUNNING


def _run_refresh() -> None:
    global _REFRESH_RUNNING
    try:
        logger.info("SK exploration cost snapshot refresh started (by-campaign)")
        data = fetch_exploration_cost_widget(days=7)
        data["status"] = "ready" if not data.get("error") else "error"
        save_snapshot(data)
        logger.info(
            "SK exploration cost snapshot saved (%s campaigns, %.1fs, partial_errors=%s)",
            data.get("campaign_count"),
            data.get("fetch_seconds"),
            data.get("partial_errors"),
        )
    except Exception as e:
        logger.exception("SK exploration cost refresh failed")
        save_snapshot(_empty_payload(error=str(e)))
    finally:
        with _REFRESH_LOCK:
            _REFRESH_RUNNING = False


def queue_refresh() -> bool:
    """Start background refresh if not already running. Returns True if started."""
    global _REFRESH_RUNNING
    with _REFRESH_LOCK:
        if _REFRESH_RUNNING:
            return False
        _REFRESH_RUNNING = True
    t = threading.Thread(target=_run_refresh, name="sk-exploration-cost-refresh", daemon=True)
    t.start()
    return True


def payload_for_api(*, force_refresh: bool = False) -> Dict[str, Any]:
    """UI/API payload: snapshot if present; queue refresh when missing or forced."""
    snap = load_snapshot()
    if force_refresh or snap is None:
        started = queue_refresh()
        if snap is None:
            return {
                "status": "building",
                "refresh_running": True,
                "yesterday": None,
                "total_7d": None,
                "daily": [],
                "campaign_count": 0,
                "error": None,
                "message": "Fetching exploration spend from SK API…",
                "active_only": not _ALL_STATUSES,
                "source": "by-campaign",
            }
        out = dict(snap)
        out["status"] = "stale"
        out["refresh_running"] = refresh_running() or started
        out["cached"] = True
        return out

    out = dict(snap)
    out["status"] = out.get("status") or "ready"
    out["refresh_running"] = refresh_running()
    out["cached"] = True
    return out
