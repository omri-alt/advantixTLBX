"""
Traffic-source spend for the overview dashboard (Zeropark, SourceKnowledge, Ecomnia).

Uses **account-level** APIs only (no per-campaign loops for EC/SK).

Each public ``fetch_*`` returns::

    {"yesterday": float | None, "mtd": float | None, "error": str | None}
"""
from __future__ import annotations

import hashlib
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from config import (
    EC_ADVERTISER_KEY,
    EC_AUTH_KEY,
    EC_SECRET_KEY,
    ECOMNIA_REPORT_BASE,
    KEYZP,
    SK_ACCOUNT_STATS_URL,
    SK_OVERVIEW_MAX_CAMPAIGNS,
    SOURCEKNOWLEDGE_API_KEY,
)

logger = logging.getLogger(__name__)

ZEROPARK_PANEL = "https://panel.zeropark.com"
SK_API_BASE = "https://api.sourceknowledge.com/affiliate/v2"
# Tight timeouts so a hung SK node does not block the whole dashboard for minutes.
_SK_HTTP_TIMEOUT = 22


def _ec_report_authtoken(secret: str, start: str, end: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    raw = f"{ts}{start}{end}{secret}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest().upper()


def _ec_auth_params_report(secret: str, start: str, end: str) -> Dict[str, str]:
    return {
        "advertiserkey": EC_ADVERTISER_KEY,
        "authkey": EC_AUTH_KEY,
        "authtoken": _ec_report_authtoken(secret, start, end),
    }


def _zp_dd_mm_yyyy(d: date) -> str:
    """Zeropark CUSTOM interval: ``dd/mm/yyyy`` (per panel API docs)."""
    return f"{d.day:02d}/{d.month:02d}/{d.year}"


def _zp_summary_spent(token: str, payload: Dict[str, Any]) -> Optional[float]:
    """
    GET ``/api/stats/campaign/all`` with ``api-token`` header.
    Returns ``summary.spent`` when present.
    """
    url = f"{ZEROPARK_PANEL}/api/stats/campaign/all"
    headers = {"api-token": token, "Accept": "application/json"}
    try:
        r = requests.get(url, headers=headers, params=payload, timeout=90)
    except requests.RequestException as e:
        logger.warning("Zeropark stats API: %s", e)
        return None
    if not r.ok:
        logger.warning("Zeropark stats API HTTP %s: %s", r.status_code, (r.text or "")[:200])
        return None
    try:
        data = r.json()
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    summary = data.get("summary")
    if isinstance(summary, dict) and summary.get("spent") is not None:
        try:
            return float(summary["spent"])
        except (TypeError, ValueError):
            pass
    if data.get("spent") is not None:
        try:
            return float(data["spent"])
        except (TypeError, ValueError):
            pass
    return None


def fetch_zeropark_cost(*, yesterday: date, mtd_start: date, mtd_end: date) -> Dict[str, Any]:
    token = (KEYZP or "").strip()
    if not token:
        return {"yesterday": None, "mtd": None, "error": "KEYZP not set"}

    y_spent = _zp_summary_spent(
        token,
        {"interval": "YESTERDAY", "page": 0, "limit": 10000},
    )

    mtd_spent: Optional[float] = None
    if mtd_start <= mtd_end:
        mtd_spent = _zp_summary_spent(
            token,
            {
                "interval": "CUSTOM",
                "startDate": _zp_dd_mm_yyyy(mtd_start),
                "endDate": _zp_dd_mm_yyyy(mtd_end),
                "page": 0,
                "limit": 10000,
            },
        )
    else:
        mtd_spent = 0.0

    err: str | None = None
    if y_spent is None and mtd_spent is None:
        err = "Zeropark /api/stats/campaign/all did not return summary.spent (check KEYZP and API access)"
    elif y_spent is None:
        err = "Zeropark: yesterday interval returned no spend (MTD may still be valid)"
    elif mtd_spent is None and mtd_start <= mtd_end:
        err = "Zeropark: CUSTOM MTD interval returned no spend"

    return {
        "yesterday": None if y_spent is None else round(float(y_spent), 4),
        "mtd": None if mtd_spent is None else round(float(mtd_spent), 4),
        "error": err,
    }


def _sk_headers(api_key: str) -> Dict[str, str]:
    return {"X-API-KEY": api_key, "Accept": "application/json"}


def _sk_extract_spend_from_payload(data: Any) -> Optional[float]:
    """Best-effort: ``summary.spent``, root ``spent``, or sum of ``items[].spend``."""
    if not isinstance(data, dict):
        return None
    if isinstance(data.get("summary"), dict):
        s = data["summary"].get("spent")
        if s is not None:
            try:
                return float(s)
            except (TypeError, ValueError):
                pass
    if data.get("spent") is not None:
        try:
            return float(data["spent"])
        except (TypeError, ValueError):
            pass
    items = data.get("items")
    if isinstance(items, list) and items:
        total = 0.0
        any_spend = False
        for it in items:
            if not isinstance(it, dict):
                continue
            v = it.get("spend")
            if v is None:
                continue
            try:
                total += float(v)
                any_spend = True
            except (TypeError, ValueError):
                continue
        if any_spend:
            return total
    return None


def _sk_get_aggregate_spend(api_key: str, d0: str, d1: str) -> Optional[float]:
    """
    Single account-level request when possible.

    1. ``SK_ACCOUNT_STATS_URL`` from env — ``{from}`` and ``{to}`` replaced with ``YYYY-MM-DD``.
    2. A few common relative paths on ``SK_API_BASE`` (may 404 on your tenant).
    """
    headers = _sk_headers(api_key)
    urls: List[str] = []
    tpl = (SK_ACCOUNT_STATS_URL or "").strip()
    if tpl:
        urls.append(tpl.format(**{"from": d0, "to": d1}))
    urls.extend(
        [
            f"{SK_API_BASE}/stats/summary?from={d0}&to={d1}",
            f"{SK_API_BASE}/stats?from={d0}&to={d1}",
        ]
    )
    for url in urls:
        if not url:
            continue
        try:
            r = requests.get(url, headers=headers, timeout=_SK_HTTP_TIMEOUT)
        except requests.RequestException as e:
            logger.info("SK overview GET %s: %s", url[:80], e)
            continue
        if r.status_code == 429:
            time.sleep(2.0)
            r = requests.get(url, headers=headers, timeout=_SK_HTTP_TIMEOUT)
        if r.status_code != 200:
            continue
        try:
            data = r.json()
        except Exception:
            continue
        if isinstance(data, dict) and data.get("error"):
            continue
        val = _sk_extract_spend_from_payload(data)
        if val is not None:
            return val
    return None


def _sk_list_campaign_ids_paginated(api_key: str) -> List[int]:
    headers = _sk_headers(api_key)
    ids: List[int] = []
    page = 1
    while page < 5000:
        try:
            r = requests.get(
                f"{SK_API_BASE}/campaigns",
                headers=headers,
                params={"page": page},
                timeout=_SK_HTTP_TIMEOUT,
            )
        except requests.RequestException:
            break
        if r.status_code == 429:
            time.sleep(2.0)
            continue
        if r.status_code != 200:
            break
        try:
            data = r.json()
        except Exception:
            break
        if isinstance(data, dict) and data.get("error"):
            break
        items = data.get("items") if isinstance(data, dict) else None
        if not isinstance(items, list) or not items:
            break
        for it in items:
            if isinstance(it, dict) and it.get("id") is not None:
                try:
                    ids.append(int(it["id"]))
                except (TypeError, ValueError):
                    pass
        page += 1
        time.sleep(0.06)
    return ids


def _sk_by_publisher_spend_campaign(api_key: str, campaign_id: int, d0: str, d1: str) -> float:
    """Sum ``spend`` across all ``by-publisher`` pages (``hasMore`` / ``page``)."""
    headers = _sk_headers(api_key)
    url = f"{SK_API_BASE}/stats/campaigns/{campaign_id}/by-publisher"
    total = 0.0
    page = 1
    for _ in range(500):
        try:
            r = requests.get(
                url,
                headers=headers,
                params={"from": d0, "to": d1, "page": page},
                timeout=_SK_HTTP_TIMEOUT,
            )
        except requests.RequestException:
            break
        if r.status_code == 429:
            time.sleep(2.0)
            continue
        if r.status_code != 200:
            break
        try:
            data = r.json()
        except Exception:
            break
        if isinstance(data, dict) and data.get("error"):
            break
        items = data.get("items") if isinstance(data, dict) else None
        if not isinstance(items, list):
            break
        for it in items:
            if isinstance(it, dict) and it.get("spend") is not None:
                try:
                    total += float(it["spend"])
                except (TypeError, ValueError):
                    pass
        if not isinstance(data, dict) or not data.get("hasMore"):
            break
        page += 1
        time.sleep(0.05)
    return total


def _sk_apply_campaign_cap(cids: List[int]) -> tuple[List[int], Optional[str]]:
    cap = int(SK_OVERVIEW_MAX_CAMPAIGNS or 0)
    if cap <= 0 or len(cids) <= cap:
        return cids, None
    msg = (
        f"SK spend sampled from {cap} of {len(cids)} campaigns "
        "(set SK_ACCOUNT_STATS_URL or SK_OVERVIEW_MAX_CAMPAIGNS=0 for full coverage)."
    )
    return cids[:cap], msg


def _sk_spend_by_publisher_batched(api_key: str, cids: List[int], d0: date, d1: date) -> float:
    """Sum by-publisher spend for ``[d0, d1]`` in ≤90-day chunks (SK stats window)."""
    if not cids or d0 > d1:
        return 0.0
    total = 0.0
    cur = d0
    while cur <= d1:
        chunk_end = min(cur + timedelta(days=89), d1)
        d0s, d1s = cur.isoformat(), chunk_end.isoformat()
        for cid in cids:
            total += _sk_by_publisher_spend_campaign(api_key, cid, d0s, d1s)
            time.sleep(0.02)
        cur = chunk_end + timedelta(days=1)
    return total


def fetch_sk_cost(*, yesterday: date, mtd_start: date, mtd_end: date) -> Dict[str, Any]:
    api_key = (SOURCEKNOWLEDGE_API_KEY or "").strip()
    if not api_key:
        return {"yesterday": None, "mtd": None, "error": "SOURCEKNOWLEDGE_API_KEY / KEYSK not set"}

    y0s = yesterday.isoformat()
    y_agg = _sk_get_aggregate_spend(api_key, y0s, y0s)
    if mtd_start <= mtd_end:
        m_agg = _sk_get_aggregate_spend(api_key, mtd_start.isoformat(), mtd_end.isoformat())
    else:
        m_agg = None

    if y_agg is not None and (mtd_start > mtd_end or m_agg is not None):
        mtd_val = 0.0 if mtd_start > mtd_end else float(m_agg)
        return {"yesterday": round(float(y_agg), 4), "mtd": round(mtd_val, 4), "error": None}

    cids = _sk_list_campaign_ids_paginated(api_key)
    cids, cap_msg = _sk_apply_campaign_cap(cids)

    def spend_range(d0: date, d1: date, pre: Optional[float]) -> float:
        if pre is not None:
            return float(pre)
        return _sk_spend_by_publisher_batched(api_key, cids, d0, d1)

    with ThreadPoolExecutor(max_workers=2) as pool:
        fy = pool.submit(spend_range, yesterday, yesterday, y_agg)
        if mtd_start <= mtd_end:
            fm = pool.submit(spend_range, mtd_start, mtd_end, m_agg)
            mtd_spent = fm.result()
        else:
            mtd_spent = 0.0
        y_spent = fy.result()

    return {
        "yesterday": round(float(y_spent), 4),
        "mtd": round(float(mtd_spent), 4),
        "error": cap_msg,
    }


def _ec_sum_adv_stats_account(start: str, end: str) -> float:
    """
    GET ``adv-stats-by-date`` **without** ``campaignid`` — advertiser totals (max 60 days per call).
    """
    base = (ECOMNIA_REPORT_BASE or "https://report.ecomnia.com").rstrip("/")
    url = f"{base}/adv-stats-by-date"
    params = {
        **_ec_auth_params_report(EC_SECRET_KEY, start, end),
        "startdate": start,
        "enddate": end,
    }
    r = requests.get(url, params=params, headers={"content-type": "application/json"}, timeout=90)
    if r.status_code != 200:
        logger.warning("Ecomnia adv-stats-by-date %s..%s: HTTP %s", start, end, r.status_code)
        return 0.0
    try:
        data = r.json()
    except Exception:
        return 0.0
    stats = data.get("stats") if isinstance(data, dict) else None
    if not isinstance(stats, list):
        return 0.0
    total = 0.0
    for row in stats:
        if not isinstance(row, dict):
            continue
        try:
            total += float(row.get("spend") or row.get("cost") or 0)
        except (TypeError, ValueError):
            continue
    return total


def _ec_sum_range_account(d0: date, d1: date) -> float:
    """Sum spend over ``[d0, d1]`` in chunks of at most 60 days (Ecomnia API limit)."""
    if d0 > d1:
        return 0.0
    total = 0.0
    cur = d0
    while cur <= d1:
        chunk_end = min(cur + timedelta(days=59), d1)
        total += _ec_sum_adv_stats_account(cur.isoformat(), chunk_end.isoformat())
        cur = chunk_end + timedelta(days=1)
    return total


def fetch_ecomnia_cost(*, yesterday: date, mtd_start: date, mtd_end: date) -> Dict[str, Any]:
    if not (EC_ADVERTISER_KEY and EC_AUTH_KEY and EC_SECRET_KEY):
        return {"yesterday": None, "mtd": None, "error": "EC advertiser credentials not set"}

    y_tot = _ec_sum_range_account(yesterday, yesterday)
    if mtd_start > mtd_end:
        m_tot = 0.0
    else:
        m_tot = _ec_sum_range_account(mtd_start, mtd_end)

    return {"yesterday": round(y_tot, 4), "mtd": round(m_tot, 4), "error": None}
