"""
Assemble ``/api/overview`` JSON: Keitaro revenue + traffic-source costs + totals.

Also exposes **per-slice** payloads for ``GET /api/overview/slice/...`` so the UI can load
each source independently.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Tuple

from config import KEITARO_API_KEY, KEITARO_BASE_URL
from integrations.overview_costs import (
    fetch_ecomnia_cost,
    fetch_sk_cost,
    fetch_zeropark_cost,
)
from integrations.overview_revenue import fetch_keitaro_revenue_overview


def _nz(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def overview_period() -> Tuple[date, date, date]:
    """UTC calendar: yesterday, first of month, MTD end (= yesterday)."""
    now = datetime.now(timezone.utc).date()
    yesterday = now - timedelta(days=1)
    mtd_start = now.replace(day=1)
    mtd_end = yesterday
    return yesterday, mtd_start, mtd_end


def ranges_dict(yesterday: date, mtd_start: date, mtd_end: date) -> Dict[str, str]:
    return {
        "yesterday": yesterday.isoformat(),
        "mtd_from": mtd_start.isoformat(),
        "mtd_to": mtd_end.isoformat(),
    }


def _slice_envelope(slice_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
    yesterday, mtd_start, mtd_end = overview_period()
    return {
        "slice": slice_id,
        "data": data,
        "ranges": ranges_dict(yesterday, mtd_start, mtd_end),
        "as_of_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def slice_revenue() -> Dict[str, Any]:
    yesterday, mtd_start, mtd_end = overview_period()

    def safe_revenue() -> Dict[str, Any]:
        try:
            return fetch_keitaro_revenue_overview(
                yesterday=yesterday,
                mtd_start=mtd_start,
                mtd_end=mtd_end,
                base_url=KEITARO_BASE_URL,
                api_key=KEITARO_API_KEY,
            )
        except Exception as e:
            return {"yesterday": None, "mtd": None, "error": str(e)}

    return _slice_envelope("revenue", safe_revenue())


def slice_zeropark() -> Dict[str, Any]:
    yesterday, mtd_start, mtd_end = overview_period()
    try:
        zp = fetch_zeropark_cost(yesterday=yesterday, mtd_start=mtd_start, mtd_end=mtd_end)
    except Exception as e:
        zp = {"yesterday": None, "mtd": None, "error": str(e)}
    return _slice_envelope("zeropark", zp)


def slice_sourceknowledge() -> Dict[str, Any]:
    yesterday, mtd_start, mtd_end = overview_period()
    try:
        sk = fetch_sk_cost(yesterday=yesterday, mtd_start=mtd_start, mtd_end=mtd_end)
    except Exception as e:
        sk = {"yesterday": None, "mtd": None, "error": str(e)}
    return _slice_envelope("sourceknowledge", sk)


def slice_ecomnia() -> Dict[str, Any]:
    yesterday, mtd_start, mtd_end = overview_period()
    try:
        ec = fetch_ecomnia_cost(yesterday=yesterday, mtd_start=mtd_start, mtd_end=mtd_end)
    except Exception as e:
        ec = {"yesterday": None, "mtd": None, "error": str(e)}
    return _slice_envelope("ecomnia", ec)


def build_overview_json() -> Dict[str, Any]:
    yesterday, mtd_start, mtd_end = overview_period()

    def safe_cost(fn):
        try:
            return fn(yesterday=yesterday, mtd_start=mtd_start, mtd_end=mtd_end)
        except Exception as e:
            return {"yesterday": None, "mtd": None, "error": str(e)}

    def safe_revenue():
        try:
            return fetch_keitaro_revenue_overview(
                yesterday=yesterday,
                mtd_start=mtd_start,
                mtd_end=mtd_end,
                base_url=KEITARO_BASE_URL,
                api_key=KEITARO_API_KEY,
            )
        except Exception as e:
            return {"yesterday": None, "mtd": None, "error": str(e)}

    with ThreadPoolExecutor(max_workers=4) as pool:
        f_rev = pool.submit(safe_revenue)
        f_zp = pool.submit(safe_cost, fetch_zeropark_cost)
        f_sk = pool.submit(safe_cost, fetch_sk_cost)
        f_ec = pool.submit(safe_cost, fetch_ecomnia_cost)
        revenue = f_rev.result()
        zp = f_zp.result()
        sk = f_sk.result()
        ec = f_ec.result()

    ty = _nz(zp.get("yesterday")) + _nz(sk.get("yesterday")) + _nz(ec.get("yesterday"))
    tm = _nz(zp.get("mtd")) + _nz(sk.get("mtd")) + _nz(ec.get("mtd"))

    ry = revenue.get("yesterday")
    rm = revenue.get("mtd")
    net_y = _nz(ry) - ty
    net_m = _nz(rm) - tm

    out: Dict[str, Any] = {
        "revenue": {
            "yesterday": revenue.get("yesterday"),
            "mtd": revenue.get("mtd"),
            "error": revenue.get("error"),
        },
        "costs": {
            "zeropark": zp,
            "sourceknowledge": sk,
            "ecomnia": ec,
            "thrillion": None,
            "yesshh": None,
        },
        "total_cost": {"yesterday": round(ty, 4), "mtd": round(tm, 4)},
        "net": {"yesterday": round(net_y, 4), "mtd": round(net_m, 4)},
        "as_of_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ranges": ranges_dict(yesterday, mtd_start, mtd_end),
    }
    return out
