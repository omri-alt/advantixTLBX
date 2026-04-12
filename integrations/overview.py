"""
Assemble ``/api/overview`` JSON: Keitaro revenue + traffic-source costs + totals.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict

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


def build_overview_json() -> Dict[str, Any]:
    now = datetime.now(timezone.utc).date()
    yesterday = now - timedelta(days=1)
    mtd_start = now.replace(day=1)
    mtd_end = yesterday

    def safe_cost(fn):
        try:
            return fn(yesterday=yesterday, mtd_start=mtd_start, mtd_end=mtd_end)
        except Exception as e:
            return {"yesterday": None, "mtd": None, "error": str(e)}

    try:
        revenue = fetch_keitaro_revenue_overview(
            yesterday=yesterday,
            mtd_start=mtd_start,
            mtd_end=mtd_end,
            base_url=KEITARO_BASE_URL,
            api_key=KEITARO_API_KEY,
        )
    except Exception as e:
        revenue = {"yesterday": None, "mtd": None, "error": str(e)}

    zp = safe_cost(fetch_zeropark_cost)
    sk = safe_cost(fetch_sk_cost)
    ec = safe_cost(fetch_ecomnia_cost)

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
        "ranges": {
            "yesterday": yesterday.isoformat(),
            "mtd_from": mtd_start.isoformat(),
            "mtd_to": mtd_end.isoformat(),
        },
    }
    return out
