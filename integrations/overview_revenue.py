"""
Keitaro revenue for overview dashboard (postback / conversion revenue from reports).

Uses ``KeitaroClient.build_report`` (``POST admin_api/v1/report/build``). Parses common
response shapes and metric keys (``revenue``, ``payout``, etc.).
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional

from integrations.keitaro import KeitaroClient, KeitaroClientError

logger = logging.getLogger(__name__)

# Prefer a single canonical revenue column when present (avoid double-counting).
_REV_KEYS_ORDER = (
    "revenue",
    "campaign_revenue",
    "sale_revenue",
    "conversions_revenue",
    "sales_revenue",
    "payout",
    "earn",
)


def _lower_keys(d: dict) -> Dict[str, Any]:
    return {str(k).lower(): v for k, v in d.items()}


def _row_revenue(row: dict) -> float:
    lk = _lower_keys(row)
    for k in _REV_KEYS_ORDER:
        if k in lk and lk[k] is not None:
            try:
                return float(lk[k])
            except (TypeError, ValueError):
                continue
    return 0.0


def _rows_from_report(report: Any) -> List[dict]:
    if not isinstance(report, dict):
        return []
    for k in ("rows", "data", "result", "body"):
        v = report.get(k)
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return [x for x in v if isinstance(x, dict)]
    for k in ("total", "totals", "summary"):
        v = report.get(k)
        if isinstance(v, dict) and any(str(x).lower() in _REV_KEYS_ORDER for x in v.keys()):
            return [v]
    return []


def _extract_day_key(row: dict) -> str:
    lk = _lower_keys(row)
    for k in ("day", "date", "datetime", "click_date", "conversion_date"):
        if k in lk and lk[k] is not None:
            s = str(lk[k]).strip()
            if len(s) >= 10 and s[4] == "-" and s[7] == "-":
                return s[:10]
    return ""


def _report_payloads(d_from: date, d_to: date) -> List[Dict[str, Any]]:
    a = d_from.isoformat()
    b = d_to.isoformat()
    return [
        {
            "range": {"from": f"{a} 00:00:00", "to": f"{b} 23:59:59"},
            "grouping": ["day"],
            "metrics": ["revenue", "conversions"],
        },
        {
            "range": {"from": a, "to": b},
            "grouping": ["day"],
            "metrics": ["revenue", "conversions"],
        },
        {
            "range": {"interval": "custom", "from": f"{a} 00:00:00", "to": f"{b} 23:59:59"},
            "grouping": ["day"],
            "metrics": ["revenue", "conversions"],
        },
    ]


def _sum_revenue_from_report(report: Any, *, yesterday: Optional[date] = None) -> tuple[float, float]:
    """
    Returns (yesterday_revenue, mtd_total_revenue).
    If ``yesterday`` is None, yesterday_revenue is 0.0 and only mtd total is meaningful.
    """
    rows = _rows_from_report(report)

    mtd = 0.0
    yday = 0.0
    y_str = yesterday.isoformat() if yesterday else ""

    for row in rows:
        r = _row_revenue(row)
        mtd += r
        dk = _extract_day_key(row)
        if y_str and dk == y_str:
            yday += r

    return yday, mtd


def fetch_keitaro_revenue_overview(
    *,
    yesterday: date,
    mtd_start: date,
    mtd_end: date,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    ``mtd_end`` should be yesterday (inclusive). Uses one MTD report when possible.

    Returns:
      ``{"yesterday": float|None, "mtd": float|None, "error": str|None}``
    """
    if mtd_start > mtd_end:
        return {"yesterday": 0.0, "mtd": 0.0, "error": None}

    if not (api_key or "").strip():
        return {"yesterday": None, "mtd": None, "error": "KEITARO_API_KEY not set"}

    client = KeitaroClient(base_url=base_url, api_key=api_key)
    last_err: str | None = None
    y_rev = 0.0
    mtd_rev = 0.0

    for payload in _report_payloads(mtd_start, mtd_end):
        try:
            report = client.build_report(payload)
            y_rev, mtd_rev = _sum_revenue_from_report(report, yesterday=yesterday)
            if yesterday < mtd_start or yesterday > mtd_end:
                y_rev = 0.0
            break
        except KeitaroClientError as e:
            last_err = str(e)
            logger.info("Keitaro report attempt failed: %s", last_err[:200])
        except Exception as e:
            last_err = str(e)
            logger.info("Keitaro report attempt failed: %s", last_err[:200])
    else:
        return {"yesterday": None, "mtd": None, "error": last_err or "Keitaro report failed"}

    if mtd_start <= yesterday <= mtd_end and y_rev == 0.0 and mtd_rev > 0.0:
        y_only = [
            {
                "range": {"from": f"{yesterday.isoformat()} 00:00:00", "to": f"{yesterday.isoformat()} 23:59:59"},
                "metrics": ["revenue", "conversions"],
            },
            {
                "range": {"from": yesterday.isoformat(), "to": yesterday.isoformat()},
                "metrics": ["revenue", "conversions"],
            },
        ]
        for payload in y_only:
            try:
                report_y = client.build_report(payload)
                y2, _ = _sum_revenue_from_report(report_y, yesterday=None)
                if y2 > 0:
                    y_rev = y2
                    break
                y3, _ = _sum_revenue_from_report(report_y, yesterday=yesterday)
                if y3 > 0:
                    y_rev = y3
                    break
            except Exception:
                continue

    return {"yesterday": round(y_rev, 4), "mtd": round(mtd_rev, 4), "error": None}
