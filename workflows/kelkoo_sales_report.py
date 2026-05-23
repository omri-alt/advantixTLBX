"""
Month-to-date sales export for late-conversion workbook (all feeds).

Legacy daily ``SalesReport_*`` / 7-day tabs are no longer written here; see
``late_conversion_sales.refresh_mtd_sales_sheets``.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List

from config import KELKOO_LATE_SALES_SPREADSHEET_ID
from late_conversion_sales import refresh_mtd_sales_sheets, sheet_title_a1_range

logger = logging.getLogger(__name__)

# Re-export for any code that imported sale-row helpers from this module.
from late_conversion_sales import fetch_all_mtd_sales, late_sale_date_window  # noqa: F401

# Kelkoo TSV parsing (used by late_conversion_sales Kelkoo fetch).
import csv
from datetime import datetime, timedelta, timezone
from io import StringIO


def _utc_yesterday_iso() -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()


def _click_id_from_raw_row(r: Dict[str, str], feed_index: int) -> str:
    from config import kelkoo_raw_report_uses_custom1_subid

    if kelkoo_raw_report_uses_custom1_subid(feed_index=feed_index):
        for key in ("custom1", "Custom1"):
            v = r.get(key)
            if v is not None and str(v).strip():
                return str(v).strip()
    for key in ("publisherClickId", "PublisherClickId"):
        v = r.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def _sale_rows_from_tsv(tsv: str, feed_index: int) -> List[Dict[str, Any]]:
    """Rows with ``sale`` true and ``leadValid`` true (aligned with daily postback parsing)."""
    reader = csv.DictReader(StringIO(tsv), delimiter="\t")
    out: List[Dict[str, Any]] = []
    for row in reader:
        if not isinstance(row, dict):
            continue
        r = {str(k): ("" if v is None else str(v)) for k, v in row.items()}
        if (r.get("sale") or "").lower() != "true":
            continue
        if (r.get("leadValid") or "").lower() != "true":
            continue
        click_id = _click_id_from_raw_row(r, feed_index)
        if not click_id:
            continue
        sale_value = (r.get("saleValueInUsd") or "0").strip() or "0"
        cpc = (r.get("leadEstimatedRevenueInUsd") or "0").strip() or "0"
        merchant = (r.get("merchantName") or r.get("MerchantName") or "").strip()
        country = (r.get("country") or r.get("Country") or "").strip()
        dt_raw = (r.get("dateTime") or r.get("DateTime") or "")[:10]
        out.append(
            {
                "merchant": merchant,
                "date": dt_raw,
                "click_id": click_id,
                "lead_valid": (r.get("leadValid") or "").strip(),
                "sale": (r.get("sale") or "").strip(),
                "sale_value_usd": sale_value,
                "cpc": cpc,
                "country": country,
            }
        )
    return out


def run_yesterday_sales_reports(
    service: Any,
    *,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
  Refresh month-to-date ``SalesMTD_{feed}_{YYYY-MM}`` tabs (replaces legacy daily/7-day reports).
    """
    sid = (KELKOO_LATE_SALES_SPREADSHEET_ID or "").strip()
    if not sid:
        msg = "KELKOO_LATE_SALES_SPREADSHEET_ID is empty; skipping sales report."
        logger.warning(msg)
        print(f"   {msg}")
        return {"ok": False, "error": "no_spreadsheet", "tabs": []}

    if dry_run:
        month_start, hi_date, yesterday, month_key = late_sale_date_window()
        print(
            f"   [dry-run] Would refresh SalesMTD_* tabs for {month_start} .. {hi_date} "
            f"(month {month_key}); yesterday={yesterday} excluded."
        )
        return {"ok": True, "dry_run": True, "month_key": month_key}

    summary = refresh_mtd_sales_sheets(service, sid, dry_run=False)
    print(f"   MTD sales sheets refreshed: window {summary.get('sale_window')}")
    for feed, info in (summary.get("feeds") or {}).items():
        print(f"      {feed}: {info.get('rows', 0)} rows → {info.get('tab')}")
    return {"ok": True, "mtd": summary}
