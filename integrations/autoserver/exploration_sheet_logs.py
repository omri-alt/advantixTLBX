"""
Shared ``logs`` tab helpers for EC and SK optimizer workbooks (Google Sheets).

Row shape matches legacy EC ``logs`` tab: ``campId``, ``campName``, ``verify``, ``date``, ``response``.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from integrations.autoserver import gdocs_as as gd

logger = logging.getLogger(__name__)

LOG_TAB = "logs"
LOG_HEADERS = ["campId", "campName", "verify", "date", "response"]


def ensure_logs_worksheet(spreadsheet_id: str) -> None:
    if not (spreadsheet_id or "").strip():
        return
    gd.append_missing_headers_row1(
        spreadsheet_id.strip(), LOG_TAB, LOG_HEADERS, create_if_missing=True
    )


def _fmt_response(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, (dict, list)):
        try:
            return json.dumps(val, ensure_ascii=False)[:8000]
        except Exception:
            return str(val)[:8000]
    return str(val)[:8000]


def append_exploration_log_row(
    spreadsheet_id: str,
    *,
    camp_id: str,
    camp_name: str,
    verify: str,
    response: Any = "",
) -> None:
    """Append one row to ``logs`` (creates tab + headers if missing)."""
    sid = (spreadsheet_id or "").strip()
    if not sid:
        return
    try:
        ensure_logs_worksheet(sid)
        rows = gd.read_sheet_withID(sid, LOG_TAB)
    except Exception as e:
        logger.warning("exploration logs read failed (%s): %s", sid[:12], e)
        return
    date_s = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows.append(
        {
            "campId": str(camp_id or ""),
            "campName": str(camp_name or ""),
            "verify": str(verify or "")[:4000],
            "date": date_s,
            "response": _fmt_response(response),
        }
    )
    try:
        gd.create_or_update_sheet_from_dicts_withID(sid, LOG_TAB, rows)
    except Exception as e:
        logger.warning("exploration logs write failed (%s): %s", sid[:12], e)


def fetch_log_tail(spreadsheet_id: str, *, limit: int = 100) -> List[Dict[str, str]]:
    """Last ``limit`` data rows from ``logs`` (sheet order; newest assumed at bottom)."""
    sid = (spreadsheet_id or "").strip()
    if not sid or limit <= 0:
        return []
    try:
        rows = gd.read_sheet_withID(sid, LOG_TAB)
    except Exception:
        return []
    if not rows:
        return []
    tail = rows[-limit:]
    out: List[Dict[str, str]] = []
    for r in tail:
        if not isinstance(r, dict):
            continue
        out.append(
            {
                "campId": str(r.get("campId") or ""),
                "campName": str(r.get("campName") or ""),
                "verify": str(r.get("verify") or ""),
                "date": str(r.get("date") or ""),
                "response": str(r.get("response") or ""),
            }
        )
    return out
