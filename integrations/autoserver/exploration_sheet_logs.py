"""
Shared ``logs`` tab helpers for EC and SK optimizer workbooks (Google Sheets).

Row shape matches legacy EC ``logs`` tab: ``campId``, ``campName``, ``verify``, ``date``, ``response``.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

from integrations.autoserver import gdocs_as as gd

logger = logging.getLogger(__name__)
_ENSURED_LOG_TABS: set[str] = set()
_LOG_WRITE_DISABLED_UNTIL: Dict[str, float] = {}

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
    """Append one row to ``logs`` (creates tab + headers if missing).

    Durable mode: append directly (no full-sheet read + rewrite) with light retry on
    transient quota/rate failures.
    """
    sid = (spreadsheet_id or "").strip()
    if not sid:
        return
    try:
        if sid not in _ENSURED_LOG_TABS:
            ensure_logs_worksheet(sid)
            _ENSURED_LOG_TABS.add(sid)
    except Exception as e:
        logger.warning("exploration logs read failed (%s): %s", sid[:12], e)
        return
    date_s = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    row = [
        str(camp_id or ""),
        str(camp_name or ""),
        str(verify or "")[:4000],
        date_s,
        _fmt_response(response),
    ]
    now_ts = time.time()
    if _LOG_WRITE_DISABLED_UNTIL.get(sid, 0.0) > now_ts:
        return
    wait_s = 0.6
    for attempt in range(3):
        try:
            ws = gd.client.open_by_key(sid).worksheet(LOG_TAB)
            ws.append_row(row, value_input_option="USER_ENTERED")
            return
        except Exception as e:
            msg = str(e)
            if ("429" in msg or "Quota exceeded" in msg or "Rate Limit" in msg) and attempt < 2:
                time.sleep(wait_s)
                wait_s *= 2.0
                continue
            if "429" in msg or "Quota exceeded" in msg or "Rate Limit" in msg:
                _LOG_WRITE_DISABLED_UNTIL[sid] = time.time() + 120.0
            logger.warning("exploration logs write failed (%s): %s", sid[:12], e)
            return


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
