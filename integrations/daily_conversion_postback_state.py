"""
Persistent JSON state for daily conversion postbacks (resume without doubling).

File path: ``DAILY_CONVERSION_POSTBACK_STATE_PATH`` from config (default under ``runtime/``).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

logger = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def default_geo_state() -> Dict[str, Any]:
    return {
        "status": "pending",  # pending | partial | done | error
        "fetch_http_status": None,
        "fetch_ok": None,
        "fetch_at_utc": None,
        "rows_in_file": None,
        "next_row_index": 0,
        "row_stage": None,  # None | "after_click" (sale row: click sent, sale pending)
        "eligible_rows": 0,
        "postbacks_sent": 0,
        "last_error": None,
        "last_updated_utc": None,
        "completed_at_utc": None,
    }


def default_flat_run_state() -> Dict[str, Any]:
    """Single global list (Adexa / Yadore): one report, resume by index."""
    return {
        "status": "pending",
        "fetch_at_utc": None,
        "total_items": None,
        "next_index": 0,
        "row_stage": None,
        "postbacks_sent": 0,
        "last_error": None,
        "last_updated_utc": None,
        "completed_at_utc": None,
    }


def load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"v": 1, "sources": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {"v": 1, "sources": {}}
        data.setdefault("v", 1)
        data.setdefault("sources", {})
        return data
    except Exception as e:
        logger.warning("State read failed %s: %s — starting fresh", path, e)
        return {"v": 1, "sources": {}}


def save_state_atomic(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def reset_source_date(path: Path, source_key: str, report_date: str) -> None:
    data = load_state(path)
    sources = data.setdefault("sources", {})
    src = sources.get(source_key)
    if isinstance(src, dict) and report_date in src:
        del src[report_date]
    save_state_atomic(path, data)
