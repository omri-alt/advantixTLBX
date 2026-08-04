"""
Append-only run log for scheduled postback jobs (Kelkoo, Adexa/Yadore, …).

Separate from resume state and the UI last-run snapshot.
"""
from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

_lock = threading.Lock()


def _utc_iso(dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _resolve_path(raw: Union[str, Path]) -> Path:
    p = Path(raw)
    return p if p.is_absolute() else (Path(__file__).resolve().parents[1] / p)


def append_run_log(
    entry: Dict[str, Any],
    *,
    log_path: Union[str, Path],
    max_entries: int = 500,
) -> None:
    """Append one run entry to ``log_path``; caps list length."""
    row = dict(entry)
    row.setdefault("at_utc", _utc_iso())
    path = _resolve_path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with _lock:
        rows: List[Dict[str, Any]] = []
        if path.exists():
            try:
                raw = path.read_text(encoding="utf-8")
                rows = json.loads(raw) if raw.strip() else []
            except Exception as e:
                logger.warning("Scheduled postback run log corrupt %s; resetting: %s", path, e)
                rows = []
        if not isinstance(rows, list):
            rows = []
        rows.append(row)
        cap = max(10, int(max_entries))
        if len(rows) > cap:
            rows = rows[-cap:]
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)


def read_run_log_newest_first(
    log_path: Union[str, Path],
    *,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    path = _resolve_path(log_path)
    if not path.exists():
        return []
    try:
        rows = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(rows, list):
        return []
    rev = list(reversed(rows))
    if limit <= 0:
        return rev
    return rev[:limit]


# --- Kelkoo helpers (compat) -------------------------------------------------


def _kelkoo_log_path() -> Path:
    from config import KELKOO_DAILY_POSTBACK_RUN_LOG_PATH

    return _resolve_path(KELKOO_DAILY_POSTBACK_RUN_LOG_PATH)


def append_run(entry: Dict[str, Any]) -> None:
    """Append one Kelkoo daily postback run entry."""
    from config import KELKOO_DAILY_POSTBACK_RUN_LOG_MAX, KELKOO_DAILY_POSTBACK_RUN_LOG_PATH

    append_run_log(
        entry,
        log_path=KELKOO_DAILY_POSTBACK_RUN_LOG_PATH,
        max_entries=int(KELKOO_DAILY_POSTBACK_RUN_LOG_MAX),
    )


def read_entries_newest_first(limit: int = 20) -> List[Dict[str, Any]]:
    from config import KELKOO_DAILY_POSTBACK_RUN_LOG_PATH

    return read_run_log_newest_first(KELKOO_DAILY_POSTBACK_RUN_LOG_PATH, limit=limit)
