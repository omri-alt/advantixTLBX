"""
Persisted Ecomnia console UI state: last-run timestamps, cached audit tables, geo list snapshot.

Path: ``runtime/ecomnia_console_state.json`` (override with ``ECOMNIA_CONSOLE_STATE_PATH``).
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent


def state_path() -> Path:
    raw = (os.getenv("ECOMNIA_CONSOLE_STATE_PATH") or "").strip()
    if raw:
        return Path(raw)
    return ROOT / "runtime" / "ecomnia_console_state.json"


def _empty_console_state() -> Dict[str, Any]:
    return {
        "runs": {},
        "whitelist_campaign_source_rows": [],
        "geo_map": {},
        "action_items_block": {},
    }


def load_state() -> Dict[str, Any]:
    p = state_path()
    if not p.exists():
        return _empty_console_state()
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return _empty_console_state()
        data.setdefault("runs", {})
        data.setdefault("whitelist_campaign_source_rows", [])
        data.setdefault("geo_map", {})
        data.setdefault("action_items_block", {})
        return data
    except Exception as e:
        logger.warning("ecomnia console state read failed %s: %s", p, e)
        return _empty_console_state()


def save_state_atomic(data: Dict[str, Any]) -> None:
    p = state_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def record_run(kind: str, ok: bool, detail: Optional[Dict[str, Any]] = None) -> None:
    data = load_state()
    data.setdefault("runs", {})
    data["runs"][kind] = {
        "at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ok": bool(ok),
        "detail": detail or {},
    }
    save_state_atomic(data)


def update_cache(
    *,
    geo_map: Optional[Dict[str, Any]] = None,
    whitelist_rows: Optional[List[Dict[str, Any]]] = None,
    action_items_block: Optional[Dict[str, Any]] = None,
) -> None:
    data = load_state()
    if geo_map is not None:
        data["geo_map"] = geo_map
    if whitelist_rows is not None:
        data["whitelist_campaign_source_rows"] = whitelist_rows
    if action_items_block is not None:
        data["action_items_block"] = action_items_block
    save_state_atomic(data)
