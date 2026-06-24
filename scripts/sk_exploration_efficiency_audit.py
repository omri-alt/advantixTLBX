#!/usr/bin/env python3
"""CLI: run SK buying-efficiency audit and write SK tools sheet tabs."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:
    pass

from integrations.sk_exploration_efficiency_audit import (  # noqa: E402
    _release_audit_lock,
    _run_efficiency_audit_body,
    _try_acquire_audit_lock,
    _write_state,
    _utc_now,
)


def main() -> None:
    if not _try_acquire_audit_lock():
        print("Buying efficiency audit is already running (lock file present).")
        raise SystemExit(2)
    try:
        _write_state(
            {
                "status": "running",
                "progress": "starting",
                "started_at": _utc_now(),
                "error": None,
            }
        )
        result = _run_efficiency_audit_body()
    except Exception as e:
        _write_state(
            {
                "status": "error",
                "error": str(e),
                "finished_at_utc": _utc_now(),
                "progress": "failed",
            }
        )
        raise
    finally:
        _release_audit_lock()

    print(f"Done in {result.get('fetch_seconds')}s — {result.get('campaigns_audited')} campaigns")
    print(f"Lifetime garbage: {result.get('lifetime_garbage_pct')}%")
    print(f"Yesterday garbage: {result.get('yesterday_garbage_pct')}%")
    sheets = result.get("sheets") or {}
    print(f"Sheets: {sheets.get('campaigns_tab')}, {sheets.get('summary_tab')}")


if __name__ == "__main__":
    main()
