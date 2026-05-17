#!/usr/bin/env python3
"""Rebuild ``runtime/overview_snapshot.json`` (same work as ``POST /api/overview/refresh``)."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild overview snapshot from live APIs.")
    parser.add_argument("--reason", default="cli", help="Tag for overview_refresh_state.json")
    parser.add_argument(
        "--started-utc",
        default="",
        help="Must match the started_utc written when the job was queued (subprocess mode).",
    )
    args = parser.parse_args()

    try:
        from dotenv import load_dotenv
    except ImportError:
        load_dotenv = None  # type: ignore[assignment]
    if load_dotenv:
        load_dotenv(ROOT / ".env")

    from integrations.overview_snapshot import run_overview_refresh_job

    started = (args.started_utc or "").strip() or None
    run_overview_refresh_job(reason=(args.reason or "cli").strip(), started_utc=started)
    st_path = ROOT / "runtime" / "overview_refresh_state.json"
    if st_path.is_file():
        print(st_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
