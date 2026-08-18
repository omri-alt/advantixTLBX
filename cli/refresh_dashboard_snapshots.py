#!/usr/bin/env python3
"""Rebuild Control Center dashboard caches (overview, domain-demand, Blend cap)."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Refresh homepage overview + domain-demand + Blend-cap snapshots."
    )
    parser.add_argument("--reason", default="cli", help="Tag written into refresh state / logs.")
    args = parser.parse_args()
    try:
        from dotenv import load_dotenv
    except ImportError:
        load_dotenv = None  # type: ignore[assignment]
    if load_dotenv:
        load_dotenv(ROOT / ".env")

    from integrations.dashboard_snapshots import refresh_dashboard_snapshots

    result = refresh_dashboard_snapshots(reason=(args.reason or "cli").strip())
    print(json.dumps(result, indent=2, default=str))
    errors = [j for j in (result.get("jobs") or []) if j.get("status") == "error"]
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
