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

from integrations.sk_exploration_efficiency_audit import run_efficiency_audit


def main() -> None:
    result = run_efficiency_audit()
    print(f"Done in {result.get('fetch_seconds')}s — {result.get('campaigns_audited')} campaigns")
    print(f"Lifetime garbage: {result.get('lifetime_garbage_pct')}%")
    print(f"Yesterday garbage: {result.get('yesterday_garbage_pct')}%")
    sheets = result.get("sheets") or {}
    print(f"Sheets: {sheets.get('campaigns_tab')}, {sheets.get('summary_tab')}")


if __name__ == "__main__":
    main()
