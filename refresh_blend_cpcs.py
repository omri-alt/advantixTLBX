#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None  # type: ignore[assignment]


def _parse_date(value: str) -> date:
    return date.fromisoformat((value or "").strip())


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh Blend sheet CPCs from Keitaro EPC with sheet fallback.")
    parser.add_argument("--dry-run", action="store_true", help="Compute updates without writing to the sheet.")
    parser.add_argument("--date-from", default="", help="Optional report start date (YYYY-MM-DD).")
    parser.add_argument("--date-to", default="", help="Optional report end date (YYYY-MM-DD).")
    args = parser.parse_args()

    if load_dotenv:
        load_dotenv(ROOT / ".env")

    from integrations.blend_cpc_refresh import refresh_blend_cpcs

    d_from = _parse_date(args.date_from) if args.date_from else None
    d_to = _parse_date(args.date_to) if args.date_to else None
    payload = refresh_blend_cpcs(dry_run=bool(args.dry_run), d_from=d_from, d_to=d_to)
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
