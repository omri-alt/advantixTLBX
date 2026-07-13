#!/usr/bin/env python3
"""Wire hub blend offers on campaign 94 + sub_id_15=domain flows on child campaigns."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from config import KEITARO_HUB_BLEND_DOMAIN_ENABLED  # noqa: E402
from integrations.hub_blend_child_flows import run_hub_blend_child_flows  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(description="Hub blend child flow wiring.")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--date", metavar="YYYY-MM-DD")
    args = p.parse_args()

    if not KEITARO_HUB_BLEND_DOMAIN_ENABLED:
        print("KEITARO_HUB_BLEND_DOMAIN_ENABLED=0 — skipped.")
        return 0

    result = run_hub_blend_child_flows(date_str=args.date, dry_run=args.dry_run)
    for line in result.get("logs") or []:
        print(line)
    print(f"status={result.get('status')} pool={result.get('pool_blend_rows')} quality={result.get('quality_merchants')}")
    return 0 if result.get("status") in ("ok", "dry_run") else 1


if __name__ == "__main__":
    raise SystemExit(main())
