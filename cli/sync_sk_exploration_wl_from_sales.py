#!/usr/bin/env python3
"""Sync SKtrackExploration.wl from Keitaro SaleOur/LateSale conversions (SK sub_id_6 tags)."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:
    pass

from integrations.autoserver.sk_exploration_wl_sync import (
    normalize_exploration_wl_format,
    sync_exploration_wl_from_keitaro_sales,
)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Append converting SK sources to SKtrackExploration.wl and QualityWL from Keitaro sales."
    )
    ap.add_argument("--dry-run", action="store_true", help="Report only; do not write sheet")
    ap.add_argument("--lookback-days", type=int, default=0, help="Override SK_EXPLORATION_WL_LOOKBACK_DAYS")
    ap.add_argument(
        "--normalize-wl-format",
        action="store_true",
        help="Rewrite all wl cells to single-quote format ['sub1', 'sub2'] (no sales scan)",
    )
    args = ap.parse_args()

    if args.normalize_wl_format:
        result = normalize_exploration_wl_format(dry_run=args.dry_run)
    else:
        lookback = args.lookback_days if args.lookback_days > 0 else None
        result = sync_exploration_wl_from_keitaro_sales(dry_run=args.dry_run, lookback_days=lookback)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
