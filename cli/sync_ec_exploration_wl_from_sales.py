#!/usr/bin/env python3
"""Sync trackExploration.wl from Keitaro SaleOur/LateSale conversions (EC sub_id_6 tags)."""
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

from integrations.autoserver.ec_exploration_wl_sync import (  # noqa: E402
    refresh_ec_quality_wl,
    sync_ec_exploration_wl_from_keitaro_sales,
)


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Append converting EC sources to trackExploration.wl + ECQualityWL from Keitaro sales. "
            "Reactivates via absolute cpcbysource (not bidFactor)."
        )
    )
    ap.add_argument("--dry-run", action="store_true", help="Report only; do not write sheet/API")
    ap.add_argument("--lookback-days", type=int, default=0, help="Override EC_EXPLORATION_WL_LOOKBACK_DAYS")
    ap.add_argument(
        "--refresh-quality-wl-only",
        action="store_true",
        help="Only refresh ECQualityWL clicks/bid/status (no sales scan)",
    )
    args = ap.parse_args()

    if args.refresh_quality_wl_only:
        result = refresh_ec_quality_wl()
    else:
        lookback = args.lookback_days if args.lookback_days > 0 else None
        result = sync_ec_exploration_wl_from_keitaro_sales(
            dry_run=args.dry_run, lookback_days=lookback
        )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
