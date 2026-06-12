#!/usr/bin/env python3
"""Bootstrap Keitaro feed-balance note templates on multi-feed brand campaigns."""
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

from integrations.keitaro_feed_balance import bootstrap_campaign_notes


def main() -> int:
    ap = argparse.ArgumentParser(description="Write feed-balance config templates into Keitaro campaign notes.")
    ap.add_argument("--dry-run", action="store_true", help="Print actions without updating Keitaro")
    ap.add_argument("--force", action="store_true", help="Overwrite existing feed-balance config blocks")
    ap.add_argument("--campaign-id", type=int, action="append", dest="campaign_ids", help="Limit to campaign id(s)")
    ap.add_argument("--json", action="store_true", help="Print full JSON summary")
    args = ap.parse_args()

    result = bootstrap_campaign_notes(
        dry_run=args.dry_run,
        force=args.force,
        campaign_ids=args.campaign_ids,
    )
    if args.json:
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print(
            f"discovered={result['discovered']} updated={result['updated']} "
            f"skipped_has_config={result['skipped_has_config']} errors={result['errors']}"
        )
        for row in result.get("rows") or []:
            if row.get("action") not in ("updated", "dry_run"):
                continue
            url = row.get("url") or "(empty — fill manually)"
            geo = row.get("geo") or "(empty — fill manually)"
            print(f"  [{row.get('campaignId')}] {row.get('name')}: geo={geo} url={url}")
    return 1 if result.get("errors") else 0


if __name__ == "__main__":
    raise SystemExit(main())
