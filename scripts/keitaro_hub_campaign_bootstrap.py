#!/usr/bin/env python3
"""
Bootstrap Keitaro hub campaign (Domain / id 94): child campaigns + hub offers + geo routing.

  python scripts/keitaro_hub_campaign_bootstrap.py              # dry-run
  python scripts/keitaro_hub_campaign_bootstrap.py --apply      # create + wire live
  python scripts/keitaro_hub_campaign_bootstrap.py --apply --skip-child-streams

  python scripts/keitaro_hub_campaign_bootstrap.py --apply --rewire-only

Creates 6 Blend child campaigns (clone of Blend id 2) and 6 Nipuhim child campaigns
(clone of Nipuh / HrQBXp id 1 — static PLA offers per country, not KL-Main feeds).
Hub stream weights honor ``KEITARO_HUB_ACTIVE_FEEDS`` (default kelkoo1,kelkoo2,kelkoo5);
inactive feeds stay attached at share 0.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from config import KEITARO_API_KEY, KEITARO_BASE_URL, KEITARO_HUB_ACTIVE_FEEDS, KEITARO_HUB_CAMPAIGN_ID
from integrations.keitaro import KeitaroClientError
from integrations.keitaro_hub import format_weights_table, run_hub_bootstrap, run_hub_rewire_weights


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Bootstrap Keitaro hub campaign 94 routing.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes (default is dry-run only).",
    )
    parser.add_argument(
        "--hub-campaign-id",
        type=int,
        default=KEITARO_HUB_CAMPAIGN_ID,
        help=f"Hub campaign id (default {KEITARO_HUB_CAMPAIGN_ID}).",
    )
    parser.add_argument(
        "--rewire-only",
        action="store_true",
        help="Only re-apply hub stream weights (no child/offer creation).",
    )
    parser.add_argument(
        "--skip-child-streams",
        action="store_true",
        help="Skip creating geo desktop/mobile streams on new Blend child campaigns.",
    )
    parser.add_argument(
        "--state-path",
        default="",
        help="Override state JSON path (default data/keitaro_hub_state.json).",
    )
    args = parser.parse_args()

    if not KEITARO_BASE_URL or not KEITARO_API_KEY:
        print("Error: set KEITARO_BASE_URL and KEITARO_API_KEY in .env")
        return 1

    dry_run = not args.apply
    print(f"Keitaro hub bootstrap — hub campaign id={args.hub_campaign_id}")
    print(f"Active feeds: {', '.join(KEITARO_HUB_ACTIVE_FEEDS)}")
    print(f"Mode: {'DRY-RUN' if dry_run else 'APPLY'}"
          f"{' (rewire weights only)' if args.rewire_only else ''}")
    print()

    try:
        if args.rewire_only:
            result = run_hub_rewire_weights(
                dry_run=dry_run,
                hub_campaign_id=args.hub_campaign_id,
                state_path=args.state_path or None,
            )
        else:
            result = run_hub_bootstrap(
                dry_run=dry_run,
                skip_child_streams=args.skip_child_streams,
                hub_campaign_id=args.hub_campaign_id,
                state_path=args.state_path or None,
            )
    except (ValueError, KeitaroClientError) as e:
        print(f"Error: {e}")
        return 1

    for line in result.get("logs") or []:
        print(line)

    print()
    print(format_weights_table(result.get("weights") or {}))
    print()
    if dry_run:
        print("Dry-run complete. Re-run with --apply to push to Keitaro.")
    elif args.rewire_only:
        print("Hub weights re-applied.")
    else:
        print(f"Done. State saved ({len(result.get('child_campaigns') or {})} children, "
              f"{len(result.get('hub_offers') or {})} hub offers).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
