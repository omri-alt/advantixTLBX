#!/usr/bin/env python3
"""One-off test: strip shopli rainotest shell from AT offers on NIPUHIM-feed1/2/5."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from assistance import get_geo_offers_sorted, strip_nipuhim_rain_shell, update_offer_action_payload
from integrations.keitaro_child_campaigns import nipuhim_child_campaign_id_for_account

RAIN_PREFIX = "https://shopli.city/rainotest?rain="


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--geo", default="at", help="2-letter geo (default: at)")
    parser.add_argument("--apply", action="store_true", help="Push stripped URLs to Keitaro")
    args = parser.parse_args()
    geo = (args.geo or "at").strip().lower()[:2]

    updated = 0
    for account in (1, 2, 5):
        cid, feed_key, feed_prefix = nipuhim_child_campaign_id_for_account(account)
        offers = get_geo_offers_sorted(geo, feed_prefix=feed_prefix)
        print(f"=== {feed_key} (campaign {cid}): {len(offers)} {geo.upper()} offers ===")
        for offer in offers:
            oid = int(offer["id"])
            name = offer.get("name") or ""
            payload = (offer.get("action_payload") or "").strip()
            if not payload.startswith(RAIN_PREFIX) and strip_nipuhim_rain_shell(payload) == payload:
                print(f"  {name} id={oid}: no rainotest prefix — skip")
                continue
            new_payload = strip_nipuhim_rain_shell(payload)
            print(f"  {name} id={oid}:")
            print(f"    was: {payload[:100]}...")
            print(f"    now: {new_payload[:100]}...")
            if args.apply:
                update_offer_action_payload(oid, new_payload)
                updated += 1
        print()

    if args.apply:
        print(f"Applied {updated} offer URL update(s).")
    else:
        print("Dry-run only. Re-run with --apply to push to Keitaro.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
