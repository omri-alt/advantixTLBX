#!/usr/bin/env python3
"""
Replace a substring in all offers' action_payload (e.g. in the offer URL).

  python replace_offer_url_part.py "publisherClickId={clickid}" "publisherClickId={subid}"
  python replace_offer_url_part.py "publisherClickId={clickid}" "publisherClickId={subid}" --dry-run
  python replace_offer_url_part.py "publisherClickId={clickid}" "publisherClickId={subid}" --geo uk

Requires KEITARO_BASE_URL and KEITARO_API_KEY in .env.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv
load_dotenv()

from config import KEITARO_BASE_URL, KEITARO_API_KEY
from assistance import get_offers_data, update_offer_action_payload
from integrations.keitaro import KeitaroClientError


def main():
    if not KEITARO_BASE_URL or not KEITARO_API_KEY:
        print("Error: Set KEITARO_BASE_URL and KEITARO_API_KEY in .env")
        sys.exit(1)

    argv = sys.argv[1:]
    dry_run = "--dry-run" in argv
    geo_filter = None
    args = []
    i = 0
    while i < len(argv):
        if argv[i] == "--dry-run":
            i += 1
            continue
        if argv[i] == "--geo" and i + 1 < len(argv):
            geo_filter = argv[i + 1].strip().lower()
            i += 2
            continue
        args.append(argv[i])
        i += 1

    if len(args) < 2:
        print("Usage: python replace_offer_url_part.py <FROM> <TO> [--dry-run] [--geo XX]")
        print("  FROM = substring to find in action_payload")
        print("  TO   = replacement string")
        print("Example: python replace_offer_url_part.py \"publisherClickId={clickid}\" \"publisherClickId={subid}\"")
        sys.exit(1)

    from_str = args[0]
    to_str = args[1]

    print(f"Replace in offer URLs: {from_str!r} -> {to_str!r}")
    if geo_filter:
        print(f"Filter: geo = {geo_filter}")
    if dry_run:
        print("DRY RUN (no changes will be made)")
    print()

    offers = get_offers_data()
    if geo_filter:
        prefix = f"{geo_filter}_product"
        offers = [o for o in offers if (o.get("name") or "").startswith(prefix)]
    updated = 0
    for offer in offers:
        payload = offer.get("action_payload") or ""
        if from_str not in payload:
            continue
        new_payload = payload.replace(from_str, to_str)
        if new_payload == payload:
            continue
        name = offer.get("name") or f"id={offer.get('id')}"
        if dry_run:
            print(f"  [dry-run] would update {name} id={offer.get('id')}")
            updated += 1
            continue
        try:
            update_offer_action_payload(offer["id"], new_payload)
            print(f"  updated {name} id={offer['id']}")
            updated += 1
        except KeitaroClientError as e:
            print(f"  ERROR {name}: {e}")
            if e.response_body:
                print(f"    {e.response_body[:300]}")
            sys.exit(1)

    print()
    print(f"Done. {'Would update' if dry_run else 'Updated'} {updated} offers.")


if __name__ == "__main__":
    main()
