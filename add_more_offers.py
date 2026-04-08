#!/usr/bin/env python3
"""
Create 3 more offers for a geo (or for all geos): {geo}_product(n+1), (n+2), (n+3).
Then attach them to the flow so the flow has equal share across all offers.

  python add_more_offers.py uk              # add 3 offers for UK (uk_product4,5,6 if 1,2,3 exist)
  python add_more_offers.py all            # add 3 more offers for every geo
  python add_more_offers.py es --count 5    # add 5 more for ES
  python add_more_offers.py uk --campaign 1
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv
load_dotenv()

from config import KEITARO_BASE_URL, KEITARO_API_KEY, KEITARO_CAMPAIGN_ID, KEITARO_CAMPAIGN_ALIAS
from geos import SUPPORTED_GEOS
from assistance import (
    get_campaigns_data,
    find_campaign_by_alias_or_name,
    get_campaign_streams,
    create_next_geo_offers,
    set_flow_offers,
    flow_name_to_geo,
    stream_offer_ids,
    get_geo_offer_numbers,
)
from integrations.keitaro import KeitaroClientError


def get_campaign_id(campaign_arg=None):
    if campaign_arg is not None:
        return int(campaign_arg)
    if KEITARO_CAMPAIGN_ID:
        return int(KEITARO_CAMPAIGN_ID)
    campaigns = get_campaigns_data()
    alias = KEITARO_CAMPAIGN_ALIAS or "HrQBXp"
    camp = find_campaign_by_alias_or_name(campaigns, alias=alias, name=alias)
    if not camp:
        camp = campaigns[0] if campaigns else None
    if not camp:
        return None
    return int(camp["id"])


def main():
    if not KEITARO_BASE_URL or not KEITARO_API_KEY:
        print("Error: Set KEITARO_BASE_URL and KEITARO_API_KEY in .env")
        sys.exit(1)

    args = []
    campaign_arg = None
    count = 3
    i = 1
    while i < len(sys.argv):
        a = sys.argv[i]
        if a == "--campaign" and i + 1 < len(sys.argv):
            campaign_arg = sys.argv[i + 1]
            i += 2
            continue
        if a == "--count" and i + 1 < len(sys.argv):
            count = int(sys.argv[i + 1])
            i += 2
            continue
        if not a.startswith("--"):
            args.append(a)
        i += 1

    if not args:
        print("Usage: python add_more_offers.py <geo|all> [--count 3] [--campaign 1]")
        print("       python add_more_offers.py uk")
        print("       python add_more_offers.py all")
        sys.exit(1)

    geo_arg = args[0].strip().lower()
    if geo_arg == "all":
        geos = list(SUPPORTED_GEOS)
    else:
        if geo_arg not in SUPPORTED_GEOS:
            print(f"Error: geo {geo_arg!r} not in supported list. Use one of: {SUPPORTED_GEOS}")
            sys.exit(1)
        geos = [geo_arg]

    campaign_id = get_campaign_id(campaign_arg)
    if campaign_id is None:
        print("Error: No campaign found. Set KEITARO_CAMPAIGN_ID=1 in .env or pass --campaign 1")
        sys.exit(1)

    streams = get_campaign_streams(campaign_id)
    stream_by_geo = {}
    for s in streams:
        g = flow_name_to_geo(s.get("name") or "")
        if g is not None:
            stream_by_geo[g] = s

    print(f"Adding {count} more offers per geo for: {geos}")
    print(f"Campaign id={campaign_id}")
    print()

    for geo in geos:
        try:
            existing = get_geo_offer_numbers(geo)
            start = (max(existing) + 1) if existing else 1
            names = [f"{geo}_product{i}" for i in range(start, start + count)]
            print(f"  {geo}: creating {names} ...")
            new_ids = create_next_geo_offers(geo, count=count)
            print(f"  {geo}: created {new_ids}")

            if geo not in stream_by_geo:
                print(f"  {geo}: no flow found, skip attaching")
                continue
            stream = stream_by_geo[geo]
            sid = stream["id"]
            current_ids = stream_offer_ids(stream)
            all_ids = current_ids + new_ids
            set_flow_offers(sid, all_ids)
            print(f"  {geo}: flow id={sid} now has {len(all_ids)} offers (added {len(new_ids)})")
        except KeitaroClientError as e:
            print(f"  {geo}: ERROR {e}")
            if e.response_body:
                print(f"      {e.response_body[:400]}")
            sys.exit(1)
        except ValueError as e:
            print(f"  {geo}: ERROR {e}")
            sys.exit(1)

    print()
    print("Done.")


if __name__ == "__main__":
    main()
