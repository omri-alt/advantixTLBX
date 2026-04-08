#!/usr/bin/env python3
"""
Ensure every country (geo) has 3 offers ({geo}_product1, _product2, _product3) and that
each flow is linked to its 3 offers. Offer action_payload uses the Kelkoo URL template
with geo and placeholder product URLs (update later from Google Sheets).

  python ensure_geo_offers.py              # default campaign
  python ensure_geo_offers.py --campaign 1
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv
load_dotenv()

from config import KEITARO_BASE_URL, KEITARO_API_KEY, KEITARO_CAMPAIGN_ID, KEITARO_CAMPAIGN_ALIAS
from geos import SUPPORTED_GEOS, GEO_LABELS
from assistance import (
    get_campaigns_data,
    find_campaign_by_alias_or_name,
    get_campaign_streams,
    ensure_geo_offers,
    set_flow_offers,
    flow_name_to_geo,
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

    campaign_arg = None
    for i, a in enumerate(sys.argv[1:], 1):
        if a == "--campaign" and i < len(sys.argv) - 1:
            campaign_arg = sys.argv[i + 1]
            break

    campaign_id = get_campaign_id(campaign_arg)
    if campaign_id is None:
        print("Error: No campaign found. Set KEITARO_CAMPAIGN_ID=1 in .env or pass --campaign 1")
        sys.exit(1)

    total_geos = len(SUPPORTED_GEOS)
    print(f"Ensuring 3 offers per geo and attaching to flows (campaign_id={campaign_id})")
    print()

    # 1) Ensure 3 offers exist for each geo (skip if already exist)
    geo_offer_ids = {}
    for i, geo in enumerate(SUPPORTED_GEOS, 1):
        try:
            ids = ensure_geo_offers(geo, skip_if_exists=True)
            geo_offer_ids[geo] = ids
            print(f"  [{i}/{total_geos}] {geo}: offers {ids} ({geo}_product1, {geo}_product2, {geo}_product3)")
        except KeitaroClientError as e:
            print(f"  [{i}/{total_geos}] {geo}: ERROR {e}")
            if e.response_body:
                print(f"      {e.response_body[:400]}")
            sys.exit(1)
        except ValueError as e:
            print(f"  [{i}/{total_geos}] {geo}: ERROR {e}")
            sys.exit(1)

    # 2) Get streams and attach the 3 offers to each flow by geo
    streams = get_campaign_streams(campaign_id)
    print()
    print("Attaching offers to flows:")
    for s in streams:
        name = s.get("name") or ""
        sid = s.get("id")
        geo = flow_name_to_geo(name)
        if geo is None:
            print(f"  Flow id={sid} name={name!r}: no geo match, skip")
            continue
        if geo not in geo_offer_ids:
            print(f"  Flow id={sid} name={name!r} geo={geo}: no offers, skip")
            continue
        offer_ids = geo_offer_ids[geo]
        try:
            set_flow_offers(sid, offer_ids)
            print(f"  Flow id={sid} name={name!r} geo={geo}: set 3 offers (ids={offer_ids})")
        except KeitaroClientError as e:
            print(f"  Flow id={sid} name={name!r}: ERROR {e}")
            if e.response_body:
                print(f"      {e.response_body[:400]}")
            sys.exit(1)

    print()
    print("Done. Each country has 3 offers and flows are linked. Update product URLs later (e.g. from Sheets).")


if __name__ == "__main__":
    main()
