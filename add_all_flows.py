#!/usr/bin/env python3
"""
Ensure a country flow exists for every geo in the list. Runs in a loop;
existing flows are skipped (add_country_flow(skip_if_exists=True)).

  python add_all_flows.py              # use default campaign (KEITARO_CAMPAIGN_ID or HrQBXp)
  python add_all_flows.py --campaign 1
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
    add_country_flow,
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

    total = len(SUPPORTED_GEOS)
    print(f"Ensuring flows for {total} countries in campaign_id={campaign_id}")
    print()

    created = 0
    skipped = 0
    for i, geo in enumerate(SUPPORTED_GEOS, 1):
        flow_name = GEO_LABELS.get(geo, geo.upper())
        try:
            result = add_country_flow(
                campaign_id, geo, flow_name, skip_if_exists=True
            )
            if result.get("_skipped"):
                skipped += 1
                print(f"  [{i}/{total}] {geo} {flow_name!r}: already exists (id={result.get('id')}), skip")
            else:
                created += 1
                print(f"  [{i}/{total}] {geo} {flow_name!r}: created (id={result.get('id')})")
        except KeitaroClientError as e:
            print(f"  [{i}/{total}] {geo} {flow_name!r}: ERROR {e}")
            if e.response_body:
                print(f"      {e.response_body[:400]}")
            sys.exit(1)
        except ValueError as e:
            print(f"  [{i}/{total}] {geo} {flow_name!r}: ERROR {e}")
            sys.exit(1)

    print()
    print(f"Done. Created {created}, skipped {skipped}, total {total}.")


if __name__ == "__main__":
    main()
