#!/usr/bin/env python3
"""
Add a country flow to the campaign (like the Spain flow: country filter + offers, equal share).

  python add_flow.py es Spain              # add flow "Spain" with country es
  python add_flow.py uk "United Kingdom"  # add flow for UK
  python add_flow.py --campaign 1 fr France

Geos: ae, at, au, be, ca, ch, cz, de, es, fi, fr, gr, hk, hu, ie, it, kr, mx, nl, no, nz, pl, pt, ro, se, sg, sk, uk, us, dk
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv
load_dotenv()

from config import KEITARO_BASE_URL, KEITARO_API_KEY, KEITARO_CAMPAIGN_ID, KEITARO_CAMPAIGN_ALIAS
from assistance import get_campaigns_data, find_campaign_by_alias_or_name, add_country_flow
from integrations.keitaro import KeitaroClientError


def main():
    if not KEITARO_BASE_URL or not KEITARO_API_KEY:
        print("Error: Set KEITARO_BASE_URL and KEITARO_API_KEY in .env")
        sys.exit(1)

    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    campaign_arg = None
    for i, a in enumerate(sys.argv[1:], 1):
        if a == "--campaign" and i < len(sys.argv) - 1:
            campaign_arg = sys.argv[i + 1]
            break

    if len(args) < 2:
        print("Usage: python add_flow.py <country_code> <flow_name>")
        print("       python add_flow.py ES Spain")
        print("       python add_flow.py UK \"United Kingdom\"")
        print("       python add_flow.py --campaign 1 FR France")
        sys.exit(1)

    country_code = args[0].upper()
    flow_name = args[1]

    campaign_id = None
    if campaign_arg:
        campaign_id = int(campaign_arg)
    elif KEITARO_CAMPAIGN_ID:
        campaign_id = int(KEITARO_CAMPAIGN_ID)
    else:
        campaigns = get_campaigns_data()
        alias = KEITARO_CAMPAIGN_ALIAS or "HrQBXp"
        camp = find_campaign_by_alias_or_name(campaigns, alias=alias, name=alias)
        if not camp:
            camp = campaigns[0] if campaigns else None
        if camp:
            campaign_id = int(camp["id"])
        else:
            print("Error: No campaign found. Set KEITARO_CAMPAIGN_ID=1 in .env or pass --campaign 1")
            sys.exit(1)

    print(f"Adding flow: {flow_name!r} (country={country_code}) to campaign_id={campaign_id}")
    try:
        result = add_country_flow(campaign_id, country_code, flow_name, skip_if_exists=True)
        if result.get("_skipped"):
            print(f"Flow {flow_name!r} already exists (id={result.get('id')}), skipping.")
        else:
            print(f"Done. Flow id={result.get('id')} name={result.get('name')}")
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except KeitaroClientError as e:
        print(f"Keitaro API error: {e}")
        if e.response_body:
            print(e.response_body[:600])
        sys.exit(1)


if __name__ == "__main__":
    main()
