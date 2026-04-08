#!/usr/bin/env python3
"""
Fix the country filter on an existing flow (e.g. the UK flow that was created without a country).

  python fix_flow_country.py "United Kingdom" uk   # set flow to country uk
  python fix_flow_country.py 4 es                   # set stream_id 4 to country es

Uses campaign 1 (HrQBXp) to find flow by name if you pass the flow name.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv
load_dotenv()

from config import KEITARO_BASE_URL, KEITARO_API_KEY, KEITARO_CAMPAIGN_ID, KEITARO_CAMPAIGN_ALIAS
from assistance import get_campaigns_data, find_campaign_by_alias_or_name, get_campaign_streams, set_flow_country_filter
from integrations.keitaro import KeitaroClientError


def main():
    if not KEITARO_BASE_URL or not KEITARO_API_KEY:
        print("Error: Set KEITARO_BASE_URL and KEITARO_API_KEY in .env")
        sys.exit(1)

    if len(sys.argv) < 3:
        print("Usage: python fix_flow_country.py <flow_name_or_stream_id> <country_code>")
        print('       python fix_flow_country.py "United Kingdom" GB')
        print("       python fix_flow_country.py 4 GB")
        sys.exit(1)

    flow_arg = sys.argv[1]
    country_code = sys.argv[2].strip()

    stream_id = None
    if flow_arg.isdigit():
        stream_id = int(flow_arg)
        print(f"Setting stream_id={stream_id} country filter to {country_code}")
    else:
        flow_name = flow_arg
        campaigns = get_campaigns_data()
        alias = KEITARO_CAMPAIGN_ALIAS or "HrQBXp"
        camp = find_campaign_by_alias_or_name(campaigns, alias=alias, name=alias)
        if not camp:
            camp = campaigns[0] if campaigns else None
        if not camp:
            print("Error: No campaign found")
            sys.exit(1)
        cid = int(camp["id"])
        streams = get_campaign_streams(cid)
        for s in streams:
            if (s.get("name") or "").strip().lower() == flow_name.strip().lower():
                stream_id = int(s["id"])
                print(f"Found flow id={stream_id} {s.get('name')!r}, setting country to {country_code}")
                break
        if stream_id is None:
            print(f"Error: No flow named {flow_name!r}. Found: {[s.get('name') for s in streams]}")
            sys.exit(1)

    try:
        result = set_flow_country_filter(stream_id, country_code)
        print(f"Done. Flow id={result.get('id')} filters={result.get('filters')}")
    except KeitaroClientError as e:
        print(f"Keitaro API error: {e}")
        if e.response_body:
            print(e.response_body[:600])
        sys.exit(1)


if __name__ == "__main__":
    main()
