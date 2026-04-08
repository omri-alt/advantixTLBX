#!/usr/bin/env python3
"""
Dump your current Keitaro setup (campaign + flows + offers) to JSON.
Use the output to see real payloads for create/update.

  python dump_setup.py                    # if only one campaign, use it; else "campaign_setup"
  python dump_setup.py HrQBXp             # dump this campaign and its flows + all offers
  python dump_setup.py --out my_dump.json # write to specific file (default: payloads_dump.json)

Output: one JSON file with campaign, streams, offers. Use these payloads as reference.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv
load_dotenv()

from config import KEITARO_BASE_URL, KEITARO_API_KEY
from assistance import get_campaigns_data, get_full_setup
from integrations.keitaro import KeitaroClientError


def main():
    if not KEITARO_BASE_URL or not KEITARO_API_KEY:
        print("Error: Set KEITARO_BASE_URL and KEITARO_API_KEY in .env")
        sys.exit(1)

    out_file = "payloads_dump.json"
    skip_next = False
    args = []
    for i, a in enumerate(sys.argv[1:], 1):
        if skip_next:
            skip_next = False
            continue
        if a == "--out" and i < len(sys.argv) - 1:
            out_file = sys.argv[i + 1]
            skip_next = True
            continue
        if a.startswith("--out="):
            out_file = a.split("=", 1)[1]
            continue
        if not a.startswith("--"):
            args.append(a)

    campaign_alias = args[0] if args else None
    if campaign_alias is None:
        if KEITARO_CAMPAIGN_ALIAS:
            campaign_alias = KEITARO_CAMPAIGN_ALIAS
            print(f"No campaign specified; using KEITARO_CAMPAIGN_ALIAS: {campaign_alias!r}")
        else:
            campaigns = get_campaigns_data()
            if len(campaigns) == 1:
                campaign_alias = campaigns[0].get("alias") or campaigns[0].get("name") or "campaign_setup"
                print(f"No campaign specified; using only campaign: {campaign_alias!r}")
            else:
                campaign_alias = "campaign_setup"
    print(f"Dumping setup for campaign: {campaign_alias!r}")
    print(f"Output file: {out_file}")
    print()

    try:
        data = get_full_setup(campaign_alias)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except KeitaroClientError as e:
        print(f"Keitaro API error: {e}")
        if e.response_body:
            print(e.response_body[:500])
        sys.exit(1)

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print("Done.")
    print(f"  Campaign: id={data['campaign'].get('id')} alias={data['campaign'].get('alias')} name={data['campaign'].get('name')}")
    print(f"  Streams:  {len(data['streams'])}")
    print(f"  Offers:   {len(data['offers'])}")
    print(f"  Edit {out_file} to inspect real payloads for create/update.")


if __name__ == "__main__":
    main()
