#!/usr/bin/env python3
"""
Simple test script: run from project root to verify Keitaro connection and list campaigns.

  python run_test.py                 # fetch and print campaigns
  python run_test.py --streams X     # list flows (streams) for campaign X
  python run_test.py --clone X       # clone campaign with alias/name X
"""
import json
import sys
from pathlib import Path

# Ensure .env is loaded when run from project root
sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv
load_dotenv()

from config import KEITARO_BASE_URL, KEITARO_API_KEY
from assistance import get_campaigns_data, get_campaigns_then_clone_setup, get_campaign_streams_by_alias
from integrations.keitaro import KeitaroClientError


def main():
    if not KEITARO_BASE_URL or not KEITARO_API_KEY:
        print("Error: Set KEITARO_BASE_URL and KEITARO_API_KEY in .env (see .env.example)")
        sys.exit(1)

    do_clone = len(sys.argv) >= 3 and sys.argv[1] == "--clone"
    do_streams = len(sys.argv) >= 3 and sys.argv[1] == "--streams"
    alias_or_name = sys.argv[2] if (do_clone or do_streams) else None

    print("Keitaro test")
    print("  Base URL:", KEITARO_BASE_URL)
    print()

    try:
        print("Fetching campaigns...")
        campaigns = get_campaigns_data()
        print(f"  OK – got {len(campaigns)} campaign(s)\n")

        if not campaigns:
            print("No campaigns yet. Create one in the Keitaro UI or via POST /api/v1/workflows/create-campaign")
            return

        print("Campaigns:")
        for c in campaigns:
            cid = c.get("id")
            alias = c.get("alias", "")
            name = c.get("name", "")
            state = c.get("state", "")
            print(f"  id={cid}  alias={alias!r}  name={name!r}  state={state}")

        if do_streams and alias_or_name:
            print()
            print(f"Fetching streams (flows) for campaign {alias_or_name!r}...")
            streams = get_campaign_streams_by_alias(alias_or_name=alias_or_name)
            print(f"  OK – got {len(streams)} stream(s)\n")
            for s in streams:
                sid = s.get("id")
                name = s.get("name", "")
                stype = s.get("type", "")
                state = s.get("state", "")
                pos = s.get("position")
                weight = s.get("weight")
                print(f"  stream id={sid}  name={name!r}  type={stype!r}  state={state}  position={pos}  weight={weight}")
            if streams and len(streams) <= 3:
                print("\n  Full JSON of first stream:")
                print(json.dumps(streams[0], indent=2))
        elif do_clone and alias_or_name:
            print()
            match = next((c for c in campaigns if (c.get("alias") or "").lower() == alias_or_name.lower() or (c.get("name") or "").lower() == alias_or_name.lower()), None)
            if match:
                print(f"Cloning campaign id={match.get('id')} alias={match.get('alias')!r} ...")
            else:
                print(f"Cloning campaign with alias/name {alias_or_name!r}...")
            cloned = get_campaigns_then_clone_setup(alias_or_name=alias_or_name)
            print("  OK – cloned campaign:")
            print(json.dumps(cloned, indent=2))
        elif not do_clone and not do_streams:
            print()
            print("To list flows for a campaign:  python run_test.py --streams <alias_or_name>")
            print("To clone a campaign:           python run_test.py --clone <alias_or_name>")

    except KeitaroClientError as e:
        print(f"Keitaro API error: {e}")
        if e.status_code:
            print(f"  Status: {e.status_code}")
        if e.response_body:
            print(f"  Response (full):\n{e.response_body}")
        sys.exit(1)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
