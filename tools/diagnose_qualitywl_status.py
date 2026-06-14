#!/usr/bin/env python3
"""Diagnose QualityWL SKstatus / bid population failures."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from integrations.autoserver.env import ensure_autoserver_env

ensure_autoserver_env()

from integrations.autoserver import gdocs_as as gd
from integrations.autoserver import sk as sk_mod


def main() -> None:
    sheet = gd.read_sheet("QualityWL")
    print(f"QualityWL rows: {len(sheet)}")
    if not sheet:
        return
    print("Headers:", list(sheet[0].keys()))

    bad_status = 0
    bad_bid = 0
    for i, row in enumerate(sheet):
        cid = row.get("CampaignID")
        sub = row.get("SUBID")
        status = row.get("SKstatus")
        bid = row.get("bid")
        if str(status).startswith("could not obtain"):
            bad_status += 1
        if str(bid).startswith("could not obtain"):
            bad_bid += 1

        camp = sk_mod.get_campaignById(cid)
        ok = isinstance(camp, dict) and "active" in camp
        live = sk_mod.findSourceinCampaign(sub, cid)
        print(
            f"row {i + 2}: CampaignID={cid!r} ({type(cid).__name__}) "
            f"sheet SKstatus={status!r} sheet bid={str(bid)[:50]!r}"
        )
        print(
            f"  live GET active={camp.get('active') if ok else camp!r} "
            f"live findSource SKstatus={live.get('SKstatus')!r} bid={str(live.get('bid'))[:50]!r}"
        )

    print(f"\nSheet rows with bad SKstatus: {bad_status}/{len(sheet)}")
    print(f"Sheet rows with bad bid: {bad_bid}/{len(sheet)}")


if __name__ == "__main__":
    main()
