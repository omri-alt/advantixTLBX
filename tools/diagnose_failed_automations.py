#!/usr/bin/env python3
"""Diagnose MehilotAuto / KLWL / QualityWL failure modes."""
from __future__ import annotations

import sys
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from integrations.autoserver.env import ensure_autoserver_env

ensure_autoserver_env()


def klwl_empty_campaigns() -> None:
    from integrations.autoserver import sk as sk_mod

    _ids, advs = sk_mod.get_KLWLadv()
    print(f"KLWL advertisers: {len(advs)}")
    empty: list[str] = []
    bad: list[str] = []
    for adv in advs:
        camps = sk_mod.get_campaignsByAdvid(adv["id"])
        if not camps:
            empty.append(str(adv.get("name") or adv.get("id")))
        elif isinstance(camps, dict):
            bad.append(f"{adv.get('name')}: {camps}")
        elif not isinstance(camps, list):
            bad.append(f"{adv.get('name')}: unexpected type {type(camps)}")
    print(f"  no campaigns: {len(empty)}")
    for x in empty[:20]:
        print(f"    - {x}")
    print(f"  bad response: {len(bad)}")
    for x in bad[:10]:
        print(f"    - {x}")


def mehilot_oadest_risk() -> None:
    from integrations.autoserver import zp as zp_mod
    from integrations.autoserver import gdocs_as as gd

    sheet_id = zp_mod.sheetId
    sheet = gd.read_sheet_withID(sheet_id, "Plan")
    camps = zp_mod.campaigns_data_domainToday()
    print(f"Mehilot Plan rows: {len(sheet)}, ZP campaigns today: {len(camps)}")
    risky = 0
    for plan in sheet:
        for camp in camps:
            if camp["details"]["name"].split("-")[0] != plan["brand"]:
                continue
            url = camp["details"].get("url") or ""
            parts = url.split("oadest=", 1)
            if len(parts) < 2:
                risky += 1
                print(
                    f"  RISK no oadest: brand={plan['brand']!r} "
                    f"camp={camp['details']['name']!r} url={url[:80]!r}"
                )
            break
    print(f"  campaigns matched to plan without oadest in URL: {risky}")


def main() -> int:
    print("=== KLWL empty campaigns (optimize_KLWL1 crash) ===")
    try:
        klwl_empty_campaigns()
    except Exception:
        traceback.print_exc()

    print("\n=== Mehilot oadest URL risk (list index crash) ===")
    try:
        mehilot_oadest_risk()
    except Exception:
        traceback.print_exc()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
