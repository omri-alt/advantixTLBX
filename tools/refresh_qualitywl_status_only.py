#!/usr/bin/env python3
"""Fast QualityWL SKstatus refresh — one campaign GET per unique CampaignID."""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

load_dotenv()

from integrations.autoserver.env import ensure_autoserver_env

ensure_autoserver_env()

from integrations.autoserver import gdocs_as as gd
from integrations.autoserver import sk as sk_mod


def main() -> int:
    sk_mod.refresh_sk_headers()
    sheet = gd.read_sheet("QualityWL")
    if not sheet:
        print("QualityWL empty")
        return 0

    by_cid: dict[str, tuple[str, str]] = {}
    for row in sheet:
        cid = str(row.get("CampaignID") or "").strip()
        if not cid or cid in by_cid:
            continue
        campaign = sk_mod.get_campaignById(cid)
        if isinstance(campaign, dict) and "active" in campaign:
            status = sk_mod._format_sk_status(campaign["active"])
            try:
                bid_hint = str(campaign.get("cpc") or "")
            except Exception:
                bid_hint = ""
            by_cid[cid] = (status, bid_hint)
        else:
            by_cid[cid] = ("could not obtain status", "")

    bad_before = sum(1 for r in sheet if str(r.get("SKstatus", "")).startswith("could not"))
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    for row in sheet:
        cid = str(row.get("CampaignID") or "").strip()
        if not cid:
            row["SKstatus"] = "missing CampaignID"
            if str(row.get("bid") or "").startswith("could not"):
                row["bid"] = "missing CampaignID"
            row["lastUpdate"] = now
            continue
        status, cpc_hint = by_cid.get(cid, ("could not obtain status", ""))
        row["SKstatus"] = status
        if str(row.get("bid") or "").startswith("could not") and cpc_hint:
            row["bid"] = f"(cpc {cpc_hint}; rerun QualityWL for bid factor)"
        row["lastUpdate"] = now

    gd.create_or_update_sheet_from_dicts("QualityWL", sheet)
    bad_after = sum(1 for r in sheet if str(r.get("SKstatus", "")).startswith("could not"))
    print(
        f"QualityWL status refresh: {bad_before} -> {bad_after} bad rows "
        f"({len(by_cid)} unique campaigns)"
    )
    return 0 if bad_after == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
