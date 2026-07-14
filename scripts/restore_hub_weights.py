#!/usr/bin/env python3
"""Restore campaign 94 hub flow weights from Blend clickCaps + Nipuhim equal split."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from integrations.domain_demand_guard import restore_hub_stream_weights_from_click_caps  # noqa: E402
from integrations.keitaro import KeitaroClient  # noqa: E402
from integrations.keitaro_hub import _hub_device_streams, load_hub_state  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(description="Restore hub campaign 94 offer shares.")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    result = restore_hub_stream_weights_from_click_caps(dry_run=args.dry_run)
    for line in result.get("logs") or []:
        print(line)
    print(f"status={result.get('status')} updated={result.get('hub_streams_updated')}")

    if args.dry_run or result.get("status") != "ok":
        return 0 if result.get("status") in ("ok", "dry_run") else 1

    hub = int(load_hub_state().get("hub_campaign_id") or 94)
    streams = _hub_device_streams(KeitaroClient(), hub)
    pos = sum(
        1
        for s in streams
        if sum(float(o.get("share") or 0) for o in (s.get("offers") or [])) > 0
    )
    print(f"verify: {pos}/{len(streams)} flows have positive share")
    return 0 if pos == len(streams) and streams else 1


if __name__ == "__main__":
    raise SystemExit(main())
