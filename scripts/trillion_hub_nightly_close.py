#!/usr/bin/env python3
"""
Dry-run or apply nightly pause for Trillion campaigns targeting Keitaro hub campaign 94.

Examples:
  python scripts/trillion_hub_nightly_close.py --dry-run
  python scripts/trillion_hub_nightly_close.py --apply
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(_ROOT / ".env")
except ImportError:
    pass

from integrations.nipuhim_tr_nightly_close import pause_trillion_hub_campaigns, resolve_hub_close_alias


def main() -> int:
    ap = argparse.ArgumentParser(description="Pause Trillion hub-routed campaigns (campaign 94 alias)")
    ap.add_argument("--dry-run", action="store_true", help="Plan only (default)")
    ap.add_argument("--apply", action="store_true", help="Pause active matched campaigns")
    args = ap.parse_args()

    dry_run = not args.apply
    alias = resolve_hub_close_alias()
    print(f"Hub alias: {alias}")
    print(f"Mode: {'dry-run' if dry_run else 'apply'}")
    print()

    payload = pause_trillion_hub_campaigns(dry_run=dry_run, reason="cli")
    print(json.dumps({k: payload[k] for k in payload if k != "actions"}, indent=2))
    print()
    for act in payload.get("actions") or []:
        print(
            f"{act.get('status'):15s}  {act.get('folder') or '-':12s}  {act.get('campaign')}  "
            f"({act.get('status_before')})"
        )
    return 1 if payload.get("errors") else 0


if __name__ == "__main__":
    raise SystemExit(main())
