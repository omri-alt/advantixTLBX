#!/usr/bin/env python3
"""
Sync today's offers sheet into a per-feed NIPUHIM-feed* Keitaro campaign (v2 hub children).

Does not modify legacy Nipuh (HrQBXp). Uses geo × device_type flows on the child campaign.

  python update_offers_from_sheet_v2.py --sheet 2026-06-25_offers_1
  python update_offers_from_sheet_v2.py --sheet 2026-06-25_offers_2 --account 2
  python update_offers_from_sheet_v2.py --sheet 2026-06-25_offers_5 --account 5
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dotenv import load_dotenv

load_dotenv()

from config import KEITARO_API_KEY, KEITARO_BASE_URL
from integrations.nipuhim_v2_sync import sync_sheet_to_nipuhim_v2
from update_offers_from_sheet import MAX_OFFERS_PER_GEO


def main() -> None:
    if not KEITARO_BASE_URL or not KEITARO_API_KEY:
        print("Error: Set KEITARO_BASE_URL and KEITARO_API_KEY in .env")
        sys.exit(1)

    argv = sys.argv[1:]
    sheet_name = ""
    account = 1
    max_offers = MAX_OFFERS_PER_GEO
    i = 0
    while i < len(argv):
        if argv[i] == "--sheet" and i + 1 < len(argv):
            sheet_name = argv[i + 1]
            i += 2
            continue
        if argv[i] == "--account" and i + 1 < len(argv):
            account = int(argv[i + 1])
            i += 2
            continue
        if argv[i] == "--max-offers" and i + 1 < len(argv):
            max_offers = int(argv[i + 1])
            i += 2
            continue
        i += 1

    if not sheet_name:
        print("Usage: python update_offers_from_sheet_v2.py --sheet YYYY-MM-DD_offers_N [--account 1|2|5]")
        sys.exit(1)

    sys.exit(sync_sheet_to_nipuhim_v2(sheet_name, account, max_offers=max_offers))


if __name__ == "__main__":
    main()
