#!/usr/bin/env python3
"""
Sync Blend sheet into BLEND-feed* hub child campaigns only (v2).

Legacy blend_sync_from_sheet (campaign 9Xq9dSMh) is unchanged.

  python blend_sync_from_sheet_v2.py
  python blend_sync_from_sheet_v2.py --geo fr
  python blend_sync_from_sheet_v2.py --feed kelkoo1,kelkoo2
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dotenv import load_dotenv

load_dotenv()


def main() -> int:
    argv = sys.argv[1:]
    only_geo = None
    feed_keys: list[str] | None = None
    i = 0
    while i < len(argv):
        if argv[i] == "--geo" and i + 1 < len(argv):
            from integrations.monetization_geo import geo_for_blend

            only_geo = geo_for_blend(argv[i + 1])
            i += 2
            continue
        if argv[i] == "--feed" and i + 1 < len(argv):
            feed_keys = [x.strip() for x in argv[i + 1].split(",") if x.strip()]
            i += 2
            continue
        print(f"Unknown arg: {argv[i]}")
        return 2

    from blend_sync_from_sheet import get_sheets_service
    from integrations.blend_v2_sync import run_blend_v2_sync

    service = get_sheets_service()
    ok = run_blend_v2_sync(service, only_geo=only_geo, feed_keys=feed_keys)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
