#!/usr/bin/env python3
"""Scan Blend tab for duplicate rows (read-only)."""
from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from populate_blend_from_potential import BLEND_SHEET, get_sheets_service, read_values


def main() -> None:
    service = get_sheets_service()
    vals = read_values(service, BLEND_SHEET)
    header = [str(c or "").strip() for c in vals[0]]
    idx = {h.strip().lower(): i for i, h in enumerate(header)}

    def cell(row, name: str) -> str:
        i = idx.get(name.lower(), -1)
        return str(row[i] or "").strip() if i >= 0 and i < len(row) else ""

    rows = vals[1:]
    by_url = defaultdict(list)
    for n, row in enumerate(rows, start=2):
        geo = cell(row, "geo").lower()[:2]
        feed = cell(row, "feed").lower()
        url = cell(row, "offerUrl").lower().rstrip("/")
        if geo and url and feed:
            by_url[(geo, url, feed)].append(n)

    print("Duplicate groups (geo, offerUrl, feed):\n")
    for key, line_nums in sorted(by_url.items(), key=lambda x: x[0]):
        if len(line_nums) < 2:
            continue
        print(f"KEY {key} -> sheet rows {line_nums}")
        for n in line_nums:
            row = vals[n - 1]
            print(
                f"  row {n}: brand={cell(row, 'brandName')!r} "
                f"mid={cell(row, 'merchantId')!r} auto={cell(row, 'auto')!r} "
                f"cap={cell(row, 'clickCap')!r}"
            )
        print()


if __name__ == "__main__":
    main()
