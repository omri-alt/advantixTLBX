#!/usr/bin/env python3
"""
Explain why a merchant on a Blend potential sheet may not appear on the Blend tab or Keitaro.

Reads the Blend spreadsheet (same IDs as populate_blend_from_potential / blend_sync_from_sheet).
No API writes.

  python tools/diagnose_blend_potential_merchant.py --feed kelkoo2 --brand cocooncenter
  python tools/diagnose_blend_potential_merchant.py --feed kelkoo2 --brand cocoon --geo fr
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, List, Set, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv()

from populate_blend_from_potential import (  # noqa: E402
    BLEND_SHEET,
    BLEND_SPREADSHEET_ID,
    get_sheets_service,
    read_values,
)


def _norm_geo(s: str) -> str:
    return (s or "").strip().lower()[:2]


def _blend_existing_keys(service, feed_tag: str) -> Set[Tuple[str, str, str]]:
    blend_vals = read_values(service, BLEND_SHEET)
    if not blend_vals or len(blend_vals) < 2:
        return set()
    header = [str(c or "").strip().lower() for c in blend_vals[0]]

    def col(name: str) -> int:
        try:
            return header.index(name.lower())
        except ValueError:
            return -1

    ig = col("geo")
    im = col("merchantid")
    ifeed = col("feed")
    out: Set[Tuple[str, str, str]] = set()
    for row in blend_vals[1:]:
        geo = _norm_geo(str(row[ig] if ig >= 0 and ig < len(row) else ""))
        mid = str(row[im] if im >= 0 and im < len(row) else "").strip()
        ft = str(row[ifeed] if ifeed >= 0 and ifeed < len(row) else "").strip().lower()
        if geo and mid and ft:
            out.add((geo, mid, ft))
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Diagnose Blend potential → Blend sheet path for one merchant.")
    p.add_argument("--feed", required=True, choices=["kelkoo1", "kelkoo2", "adexa", "yadore"])
    p.add_argument("--brand", required=True, help="Case-insensitive substring matched against merchant column")
    p.add_argument("--geo", default="", help="Optional 2-letter geo filter when printing matches")
    args = p.parse_args()

    potential_sheet = {
        "kelkoo1": "potentialKelkoo1",
        "kelkoo2": "potentialKelkoo2",
        "adexa": "potentialAdexa",
        "yadore": "potentialYadore",
    }[args.feed]

    geo_filter = _norm_geo(args.geo) if args.geo else ""
    brand_needle = (args.brand or "").strip().lower()

    service = get_sheets_service()
    pot_vals = read_values(service, potential_sheet)
    existing = _blend_existing_keys(service, args.feed)

    if not pot_vals or len(pot_vals) < 2:
        print(f"No data in {potential_sheet!r}.")
        return

    header = [str(c or "").strip().lower() for c in pot_vals[0]]

    def idx(name: str) -> int:
        try:
            return header.index(name)
        except ValueError:
            return -1

    i_mid = idx("merchantid")
    i_name = idx("merchant")
    i_domain = idx("domain")
    i_geo = idx("geo_origin")
    i_monet = idx("kelkoo_monetization")
    if min(i_mid, i_name, i_domain, i_geo, i_monet) < 0:
        print(f"Missing columns in {potential_sheet!r}: {pot_vals[0]}")
        return

    matches: List[Tuple[int, List[Any]]] = []
    for row_i, row in enumerate(pot_vals[1:], start=2):
        name = str(row[i_name] if i_name < len(row) else "").strip()
        if brand_needle not in name.lower():
            continue
        g = _norm_geo(str(row[i_geo] if i_geo < len(row) else ""))
        if geo_filter and g != geo_filter:
            continue
        matches.append((row_i, row))

    if not matches:
        print(
            f"No rows in {potential_sheet!r} where merchant contains {args.brand!r}"
            + (f" and geo_origin is {geo_filter!r}" if geo_filter else "")
            + "."
        )
        return

    print(f"Spreadsheet: {BLEND_SPREADSHEET_ID}")
    print(f"Potential tab: {potential_sheet} (feed tag {args.feed})")
    print(f"Matches: {len(matches)}\n")

    for row_i, row in matches:
        monet = str(row[i_monet] if i_monet < len(row) else "").strip()
        monet_l = monet.lower()
        geo = _norm_geo(str(row[i_geo] if i_geo < len(row) else ""))
        mid = str(row[i_mid] if i_mid < len(row) else "").strip()
        name = str(row[i_name] if i_name < len(row) else "").strip()
        domain = str(row[i_domain] if i_domain < len(row) else "").strip()
        key = (geo, mid, args.feed)

        print(f"--- Sheet row {row_i} ---")
        print(f"  merchant={name!r} merchantId={mid!r} geo_origin={geo!r} domain={domain!r}")
        print(f"  kelkoo_monetization={monet!r}")

        reasons: List[str] = []
        if not monet_l.startswith("monetized"):
            reasons.append(
                "populate_blend_from_potential skips: kelkoo_monetization does not start with 'monetized'."
            )
        if not geo or not mid or not domain:
            reasons.append(
                "populate skips: need non-empty geo_origin, merchantId, and domain "
                "(domain is the Blend offerUrl / merchant homepage)."
            )
        if key in existing:
            reasons.append(
                f"populate skips: Blend sheet already has (geo={geo!r}, merchantId={mid!r}, feed={args.feed!r})."
            )
        if not reasons:
            reasons.append(
                "This row qualifies for populate (monetized + geo/mid/domain + not a duplicate key)."
            )
            reasons.append(
                "If it is still missing from Blend: daily run may hit BLEND_POPULATE_MAX_ADD "
                "(safety ceiling; default is high) before this row in sheet order — raise the env or run "
                "`python populate_blend_from_potential.py --feed ... --max-add N "
                "--prioritize-brand <substring>` once."
            )
            reasons.append(
                "Keitaro: blend_sync_from_sheet does not attach auto=v rows with 0 month-to-date "
                "Kelkoo sales (sheet row can remain)."
            )

        for r in reasons:
            print(f"  → {r}")
        print()


if __name__ == "__main__":
    main()
