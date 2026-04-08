#!/usr/bin/env python3
"""
Populate the `Blend` sheet from a feed-specific potential sheet in the Blend spreadsheet.

Reads:
  - potentialKelkoo1 or potentialKelkoo2

Writes (upserts) into:
  - Blend tab

Rules:
  - only rows with `kelkoo_monetization` starting with "monetized" are inserted
  - inserted rows get:
      clickCap = 50
      auto = v
      feed = kelkoo1/kelkoo2 (based on --feed)
  - avoids duplicates by (geo, merchantId, feed)

Usage:
  python populate_blend_from_potential.py --feed kelkoo1
  python populate_blend_from_potential.py --feed kelkoo2
  python populate_blend_from_potential.py --feed kelkoo1 --max-add 50
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dotenv import load_dotenv

load_dotenv()

from config import BLEND_SHEETS_SPREADSHEET_ID

BLEND_SPREADSHEET_ID = BLEND_SHEETS_SPREADSHEET_ID
BLEND_SHEET = "Blend"


def get_credentials_path() -> str:
    p = Path(__file__).resolve().parent / "credentials.json"
    if not p.exists():
        raise FileNotFoundError(f"credentials.json not found at {p}")
    return str(p)


def get_sheets_service():
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds = service_account.Credentials.from_service_account_file(get_credentials_path())
    return build("sheets", "v4", credentials=creds).spreadsheets()


def read_values(service, sheet_title: str) -> List[List[Any]]:
    quoted = sheet_title.replace("'", "''")
    try:
        return (
            service.values()
            .get(spreadsheetId=BLEND_SPREADSHEET_ID, range=f"'{quoted}'!A:Z")
            .execute()
            .get("values")
            or []
        )
    except Exception:
        return []


def ensure_blend_headers(service) -> List[str]:
    # Reuse the existing header if present; otherwise create minimal header.
    quoted = BLEND_SHEET.replace("'", "''")
    result = service.values().get(spreadsheetId=BLEND_SPREADSHEET_ID, range=f"'{quoted}'!1:1").execute()
    rows = result.get("values") or [[]]
    header = [str(c or "").strip() for c in (rows[0] if rows else [])]
    if not header or all(not h for h in header):
        header = ["brandName", "offerUrl", "clickCap", "geo", "merchantId", "auto", "feed"]
    required = ["brandName", "offerUrl", "clickCap", "geo", "merchantId", "auto", "feed"]
    for r in required:
        if r not in header:
            header.append(r)
    service.values().update(
        spreadsheetId=BLEND_SPREADSHEET_ID,
        range=f"'{quoted}'!A1",
        valueInputOption="RAW",
        body={"values": [header]},
    ).execute()
    return header


def main() -> None:
    p = argparse.ArgumentParser(description="Upsert monetized potential merchants into Blend sheet.")
    p.add_argument("--feed", required=True, choices=["kelkoo1", "kelkoo2"])
    p.add_argument("--max-add", type=int, default=200, help="Max new rows to add this run")
    args = p.parse_args()

    potential_sheet = "potentialKelkoo1" if args.feed == "kelkoo1" else "potentialKelkoo2"

    service = get_sheets_service()
    header_blend = ensure_blend_headers(service)
    blend_vals = read_values(service, BLEND_SHEET)
    pot_vals = read_values(service, potential_sheet)

    if not pot_vals or len(pot_vals) < 2:
        print(f"No data in {potential_sheet!r}.")
        return

    # Index columns for potential sheet
    pot_header = [str(c or "").strip().lower() for c in pot_vals[0]]
    def pot_idx(name: str) -> int:
        try:
            return pot_header.index(name)
        except ValueError:
            return -1

    i_mid = pot_idx("merchantid")
    i_name = pot_idx("merchant")
    i_domain = pot_idx("domain")
    i_geo = pot_idx("geo_origin")
    i_monet = pot_idx("kelkoo_monetization")
    if min(i_mid, i_name, i_domain, i_geo, i_monet) < 0:
        print(f"Potential sheet header missing required columns: {pot_vals[0]}")
        return

    # Index for blend sheet columns
    blend_header = [str(c or "").strip() for c in (blend_vals[0] if blend_vals else header_blend)]
    idx_blend = {h.strip().lower(): i for i, h in enumerate(blend_header)}

    def b_i(name: str) -> int:
        return idx_blend.get(name.lower(), -1)

    # Existing keys (geo, merchantId, feed)
    existing = set()
    if blend_vals and len(blend_vals) >= 2:
        for row in blend_vals[1:]:
            geo = (row[b_i("geo")] if b_i("geo") >= 0 and b_i("geo") < len(row) else "") or ""
            mid = (row[b_i("merchantid")] if b_i("merchantid") >= 0 and b_i("merchantid") < len(row) else "") or ""
            feed = (row[b_i("feed")] if b_i("feed") >= 0 and b_i("feed") < len(row) else "") or ""
            geo = str(geo).strip().lower()[:2]
            mid = str(mid).strip()
            feed = str(feed).strip().lower()
            if geo and mid and feed:
                existing.add((geo, mid, feed))

    # Build rows to append
    to_append: List[List[Any]] = []
    total_potential = max(len(pot_vals) - 1, 0)
    monetized_rows = 0
    eligible_rows = 0
    dup_rows = 0
    for row in pot_vals[1:]:
        monet = str(row[i_monet] or "").strip().lower()
        if not monet.startswith("monetized"):
            continue
        monetized_rows += 1
        geo = str(row[i_geo] or "").strip().lower()[:2]
        mid = str(row[i_mid] or "").strip()
        name = str(row[i_name] or "").strip()
        domain = str(row[i_domain] or "").strip()
        if not geo or not mid or not domain:
            continue
        eligible_rows += 1
        key = (geo, mid, args.feed)
        if key in existing:
            dup_rows += 1
            continue

        new_row = [""] * max(len(blend_header), len(header_blend))
        # Fill known columns
        new_row[b_i("brandname")] = name
        new_row[b_i("offerurl")] = domain
        new_row[b_i("clickcap")] = "50"
        new_row[b_i("geo")] = geo
        new_row[b_i("merchantid")] = mid
        new_row[b_i("auto")] = "v"
        new_row[b_i("feed")] = args.feed
        to_append.append(new_row[: len(blend_header)])
        existing.add(key)
        if len(to_append) >= args.max_add:
            break

    if not to_append:
        print(
            "Nothing new to add into Blend for this run. "
            f"(potential rows={total_potential}, monetized={monetized_rows}, "
            f"eligible={eligible_rows}, duplicates_skipped={dup_rows}, "
            f"max_add={args.max_add})"
        )
        return

    # Append rows
    print(
        f"populate_blend_from_potential summary (feed={args.feed}): "
        f"potential rows={total_potential}, monetized={monetized_rows}, "
        f"eligible={eligible_rows}, duplicates_skipped={dup_rows}, "
        f"added={len(to_append)}, max_add={args.max_add}"
    )
    quoted = BLEND_SHEET.replace("'", "''")
    start_row = (len(blend_vals) + 1) if blend_vals else 2
    range_a1 = f"'{quoted}'!A{start_row}"
    service.values().update(
        spreadsheetId=BLEND_SPREADSHEET_ID,
        range=range_a1,
        valueInputOption="RAW",
        body={"values": to_append},
    ).execute()
    print(f"Added {len(to_append)} rows from {potential_sheet} to Blend (feed={args.feed}).")


if __name__ == "__main__":
    main()

