#!/usr/bin/env python3
"""
Populate the `Blend` sheet from a feed-specific potential sheet in the Blend spreadsheet.

Reads:
  - potentialKelkoo1, potentialKelkoo2, potentialKelkoo5, potentialAdexa, or potentialYadore

Writes (upserts) into:
  - Blend tab

Rules:
  - only rows with `kelkoo_monetization` starting with "monetized" are inserted
  - inserted rows get:
      clickCap = 50
      auto = v
      feed = kelkoo1 / kelkoo2 / kelkoo5 / adexa / yadore (based on --feed)
  - avoids duplicates by (geo, merchantId, feed)
  - after each run, existing Blend rows with auto=v and clickCap<=0 are restored to
    clickCap=50 when the matching potential row is monetized (also refreshes offerUrl)
  - ``--max-add`` is a safety ceiling on **new** rows per run (daily uses env ``BLEND_POPULATE_MAX_ADD``,
    default large so monetized merchants from the potential sheet are not dropped arbitrarily).
    Use ``--prioritize-brand`` / ``--prioritize-merchant-id`` only for targeted one-offs.

Usage:
  python populate_blend_from_potential.py --feed kelkoo1
  python populate_blend_from_potential.py --feed adexa
  python populate_blend_from_potential.py --feed kelkoo1 --max-add 200
  python populate_blend_from_potential.py --feed kelkoo2 --prioritize-brand cocooncenter --max-add 5
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dotenv import load_dotenv

load_dotenv()

from config import BLEND_FEED_CHOICES, BLEND_SHEETS_SPREADSHEET_ID
from integrations.blend_device import DEVICE_MODE_LEGACY, normalize_device_mode
from integrations.monetization_geo import geo_for_blend

BLEND_SPREADSHEET_ID = BLEND_SHEETS_SPREADSHEET_ID
BLEND_SHEET = "Blend"
BLEND_DEFAULT_CLICK_CAP = 50.0

POTENTIAL_SHEET_BY_FEED: Dict[str, str] = {
    "kelkoo1": "potentialKelkoo1",
    "kelkoo2": "potentialKelkoo2",
    "kelkoo5": "potentialKelkoo5",
    "adexa": "potentialAdexa",
    "yadore": "potentialYadore",
}


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
    required = [
        "brandName",
        "offerUrl",
        "clickCap",
        "geo",
        "merchantId",
        "auto",
        "feed",
        "device_mode",
        "weight_desktop",
        "weight_mobile",
        "cpc_desktop",
        "cpc_mobile",
    ]
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


def _parse_click_cap(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _column_a1(col_idx: int) -> str:
    """0-based column index → A1 column letter(s)."""
    n = col_idx + 1
    letters = ""
    while n:
        n, rem = divmod(n - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


def _offer_url_from_potential_row(feed: str, monet: str, domain: str, geo: str) -> str:
    offer_url = domain
    if feed == "adexa" and monet == "monetized_adexa_smartlink":
        from integrations.adexa import (
            is_adexa_golink_url,
            merchant_monetization_check,
            normalize_adexa_golink_url,
        )

        if is_adexa_golink_url(domain):
            offer_url = normalize_adexa_golink_url(domain) or domain
        else:
            probe = domain if domain.lower().startswith("http") else f"https://{domain.lstrip('/')}"
            try:
                ax = merchant_monetization_check(probe, geo)
                golink = str(ax.get("smartlink_url") or "").strip()
                if ax.get("mode") == "smartlink" and golink:
                    offer_url = normalize_adexa_golink_url(golink) or golink
            except Exception:
                pass
    return offer_url


def _load_monetized_potential_index(
    service,
    feed: str,
) -> Dict[Tuple[str, str, str], Dict[str, str]]:
    """Map (geo, merchantId, feed) → potential row fields for monetized merchants."""
    potential_sheet = POTENTIAL_SHEET_BY_FEED[feed]
    pot_vals = read_values(service, potential_sheet)
    if not pot_vals or len(pot_vals) < 2:
        return {}

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
        return {}

    out: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    for row in pot_vals[1:]:
        monet = str(row[i_monet] or "").strip().lower()
        if not monet.startswith("monetized"):
            continue
        geo = geo_for_blend(str(row[i_geo] or ""))
        mid = str(row[i_mid] or "").strip()
        domain = str(row[i_domain] or "").strip()
        if not geo or not mid or not domain:
            continue
        out[(geo, mid, feed)] = {
            "name": str(row[i_name] or "").strip(),
            "domain": domain,
            "monet": monet,
            "geo": geo,
        }
    return out


def restore_blend_rows_from_potential(
    service,
    *,
    feed: str,
    default_cap: float = BLEND_DEFAULT_CLICK_CAP,
    dry_run: bool = False,
) -> Dict[str, int]:
    """
    For existing Blend rows (auto=v, clickCap<=0), restore clickCap and offerUrl when
    the merchant is monetized on the matching potential sheet.
    """
    pot_index = _load_monetized_potential_index(service, feed)
    blend_vals = read_values(service, BLEND_SHEET)
    stats = {
        "zero_cap_scanned": 0,
        "restored": 0,
        "url_updated": 0,
        "skipped_not_monetized": 0,
    }
    if not blend_vals or len(blend_vals) < 2 or not pot_index:
        return stats

    blend_header = [str(c or "").strip() for c in blend_vals[0]]
    idx_blend = {h.strip().lower(): i for i, h in enumerate(blend_header)}

    def b_i(name: str) -> int:
        return idx_blend.get(name.lower(), -1)

    i_cap = b_i("clickcap")
    i_url = b_i("offerurl")
    i_geo = b_i("geo")
    i_mid = b_i("merchantid")
    i_auto = b_i("auto")
    i_feed = b_i("feed")
    if min(i_cap, i_geo, i_mid, i_auto, i_feed) < 0:
        return stats

    quoted = BLEND_SHEET.replace("'", "''")
    batch_data: List[Dict[str, Any]] = []

    for sheet_row, row in enumerate(blend_vals[1:], start=2):
        auto = str(row[i_auto] if i_auto < len(row) else "").strip().lower()
        if auto != "v":
            continue
        cap = _parse_click_cap(row[i_cap] if i_cap < len(row) else "")
        if cap is None or cap > 0:
            continue
        stats["zero_cap_scanned"] += 1

        geo = geo_for_blend(str(row[i_geo] if i_geo < len(row) else ""))
        mid = str(row[i_mid] if i_mid < len(row) else "").strip()
        feed_tag = str(row[i_feed] if i_feed < len(row) else "").strip().lower()
        if not geo or not mid or feed_tag != feed:
            if feed_tag == feed:
                stats["skipped_not_monetized"] += 1
            continue

        pot = pot_index.get((geo, mid, feed))
        if not pot:
            stats["skipped_not_monetized"] += 1
            continue

        new_url = _offer_url_from_potential_row(feed, pot["monet"], pot["domain"], geo)
        cur_url = str(row[i_url] if i_url < len(row) else "").strip()
        cap_str = str(int(default_cap) if default_cap == int(default_cap) else default_cap)

        stats["restored"] += 1
        if dry_run:
            print(
                f"  [dry-run] row {sheet_row}: {pot.get('name') or mid} "
                f"({geo}/{feed}) clickCap {cap} -> {cap_str}"
                + (f", offerUrl -> {new_url[:80]}..." if new_url != cur_url and new_url else "")
            )
            continue

        batch_data.append(
            {
                "range": f"'{quoted}'!{_column_a1(i_cap)}{sheet_row}",
                "values": [[cap_str]],
            }
        )
        if new_url and new_url != cur_url and i_url >= 0:
            batch_data.append(
                {
                    "range": f"'{quoted}'!{_column_a1(i_url)}{sheet_row}",
                    "values": [[new_url]],
                }
            )
            stats["url_updated"] += 1

    if batch_data and not dry_run:
        service.values().batchUpdate(
            spreadsheetId=BLEND_SPREADSHEET_ID,
            body={"valueInputOption": "RAW", "data": batch_data},
        ).execute()

    return stats


def _reorder_potential_body_rows(
    body_rows: List[List[Any]],
    *,
    i_name: int,
    i_mid: int,
    brand_sub: str,
    merchant_id: str,
) -> List[List[Any]]:
    """Move rows matching brand substring (case-insensitive) or exact merchantId to the front."""
    bs = (brand_sub or "").strip().lower()
    mid = (merchant_id or "").strip()
    if not bs and not mid:
        return body_rows
    head: List[List[Any]] = []
    tail: List[List[Any]] = []
    for row in body_rows:
        name = str(row[i_name] if i_name < len(row) else "").strip().lower()
        m = str(row[i_mid] if i_mid < len(row) else "").strip()
        if (bs and bs in name) or (mid and m == mid):
            head.append(row)
        else:
            tail.append(row)
    return head + tail


def _populate_feed(service, feed: str, args: argparse.Namespace) -> None:
    potential_sheet = POTENTIAL_SHEET_BY_FEED[feed]

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
    i_mode = pot_idx("device_mode")
    i_wd = pot_idx("weight_desktop")
    i_wm = pot_idx("weight_mobile")
    i_cpc_d = pot_idx("cpc_desktop")
    i_cpc_m = pot_idx("cpc_mobile")
    if min(i_mid, i_name, i_domain, i_geo, i_monet) < 0:
        print(f"Potential sheet header missing required columns: {pot_vals[0]}")
        return

    body_rows = _reorder_potential_body_rows(
        list(pot_vals[1:]),
        i_name=i_name,
        i_mid=i_mid,
        brand_sub=args.prioritize_brand,
        merchant_id=args.prioritize_merchant_id,
    )

    # Index for blend sheet columns
    blend_header = [str(c or "").strip() for c in (blend_vals[0] if blend_vals else header_blend)]
    idx_blend = {h.strip().lower(): i for i, h in enumerate(blend_header)}

    def b_i(name: str) -> int:
        return idx_blend.get(name.lower(), -1)

    # Existing keys (geo, merchantId, feed)
    existing = set()
    if blend_vals and len(blend_vals) >= 2:
        for row in blend_vals[1:]:
            geo = geo_for_blend(str(row[b_i("geo")] if b_i("geo") >= 0 and b_i("geo") < len(row) else ""))
            mid = str(row[b_i("merchantid")] if b_i("merchantid") >= 0 and b_i("merchantid") < len(row) else "").strip()
            feed = str(row[b_i("feed")] if b_i("feed") >= 0 and b_i("feed") < len(row) else "").strip().lower()
            if geo and mid and feed:
                existing.add((geo, mid, feed))

    # Build all new rows (then cap by max_add) so we can report how many were left out.
    candidates: List[List[Any]] = []
    total_potential = max(len(pot_vals) - 1, 0)
    monetized_rows = 0
    eligible_rows = 0
    dup_rows = 0
    for row in body_rows:
        monet = str(row[i_monet] or "").strip().lower()
        if not monet.startswith("monetized"):
            continue
        monetized_rows += 1
        geo = geo_for_blend(str(row[i_geo] or ""))
        mid = str(row[i_mid] or "").strip()
        name = str(row[i_name] or "").strip()
        domain = str(row[i_domain] or "").strip()
        if not geo or not mid or not domain:
            continue
        eligible_rows += 1
        key = (geo, mid, feed)
        if key in existing:
            dup_rows += 1
            continue

        offer_url = _offer_url_from_potential_row(feed, monet, domain, geo)

        new_row = [""] * max(len(blend_header), len(header_blend))
        # Fill known columns
        new_row[b_i("brandname")] = name
        new_row[b_i("offerurl")] = offer_url
        new_row[b_i("clickcap")] = "50"
        new_row[b_i("geo")] = geo
        new_row[b_i("merchantid")] = mid
        new_row[b_i("auto")] = "v"
        new_row[b_i("feed")] = feed
        if i_mode >= 0 and i_mode < len(row):
            mode = normalize_device_mode(str(row[i_mode] or ""))
        else:
            mode = DEVICE_MODE_LEGACY
        if b_i("device_mode") >= 0:
            new_row[b_i("device_mode")] = mode
        if i_wd >= 0 and i_wd < len(row) and b_i("weight_desktop") >= 0:
            new_row[b_i("weight_desktop")] = str(row[i_wd] or "")
        if i_wm >= 0 and i_wm < len(row) and b_i("weight_mobile") >= 0:
            new_row[b_i("weight_mobile")] = str(row[i_wm] or "")
        if i_cpc_d >= 0 and i_cpc_d < len(row) and b_i("cpc_desktop") >= 0:
            new_row[b_i("cpc_desktop")] = str(row[i_cpc_d] or "")
        if i_cpc_m >= 0 and i_cpc_m < len(row) and b_i("cpc_mobile") >= 0:
            new_row[b_i("cpc_mobile")] = str(row[i_cpc_m] or "")
        candidates.append(new_row[: len(blend_header)])
        existing.add(key)

    omitted_by_cap = max(0, len(candidates) - args.max_add)
    to_append = candidates[: args.max_add]

    if not to_append:
        print(
            "Nothing new to add into Blend for this run. "
            f"(potential rows={total_potential}, monetized={monetized_rows}, "
            f"eligible={eligible_rows}, duplicates_skipped={dup_rows}, "
            f"new_merchants_ready={len(candidates)}, max_add={args.max_add})"
        )
        return

    print(
        f"populate_blend_from_potential summary (feed={feed}): "
        f"potential rows={total_potential}, monetized={monetized_rows}, "
        f"eligible={eligible_rows}, duplicates_skipped={dup_rows}, "
        f"new_merchants_ready={len(candidates)}, added={len(to_append)}, "
        f"omitted_by_max_add_cap={omitted_by_cap}, max_add={args.max_add}"
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
    print(f"Added {len(to_append)} rows from {potential_sheet} to Blend (feed={feed}).")


def _run_restore(service, feed: str, args: argparse.Namespace) -> None:
    if args.skip_restore_caps:
        return
    stats = restore_blend_rows_from_potential(
        service,
        feed=feed,
        default_cap=BLEND_DEFAULT_CLICK_CAP,
        dry_run=args.dry_run,
    )
    if stats["restored"] or stats["zero_cap_scanned"]:
        label = "would restore" if args.dry_run else "restored"
        print(
            f"restore_blend_caps (feed={feed}): zero_cap_rows={stats['zero_cap_scanned']}, "
            f"{label}={stats['restored']}, url_updated={stats['url_updated']}, "
            f"skipped_not_monetized={stats['skipped_not_monetized']}"
        )


def main() -> None:
    p = argparse.ArgumentParser(description="Upsert monetized potential merchants into Blend sheet.")
    p.add_argument("--feed", required=True, choices=list(BLEND_FEED_CHOICES))
    p.add_argument(
        "--max-add",
        type=int,
        default=200,
        help="Max new rows to add this run (daily workflow passes BLEND_POPULATE_MAX_ADD from config)",
    )
    p.add_argument(
        "--prioritize-brand",
        default="",
        help="Substring of merchant name; matching potential rows are processed first (case-insensitive)",
    )
    p.add_argument(
        "--prioritize-merchant-id",
        default="",
        help="Exact merchantId; that row is processed first if present",
    )
    p.add_argument(
        "--restore-caps-only",
        action="store_true",
        help="Only restore clickCap=50 (+ offerUrl) for monetized potential matches; do not append new rows",
    )
    p.add_argument(
        "--all-feeds",
        action="store_true",
        help="Run for every feed (kelkoo1/2/5, adexa, yadore); with --restore-caps-only fixes all zero caps",
    )
    p.add_argument(
        "--skip-restore-caps",
        action="store_true",
        help="Skip restoring zero clickCap rows from potential monetization",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="For cap restore: print rows that would be updated without writing the sheet",
    )
    args = p.parse_args()

    feeds = list(BLEND_FEED_CHOICES) if args.all_feeds else [args.feed]
    service = get_sheets_service()

    for feed in feeds:
        if not args.restore_caps_only:
            _populate_feed(service, feed, args)
        _run_restore(service, feed, args)


if __name__ == "__main__":
    main()

