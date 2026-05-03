#!/usr/bin/env python3
"""
Detach Nipuhim (Kelkoo) offers that point at given merchant id(s) from a country's Keitaro flow.

For each offer attached to the geo's stream, the offer's ``action_payload`` is URL-decoded
(repeatedly) and scanned for ``merchantId=`` / ``websiteId=`` (Kelkoo PLA / permanentLinkGo).
Matching offers are removed from the flow; remaining offers get equal traffic shares.

Does not edit Google Sheets. Re-run ``update_offers_from_sheet`` / daily workflow when you want
the sheet and Keitaro URLs aligned again.

  python detach_keitaro_merchant_offers.py --geo es --merchant-ids 15248713
  python detach_keitaro_merchant_offers.py --geo es --merchant-id 15248713 --merchant-id 99887766 --dry-run
  python detach_keitaro_merchant_offers.py --geo es --merchant-ids 15248713 --delete-detached
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Set
from urllib.parse import unquote

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv

load_dotenv()

from config import KEITARO_API_KEY, KEITARO_BASE_URL, KEITARO_CAMPAIGN_ALIAS
from assistance import (
    flow_name_to_geo,
    get_campaign_streams_by_alias,
    set_flow_offers,
    stream_offer_ids,
    remove_offer_best_effort,
)
from integrations.keitaro import KeitaroClient, KeitaroClientError


MID_PAIR_RE = re.compile(r"(?i)(?:merchantId|websiteId)=(\d+)")


def _normalize_merchant_id(raw: str) -> str:
    s = (raw or "").strip().replace(" ", "").replace(",", "")
    return re.sub(r"\D", "", s)


def extract_merchant_like_ids_from_payload(payload: str) -> Set[str]:
    """Return digit-only ids found via merchantId=/websiteId= after progressive URL decode."""
    found: Set[str] = set()
    cur = payload or ""
    for _ in range(10):
        for m in MID_PAIR_RE.finditer(cur):
            found.add(m.group(1))
        nxt = unquote(cur)
        if nxt == cur:
            break
        cur = nxt
    return found


def _offers_by_id() -> Dict[int, Dict[str, Any]]:
    client = KeitaroClient()
    out: Dict[int, Dict[str, Any]] = {}
    for o in client.get_offers():
        oid = o.get("id")
        if oid is not None:
            out[int(oid)] = o
    return out


def main() -> None:
    p = argparse.ArgumentParser(
        description="Remove offers for given Kelkoo merchant id(s) from a country's Keitaro flow."
    )
    p.add_argument("--geo", required=True, help="Two-letter geo, e.g. es")
    p.add_argument(
        "--merchant-id",
        action="append",
        default=[],
        metavar="ID",
        help="Merchant id to strip (repeatable)",
    )
    p.add_argument(
        "--merchant-ids",
        default="",
        help="Comma-separated merchant ids (merged with --merchant-id)",
    )
    p.add_argument("--dry-run", action="store_true", help="Print actions only; do not call Keitaro writes")
    p.add_argument(
        "--delete-detached",
        action="store_true",
        help="After detaching, best-effort delete each removed offer from Keitaro",
    )
    args = p.parse_args()

    if not KEITARO_BASE_URL or not KEITARO_API_KEY:
        print("Error: Set KEITARO_BASE_URL and KEITARO_API_KEY in .env")
        sys.exit(1)

    geo = (args.geo or "").strip().lower()[:2]
    if len(geo) != 2:
        print("Error: --geo must be a two-letter country code")
        sys.exit(1)

    targets: Set[str] = set()
    for chunk in args.merchant_id:
        n = _normalize_merchant_id(chunk)
        if n:
            targets.add(n)
    for part in (args.merchant_ids or "").split(","):
        n = _normalize_merchant_id(part)
        if n:
            targets.add(n)

    if not targets:
        print("Error: pass at least one merchant id via --merchant-id and/or --merchant-ids")
        sys.exit(1)

    alias = (KEITARO_CAMPAIGN_ALIAS or "HrQBXp").strip()
    print(f"Campaign alias/name: {alias}")
    print(f"Geo: {geo}, merchant id(s): {sorted(targets, key=int)}")
    print()

    try:
        streams = get_campaign_streams_by_alias(alias)
    except (ValueError, KeitaroClientError) as e:
        print(f"Error loading streams: {e}")
        sys.exit(1)

    stream = None
    for s in streams:
        g = flow_name_to_geo(s.get("name") or "")
        if g == geo:
            stream = s
            break

    if not stream:
        print(f"Error: no stream whose name maps to geo {geo!r} (see geos.GEO_LABELS / flow names)")
        sys.exit(1)

    sid = int(stream["id"])
    on_flow = stream_offer_ids(stream)
    print(f"Stream id={sid} name={stream.get('name')!r}: {len(on_flow)} offer(s) on flow")

    by_id = _offers_by_id()
    matched: List[int] = []
    for oid in on_flow:
        offer = by_id.get(oid)
        if not offer:
            print(f"  warning: offer id={oid} on flow but not returned by GET offers — skip")
            continue
        payload = str(offer.get("action_payload") or "")
        found = extract_merchant_like_ids_from_payload(payload)
        hit = found & targets
        if hit:
            matched.append(oid)
            print(
                f"  match offer id={oid} name={offer.get('name')!r} "
                f"payload_ids={sorted(found, key=int)} hit={sorted(hit, key=int)}"
            )

    if not matched:
        print("No offers on this flow matched those merchant id(s). Nothing to do.")
        return

    remaining = [oid for oid in on_flow if oid not in matched]
    if not remaining:
        print(
            "Error: detaching all offers would leave the flow empty; refusing. "
            "Add other offers or run Keitaro sync first."
        )
        sys.exit(1)

    if args.dry_run:
        print()
        print(f"Dry-run: would detach {len(matched)} offer(s); flow would keep {len(remaining)} offer(s).")
        return

    try:
        set_flow_offers(sid, remaining)
        print()
        print(f"Updated flow id={sid}: detached {len(matched)} offer(s); {len(remaining)} remain.")
    except KeitaroClientError as e:
        print(f"Error updating flow: {e}")
        if e.response_body:
            print((e.response_body or "")[:400])
        sys.exit(1)

    if args.delete_detached:
        for oid in matched:
            ok = remove_offer_best_effort(oid)
            print(f"  delete offer id={oid}: {'ok' if ok else 'failed (still detached from flow)'}")


if __name__ == "__main__":
    main()
