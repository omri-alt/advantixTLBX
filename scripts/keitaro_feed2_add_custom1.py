#!/usr/bin/env python3
"""
Add ``&custom2={subid}`` to Keitaro feed2 (sidehustlerbaby) offer URLs.

Keeps ``pub_click_id={subid}`` for the partner log. Sidehustler uses ``custom1`` and
``publisherClickId`` internally; Kelkoo attribution needs ``custom2``.

Also rewrites legacy ``custom1={subid}`` → ``custom2={subid}`` if present.

  python scripts/keitaro_feed2_add_custom1.py           # dry-run
  python scripts/keitaro_feed2_add_custom1.py --apply
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from assistance import get_offers_data, update_offer_action_payload  # noqa: E402
from integrations.keitaro import KeitaroClientError  # noqa: E402

MARKER = "sidehustlerbaby.com/klk-merchant"
NEEDLE = "pub_click_id={subid}"
CUSTOM2 = "custom2={subid}"
CUSTOM1 = "custom1={subid}"
REPL = f"{NEEDLE}&{CUSTOM2}"


def transform(payload: str) -> str | None:
    """Return new action_payload, or None if no change needed."""
    p = payload or ""
    if MARKER not in p:
        return None
    if CUSTOM2 in p:
        return None
    if CUSTOM1 in p:
        return p.replace(CUSTOM1, CUSTOM2, 1)
    if NEEDLE not in p:
        return None
    return p.replace(NEEDLE, REPL, 1)


def main() -> int:
    apply = "--apply" in sys.argv
    print(
        "Feed2 URL rewrite: keep pub_click_id={subid}, set custom2={subid} "
        f"on {MARKER} offers"
    )
    print("DRY RUN" if not apply else "APPLY")
    print()

    offers = get_offers_data()
    candidates = []
    for offer in offers:
        new_p = transform(offer.get("action_payload") or "")
        if new_p is None:
            continue
        candidates.append((offer, new_p))

    print(f"Matched {len(candidates)} offer(s)")
    updated = 0
    errors = 0
    for offer, new_p in candidates:
        oid = offer.get("id")
        name = offer.get("name") or f"id={oid}"
        old = offer.get("action_payload") or ""
        if not apply:
            print(f"  [dry] {name} id={oid}")
            print(f"        old …{old[-80:]}")
            print(f"        new …{new_p[-95:]}")
            updated += 1
            continue
        try:
            update_offer_action_payload(int(oid), new_p)
            print(f"  updated {name} id={oid}")
            updated += 1
        except KeitaroClientError as e:
            errors += 1
            print(f"  ERROR {name} id={oid}: {e}")
            if e.response_body:
                print(f"    {e.response_body[:300]}")

    print()
    print(f"Done. {'Would update' if not apply else 'Updated'} {updated}; errors={errors}")
    from assistance import build_nipuhim_v2_action_payload, build_offer_action_payload

    v2 = build_nipuhim_v2_action_payload("fr", "https://example.com/", feed=2)
    blend = build_offer_action_payload("fr", "https://example.com/", feed=2)
    assert CUSTOM2 in v2 and NEEDLE in v2, v2
    assert CUSTOM2 in blend and NEEDLE in blend, blend
    print("Builder check OK")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
