#!/usr/bin/env python3
"""Audit streams on NIPUHIM-feed1/2/5 child campaigns."""
from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from assistance import parse_blend_stream_geo_channel
from integrations.keitaro import KeitaroClient
from integrations.keitaro_child_campaigns import nipuhim_child_campaign_id_for_account


def _classify_stream(stream: dict) -> str:
    stype = (stream.get("type") or "").lower()
    name = (stream.get("name") or "").strip()
    nlower = name.lower()
    if stype == "default":
        return "default/fallback"
    if "fallback" in nlower:
        return "fallback-named"
    geo, ch = parse_blend_stream_geo_channel(name)
    if geo and ch in ("desktop", "mobile"):
        return f"device:{geo}_{ch}"
    if geo and ch == "legacy":
        return f"legacy-geo:{geo}"
    return f"other:{name or stype}"


def audit_campaign(cid: int, label: str) -> dict:
    client = KeitaroClient()
    streams = client.get_streams(cid)
    # Keitaro returns streams in position order typically; sort by position if present
    streams = sorted(streams, key=lambda s: (s.get("position") is None, s.get("position") or 9999, s.get("id") or 0))

    by_class: dict[str, list] = defaultdict(list)
    name_counts: dict[str, int] = defaultdict(int)
    for s in streams:
        name = (s.get("name") or "").strip()
        name_counts[name.lower()] += 1
        by_class[_classify_stream(s)].append(s)

    print(f"\n{'='*70}")
    print(f"{label} (campaign {cid}) — {len(streams)} streams")
    print(f"{'='*70}")
    print(f"{'pos':>4}  {'id':>6}  {'type':<10}  {'offers':>6}  name")
    print("-" * 70)
    for s in streams:
        pos = s.get("position", "?")
        sid = s.get("id", "?")
        stype = (s.get("type") or "")[:10]
        offers = len(s.get("offers") or [])
        name = s.get("name") or ""
        dup = " DUPLICATE" if name_counts[(name or "").lower()] > 1 else ""
        print(f"{str(pos):>4}  {str(sid):>6}  {stype:<10}  {offers:>6}  {name}{dup}")

    dups = [n for n, c in name_counts.items() if c > 1 and n]
    if dups:
        print(f"\nDuplicate names: {', '.join(sorted(dups))}")

    defaults = [s for s in streams if (s.get("type") or "").lower() == "default"]
    device = [s for s in streams if parse_blend_stream_geo_channel(s.get("name") or "")[1] in ("desktop", "mobile")]
    legacy = [s for s in streams if _classify_stream(s).startswith("legacy-geo:")]

    if defaults:
        first_default_pos = defaults[0].get("position")
        first_device_pos = device[0].get("position") if device else None
        print(f"\nDefault/fallback streams: {len(defaults)} (first position={first_default_pos})")
        if first_device_pos is not None and first_default_pos is not None:
            if first_default_pos < first_device_pos:
                print("  WARNING: fallback appears BEFORE device geo streams in position order")

    if legacy:
        print(f"Legacy undivided geo streams (no _desktop/_mobile): {len(legacy)}")
        for s in legacy[:5]:
            print(f"  id={s.get('id')} name={s.get('name')}")
        if len(legacy) > 5:
            print(f"  ... +{len(legacy)-5} more")

    return {
        "campaign_id": cid,
        "label": label,
        "streams": streams,
        "defaults": defaults,
        "device": device,
        "legacy": legacy,
        "duplicates": dups,
    }


def main() -> int:
    for account in (1, 2, 5):
        cid, feed_key, _ = nipuhim_child_campaign_id_for_account(account)
        audit_campaign(cid, f"NIPUHIM-{feed_key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
