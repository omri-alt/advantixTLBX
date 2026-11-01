#!/usr/bin/env python3
"""
Restore Quality/effinity/flexoffers Flow 2 shares from the last checkmon notes
snapshot (written *before* the share PUT on that run).

  python scripts/keitaro_feed_balance_restore_notes_shares.py
  python scripts/keitaro_feed_balance_restore_notes_shares.py --apply
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from assistance import get_offers_data, set_flow_offers_weighted_keep_zeros
from integrations.keitaro import KeitaroClient
from integrations.keitaro_feed_balance import (
    _DEFAULT_GROUPS,
    _FEED_LABELS,
    list_regular_flow_feed_offers,
    parse_feed_balance_config,
)

SHARE_RE = re.compile(
    r"^\s+(\S+)\s+(YES\+BOTH|SMARTLINK|YES|NO|N/A)\s+share=(\d+)%",
    re.I | re.M,
)
LABEL_TO_FEED = {v.lower(): k for k, v in _FEED_LABELS.items()}
CANONICAL_NAMES = {
    "feed1": ["KL Feed 1"],
    "feed2": ["KL Feed 2"],
    "feed5": ["KL Feed 5"],
    "feed8": ["KL feed 4", "KL Feed 4"],
    "feed3": ["YAD Feed 3 - no coupon"],
    "feed4": ["Adexa feed4"],
    "feed6": ["shopnomix Feed6 -coupons", "shopnomix Feed6 - non-coupon"],
}
# Notes listed every shopnomix slot as 0; live coupons offer is the real split.
SKIP_CAMPAIGN_IDS = {153}


def parse_notes_shares(notes: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for lab, _status, share in SHARE_RE.findall(notes or ""):
        fk = LABEL_TO_FEED.get(lab.strip().lower())
        if not fk:
            continue
        out[fk] = int(share)
    return out


def _offers_by_name(offers: List[Dict[str, Any]]) -> Dict[str, int]:
    idx: Dict[str, int] = {}
    for o in offers:
        name = str(o.get("name") or "").strip()
        oid = int(o.get("id") or 0)
        if name and oid:
            idx[name] = oid
    return idx


def restore_campaign(
    client: KeitaroClient,
    camp: Dict[str, Any],
    name_to_id: Dict[str, int],
    *,
    apply: bool,
) -> Optional[Dict[str, Any]]:
    cid = int(camp.get("id") or 0)
    if cid in SKIP_CAMPAIGN_IDS:
        return None
    notes = str(camp.get("notes") or "")
    wanted = parse_notes_shares(notes)
    if not wanted:
        return None
    sid, flow = list_regular_flow_feed_offers(client, cid)
    if not sid:
        return {"campaign_id": cid, "name": camp.get("name"), "action": "skip_no_stream"}

    by_feed: Dict[str, List[Tuple[int, str, int]]] = {}
    for fo in flow:
        by_feed.setdefault(fo.feed_key, []).append((fo.offer_id, fo.offer_name, fo.share))

    weights: Dict[int, float] = {}
    zeros: List[int] = []
    attached: List[str] = []
    used_ids = {fo.offer_id for fo in flow}

    for fk, share in wanted.items():
        slots = by_feed.get(fk) or []
        if not slots:
            for name in CANONICAL_NAMES.get(fk, []):
                oid = name_to_id.get(name)
                if oid and oid not in used_ids:
                    slots.append((oid, name, 0))
                    used_ids.add(oid)
                    attached.append(f"{fk}:{name}:{oid}")
                    break
        if not slots:
            continue
        slots_sorted = sorted(slots, key=lambda x: (-x[2], x[0]))
        primary = slots_sorted[0][0]
        if share > 0:
            weights[primary] = float(share)
        else:
            zeros.append(primary)
        for oid, _n, _s in slots_sorted[1:]:
            zeros.append(oid)

    for fo in flow:
        if fo.offer_id in weights or fo.offer_id in zeros:
            continue
        if fo.share > 0 and fo.offer_id not in weights:
            zeros.append(fo.offer_id)
        else:
            zeros.append(fo.offer_id)

    if not weights:
        return None

    live_map = {fo.feed_key: fo.share for fo in flow}
    changed = any(int(wanted.get(fk, live_map.get(fk, 0))) != int(live_map.get(fk, -1)) for fk in wanted)
    missing = [fk for fk in wanted if fk not in by_feed]
    if not changed and not missing:
        return None

    rec = {
        "campaign_id": cid,
        "name": camp.get("name"),
        "stream_id": sid,
        "wanted": wanted,
        "live": live_map,
        "attach": attached,
        "weights": {str(k): v for k, v in weights.items()},
    }
    if not apply:
        rec["action"] = "would_restore"
        return rec
    set_flow_offers_weighted_keep_zeros(int(sid), weights, zero_offer_ids=zeros)
    rec["action"] = "restored"
    return rec


def main() -> int:
    ap = argparse.ArgumentParser(description="Restore Flow 2 shares from last checkmon notes.")
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    client = KeitaroClient()
    want = set(_DEFAULT_GROUPS)
    camps: List[Dict[str, Any]] = []
    offset = 0
    while True:
        batch = client.get_campaigns(offset=offset, limit=250)
        if not batch:
            break
        for camp in batch:
            if str(camp.get("group") or "").strip() not in want:
                continue
            if str(camp.get("state") or "").lower() == "disabled":
                continue
            camps.append(camp)
        if len(batch) < 250:
            break
        offset += 250

    name_to_id = _offers_by_name(get_offers_data())
    rows = []
    for camp in camps:
        rec = restore_campaign(client, camp, name_to_id, apply=args.apply)
        if rec:
            rows.append(rec)
            print(
                f"{rec['action']:14} {rec['campaign_id']:>4} {rec['name']} "
                f"wanted={rec['wanted']} attach={rec['attach']}"
            )

    out = {
        "apply": bool(args.apply),
        "restored": sum(1 for r in rows if r.get("action") == "restored"),
        "would_restore": sum(1 for r in rows if r.get("action") == "would_restore"),
        "rows": rows,
    }
    path = ROOT / "data" / "feed_balance_restore_notes.json"
    path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print()
    print(f"{'Applied' if args.apply else 'Dry-run'}: {len(rows)} campaign(s) -> {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
