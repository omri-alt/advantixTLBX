#!/usr/bin/env python3
"""
During-day Blend monetization stopper.

Reads the `Blend` sheet rows with `auto=v` and checks Kelkoo monetization based on
the `feed` column (kelkoo1/kelkoo2).

Then updates the Blend Keitaro flow for each geo:
  - non-monetized offers get share=0
  - monetized offers are renormalized so shares sum to 100

It does NOT delete rows from the sheet (so you can inspect them tomorrow).

Usage:
  python blend_stop_closed_merchants.py
  python blend_stop_closed_merchants.py --geo it
"""
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dotenv import load_dotenv

load_dotenv()

from config import FEED1_API_KEY, FEED2_API_KEY
from assistance import (
    add_country_flow,
    flow_name_to_geo,
    get_campaign_streams,
    find_campaign_by_alias_or_name,
    get_campaigns_data,
)
from integrations.keitaro import KeitaroClient
from integrations.kelkoo_search import kelkoo_merchant_link_check

from google.oauth2 import service_account
from googleapiclient.discovery import build


SPREADSHEET_ID = "1h9lBPTREEJO9VVvj6wctCgCOn3YcwJBGIk_MBwXw-xY"
BLEND_SHEET_NAME = "Blend"
BLEND_CAMPAIGN_ALIAS = "9Xq9dSMh"


def get_credentials_path() -> str:
    p = Path(__file__).resolve().parent / "credentials.json"
    if not p.exists():
        raise FileNotFoundError(f"credentials.json not found at {p}")
    return str(p)


def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(get_credentials_path())
    return build("sheets", "v4", credentials=creds).spreadsheets()


def _slug(s: str, max_len: int = 48) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        return "unknown"
    return s[:max_len].rstrip("_")


def _normalize_geo(g: str) -> str:
    return (g or "").strip().lower()[:2]


def _kelkoo_api_key_for_feed_tag(feed_tag: str) -> Optional[str]:
    ft = (feed_tag or "").strip().lower()
    if ft == "kelkoo1":
        return FEED1_API_KEY
    if ft == "kelkoo2":
        return FEED2_API_KEY
    return None


@dataclass(frozen=True)
class BlendRow:
    brand_name: str
    offer_url: str
    click_cap: float
    geo: str
    auto_flag: str
    feed_tag: str

    @property
    def offer_name(self) -> str:
        feed_slug = _slug(self.feed_tag, max_len=24)
        return f"blend_{self.geo}_{feed_slug}_{_slug(self.brand_name)}"


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


def read_auto_rows(service, only_geo: Optional[str]) -> List[BlendRow]:
    quoted = BLEND_SHEET_NAME.replace("'", "''")
    result = service.values().get(spreadsheetId=SPREADSHEET_ID, range=f"'{quoted}'!A:Z").execute()
    rows = result.get("values") or []
    if len(rows) < 2:
        return []
    header = [str(c).strip() for c in rows[0]]
    idx = {name: i for i, name in enumerate(header)}

    def get_cell(row: list, name: str) -> str:
        i = idx.get(name)
        if i is None or i >= len(row):
            return ""
        return str(row[i] or "").strip()

    out: List[BlendRow] = []
    for row in rows[1:]:
        geo = _normalize_geo(get_cell(row, "geo"))
        if only_geo and geo != only_geo:
            continue
        cap = _parse_click_cap(get_cell(row, "clickCap"))
        if cap is None or cap <= 0:
            continue
        auto_flag = (get_cell(row, "auto") or "x").lower()
        if auto_flag != "v":
            continue
        brand = get_cell(row, "brandName")
        url = get_cell(row, "offerUrl")
        if not brand or not url or not geo:
            continue
        feed_tag = (get_cell(row, "feed") or "kelkoo1").lower()
        out.append(
            BlendRow(
                brand_name=brand,
                offer_url=url,
                click_cap=cap,
                geo=geo,
                auto_flag=auto_flag,
                feed_tag=feed_tag,
            )
        )
    return out


def _shares_from_weights_allow_zero(weights: List[float]) -> List[int]:
    n = len(weights)
    if n == 0:
        return []
    w = [max(0.0, float(x)) for x in weights]
    pos = [wi for wi in w if wi > 0]
    if not pos:
        # Safety fallback: if everything is closed, keep equal shares (so API remains valid).
        base = 100 // n
        rem = 100 % n
        return [base + (1 if i < rem else 0) for i in range(n)]
    total = sum(pos)
    # Largest remainder on positive weights only; closed weights get 0.
    raw = []
    for wi in w:
        raw.append((wi / total) * 100 if wi > 0 else 0.0)
    base = [int(x) for x in raw]
    rems = [x - int(x) for x in raw]
    leftover = 100 - sum(base)
    order = sorted(range(n), key=lambda i: rems[i], reverse=True)
    for i in order[:leftover]:
        base[i] += 1
    # Drift guard
    drift = 100 - sum(base)
    if drift != 0 and n > 0:
        base[0] += drift
    return base


def main() -> None:
    parser = argparse.ArgumentParser(description="Stop non-monetized Blend merchants (share=0).")
    parser.add_argument("--geo", dest="geo", default=None, help="Optional geo filter (2-letter lower).")
    args = parser.parse_args()

    only_geo = _normalize_geo(args.geo) if args.geo else None

    service = get_sheets_service()
    auto_rows = read_auto_rows(service, only_geo=only_geo)
    if not auto_rows:
        print("No auto=v Blend rows found (nothing to check).")
        return

    campaigns = get_campaigns_data()
    c = find_campaign_by_alias_or_name(campaigns, alias=BLEND_CAMPAIGN_ALIAS, name=BLEND_CAMPAIGN_ALIAS)
    if not c or c.get("id") is None:
        raise ValueError(f"Blend campaign not found by alias {BLEND_CAMPAIGN_ALIAS!r}")
    campaign_id = int(c["id"])

    client = KeitaroClient()
    streams = get_campaign_streams(campaign_id)
    stream_by_geo: Dict[str, Dict[str, Any]] = {}
    for s in streams:
        g = flow_name_to_geo(s.get("name") or "")
        if g:
            stream_by_geo[g] = s

    offers = client.get_offers()
    offer_id_by_name: Dict[str, int] = {}
    for o in offers:
        name = (o.get("name") or "").strip()
        oid = o.get("id")
        if name and oid is not None:
            offer_id_by_name[name] = int(oid)

    rows_by_geo: Dict[str, List[BlendRow]] = {}
    for r in auto_rows:
        rows_by_geo.setdefault(r.geo, []).append(r)

    for geo, geo_rows in sorted(rows_by_geo.items()):
        stream = stream_by_geo.get(geo)
        if not stream:
            print(f"{geo}: no flow found, skipping")
            continue
        sid = int(stream["id"])

        weights: List[float] = []
        offer_ids: List[int] = []
        closed = 0
        for r in geo_rows:
            api_key = _kelkoo_api_key_for_feed_tag(r.feed_tag)
            if not api_key:
                # Unknown feed type: keep traffic unchanged.
                monetized = True
            else:
                res = kelkoo_merchant_link_check(r.offer_url, geo, api_key)
                monetized = bool(res.get("found"))
            w = r.click_cap if monetized else 0.0
            if w <= 0:
                closed += 1
            oname = r.offer_name
            oid = offer_id_by_name.get(oname)
            if oid is None:
                # If the offer doesn't exist yet, ignore it.
                continue
            offer_ids.append(oid)
            weights.append(w)

        if not offer_ids:
            print(f"{geo}: no matching Keitaro offers found, skipping")
            continue

        shares = _shares_from_weights_allow_zero(weights)
        offers_payload = [
            {"offer_id": oid, "state": "active", "share": share}
            for oid, share in zip(offer_ids, shares)
        ]
        client.update_stream(sid, {"offers": offers_payload})
        print(f"{geo}: updated {len(offer_ids)} offers; closed={closed} (share=0 when possible)")


if __name__ == "__main__":
    main()

