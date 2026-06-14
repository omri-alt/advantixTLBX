#!/usr/bin/env python3
"""Find Yadore smartlink merchants with recent conversions not on potentialYadore."""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, Set, Tuple
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv()

from config import YADORE_REPORT_DETAIL_MARKETS  # noqa: E402
from integrations.monetization_geo import geo_for_yadore  # noqa: E402
from integrations.yadore import (  # noqa: E402
    fetch_conversion_detail_merchant,
    fetch_deeplink_merchants,
    merchant_monetization_check,
    parse_conversion_detail_merchant_rows,
)
from populate_blend_from_potential import get_sheets_service, read_values  # noqa: E402


def _host(s: str) -> str:
    t = (s or "").strip().lower()
    if not t:
        return ""
    if "://" in t:
        t = urlparse(t).netloc or t.split("://", 1)[-1]
    if t.startswith("www."):
        t = t[4:]
    return t.split("/")[0]


def _potential_hosts() -> Set[Tuple[str, str]]:
    rows = read_values(get_sheets_service(), "potentialYadore")
    if not rows or len(rows) < 2:
        return set()
    h = [str(c or "").strip().lower() for c in rows[0]]
    ig = h.index("geo_origin") if "geo_origin" in h else h.index("geo")
    idom = h.index("domain") if "domain" in h else h.index("url")
    out: Set[Tuple[str, str]] = set()
    for row in rows[1:]:
        geo = str(row[ig] if ig < len(row) else "").strip().lower()[:2]
        dom = _host(str(row[idom] if idom < len(row) else ""))
        if geo and dom:
            out.add((geo, dom))
    return out


def main() -> None:
    end = date.today()
    start = end - timedelta(days=30)
    markets = [geo_for_yadore(x) for x in (YADORE_REPORT_DETAIL_MARKETS or ["de", "fr", "uk", "nl", "it", "es"])]
    have = _potential_hosts()
    print(f"potentialYadore hosts: {len(have)} | markets: {','.join(markets)}")
    print(f"Conversion window: {start} .. {end}")

    catalog: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for mkt in markets:
        for row in fetch_deeplink_merchants(mkt):
            host = _host(str(row.get("name") or ""))
            if host:
                catalog[(mkt, host)] = row

    smart = {k: v for k, v in catalog.items() if v.get("isSmartlink")}
    print(f"Catalog smartlink merchants: {len(smart)}")

    conv_rows = []
    for mkt in markets:
        payload = fetch_conversion_detail_merchant(start.isoformat(), end.isoformat(), market=mkt)
        conv_rows.extend(parse_conversion_detail_merchant_rows(payload))

    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in conv_rows:
        geo = str(row.get("market") or "").strip().lower()[:2]
        host = _host(str(row.get("merchant_url") or "")) or _host(str(row.get("merchant_name") or ""))
        if not geo or not host:
            continue
        key = (geo, host)
        if key not in agg:
            agg[key] = {"clicks": 0, "sales": 0, "name": row.get("merchant_name") or host, "mid": row.get("merchant_id") or ""}
        agg[key]["clicks"] += int(row.get("clicks") or 0)
        agg[key]["sales"] += int(row.get("sales") or 0)

    missing: list[tuple] = []
    for key, rec in agg.items():
        geo, host = key
        mkt = geo_for_yadore(geo)
        if key in have:
            continue
        cat = catalog.get((mkt, host))
        if not cat or not cat.get("isSmartlink"):
            continue
        if rec["sales"] < 1 and rec["clicks"] < 20:
            continue
        missing.append((rec["sales"], rec["clicks"], geo, host, rec["name"], cat))

    missing.sort(reverse=True)
    print(f"\nSmartlink merchants with recent traffic NOT on potentialYadore: {len(missing)}")
    print("(first 25 by sales, then clicks)\n")
    for sales, clicks, geo, host, name, cat in missing[:25]:
        chk = merchant_monetization_check(f"https://{host}", geo, merchant_name=name, deeplink_merchants=[])
        print(
            f"  {geo} {host:35} sales={sales:3} clicks={clicks:5} "
            f"smartlink={cat.get('isSmartlink')} probe={chk.get('mode')} found={chk.get('found')}"
        )


if __name__ == "__main__":
    main()
