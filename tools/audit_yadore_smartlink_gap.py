#!/usr/bin/env python3
"""
Audit Yadore potential merchants for false negatives (especially smartlink-only).

Compares ``potentialYadore`` rows marked not monetized against:
  - POST /v2/deeplink (coupon-inclusive; deeplink vs smartlink)
  - GET /v2/deeplink/merchant catalog (``isSmartlink``, ``hasSmartlinkHomepage``)

Read-only — no sheet writes.

  python tools/audit_yadore_smartlink_gap.py
  python tools/audit_yadore_smartlink_gap.py --geo de --limit 30
  python tools/audit_yadore_smartlink_gap.py --markets de,fr,uk
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv()

from config import BLEND_SHEETS_SPREADSHEET_ID, YADORE_REPORT_DETAIL_MARKETS  # noqa: E402
from integrations.monetization_geo import geo_for_yadore  # noqa: E402
from integrations.yadore import YadoreClientError, deeplink, fetch_deeplink_merchants, find_catalog_merchant  # noqa: E402
from populate_blend_from_potential import get_sheets_service, read_values  # noqa: E402

POTENTIAL_SHEET = "potentialYadore"


def _norm_host(url_or_domain: str) -> str:
    s = (url_or_domain or "").strip().lower()
    if not s:
        return ""
    if "://" in s:
        s = urlparse(s).netloc or s.split("://", 1)[-1]
    if s.startswith("www."):
        s = s[4:]
    return s.split("/")[0].split("?")[0]


def _url_variants(domain: str, name: str) -> List[str]:
    host = _norm_host(domain) or _norm_host(name)
    if not host:
        return []
    out: List[str] = []
    seen: Set[str] = set()

    def add(u: str) -> None:
        u = u.strip()
        if u and u not in seen:
            seen.add(u)
            out.append(u)

    add(f"https://{host}")
    add(f"https://www.{host}")
    if domain and domain.strip() and domain.strip() not in seen:
        d = domain.strip()
        if not d.lower().startswith("http"):
            add(d)
        else:
            add(d)
    return out


def _probe_urls(urls: List[str], geo: str) -> Tuple[bool, bool, str]:
    """Returns (deeplink_found, smartlink_found, detail)."""
    from config import YADORE_IS_COUPONING

    deeplink_ok = False
    smartlink_ok = False
    detail = ""
    for url in urls:
        try:
            resp = deeplink(url, geo, is_couponing=YADORE_IS_COUPONING)
            found = bool(resp.get("found")) or bool(str(resp.get("clickUrl") or "").strip())
        except YadoreClientError as e:
            found = False
            detail = str(e)[:80]
        if found:
            deeplink_ok = True
            if resp.get("isSmartlink"):
                smartlink_ok = True
        if deeplink_ok:
            break
    return deeplink_ok, smartlink_ok, detail


def _catalog_index(markets: List[str]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """(market, host) -> merchant row from /v2/deeplink/merchant."""
    idx: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for mkt in markets:
        try:
            rows = fetch_deeplink_merchants(mkt)
        except YadoreClientError as e:
            print(f"WARN: deeplink/merchant {mkt}: {e}", file=sys.stderr)
            continue
        for row in rows:
            host = _norm_host(str(row.get("name") or ""))
            if host:
                idx[(mkt, host)] = row
            # also index by registrable domain for subdomain lookups
            parts = host.split(".")
            if len(parts) >= 2:
                base = ".".join(parts[-2:])
                if base != host:
                    idx.setdefault((mkt, base), row)
    return idx


def main() -> None:
    p = argparse.ArgumentParser(description="Audit Yadore smartlink / deeplink monetization gaps.")
    p.add_argument("--geo", default="", help="Filter to one 2-letter geo")
    p.add_argument("--limit", type=int, default=0, help="Max not-monetized rows to re-probe (0=all)")
    p.add_argument(
        "--markets",
        default="",
        help="Comma markets for catalog fetch (default: YADORE_REPORT_DETAIL_MARKETS or sheet geos)",
    )
    args = p.parse_args()

    service = get_sheets_service()
    rows = read_values(service, POTENTIAL_SHEET)
    if not rows or len(rows) < 2:
        print("No potentialYadore data.")
        return

    header = [str(c or "").strip().lower() for c in rows[0]]

    def col(name: str) -> int:
        for alias in (name, name.replace("_", "")):
            if alias in header:
                return header.index(alias)
        return -1

    im = col("merchantid")
    iname = col("merchant") if col("merchant") >= 0 else col("merchantname")
    iurl = col("domain") if col("domain") >= 0 else col("merchanturl") if col("merchanturl") >= 0 else col("url")
    igeo = col("geo_origin") if col("geo_origin") >= 0 else col("geo")
    imon = col("kelkoo_monetization") if col("kelkoo_monetization") >= 0 else col("monetization")

    geo_filter = (args.geo or "").strip().lower()[:2]
    all_rows: List[Dict[str, Any]] = []
    for row in rows[1:]:
        geo = str(row[igeo] if igeo >= 0 and igeo < len(row) else "").strip().lower()[:2]
        if geo_filter and geo != geo_filter:
            continue
        mon = str(row[imon] if imon >= 0 and imon < len(row) else "").strip().lower()
        all_rows.append(
            {
                "mid": str(row[im] if im >= 0 and im < len(row) else "").strip(),
                "name": str(row[iname] if iname >= 0 and iname < len(row) else "").strip(),
                "url": str(row[iurl] if iurl >= 0 and iurl < len(row) else "").strip(),
                "geo": geo,
                "mon": mon,
            }
        )

    monetized = [r for r in all_rows if r["mon"].startswith("monetized")]
    not_mon = [r for r in all_rows if r["mon"] and not r["mon"].startswith("monetized")]
    no_url = [r for r in not_mon if r["mon"] == "no_merchant_url"]

    print(f"Sheet: {POTENTIAL_SHEET} ({BLEND_SHEETS_SPREADSHEET_ID})")
    print(f"Total rows: {len(all_rows)} | monetized: {len(monetized)} | not_monetized: {len(not_mon)} | no_merchant_url: {len(no_url)}")

    if args.markets.strip():
        markets = [geo_for_yadore(x) for x in args.markets.split(",") if x.strip()]
    elif YADORE_REPORT_DETAIL_MARKETS:
        markets = [geo_for_yadore(x) for x in YADORE_REPORT_DETAIL_MARKETS if str(x).strip()]
    else:
        markets = sorted({geo_for_yadore(r["geo"]) for r in all_rows if r["geo"]})

    print(f"Loading deeplink/merchant catalog for {len(markets)} market(s)...")
    catalog = _catalog_index(markets)
    smartlink_hosts = {
        k for k, v in catalog.items() if v.get("isSmartlink")
    }
    print(f"Catalog entries: {len(catalog)} | smartlink merchants: {len(smartlink_hosts)}")

    targets = [r for r in not_mon if r["mon"] != "no_merchant_url"]
    if args.limit > 0:
        targets = targets[: args.limit]

    recovered_probe = 0
    recovered_catalog = 0
    smartlink_missed: List[Dict[str, Any]] = []

    print("\n--- Re-probe not_monetized rows (deeplink API, non-coupon + coupon) ---")
    for r in targets:
        urls = _url_variants(r["url"], r["name"])
        mkt = geo_for_yadore(r["geo"])
        host = _norm_host(r["url"]) or _norm_host(r["name"])
        cat = catalog.get((mkt, host)) if host else None
        nc, c, err = _probe_urls(urls, r["geo"]) if urls else (False, False, "no_url")
        if nc or c:
            recovered_probe += 1
            print(
                f"  RECOVERED probe  {r['geo']} {r['name'][:40]:40} "
                f"nc={nc} coupon={c} was={r['mon'][:40]}"
            )
            continue
        if cat:
            recovered_catalog += 1
            sl = bool(cat.get("isSmartlink"))
            smartlink_missed.append({**r, "catalog": cat})
            print(
                f"  IN CATALOG only {r['geo']} {host:40} smartlink={sl} "
                f"hasHome={cat.get('hasSmartlinkHomepage')} was={r['mon'][:40]}"
            )
            continue
        if err:
            print(f"  still NO       {r['geo']} {r['name'][:40]:40} err={err} was={r['mon'][:40]}")

    # Monetized sheet rows that are smartlink in catalog but we never labeled smartlink
    smart_in_mon = 0
    for r in monetized:
        mkt = geo_for_yadore(r["geo"])
        host = _norm_host(r["url"]) or _norm_host(r["name"])
        cat = catalog.get((mkt, host)) if host else None
        if cat and cat.get("isSmartlink"):
            smart_in_mon += 1

    print("\n--- Summary ---")
    print(f"Re-probed not_monetized (excl no_url): {len(targets)}")
    print(f"Recovered via deeplink re-probe: {recovered_probe}")
    print(f"In catalog but probe failed (smartlink gap candidates): {recovered_catalog}")
    print(f"Already monetized rows that are smartlink in catalog: {smart_in_mon}/{len(monetized)}")
    if smartlink_missed:
        print("\nSmartlink catalog matches we may be under-using (first 15):")
        for r in smartlink_missed[:15]:
            c = r["catalog"]
            print(
                f"  {r['geo']} {r['name'][:35]:35} "
                f"isSmartlink={c.get('isSmartlink')} ecpc={((c.get('estimatedCpc') or {}).get('amount'))}"
            )


if __name__ == "__main__":
    main()
