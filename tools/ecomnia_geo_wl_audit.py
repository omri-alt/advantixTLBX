#!/usr/bin/env python3
"""
Ecomnia: build per-geo blacklist recommendations + audit whitelist source clicks (last N days).

Reads ``globaList`` (or custom tab) from ``EC_SHEETS_SPREADSHEET_ID`` — columns ``geo``,
``blacklist``, ``whitelist`` (same idea as ``tools/ec_local_copy.py``).

For each campaign (optional name filter), pulls ``adv-stats-by-source`` and reports which
whitelist sources had **clicks > 0** vs **0** (EC lists sources with no buys as 0 clicks).

Examples:
  python tools/ecomnia_geo_wl_audit.py --days 30
  python tools/ecomnia_geo_wl_audit.py --days 30 --json-out runtime/ec_wl_audit.json
  python tools/ecomnia_geo_wl_audit.py --apply-geo-blacklist --dry-run   # print only
  python tools/ecomnia_geo_wl_audit.py --apply-geo-blacklist             # live EC updates
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

load_dotenv(ROOT / ".env")

from config import (  # noqa: E402
    EC_ADVERTISER_KEY,
    EC_AUTH_KEY,
    EC_SECRET_KEY,
    EC_SHEETS_SPREADSHEET_ID,
)
from integrations.ecomnia_geo_lists import (  # noqa: E402
    audit_whitelist_traffic,
    date_range_last_days,
    fetch_adv_stats_by_source,
    fetch_campaign_by_id,
    fetch_campaigns,
    geo_bw_map_from_rows,
    normalize_geo_key,
    post_update_advertiser_campaign,
    recommended_geo_blacklists,
)


def _get_credentials_path() -> str:
    p = ROOT / "credentials.json"
    if not p.exists():
        raise FileNotFoundError(f"credentials.json not found at {p}")
    return str(p)


def _sheet_title_a1_range(title: str, a1_suffix: str) -> str:
    esc = str(title).replace("'", "''")
    return f"'{esc}'!{a1_suffix}"


def _resolve_sheet_title(service: Any, spreadsheet_id: str, tab: str) -> str:
    meta = service.get(spreadsheetId=spreadsheet_id).execute()
    sheets = meta.get("sheets", []) or []
    pairs: List[tuple[int, str]] = []
    for sh in sheets:
        props = (sh or {}).get("properties") or {}
        sid = props.get("sheetId")
        title = props.get("title")
        if isinstance(sid, int) and isinstance(title, str):
            pairs.append((sid, title))
    titles = [t for _, t in pairs]
    if tab in titles:
        return tab
    tl = tab.lower()
    for _, title in pairs:
        if title.lower() == tl:
            return title
    if tab.isdigit():
        want = int(tab)
        for sid, title in pairs:
            if sid == want:
                return title
    raise RuntimeError(f"Tab {tab!r} not found. Available: {titles[:40]}...")


def read_tab_as_dicts(spreadsheet_id: str, tab: str) -> List[Dict[str, str]]:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds = service_account.Credentials.from_service_account_file(_get_credentials_path())
    service = build("sheets", "v4", credentials=creds).spreadsheets()
    resolved = _resolve_sheet_title(service, spreadsheet_id, tab)
    rng = _sheet_title_a1_range(resolved, "A:Z")
    res = service.values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    values = res.get("values") or []
    if not values:
        return []
    header = [str(c or "").strip().lower() for c in values[0]]
    out: List[Dict[str, str]] = []
    for row in values[1:]:
        d: Dict[str, str] = {}
        for i, key in enumerate(header):
            if not key:
                continue
            d[key] = str(row[i] if i < len(row) else "").strip()
        if any(d.values()):
            out.append(d)
    return out


def _campaign_name_suffix(name: str) -> str:
    parts = (name or "").strip().split("-")
    return parts[-1].lower() if parts else ""


def run(
    *,
    days: int,
    tab_globa: str,
    spreadsheet_id: str,
    name_contains: str,
    skip_wl_campaigns: bool,
    min_hits_in_geo: int,
    min_total_global: int,
    apply_geo_blacklist: bool,
    dry_run: bool,
    json_out: Optional[str],
    limit_campaigns: int,
) -> int:
    if not (EC_ADVERTISER_KEY and EC_AUTH_KEY and EC_SECRET_KEY):
        print("Set EC_ADVERTISER_KEY (ADVERTISER_KEY), EC_AUTH_KEY (AUTH_KEY), EC_SECRET_KEY (SECRET_KEY) in .env", file=sys.stderr)
        return 2

    sess = requests.Session()
    campaigns = fetch_campaigns(EC_ADVERTISER_KEY, EC_AUTH_KEY, EC_SECRET_KEY, session=sess)
    nc = (name_contains or "").strip().lower()
    filtered: List[Dict[str, Any]] = []
    for c in campaigns:
        if not isinstance(c, dict):
            continue
        nm = str(c.get("name") or "")
        if nc and nc not in nm.lower():
            continue
        if skip_wl_campaigns and _campaign_name_suffix(nm) == "wl":
            continue
        filtered.append(c)

    if limit_campaigns > 0:
        filtered = filtered[:limit_campaigns]

    by_geo_rec, global_candidates = recommended_geo_blacklists(
        campaigns,
        min_hits_in_geo=min_hits_in_geo,
        min_total_hits_across_geos=min_total_global,
    )

    try:
        sheet_rows = read_tab_as_dicts(spreadsheet_id, tab_globa)
    except Exception as e:
        print(f"Sheet read failed ({tab_globa}): {e}", file=sys.stderr)
        sheet_rows = []

    geo_map = geo_bw_map_from_rows(sheet_rows)

    utc_today = datetime.now(timezone.utc).date()
    start, end = date_range_last_days(utc_today, days)

    audits: List[Dict[str, Any]] = []
    for c in filtered:
        geo = normalize_geo_key(str(c.get("geo") or ""))
        wl = geo_map.get(geo, {}).get("whitelist", [])
        try:
            stats = fetch_adv_stats_by_source(
                str(c.get("id") or ""),
                start,
                end,
                EC_ADVERTISER_KEY,
                EC_AUTH_KEY,
                EC_SECRET_KEY,
                session=sess,
            )
        except Exception as e:
            audits.append(
                {
                    "campaign_id": c.get("id"),
                    "campaign_name": c.get("name"),
                    "geo": geo,
                    "error": str(e),
                }
            )
            continue
        row = audit_whitelist_traffic(c, wl, stats)
        row["stats_window_start"] = start
        row["stats_window_end"] = end
        audits.append(row)

    apply_log: List[Dict[str, Any]] = []
    if apply_geo_blacklist:
        for c in campaigns:
            if not isinstance(c, dict):
                continue
            nm = str(c.get("name") or "")
            if skip_wl_campaigns and _campaign_name_suffix(nm) == "wl":
                continue
            geo = normalize_geo_key(str(c.get("geo") or ""))
            add_list = by_geo_rec.get(geo) or []
            if not add_list:
                continue
            current = c.get("blacklistsources")
            if not isinstance(current, list):
                current = []
            to_add = [s for s in add_list if s not in current]
            if not to_add:
                continue
            cid = str(c.get("id") or "")
            if dry_run:
                apply_log.append({"campaign_id": cid, "campaign_name": nm, "would_add": to_add})
                continue
            full = fetch_campaign_by_id(cid, EC_ADVERTISER_KEY, EC_AUTH_KEY, EC_SECRET_KEY, session=sess)
            if not full:
                apply_log.append({"campaign_id": cid, "error": "fetch campaign failed"})
                continue
            bl = list(full.get("blacklistsources") or [])
            for s in to_add:
                if s not in bl:
                    bl.append(s)
            full["blacklistsources"] = bl
            try:
                resp = post_update_advertiser_campaign(
                    cid, full, EC_ADVERTISER_KEY, EC_AUTH_KEY, EC_SECRET_KEY, session=sess
                )
                apply_log.append({"campaign_id": cid, "campaign_name": nm, "added": to_add, "response_ok": True})
            except Exception as e:
                apply_log.append({"campaign_id": cid, "campaign_name": nm, "added": to_add, "error": str(e)})

    envelope: Dict[str, Any] = {
        "stats_window_days": days,
        "stats_range": {"start": start, "end": end},
        "recommended_geo_blacklist": by_geo_rec,
        "global_blacklist_candidates": global_candidates,
        "geo_sheet_rows_parsed": len(geo_map),
        "whitelist_traffic_audits": audits,
        "apply_geo_blacklist_log": apply_log if apply_geo_blacklist else [],
    }

    if json_out:
        outp = Path(json_out)
        outp.parent.mkdir(parents=True, exist_ok=True)
        outp.write_text(json.dumps(envelope, indent=2), encoding="utf-8")
        print(f"Wrote {outp}")

    # Human-readable summary
    print(f"Window: {start} .. {end} ({days}d)\n")
    print("Recommended geo blacklists (from cross-campaign counts):")
    for g, srcs in sorted(by_geo_rec.items()):
        print(f"  {g}: {len(srcs)} sources")
    print(f"\nGlobal blacklist candidates (sum counts > {min_total_global}): {len(global_candidates)}")
    no_wl_click = [a for a in audits if not a.get("error") and not a.get("any_whitelist_click") and a.get("whitelist_size", 0) > 0]
    print(f"\nCampaigns with geo WL defined but no WL-source clicks in window: {len(no_wl_click)}")
    for a in no_wl_click[:50]:
        print(f"  - {a.get('campaign_name')} ({a.get('geo')}) wl={a.get('whitelist_size')}")
    if len(no_wl_click) > 50:
        print(f"  ... and {len(no_wl_click) - 50} more")
    if apply_geo_blacklist:
        print(f"\nApply geo blacklist: {'DRY-RUN' if dry_run else 'LIVE'} — {len(apply_log)} campaign updates")
        for line in apply_log[:30]:
            print(f"  {line}")
        if len(apply_log) > 30:
            print(f"  ... and {len(apply_log) - 30} more")

    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description="Ecomnia geo BL/WL + 30d whitelist traffic audit")
    ap.add_argument("--days", type=int, default=30, help="Lookback days for adv-stats-by-source (default 30)")
    ap.add_argument("--tab-globa", default="globaList", help="Sheet tab with geo / blacklist / whitelist")
    ap.add_argument("--spreadsheet-id", default="", help="Override EC_SHEETS_SPREADSHEET_ID")
    ap.add_argument("--name-contains", default="", help="Only audit campaigns whose name contains this substring")
    ap.add_argument("--no-skip-wl", action="store_true", help="Include campaigns whose name ends with -wl")
    ap.add_argument("--min-geo-hits", type=int, default=4, help="Recommend geo BL if source blacklisted in >= this many campaigns in that geo (legacy: >3 → 4)")
    ap.add_argument("--min-global-sum", type=int, default=5, help="Global candidate if total blacklist hits across geos > this (legacy: >5)")
    ap.add_argument("--apply-geo-blacklist", action="store_true", help="Push recommended per-geo sources into each campaign's blacklistsources")
    ap.add_argument("--dry-run", action="store_true", help="With --apply-geo-blacklist, do not POST updates")
    ap.add_argument("--json-out", default="", help="Write full JSON report to this path")
    ap.add_argument("--limit", type=int, default=0, help="Max campaigns to audit (0 = all)")
    args = ap.parse_args()
    sid = (args.spreadsheet_id or "").strip() or EC_SHEETS_SPREADSHEET_ID
    rc = run(
        days=max(1, args.days),
        tab_globa=args.tab_globa,
        spreadsheet_id=sid,
        name_contains=args.name_contains,
        skip_wl_campaigns=not args.no_skip_wl,
        min_hits_in_geo=max(1, args.min_geo_hits),
        min_total_global=max(0, args.min_global_sum),
        apply_geo_blacklist=bool(args.apply_geo_blacklist),
        dry_run=bool(args.dry_run),
        json_out=(args.json_out.strip() or None),
        limit_campaigns=max(0, args.limit),
    )
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
