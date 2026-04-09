#!/usr/bin/env python3
"""
Bulk open SourceKnowledge campaigns from SK tools sheet tab.

Input sheet defaults to the same SK tools spreadsheet and tab used by legacy bulk opener.
Expected columns (case-insensitive):
  - brand
  - geo
  - url (or homepage/homepage url/businessurl)
  - hpfb (or hp/homepage fallback)
  - category (category id)
Optional:
  - costumecpc / customcpc / cpc

Examples:
  python sk_bulk_open_from_sheet.py --prefix KLFIX --alias 7FDKRK --dry-run
  python sk_bulk_open_from_sheet.py --prefix KLFLEX --alias 9Xq9dSMh --apply
"""
from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import SOURCEKNOWLEDGE_API_KEY  # noqa: E402


BASE_URL = "https://api.sourceknowledge.com/affiliate/v2"
SKTOOLS_SPREADSHEET_ID = "176wSQDDz9D1APmAXiYPeECwMqCQm3mvMBwgj8MKqmgk"
DEFAULT_INPUT_TAB = "bulkSK-KLFIX"
REQUEST_TIMEOUT = 60
COOLDOWN_SECONDS = 60


def _usage_error(msg: str) -> None:
    print(f"Error: {msg}")
    sys.exit(2)


def _headers() -> dict[str, str]:
    return {"accept": "application/json", "X-API-KEY": SOURCEKNOWLEDGE_API_KEY}


def _request(method: str, url: str, *, payload: dict[str, Any] | None = None) -> requests.Response:
    while True:
        try:
            r = requests.request(method, url, headers=_headers(), json=payload, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as e:
            print(f"  Network error: {e}. cooldown {COOLDOWN_SECONDS}s")
            time.sleep(COOLDOWN_SECONDS)
            continue
        if r.status_code == 429:
            print(f"  429 rate-limit. cooldown {COOLDOWN_SECONDS}s")
            time.sleep(COOLDOWN_SECONDS)
            continue
        return r


def _strip_brand_name(brand_name: str) -> str:
    striped = (brand_name or "").replace(" ", "").replace("-", "")
    parts = striped.split(".")
    if len(parts) <= 1:
        return striped
    if len(parts) == 2:
        return parts[0]
    return parts[-2]


def _encode_url(url: str) -> str:
    # Keep legacy behavior: only hard-replace spaces.
    return (url or "").replace(" ", "%20")


def _tracking_url(brand: str, geo: str, hp: str, prefix: str, alias: str) -> str:
    base = (
        f"https://shopli.city/raini?rain=https://trck.shopli.city/{alias}"
        "?external_id={clickid}&cost={adv_price}&sub_id_4={traffic_type}&sub_id_5={sub_id}&sub_id_3={oadest}"
    )
    brand_macro = quote(f"{brand}-{geo.upper()}-{prefix}-SK", safe="")
    hp_macro = quote(hp or "", safe="")
    macros = f"sub_id_2={geo.lower()}&sub_id_6={brand_macro}&sub_id_1={hp_macro}"
    return f"{base}&{macros}"


def _get_credentials_path() -> str:
    p = Path(__file__).resolve().parent / "credentials.json"
    if not p.exists():
        raise FileNotFoundError(f"credentials.json not found at {p}")
    return str(p)


def _read_input_rows(tab_name: str) -> list[dict[str, str]]:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds = service_account.Credentials.from_service_account_file(_get_credentials_path())
    service = build("sheets", "v4", credentials=creds).spreadsheets()

    quoted = tab_name.replace("'", "''")
    res = service.values().get(spreadsheetId=SKTOOLS_SPREADSHEET_ID, range=f"'{quoted}'!A:Z").execute()
    values = res.get("values") or []
    if not values:
        return []

    header = [str(c or "").strip().lower() for c in values[0]]

    def idx(*names: str) -> int:
        for n in names:
            if n in header:
                return header.index(n)
        return -1

    i_brand = idx("brand")
    i_geo = idx("geo")
    i_url = idx("url", "homepage", "homepage url", "businessurl", "business_url")
    i_hp = idx("hpfb", "hp", "homepage fallback", "homepage_fallback")
    i_cat = idx("category", "categoryid", "category id")
    i_cpc = idx("costumecpc", "customcpc", "cpc")

    required_missing = []
    if i_brand < 0:
        required_missing.append("brand")
    if i_geo < 0:
        required_missing.append("geo")
    if i_url < 0:
        required_missing.append("url/homepage")
    if i_hp < 0:
        required_missing.append("hpfb/hp")
    if i_cat < 0:
        required_missing.append("category")
    if required_missing:
        _usage_error(f"Input tab '{tab_name}' missing columns: {', '.join(required_missing)}")

    out: list[dict[str, str]] = []
    for r in values[1:]:
        brand = str(r[i_brand] if i_brand < len(r) else "").strip()
        geo = str(r[i_geo] if i_geo < len(r) else "").strip().lower()[:2]
        url = str(r[i_url] if i_url < len(r) else "").strip()
        hp = str(r[i_hp] if i_hp < len(r) else "").strip()
        cat = str(r[i_cat] if i_cat < len(r) else "").strip()
        cpc = str(r[i_cpc] if i_cpc >= 0 and i_cpc < len(r) else "").strip()
        if not (brand and geo and url and hp and cat):
            continue
        out.append({
            "brand": brand,
            "geo": geo,
            "url": url,
            "hpfb": hp,
            "category": cat,
            "customcpc": cpc,
        })
    return out


def _new_advertiser(name: str, business_url: str, category_id: int) -> int:
    body = {"name": name, "businessUrl": business_url, "categoryId": int(category_id)}
    r = _request("POST", f"{BASE_URL}/advertisers", payload=body)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Advertiser create failed: HTTP {r.status_code} {r.text[:200]}")
    data = r.json() if r.text else {}
    aid = data.get("id")
    if not str(aid).isdigit():
        raise RuntimeError(f"Advertiser create response missing id: {data}")
    return int(aid)


def _new_campaign(payload: dict[str, Any]) -> dict[str, Any]:
    r = _request("POST", f"{BASE_URL}/campaigns", payload=payload)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Campaign create failed: HTTP {r.status_code} {r.text[:200]}")
    return r.json() if r.text else {}


def main() -> None:
    load_dotenv()
    if not SOURCEKNOWLEDGE_API_KEY:
        _usage_error("Missing KEYSK/.env")

    prefix = ""
    alias = ""
    tab = DEFAULT_INPUT_TAB
    apply = False

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        a = args[i]
        if a == "--prefix" and i + 1 < len(args):
            prefix = args[i + 1].strip()
            i += 2
            continue
        if a == "--alias" and i + 1 < len(args):
            alias = args[i + 1].strip()
            i += 2
            continue
        if a == "--tab" and i + 1 < len(args):
            tab = args[i + 1].strip()
            i += 2
            continue
        if a == "--apply":
            apply = True
            i += 1
            continue
        if a == "--dry-run":
            apply = False
            i += 1
            continue
        if a in ("-h", "--help"):
            print(__doc__)
            return
        _usage_error(f"Unknown argument: {a}")

    if not prefix:
        _usage_error("Missing --prefix")
    if not alias:
        _usage_error("Missing --alias")

    rows = _read_input_rows(tab)
    if not rows:
        print(f"No rows found in tab '{tab}'.")
        return

    print("SK bulk campaign opener from sheet")
    print(f"Mode: {'APPLY' if apply else 'DRY-RUN'}")
    print(f"Spreadsheet: {SKTOOLS_SPREADSHEET_ID}")
    print(f"Tab: {tab}")
    print(f"Prefix: {prefix}")
    print(f"Alias: {alias}")
    print(f"Rows: {len(rows)}")
    print()

    created_adv = 0
    created_camp = 0
    failed = 0

    for idx, item in enumerate(rows, start=1):
        brand = _strip_brand_name(item["brand"])
        geo = item["geo"].lower()
        advertiser_name = f"{brand}-{geo.upper()}-{prefix}"
        campaign_name = f"{brand}{prefix}-{geo.upper()}-all"
        geo_target = ["GB" if geo == "uk" else geo.upper()]

        tracking = _encode_url(_tracking_url(brand, geo, item["hpfb"], prefix, alias))
        cpc_raw = item.get("customcpc", "")

        campaign_payload: dict[str, Any] = {
            "name": campaign_name,
            "start": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "dailyBudget": "25.0",
            "cpc": cpc_raw or "0.05",
            "trackingUrl": tracking,
            "advertiserId": None,
            "allowDeepLink": True,
            "geoTargeting": geo_target,
            "partnerChannels": ["1", "2", "3", "5", "6", "8", "9", "12", "13", "14"],
            "strategyId": 3,
        }

        if not apply:
            print(f"[{idx}/{len(rows)}] DRY advertiser={advertiser_name} campaign={campaign_name} geo={geo_target[0]} cpc={campaign_payload['cpc']}")
            continue

        try:
            adv_id = _new_advertiser(advertiser_name, item["url"], int(float(item["category"])))
            created_adv += 1
            campaign_payload["advertiserId"] = adv_id
            time.sleep(2)

            camp = _new_campaign(campaign_payload)
            created_camp += 1
            cid = camp.get("id")
            print(f"[{idx}/{len(rows)}] OK advertiser_id={adv_id} campaign_id={cid}")
            time.sleep(2)
        except Exception as e:
            failed += 1
            print(f"[{idx}/{len(rows)}] ERROR {advertiser_name}: {e}")

    print()
    print(
        f"Summary: rows={len(rows)} created_advertisers={created_adv} "
        f"created_campaigns={created_camp} failed={failed} mode={'apply' if apply else 'dry-run'}"
    )
    if apply and failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
