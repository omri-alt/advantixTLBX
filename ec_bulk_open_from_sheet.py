#!/usr/bin/env python3
"""
Bulk create Ecomnia (KL-style) campaigns from a Google Sheet tab.

Spreadsheet defaults to EC_SHEETS_SPREADSHEET_ID (see config / .env).
Expected columns (case-insensitive; supports legacy bulkEC + sheet bulk names):

  - brand
  - geo
  - url or hp (homepage URL for EC)
  - hpfb or fbhp (homepage fallback for tracking sub_id_1)

Uses the same create payload as legacy ``ec (2).py`` / ``bulkEC.py``, with
configurable Keitaro ``alias`` (trck path) and affiliation ``prefix`` (sub_id_6 / name).

Examples:
  python ec_bulk_open_from_sheet.py --prefix KLFIX --alias 7FDKRK --dry-run
  python ec_bulk_open_from_sheet.py --prefix KLFIX --alias 7FDKRK --tab bulkEC-KLFIX --apply
"""
from __future__ import annotations

import hashlib
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (  # noqa: E402
    EC_ADVERTISER_KEY,
    EC_AUTH_KEY,
    EC_SECRET_KEY,
    EC_SHEETS_SPREADSHEET_ID,
)

DEFAULT_INPUT_TAB = "bulkEC-KLFIX"
REQUEST_TIMEOUT = 60
COOLDOWN_SECONDS = 30
ROW_DELAY_SECONDS = 15


def _usage_error(msg: str) -> None:
    print(f"Error: {msg}")
    sys.exit(2)


def _ec_authtoken() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    return hashlib.md5((ts + (EC_SECRET_KEY or "")).encode("utf-8")).hexdigest().upper()


def _ec_params() -> dict[str, str]:
    return {
        "advertiserkey": EC_ADVERTISER_KEY or "",
        "authkey": EC_AUTH_KEY or "",
        "authtoken": _ec_authtoken(),
    }


def _ec_request(method: str, url: str, *, params: dict[str, Any], json_body: dict[str, Any] | None = None) -> requests.Response:
    while True:
        try:
            return requests.request(
                method,
                url,
                params=params,
                json=json_body,
                headers={"Content-Type": "application/json"},
                timeout=REQUEST_TIMEOUT,
            )
        except requests.RequestException as e:
            print(f"  Network error: {e}. cooldown {COOLDOWN_SECONDS}s")
            time.sleep(COOLDOWN_SECONDS)


def _get_credentials_path() -> str:
    p = Path(__file__).resolve().parent / "credentials.json"
    if not p.exists():
        raise FileNotFoundError(f"credentials.json not found at {p}")
    return str(p)


def _fetch_merchants_name_to_mid() -> dict[str, str]:
    """Map normalized merchant name -> mid (same idea as legacy CSV lookup)."""
    url = "https://advertiser.ecomnia.com/get-merchants"
    r = _ec_request("GET", url, params=_ec_params())
    if r.status_code != 200:
        raise RuntimeError(f"get-merchants HTTP {r.status_code}: {r.text[:400]}")
    data = r.json() if r.text else {}
    merchants = data.get("merchants", []) if isinstance(data, dict) else []
    out: dict[str, str] = {}
    if not isinstance(merchants, list):
        return out
    for m in merchants:
        if not isinstance(m, dict):
            continue
        mid = str(m.get("mid") or "").strip()
        mname = str(m.get("mname") or "").strip()
        if not mid or not mname:
            continue
        key = mname.replace(" ", "").replace("-", "").lower()
        if key and key not in out:
            out[key] = mid
    return out


def _find_mid(merchants_map: dict[str, str], brand: str) -> str:
    key = brand.replace("-", "").replace(" ", "").lower()
    return merchants_map.get(key, "")


def _normalize_hp(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    if not s.startswith("http"):
        return f"https://{s}"
    return s


def _build_tracking_url(brand_key: str, geo_sheet: str, hp_fallback: str, prefix: str, alias: str) -> str:
    """Rain shell + inner trck URL with macros (legacy bulkEC / ec (2).py shape; alias + prefix configurable)."""
    geo_s = geo_sheet.strip()
    geo_sub2 = geo_s.lower() if geo_s else ""
    sub6 = f"{brand_key}-{geo_s.lower()}-{prefix}-EC"
    hp1 = _normalize_hp(hp_fallback)
    inner = (
        f"https://trck.shopli.city/{alias}"
        "?external_id={CLICKID}&cost={CPC}&sub_id_5={SOURCEID}&sub_id_3={url}"
        f"&sub_id_2={geo_sub2}&sub_id_6={quote(sub6, safe='')}&sub_id_1={quote(hp1, safe='')}"
    )
    rain = quote(inner, safe="")
    return f"https://shopli.city/raini?rain={rain}"


def _read_input_rows(tab_name: str) -> list[dict[str, str]]:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds = service_account.Credentials.from_service_account_file(_get_credentials_path())
    service = build("sheets", "v4", credentials=creds).spreadsheets()

    quoted = tab_name.replace("'", "''")
    res = service.values().get(spreadsheetId=EC_SHEETS_SPREADSHEET_ID, range=f"'{quoted}'!A:Z").execute()
    values = res.get("values") or []
    if not values:
        return []

    header = [str(c or "").strip().lower() for c in values[0]]

    def idx(*names: str) -> int:
        for n in names:
            if n in header:
                return header.index(n)
        return -1

    i_brand = idx("brand", "brandname")
    i_geo = idx("geo")
    i_url = idx("url", "homepage", "hp")
    i_fb = idx("hpfb", "fbhp", "homepage fallback")

    missing = []
    if i_brand < 0:
        missing.append("brand")
    if i_geo < 0:
        missing.append("geo")
    if i_url < 0:
        missing.append("url or hp")
    if i_fb < 0:
        missing.append("hpfb or fbhp")
    if missing:
        _usage_error(f"Input tab {tab_name!r} missing columns: {', '.join(missing)}")

    out: list[dict[str, str]] = []
    for r in values[1:]:
        brand = str(r[i_brand] if i_brand < len(r) else "").strip()
        geo = str(r[i_geo] if i_geo < len(r) else "").strip()
        url = str(r[i_url] if i_url < len(r) else "").strip()
        fb = str(r[i_fb] if i_fb < len(r) else "").strip()
        if not (brand and geo and url and fb):
            continue
        out.append({"brand": brand, "geo": geo, "url": url, "hpfb": fb})
    return out


def _create_campaign(
    *,
    brand: str,
    geo: str,
    tracking_url: str,
    mid: str,
    prefix: str,
) -> tuple[int, Any]:
    """POST create-advertiser-campaign. Returns (http_status, json)."""
    geo_ec = geo
    if geo_ec in ("uk", "UK"):
        geo_ec = "GB"

    name = f"{brand}-{geo_ec}-{prefix.lower()}"

    payload = {
        "traffictype": "branded",
        "excludecoupon": "false",
        "ishomepageonly": "true",
        "name": name,
        "url": tracking_url,
        "geo": f"{geo_ec.lower()}",
        "dailybudget": 5,
        "dailyclicks": 300,
        "totalbudget": "nolimit",
        "bid": 0.05,
        "status": "active",
        "mid": f"{mid}",
        "whitelistdomains": [],
        "id": f"{mid}",
    }

    url = "https://advertiser.ecomnia.com/create-advertiser-campaign"
    r = _ec_request("POST", url, params=_ec_params(), json_body=payload)
    try:
        body = r.json() if r.text else {}
    except Exception:
        body = {"_raw": (r.text or "")[:500]}
    return r.status_code, body


def main() -> None:
    load_dotenv()
    if not EC_ADVERTISER_KEY or not EC_AUTH_KEY or not EC_SECRET_KEY:
        _usage_error("Missing ADVERTISER_KEY / AUTH_KEY / SECRET_KEY in .env")

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
        print(f"No data rows in tab {tab!r}.")
        return

    print("Ecomnia bulk create from sheet")
    print(f"Mode: {'APPLY' if apply else 'DRY-RUN'}")
    print(f"Spreadsheet: {EC_SHEETS_SPREADSHEET_ID}")
    print(f"Tab: {tab}")
    print(f"Prefix: {prefix}")
    print(f"Alias: {alias}")
    print(f"Rows: {len(rows)}")
    print()

    merchants_map = _fetch_merchants_name_to_mid()
    ok = 0
    failed = 0

    for idx, row in enumerate(rows, start=1):
        brand_raw = row["brand"].lower()
        geo_raw = row["geo"].strip()
        hp = row["url"].strip()
        hpfb = row["hpfb"].strip()
        if hp[:3] == "www.":
            hp = f"https://{hp}"

        brand_key = brand_raw.replace("-", "").replace(" ", "")
        track = _build_tracking_url(brand_key, geo_raw, hpfb, prefix, alias)
        mid = _find_mid(merchants_map, brand_key)

        if not mid:
            print(f"[{idx}/{len(rows)}] SKIP no merchant id for brand={brand_raw!r}")
            failed += 1
            continue

        if not apply:
            print(f"[{idx}/{len(rows)}] DRY brand={brand_raw} geo={geo_raw} mid={mid}")
            ok += 1
            continue

        http, body = _create_campaign(
            brand=brand_raw,
            geo=geo_raw,
            tracking_url=track,
            mid=mid,
            prefix=prefix,
        )
        if http == 200:
            ok += 1
            print(f"[{idx}/{len(rows)}] OK HTTP {http} mid={mid} body_keys={list(body)[:5] if isinstance(body, dict) else body}")
        else:
            failed += 1
            print(f"[{idx}/{len(rows)}] FAIL HTTP {http} {str(body)[:300]}")

        time.sleep(ROW_DELAY_SECONDS)

    print()
    print(f"Summary: rows={len(rows)} ok={ok} failed={failed} mode={'apply' if apply else 'dry-run'}")
    if apply and failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
