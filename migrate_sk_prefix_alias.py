#!/usr/bin/env python3
"""
Bulk update SK campaign tracking alias by advertiser prefix.

Example:
  python migrate_sk_prefix_alias.py --prefix KLFIX --alias 7VxRF9 --dry-run
  python migrate_sk_prefix_alias.py --prefix KLFIX --alias 7VxRF9 --apply --only-active
"""
from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import SOURCEKNOWLEDGE_API_KEY  # noqa: E402


BASE_URL = "https://api.sourceknowledge.com/affiliate/v2"
COOLDOWN_SECONDS = 60
TIMEOUT_SECONDS = 60


def _usage_error(msg: str) -> None:
    print(f"Error: {msg}")
    sys.exit(2)


def _headers() -> dict[str, str]:
    return {"accept": "application/json", "X-API-KEY": SOURCEKNOWLEDGE_API_KEY}


def _request(method: str, url: str, *, payload: dict[str, Any] | None = None) -> requests.Response:
    while True:
        try:
            r = requests.request(method, url, headers=_headers(), json=payload, timeout=TIMEOUT_SECONDS)
        except requests.RequestException as e:
            print(f"  Network error: {e}. cooldown {COOLDOWN_SECONDS}s")
            time.sleep(COOLDOWN_SECONDS)
            continue
        if r.status_code == 429:
            print(f"  429 rate-limit. cooldown {COOLDOWN_SECONDS}s")
            time.sleep(COOLDOWN_SECONDS)
            continue
        return r


def _parse_adv_prefix(advertiser_name: str) -> str | None:
    parts = [p.strip() for p in advertiser_name.strip().split("-")]
    if len(parts) < 3:
        return None
    return parts[-1]


def _replace_alias(url: str, alias: str) -> str:
    return re.sub(r"https://trck\.shopli\.city/[^?&/]+", f"https://trck.shopli.city/{alias}", url, count=1)


def _list_campaign_ids(only_active: bool) -> list[int]:
    out: list[int] = []
    page = 1
    while True:
        r = _request("GET", f"{BASE_URL}/campaigns?page={page}")
        if r.status_code != 200:
            _usage_error(f"List campaigns failed page {page}: {r.status_code} {r.text[:200]}")
        data = r.json()
        items = data.get("items", [])
        if not isinstance(items, list) or not items:
            break
        added = 0
        for it in items:
            if not isinstance(it, dict):
                continue
            if only_active and not bool(it.get("active")):
                continue
            cid = it.get("id")
            if str(cid).isdigit():
                out.append(int(cid))
                added += 1
        print(f"Listed page {page}: +{added} (total {len(out)})")
        page += 1
    return out


def main() -> None:
    load_dotenv()
    if not SOURCEKNOWLEDGE_API_KEY:
        _usage_error("Missing KEYSK/.env")

    prefix = ""
    alias = ""
    apply = False
    only_active = False

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
        if a == "--apply":
            apply = True
            i += 1
            continue
        if a == "--dry-run":
            apply = False
            i += 1
            continue
        if a == "--only-active":
            only_active = True
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

    print("SK prefix alias migration")
    print(f"Mode: {'APPLY' if apply else 'DRY-RUN'}")
    print(f"Target prefix: {prefix}")
    print(f"New alias: {alias}")
    print(f"Only active: {only_active}")
    print()

    ids = _list_campaign_ids(only_active=only_active)
    changed = 0
    skipped = 0
    failed = 0
    blocked = 0
    for idx, cid in enumerate(ids, start=1):
        r = _request("GET", f"{BASE_URL}/campaigns/{cid}")
        if r.status_code != 200:
            failed += 1
            print(f"[{idx}/{len(ids)}] {cid} ERROR GET {r.status_code}")
            continue
        camp = r.json()
        adv = camp.get("advertiser")
        adv_name = str(adv.get("name") if isinstance(adv, dict) else "").strip()
        pfx = _parse_adv_prefix(adv_name) or ""
        if pfx.upper() != prefix.upper():
            skipped += 1
            continue
        old_url = str(camp.get("trackingUrl") or "")
        if "https://trck.shopli.city/" not in old_url:
            skipped += 1
            continue
        new_url = _replace_alias(old_url, alias)
        if new_url == old_url:
            skipped += 1
            continue
        changed += 1
        if not apply:
            if changed <= 5:
                print(f"[{idx}/{len(ids)}] {cid} DRY old->new alias")
            continue
        payload = dict(camp)
        payload["trackingUrl"] = new_url
        put = _request("PUT", f"{BASE_URL}/campaigns/{cid}", payload=payload)
        if put.status_code == 200:
            pass
        elif put.status_code == 403 and "Access Denied" in (put.text or ""):
            blocked += 1
            print(f"[{idx}/{len(ids)}] {cid} BLOCKED")
        else:
            failed += 1
            print(f"[{idx}/{len(ids)}] {cid} ERROR PUT {put.status_code}: {put.text[:140]}")

        if idx % 50 == 0:
            print(f"[{idx}/{len(ids)}] changed={changed} skipped={skipped} blocked={blocked} failed={failed}")

    print()
    print(f"Summary: total={len(ids)} changed={changed} skipped={skipped} blocked={blocked} failed={failed} mode={'apply' if apply else 'dry-run'}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()

