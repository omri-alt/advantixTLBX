#!/usr/bin/env python3
"""
Bulk update Ecomnia campaign tracking alias by prefix.

Rules:
- Prefix is read from campaign name suffix (e.g. "...-KLFIX", "...-KLWL1").
- Updates only tracker alias in url: https://trck.shopli.city/{alias}
- Keeps all other URL parts/params as-is.

Examples:
  python migrate_ec_prefix_alias.py --prefix KLFIX --alias 7VxRF9 --dry-run
  python migrate_ec_prefix_alias.py --prefix KLFIX --alias 7VxRF9 --apply --only-active
"""
from __future__ import annotations

import hashlib
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import EC_ADVERTISER_KEY, EC_AUTH_KEY, EC_SECRET_KEY  # noqa: E402


BASE_URL = "https://advertiser.ecomnia.com"
GET_ENDPOINT = f"{BASE_URL}/get-advertiser-campaigns"
UPDATE_ENDPOINT = f"{BASE_URL}/update-advertiser-campaign"
COOLDOWN_SECONDS = 30
TIMEOUT_SECONDS = 60


def _usage_error(msg: str) -> None:
    print(f"Error: {msg}")
    sys.exit(2)


def _authtoken() -> str:
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    return hashlib.md5((ts + EC_SECRET_KEY).encode("utf-8")).hexdigest().upper()


def _auth_params() -> dict[str, str]:
    return {
        "advertiserkey": EC_ADVERTISER_KEY,
        "authkey": EC_AUTH_KEY,
        "authtoken": _authtoken(),
    }


def _request(method: str, url: str, *, params: dict[str, Any], payload: dict[str, Any] | None = None) -> requests.Response:
    while True:
        try:
            resp = requests.request(
                method,
                url,
                params=params,
                json=payload,
                headers={"content-type": "application/json"},
                timeout=TIMEOUT_SECONDS,
            )
        except requests.RequestException as e:
            print(f"Network error: {e}. cooldown {COOLDOWN_SECONDS}s")
            time.sleep(COOLDOWN_SECONDS)
            continue
        if resp.status_code in (429, 503):
            print(f"Rate limited ({resp.status_code}). cooldown {COOLDOWN_SECONDS}s")
            time.sleep(COOLDOWN_SECONDS)
            continue
        return resp


def _extract_prefix(name: str) -> str:
    parts = [p.strip() for p in (name or "").split("-") if p.strip()]
    return parts[-1] if parts else ""


def _replace_alias(url: str, alias: str) -> str:
    return re.sub(r"https://trck\.shopli\.city/[^?&/]+", f"https://trck.shopli.city/{alias}", url or "", count=1)


def _get_campaigns() -> list[dict[str, Any]]:
    r = _request("GET", GET_ENDPOINT, params=_auth_params())
    if r.status_code != 200:
        _usage_error(f"get campaigns failed {r.status_code}: {r.text[:200]}")
    data = r.json()
    if isinstance(data, dict) and isinstance(data.get("campaigns"), list):
        return data["campaigns"]
    if isinstance(data, list):
        return data
    return []


def _update_campaign_url(campaign_id: str, new_url: str) -> tuple[bool, str]:
    body = {"id": campaign_id, "url": new_url}
    r = _request("POST", UPDATE_ENDPOINT, params=_auth_params(), payload=body)
    if r.status_code == 200:
        return True, ""
    return False, f"{r.status_code}: {r.text[:180]}"


def main() -> None:
    load_dotenv()
    if not EC_ADVERTISER_KEY or not EC_AUTH_KEY or not EC_SECRET_KEY:
        _usage_error("Missing EC credentials in .env (ADVERTISER_KEY, AUTH_KEY, SECRET_KEY)")

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

    campaigns = _get_campaigns()
    total = len(campaigns)
    changed = 0
    skipped = 0
    failed = 0
    for idx, c in enumerate(campaigns, start=1):
        cid = str(c.get("id") or "")
        status = str(c.get("status") or "")
        name = str(c.get("name") or "")
        url = str(c.get("url") or "")
        if only_active and status.lower() != "active":
            skipped += 1
            continue
        if _extract_prefix(name).upper() != prefix.upper():
            skipped += 1
            continue
        if "https://trck.shopli.city/" not in url:
            skipped += 1
            continue
        new_url = _replace_alias(url, alias)
        if new_url == url:
            skipped += 1
            continue
        changed += 1
        if apply:
            ok, err = _update_campaign_url(cid, new_url)
            if not ok:
                failed += 1
                print(f"[{idx}/{total}] {cid} ERROR {err}")
        elif changed <= 5:
            print(f"[{idx}/{total}] DRY {cid} {name}")
        if idx % 50 == 0:
            print(f"[{idx}/{total}] changed={changed} skipped={skipped} failed={failed}")

    print(f"Summary: total={total} changed={changed} skipped={skipped} failed={failed} mode={'apply' if apply else 'dry-run'}")
    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

