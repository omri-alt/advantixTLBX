#!/usr/bin/env python3
"""
One-time SourceKnowledge helper: fetch campaign -> change trackingUrl -> PUT campaign.

Input modes:
1) Auto-build trackingUrl from advertiser name format `{brand}-{geo}-{prefix}`:
   python migrate_sk_tracking_urls.py --campaign-ids 123,456 --dry-run
   python migrate_sk_tracking_urls.py --campaign-ids 123,456 --apply

2) Same URL for many campaigns (manual override):
   python migrate_sk_tracking_urls.py --campaign-ids 123,456 --tracking-url "https://new.example/?cid={subid}" --dry-run
   python migrate_sk_tracking_urls.py --campaign-ids 123,456 --tracking-url "https://new.example/?cid={subid}" --apply

3) Per-campaign URL from CSV:
   python migrate_sk_tracking_urls.py --csv sk_tracking_migration.csv --dry-run
   python migrate_sk_tracking_urls.py --csv sk_tracking_migration.csv --apply

CSV format:
  campaign_id,tracking_url
  123,https://new.example/a
  456,https://new.example/b
"""
from __future__ import annotations

import csv
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import quote

import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import SOURCEKNOWLEDGE_API_KEY  # noqa: E402


BASE_URL = "https://api.sourceknowledge.com/affiliate/v2"
REQUEST_TIMEOUT_SECONDS = 60
TRACKING_TEMPLATE_KL = (
    "https://shopli.city/raini?rain=https://trck.shopli.city/7FDKRK"
    "?external_id={clickid}&cost={adv_price}&sub_id_4={traffic_type}&sub_id_5={sub_id}"
    "&sub_id_2=XgeoX&sub_id_6=XbrandX&sub_id_1=XhpX&sub_id_3={oadest}"
)


@dataclass(frozen=True)
class CampaignTrackingUpdate:
    campaign_id: int
    tracking_url: str | None = None


def _headers() -> dict[str, str]:
    return {
        "accept": "application/json",
        "X-API-KEY": SOURCEKNOWLEDGE_API_KEY,
    }


def _usage_error(message: str) -> None:
    print(f"Error: {message}")
    sys.exit(2)


def _parse_campaign_ids(raw: str) -> list[int]:
    out: list[int] = []
    for part in (x.strip() for x in raw.split(",")):
        if not part:
            continue
        if not part.isdigit():
            _usage_error(f"Invalid campaign id '{part}'. Expected integers.")
        out.append(int(part))
    if not out:
        _usage_error("No valid campaign ids provided.")
    return out


def _read_csv_updates(csv_path: Path) -> list[CampaignTrackingUpdate]:
    if not csv_path.exists():
        _usage_error(f"CSV file not found: {csv_path}")
    updates: list[CampaignTrackingUpdate] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required = {"campaign_id"}
        if not reader.fieldnames:
            _usage_error("CSV has no header row.")
        missing = required - set(reader.fieldnames)
        if missing:
            _usage_error(f"CSV missing required columns: {', '.join(sorted(missing))}")
        for i, row in enumerate(reader, start=2):
            cid_raw = (row.get("campaign_id") or "").strip()
            url = (row.get("tracking_url") or "").strip() if "tracking_url" in reader.fieldnames else ""
            if not cid_raw:
                print(f"Skipping row {i}: campaign_id is empty.")
                continue
            if not cid_raw.isdigit():
                print(f"Skipping row {i}: invalid campaign_id '{cid_raw}'.")
                continue
            updates.append(CampaignTrackingUpdate(campaign_id=int(cid_raw), tracking_url=url or None))
    if not updates:
        _usage_error("No valid updates found in CSV.")
    return updates


def _request_with_retry(method: str, url: str, *, headers: dict[str, str], json_body: dict | None = None) -> requests.Response:
    resp = requests.request(
        method,
        url,
        headers=headers,
        json=json_body,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    if resp.status_code != 429:
        return resp
    print("Rate-limited (429). Waiting 60 seconds and retrying once...")
    time.sleep(60)
    return requests.request(
        method,
        url,
        headers=headers,
        json=json_body,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )


def get_campaign(campaign_id: int) -> tuple[dict | None, str | None]:
    url = f"{BASE_URL}/campaigns/{campaign_id}"
    resp = _request_with_retry("GET", url, headers=_headers())
    if resp.status_code != 200:
        return None, f"GET failed ({resp.status_code}): {resp.text[:300]}"
    try:
        data = resp.json()
    except Exception:
        return None, "GET returned non-JSON response."
    if not isinstance(data, dict):
        return None, "GET returned unexpected payload (not object)."
    return data, None


def update_campaign(campaign_id: int, full_payload: dict) -> tuple[dict | None, str | None]:
    url = f"{BASE_URL}/campaigns/{campaign_id}"
    resp = _request_with_retry("PUT", url, headers=_headers(), json_body=full_payload)
    if resp.status_code != 200:
        return None, f"PUT failed ({resp.status_code}): {resp.text[:300]}"
    try:
        data = resp.json()
    except Exception:
        return None, "PUT returned non-JSON response."
    return data if isinstance(data, dict) else {"raw": data}, None


def parse_args(argv: list[str]) -> tuple[list[CampaignTrackingUpdate], bool]:
    csv_path: Path | None = None
    campaign_ids_raw: str | None = None
    one_tracking_url: str | None = None
    apply = False

    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--csv" and i + 1 < len(argv):
            csv_path = Path(argv[i + 1].strip())
            i += 2
            continue
        if a == "--campaign-ids" and i + 1 < len(argv):
            campaign_ids_raw = argv[i + 1].strip()
            i += 2
            continue
        if a == "--tracking-url" and i + 1 < len(argv):
            one_tracking_url = argv[i + 1].strip()
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
            sys.exit(0)
        _usage_error(f"Unknown argument: {a}")

    if not SOURCEKNOWLEDGE_API_KEY:
        _usage_error("Missing SourceKnowledge API key. Set KEYSK in .env.")

    if csv_path and (campaign_ids_raw or one_tracking_url):
        _usage_error("Use either --csv OR (--campaign-ids + --tracking-url), not both.")

    if csv_path:
        updates = _read_csv_updates(csv_path)
        return updates, apply

    if campaign_ids_raw and one_tracking_url:
        ids = _parse_campaign_ids(campaign_ids_raw)
        updates = [CampaignTrackingUpdate(campaign_id=cid, tracking_url=one_tracking_url) for cid in ids]
        return updates, apply

    if campaign_ids_raw and not one_tracking_url:
        ids = _parse_campaign_ids(campaign_ids_raw)
        updates = [CampaignTrackingUpdate(campaign_id=cid, tracking_url=None) for cid in ids]
        return updates, apply

    _usage_error("Provide --csv OR --campaign-ids (optional --tracking-url override).")
    return [], apply


def _parse_advertiser_name(advertiser_name: str) -> tuple[str, str, str] | None:
    """
    Expected advertiser format: {brand_name}-{geo}-{prefix}
    Brand may contain dashes; parse from the right.
    """
    name = advertiser_name.strip()
    parts = [p.strip() for p in name.split("-")]
    if len(parts) < 3:
        return None
    prefix = parts[-1]
    geo = parts[-2].lower()
    brand = "-".join(parts[:-2]).strip()
    if not brand or not geo or not prefix:
        return None
    return brand, geo, prefix


def _build_tracking_url_from_campaign(campaign: dict) -> tuple[str | None, str | None]:
    advertiser = campaign.get("advertiser")
    advertiser_name = ""
    if isinstance(advertiser, dict):
        advertiser_name = str(advertiser.get("name") or "").strip()
    if not advertiser_name:
        advertiser_name = str(campaign.get("advertiserName") or "").strip()
    if not advertiser_name:
        return None, "missing advertiser name on campaign payload"

    parsed = _parse_advertiser_name(advertiser_name)
    if not parsed:
        return None, f"advertiser name format mismatch: '{advertiser_name}'"
    brand, geo, prefix = parsed
    prefix_up = prefix.upper()
    if not (prefix_up == "KLFIX" or re.fullmatch(r"KLWL\d*", prefix_up)):
        return None, f"unsupported prefix '{prefix}' (currently only KLWL* and KLFIX)"

    url = TRACKING_TEMPLATE_KL
    url = url.replace("XgeoX", quote(geo, safe=""))
    url = url.replace("XbrandX", quote(brand, safe=""))
    url = url.replace("XhpX", quote(prefix, safe=""))
    return url, None


def _iter_updates(updates: Iterable[CampaignTrackingUpdate], *, apply: bool) -> int:
    total = 0
    changed = 0
    failed = 0
    print("SourceKnowledge trackingUrl migration")
    print(f"Mode: {'APPLY' if apply else 'DRY-RUN'}")
    print()
    for upd in updates:
        total += 1
        cid = upd.campaign_id
        print(f"[{total}] Campaign {cid}")

        campaign, err = get_campaign(cid)
        if err:
            failed += 1
            print(f"  ERROR: {err}")
            continue

        old_url = str(campaign.get("trackingUrl") or "")
        if upd.tracking_url:
            new_url = upd.tracking_url
        else:
            new_url, build_err = _build_tracking_url_from_campaign(campaign)
            if build_err:
                failed += 1
                print(f"  ERROR: {build_err}")
                continue
            assert new_url is not None

        if old_url == new_url:
            print("  No change (trackingUrl already matches).")
            continue

        payload = dict(campaign)
        payload["trackingUrl"] = new_url
        changed += 1
        print(f"  trackingUrl: {old_url[:120]}{'...' if len(old_url) > 120 else ''}")
        print(f"            -> {new_url[:120]}{'...' if len(new_url) > 120 else ''}")

        if not apply:
            print("  DRY-RUN: skipping PUT.")
            continue

        _, put_err = update_campaign(cid, payload)
        if put_err:
            failed += 1
            print(f"  ERROR: {put_err}")
            continue
        print("  Updated.")

    print()
    print(f"Done. total={total}, to_change={changed}, failed={failed}, mode={'apply' if apply else 'dry-run'}")
    return 0 if failed == 0 else 1


def main() -> None:
    load_dotenv()
    updates, apply = parse_args(sys.argv[1:])
    exit_code = _iter_updates(updates, apply=apply)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
