"""
update_tracking_urls.py
=======================
Fetches all Ecomnia advertiser campaigns, converts tracking URLs from the
old dighlyconsive.com format to the new trck.shopli.city format, then
updates each campaign via the Ecomnia API.

SETUP:
  1. pip install requests
  2. Fill in your credentials in the CONFIG section below
  3. Run with DRY_RUN = True first to preview all conversions
  4. Set DRY_RUN = False and run again to apply changes
  5. Full audit log saved to: update_results.csv

URL CONVERSION MAPPING:
  Outer shell:  https://shopli.city/raini?rain=<inner_url>   (unchanged)

  Inner URL base:
    OLD  https://dighlyconsive.com/<uuid>
    NEW  https://trck.shopli.city/7FDKRK

  Parameter remapping:
    click_id={CLICKID}      →  external_id={CLICKID}
    adv_price={CPC}         →  cost={CPC}
    sub_id={SOURCEID}       →  sub_id_5={SOURCEID}
    oadest={url}            →  sub_id_3={url}
    geo=VALUE               →  sub_id_2=VALUE      (static, kept as-is)
    brand=VALUE             →  sub_id_6=VALUE      (static, kept as-is)
    hp=VALUE                →  sub_id_1=VALUE      (static, kept as-is)
    ctrl_* / traffic_type   →  DROPPED
"""

import csv
import hashlib
import re
import sys
import time
from datetime import datetime, timezone
from urllib.parse import unquote, quote

import requests  # pip install requests

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG — fill these in before running
# ──────────────────────────────────────────────────────────────────────────────

ADVERTISER_KEY = "22974eb2-a9b8-4eb8-a0cf-735538fff4ea"
AUTH_KEY       = "535qo1bh3e8nZ8ZxJbY6UqbLAi6UroJiYrQ3"
SECRET_KEY     = "fCAu4lXj3bbu9vToAYojOZo11FlfVCX5VQq2"   # used to generate the authtoken MD5

# Preview mode: True = print conversions only, False = actually call update API
DRY_RUN = True

# Polite delay between update calls (seconds)
REQUEST_DELAY = 0.5

OUTPUT_FILE = "update_results.csv"

# ──────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ──────────────────────────────────────────────────────────────────────────────

OLD_TRACKER_DOMAIN = "dighlyconsive.com"
NEW_TRACKER_BASE   = "https://trck.shopli.city/7FDKRK"

BASE_URL    = "https://advertiser.ecomnia.com"
GET_URL     = f"{BASE_URL}/get-advertiser-campaigns"
UPDATE_URL  = f"{BASE_URL}/update-advertiser-campaign"

# Params to remove from the old inner URL entirely
PARAMS_TO_DROP = {"ctrl_pm_key", "ctrl_fetch_dest", "ctrl_id", "ctrl_ab", "traffic_type"}

# Old param name → new param name
PARAM_MAP = {
    "click_id":  "external_id",
    "adv_price": "cost",
    "sub_id":    "sub_id_5",
    "oadest":    "sub_id_3",
    "geo":       "sub_id_2",
    "brand":     "sub_id_6",
    "hp":        "sub_id_1",
}

# Desired parameter order in the new inner URL
PARAM_ORDER = [
    "external_id", "cost", "sub_id_5",
    "sub_id_2", "sub_id_6", "sub_id_1", "sub_id_3"
]

# ──────────────────────────────────────────────────────────────────────────────
# AUTH TOKEN
# ──────────────────────────────────────────────────────────────────────────────

def generate_authtoken() -> str:
    """
    MD5( 'YYYY-MM-dd HH:mm' + SECRET_KEY ) — lowercase hex, no padding.
    Generated fresh on every call so the timestamp is always current.
    """
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    raw = ts + SECRET_KEY
    return hashlib.md5(raw.encode()).hexdigest()


def auth_params() -> dict:
    return {
        "advertiserkey": ADVERTISER_KEY,
        "authkey":       AUTH_KEY,
        "authtoken":     generate_authtoken(),
    }

# ──────────────────────────────────────────────────────────────────────────────
# URL CONVERSION
# ──────────────────────────────────────────────────────────────────────────────

def parse_inner_query(query_string: str) -> list:
    """
    Parse a query string into an ordered list of (key, value) tuples.
    Handles {PLACEHOLDER} tokens safely without percent-encoding them.
    """
    pairs = []
    for part in query_string.split("&"):
        if "=" in part:
            k, v = part.split("=", 1)
            pairs.append((k.strip(), v.strip()))
        elif part.strip():
            pairs.append((part.strip(), ""))
    return pairs


def encode_value(val: str) -> str:
    """
    Percent-encode a query param value while preserving {PLACEHOLDER} tokens
    (e.g. {CLICKID}, {CPC}) so DSP macro substitution still works.
    """
    # Split on {PLACEHOLDER} patterns, encode only the non-placeholder parts
    parts = re.split(r"(\{[^}]+\})", val)
    encoded = []
    for part in parts:
        if re.match(r"^\{[^}]+\}$", part):
            encoded.append(part)          # keep DSP macro as-is
        else:
            encoded.append(quote(part, safe="-_.~%:/?"))
    return "".join(encoded)


def build_query_string(params: dict) -> str:
    return "&".join(f"{k}={encode_value(v)}" for k, v in params.items())


def convert_url(old_full_url: str) -> dict:
    """
    Convert a full old-format tracking URL to the new format.
    Returns: { new_url, geo, brand, hp, error }
    """
    result = {"new_url": None, "geo": "", "brand": "", "hp": "", "error": None}

    try:
        # The outer URL looks like:
        #   https://shopli.city/raini?rain=https://dighlyconsive.com/...?param=val&...
        # Because the inner URL is NOT percent-encoded, we can't use parse_qs
        # on the outer URL — it would treat inner params as outer params.
        # Instead, split on 'rain=' and take everything after it.

        if "rain=" not in old_full_url:
            result["error"] = "No 'rain=' parameter found"
            return result

        rain_value = old_full_url.split("rain=", 1)[1]

        # Split the inner URL into base and query string
        if "?" not in rain_value:
            result["error"] = "Inner URL has no query parameters"
            return result

        inner_base, inner_query = rain_value.split("?", 1)

        # Parse inner query params in order
        old_pairs = parse_inner_query(inner_query)

        # Remap params
        new_params = {}
        for old_key, val in old_pairs:
            if old_key in PARAMS_TO_DROP:
                continue
            new_key = PARAM_MAP.get(old_key, old_key)
            new_params[new_key] = val

            # Capture static values for the audit log
            if old_key == "geo":
                result["geo"] = val
            elif old_key == "brand":
                result["brand"] = val
            elif old_key == "hp":
                result["hp"] = unquote(val)

        # Apply desired param order
        ordered = {}
        for k in PARAM_ORDER:
            if k in new_params:
                ordered[k] = new_params[k]
        # Append any unexpected params that weren't in PARAM_ORDER
        for k, v in new_params.items():
            if k not in ordered:
                ordered[k] = v

        # Assemble the new full URL
        new_inner = f"{NEW_TRACKER_BASE}?{build_query_string(ordered)}"
        result["new_url"] = f"https://shopli.city/raini?rain={new_inner}"

    except Exception as e:
        result["error"] = str(e)

    return result

# ──────────────────────────────────────────────────────────────────────────────
# API CALLS
# ──────────────────────────────────────────────────────────────────────────────

def get_all_campaigns() -> list:
    """
    GET /get-advertiser-campaigns — returns all campaigns for this advertiser.
    Omitting campaign_id returns all campaigns per API docs.
    """
    print("Fetching campaigns from Ecomnia...")
    resp = requests.get(
        GET_URL,
        params=auth_params(),
        headers={"content-type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    # Handle different response shapes
    if isinstance(data, list):
        return data
    for key in ("campaigns", "data", "results"):
        if key in data and isinstance(data[key], list):
            return data[key]

    print(f"WARNING: Unexpected API response shape. Keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")
    print(f"Full response: {data}")
    return []


def update_campaign(campaign_id: str, new_url: str) -> dict:
    """
    POST /update-advertiser-campaign
    Auth credentials go in the query string.
    Campaign id + updated url go in the JSON body.
    Only 'id' and 'url' are sent — all other campaign fields left untouched.
    """
    resp = requests.post(
        UPDATE_URL,
        params=auth_params(),
        json={"id": campaign_id, "url": new_url},
        headers={"content-type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()

# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    # Guard: make sure credentials were filled in
    if "YOUR_" in ADVERTISER_KEY:
        print("ERROR: Please fill in your credentials in the CONFIG section before running.")
        sys.exit(1)

    # Step 1: Fetch all campaigns
    campaigns = get_all_campaigns()
    print(f"Found {len(campaigns)} campaigns.\n")

    if not campaigns:
        print("No campaigns returned. Check your API credentials and try again.")
        sys.exit(1)

    results = []

    for camp in campaigns:
        camp_id   = camp.get("id", "")
        camp_name = camp.get("name", "")
        old_url   = camp.get("url", "")

        row = {
            "campaign_id":   camp_id,
            "campaign_name": camp_name,
            "old_url":       old_url,
            "new_url":       "",
            "geo":           "",
            "brand":         "",
            "hp":            "",
            "api_response":  "",
            "status":        "",
        }

        # Skip campaigns that don't use the old tracker
        if OLD_TRACKER_DOMAIN not in (old_url or ""):
            row["status"] = "SKIPPED — not old format"
            print(f"[SKIP]  {camp_name} ({camp_id})")
            results.append(row)
            continue

        # Convert the URL
        conv = convert_url(old_url)
        row["geo"]   = conv["geo"]
        row["brand"] = conv["brand"]
        row["hp"]    = conv["hp"]

        if conv["error"]:
            row["status"] = f"CONVERSION ERROR: {conv['error']}"
            print(f"[ERROR] {camp_name} ({camp_id}) — {conv['error']}")
            results.append(row)
            continue

        row["new_url"] = conv["new_url"]

        # Print preview
        print(f"\n{'─'*65}")
        print(f"  Campaign : {camp_name}")
        print(f"  ID       : {camp_id}")
        print(f"  OLD URL  : {old_url[:90]}...")
        print(f"  NEW URL  : {conv['new_url'][:90]}...")
        print(f"  geo={conv['geo']}  brand={conv['brand']}  hp={conv['hp']}")

        if DRY_RUN:
            row["status"] = "DRY RUN — not updated"
            print(f"  → DRY RUN: skipping update call")
        else:
            try:
                api_resp = update_campaign(camp_id, conv["new_url"])
                row["api_response"] = str(api_resp)
                row["status"] = "UPDATED OK"
                print(f"  → OK: {api_resp}")
            except requests.HTTPError as e:
                row["api_response"] = str(e)
                row["status"] = f"HTTP ERROR: {e}"
                print(f"  → FAIL: {e}")
            except Exception as e:
                row["api_response"] = str(e)
                row["status"] = f"ERROR: {e}"
                print(f"  → FAIL: {e}")

            time.sleep(REQUEST_DELAY)

        results.append(row)

    # Summary
    print(f"\n{'═'*65}")
    total   = len(results)
    to_upd  = sum(1 for r in results if OLD_TRACKER_DOMAIN in r["old_url"])
    skipped = sum(1 for r in results if "SKIPPED" in r["status"])
    ok      = sum(1 for r in results if r["status"] == "UPDATED OK")
    dry     = sum(1 for r in results if "DRY RUN" in r["status"])
    errors  = sum(1 for r in results if "ERROR" in r["status"])

    print(f"  Total campaigns  : {total}")
    print(f"  Need updating    : {to_upd}")
    print(f"  Skipped          : {skipped}")
    if DRY_RUN:
        print(f"  Would update     : {dry}")
        print(f"\n  ⚠  DRY RUN mode — set DRY_RUN = False to apply changes.")
    else:
        print(f"  Updated OK       : {ok}")
        print(f"  Errors           : {errors}")

    # Save audit CSV
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)

    print(f"\n  Audit log → {OUTPUT_FILE}\n")


if __name__ == "__main__":
    main()
