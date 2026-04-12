#!/usr/bin/env python3
"""
Scan SourceKnowledge ``GET /affiliate/v2/campaigns`` (all pages) and write inactive IDs to JSON.

Overview spend (``fetch_sk_cost``) skips:
- campaigns with ``active: false`` from the live list API, and
- any IDs in ``skip_campaign_ids`` in the output file (this script), so you can re-run after
  archiving/pausing campaigns to refresh the file, or hand-edit rare edge cases.

Default output: ``runtime/sk_overview_skip_campaign_ids.json`` (override with
``SK_OVERVIEW_SKIP_CAMPAIGN_IDS_FILE`` in ``.env`` — same variable ``integrations/overview_costs`` reads).
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

SK_API_BASE = "https://api.sourceknowledge.com/affiliate/v2"
TIMEOUT = 45


def main() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        load_dotenv = None  # type: ignore[assignment]
    if load_dotenv:
        load_dotenv(ROOT / ".env")

    from config import SK_OVERVIEW_SKIP_CAMPAIGN_IDS_FILE, SOURCEKNOWLEDGE_API_KEY

    api_key = (SOURCEKNOWLEDGE_API_KEY or "").strip()
    if not api_key:
        print("Missing KEYSK / SOURCEKNOWLEDGE_API_KEY in .env", file=sys.stderr)
        sys.exit(1)

    raw_out = (SK_OVERVIEW_SKIP_CAMPAIGN_IDS_FILE or "").strip()
    out_path = Path(raw_out) if raw_out else ROOT / "runtime" / "sk_overview_skip_campaign_ids.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    headers = {"X-API-KEY": api_key, "Accept": "application/json"}
    import requests

    inactive: list[int] = []
    active = 0
    unknown = 0
    pages = 0

    page = 1
    while page < 5000:
        r = requests.get(
            f"{SK_API_BASE}/campaigns",
            headers=headers,
            params={"page": page},
            timeout=TIMEOUT,
        )
        if r.status_code == 429:
            time.sleep(2.0)
            continue
        if r.status_code != 200:
            print(f"Stop listing at page {page}: HTTP {r.status_code}", file=sys.stderr)
            break
        data = r.json()
        if not isinstance(data, dict) or data.get("error"):
            print(f"Stop listing at page {page}: {data!r:.200}", file=sys.stderr)
            break
        items = data.get("items")
        if not isinstance(items, list) or not items:
            break
        pages += 1
        for it in items:
            if not isinstance(it, dict) or it.get("id") is None:
                continue
            try:
                cid = int(it["id"])
            except (TypeError, ValueError):
                continue
            if it.get("active") is False:
                inactive.append(cid)
            elif it.get("active") is True:
                active += 1
            else:
                unknown += 1
        page += 1
        time.sleep(0.06)

    inactive_sorted = sorted(set(inactive))
    payload = {
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "skip_campaign_ids": inactive_sorted,
        "stats": {
            "pages_read": pages,
            "inactive_count": len(inactive_sorted),
            "active_true_count": active,
            "active_field_missing_count": unknown,
        },
        "note": "Overview merges skip_campaign_ids with live active:false from the campaigns list API.",
    }
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(out_path)
    print("Wrote", out_path)
    print(json.dumps(payload["stats"], indent=2))


if __name__ == "__main__":
    main()
