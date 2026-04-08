#!/usr/bin/env python3
"""
Fix SK trackingUrl sub_id_6 value after migration.

Target:
  sub_id_6 = "{brandName}-{geo}-sk-{prefix}" (URL-encoded)

Behavior:
- Reads campaigns from all pages or --ids-file.
- For each editable campaign, updates only sub_id_6 in trackingUrl.
- Keeps all other URL params unchanged.
- Uses GET -> PUT full payload.
- Supports dry-run/apply, checkpoint resume, blocked/failure lists.

Examples:
  python migrate_sk_subid6_fix.py --dry-run --only-active
  python migrate_sk_subid6_fix.py --apply --only-active
  python migrate_sk_subid6_fix.py --apply --ids-file sk_migration_failed_ids.txt
"""
from __future__ import annotations

import json
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote

import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import SOURCEKNOWLEDGE_API_KEY  # noqa: E402

BASE_URL = "https://api.sourceknowledge.com/affiliate/v2"
REQUEST_TIMEOUT_SECONDS = 60
DEFAULT_COOLDOWN_SECONDS = 60
DEFAULT_STATE_FILE = "sk_subid6_fix_state.json"
DEFAULT_FAILED_IDS_FILE = "sk_subid6_fix_failed_ids.txt"
DEFAULT_BLOCKED_IDS_FILE = "sk_subid6_fix_blocked_ids.txt"


@dataclass
class Stats:
    total: int = 0
    processed: int = 0
    changed: int = 0
    unchanged: int = 0
    skipped: int = 0
    blocked: int = 0
    failed: int = 0


def _usage_error(message: str) -> None:
    print(f"Error: {message}")
    sys.exit(2)


def _headers() -> dict[str, str]:
    return {"accept": "application/json", "X-API-KEY": SOURCEKNOWLEDGE_API_KEY}


def _load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"done_ids": [], "failed_ids": [], "blocked_ids": [], "last_index": 0, "last_campaign_id": None}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("invalid")
        data.setdefault("done_ids", [])
        data.setdefault("failed_ids", [])
        data.setdefault("blocked_ids", [])
        data.setdefault("last_index", 0)
        data.setdefault("last_campaign_id", None)
        return data
    except Exception:
        return {"done_ids": [], "failed_ids": [], "blocked_ids": [], "last_index": 0, "last_campaign_id": None}


def _write_ids(path: Path, ids: set[int]) -> None:
    out = "\n".join(str(x) for x in sorted(ids))
    if out:
        out += "\n"
    path.write_text(out, encoding="utf-8")


def _persist(
    state_file: Path,
    failed_file: Path,
    blocked_file: Path,
    state: dict[str, Any],
    done_ids: set[int],
    failed_ids: set[int],
    blocked_ids: set[int],
    idx: int,
    cid: int | None,
) -> None:
    state["done_ids"] = sorted(done_ids)
    state["failed_ids"] = sorted(failed_ids)
    state["blocked_ids"] = sorted(blocked_ids)
    state["last_index"] = idx
    state["last_campaign_id"] = cid
    state_file.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
    _write_ids(failed_file, failed_ids)
    _write_ids(blocked_file, blocked_ids)


def _request_with_retry(
    method: str,
    url: str,
    *,
    cooldown_seconds: int,
    json_body: dict[str, Any] | None = None,
) -> requests.Response:
    while True:
        try:
            resp = requests.request(method, url, headers=_headers(), json=json_body, timeout=REQUEST_TIMEOUT_SECONDS)
        except requests.RequestException as e:
            print(f"  Network error: {e}. Cooling down {cooldown_seconds}s and retrying...")
            time.sleep(cooldown_seconds)
            continue
        if resp.status_code != 429:
            return resp
        print(f"  Rate-limited (429). Cooling down {cooldown_seconds}s and retrying...")
        time.sleep(cooldown_seconds)


def _parse_advertiser_name(advertiser_name: str) -> tuple[str, str, str] | None:
    parts = [p.strip() for p in advertiser_name.strip().split("-")]
    if len(parts) < 3:
        return None
    prefix = parts[-1]
    geo = parts[-2].lower()
    brand = "-".join(parts[:-2]).strip()
    if not brand or not geo or not prefix:
        return None
    return brand, geo, prefix


def _is_supported_prefix(prefix: str) -> bool:
    p = prefix.upper()
    return p in {"KLFIX", "KLFLEX", "KLTESTED"} or re.fullmatch(r"KLWL\d*", p) is not None


def _expected_subid6(brand: str, geo: str, prefix: str) -> str:
    return f"{brand}-{geo}-sk-{prefix}"


def _replace_subid6_in_url(url: str, expected_raw: str) -> tuple[str, str | None]:
    encoded = quote(expected_raw, safe="")
    m = re.search(r"([?&]sub_id_6=)([^&]*)", url)
    if m:
        current = unquote(m.group(2))
        if current == expected_raw:
            return url, current
        start, end = m.span(2)
        return url[:start] + encoded + url[end:], current
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}sub_id_6={encoded}", None


def _list_campaign_ids(*, only_active: bool, cooldown_seconds: int) -> list[int]:
    out: list[int] = []
    page = 1
    while True:
        resp = _request_with_retry("GET", f"{BASE_URL}/campaigns?page={page}", cooldown_seconds=cooldown_seconds)
        if resp.status_code != 200:
            _usage_error(f"Failed listing campaigns page {page}: {resp.status_code} {resp.text[:240]}")
        data = resp.json()
        items = data.get("items", [])
        if not isinstance(items, list) or not items:
            break
        added = 0
        for item in items:
            if not isinstance(item, dict):
                continue
            cid = item.get("id")
            if not str(cid).isdigit():
                continue
            if only_active and not bool(item.get("active")):
                continue
            out.append(int(cid))
            added += 1
        print(f"Listed page {page}: +{added} campaigns (running total {len(out)})")
        page += 1
    return out


def _get_campaign(cid: int, *, cooldown_seconds: int) -> tuple[dict[str, Any] | None, str | None]:
    resp = _request_with_retry("GET", f"{BASE_URL}/campaigns/{cid}", cooldown_seconds=cooldown_seconds)
    if resp.status_code != 200:
        return None, f"GET failed ({resp.status_code}): {resp.text[:240]}"
    data = resp.json()
    if not isinstance(data, dict):
        return None, "GET returned non-object payload"
    return data, None


def _put_campaign(cid: int, payload: dict[str, Any], *, cooldown_seconds: int) -> str | None:
    resp = _request_with_retry("PUT", f"{BASE_URL}/campaigns/{cid}", cooldown_seconds=cooldown_seconds, json_body=payload)
    if resp.status_code == 200:
        return None
    return f"PUT failed ({resp.status_code}): {resp.text[:240]}"


def main() -> None:
    load_dotenv()
    if not SOURCEKNOWLEDGE_API_KEY:
        _usage_error("Missing SourceKnowledge API key. Set KEYSK in .env.")

    apply = False
    only_active = False
    no_resume = False
    cooldown_seconds = DEFAULT_COOLDOWN_SECONDS
    ids_file: Path | None = None
    state_file = Path(DEFAULT_STATE_FILE)
    failed_file = Path(DEFAULT_FAILED_IDS_FILE)
    blocked_file = Path(DEFAULT_BLOCKED_IDS_FILE)
    progress_every = 25

    argv = sys.argv[1:]
    i = 0
    while i < len(argv):
        a = argv[i]
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
        if a == "--no-resume":
            no_resume = True
            i += 1
            continue
        if a == "--cooldown-seconds" and i + 1 < len(argv):
            cooldown_seconds = int(argv[i + 1])
            i += 2
            continue
        if a == "--ids-file" and i + 1 < len(argv):
            ids_file = Path(argv[i + 1].strip())
            i += 2
            continue
        if a == "--state-file" and i + 1 < len(argv):
            state_file = Path(argv[i + 1].strip())
            i += 2
            continue
        if a == "--failed-ids-file" and i + 1 < len(argv):
            failed_file = Path(argv[i + 1].strip())
            i += 2
            continue
        if a == "--blocked-ids-file" and i + 1 < len(argv):
            blocked_file = Path(argv[i + 1].strip())
            i += 2
            continue
        if a == "--progress-every" and i + 1 < len(argv):
            progress_every = int(argv[i + 1])
            i += 2
            continue
        if a in ("-h", "--help"):
            print(__doc__)
            return
        _usage_error(f"Unknown argument: {a}")

    if cooldown_seconds <= 0:
        _usage_error("--cooldown-seconds must be > 0")

    print("SourceKnowledge sub_id_6 correction migration")
    print(f"Mode: {'APPLY' if apply else 'DRY-RUN'}")
    if only_active:
        print("Filter: only active campaigns")

    if ids_file:
        if not ids_file.exists():
            _usage_error(f"ids-file not found: {ids_file}")
        campaign_ids = [int(x.strip()) for x in ids_file.read_text(encoding="utf-8").splitlines() if x.strip().isdigit()]
        print(f"Loaded {len(campaign_ids)} IDs from {ids_file}")
    else:
        campaign_ids = _list_campaign_ids(only_active=only_active, cooldown_seconds=cooldown_seconds)

    state = _load_state(state_file)
    done_ids = set() if no_resume else set(int(x) for x in state.get("done_ids", []) if str(x).isdigit())
    failed_ids = set() if no_resume else set(int(x) for x in state.get("failed_ids", []) if str(x).isdigit())
    blocked_ids = set() if no_resume else set(int(x) for x in state.get("blocked_ids", []) if str(x).isdigit())
    if done_ids and not no_resume:
        before = len(campaign_ids)
        campaign_ids = [x for x in campaign_ids if x not in done_ids]
        print(f"Resume enabled: skipped {before - len(campaign_ids)} already-done campaigns")

    stats = Stats(total=len(campaign_ids))
    print(f"Total campaigns to process: {stats.total}")
    print()
    start_ts = time.time()
    try:
        for idx, cid in enumerate(campaign_ids, start=1):
            stats.processed += 1
            campaign, err = _get_campaign(cid, cooldown_seconds=cooldown_seconds)
            if err:
                stats.failed += 1
                failed_ids.add(cid)
                print(f"[{idx}/{stats.total}] {cid} ERROR: {err}")
                _persist(state_file, failed_file, blocked_file, state, done_ids, failed_ids, blocked_ids, idx, cid)
                continue

            advertiser = campaign.get("advertiser")
            advertiser_name = str(advertiser.get("name") if isinstance(advertiser, dict) else "").strip()
            parsed = _parse_advertiser_name(advertiser_name) if advertiser_name else None
            if not parsed:
                stats.skipped += 1
                done_ids.add(cid)
                print(f"[{idx}/{stats.total}] {cid} SKIP: advertiser name format mismatch '{advertiser_name}'")
                _persist(state_file, failed_file, blocked_file, state, done_ids, failed_ids, blocked_ids, idx, cid)
                continue

            brand, geo, prefix = parsed
            if not _is_supported_prefix(prefix):
                stats.skipped += 1
                done_ids.add(cid)
                print(f"[{idx}/{stats.total}] {cid} SKIP: unsupported prefix '{prefix}'")
                _persist(state_file, failed_file, blocked_file, state, done_ids, failed_ids, blocked_ids, idx, cid)
                continue

            old_url = str(campaign.get("trackingUrl") or "")
            if not old_url:
                stats.skipped += 1
                done_ids.add(cid)
                print(f"[{idx}/{stats.total}] {cid} SKIP: empty trackingUrl")
                _persist(state_file, failed_file, blocked_file, state, done_ids, failed_ids, blocked_ids, idx, cid)
                continue

            expected = _expected_subid6(brand, geo, prefix)
            new_url, current_subid6 = _replace_subid6_in_url(old_url, expected)

            if new_url == old_url:
                stats.unchanged += 1
                done_ids.add(cid)
            else:
                stats.changed += 1
                if apply:
                    payload = dict(campaign)
                    payload["trackingUrl"] = new_url
                    put_err = _put_campaign(cid, payload, cooldown_seconds=cooldown_seconds)
                    if put_err:
                        if "PUT failed (403)" in put_err and "Access Denied" in put_err:
                            stats.blocked += 1
                            blocked_ids.add(cid)
                            done_ids.add(cid)
                            failed_ids.discard(cid)
                            print(f"[{idx}/{stats.total}] {cid} BLOCKED: {put_err}")
                            _persist(state_file, failed_file, blocked_file, state, done_ids, failed_ids, blocked_ids, idx, cid)
                            continue
                        stats.failed += 1
                        failed_ids.add(cid)
                        print(f"[{idx}/{stats.total}] {cid} ERROR: {put_err}")
                        _persist(state_file, failed_file, blocked_file, state, done_ids, failed_ids, blocked_ids, idx, cid)
                        continue
                    done_ids.add(cid)
                    failed_ids.discard(cid)
                if idx <= 5:
                    print(f"[{idx}/{stats.total}] {cid} sub_id_6: '{current_subid6 or ''}' -> '{expected}'")

            _persist(state_file, failed_file, blocked_file, state, done_ids, failed_ids, blocked_ids, idx, cid)
            if idx <= 5 or idx % max(progress_every, 1) == 0 or idx == stats.total:
                elapsed = time.time() - start_ts
                rate = idx / max(elapsed, 0.001)
                eta = int((stats.total - idx) / max(rate, 0.0001))
                print(
                    f"[{idx}/{stats.total}] changed={stats.changed}, unchanged={stats.unchanged}, "
                    f"skipped={stats.skipped}, blocked={stats.blocked}, failed={stats.failed} | ETA {eta//60:02d}:{eta%60:02d}"
                )
    except KeyboardInterrupt:
        _persist(
            state_file,
            failed_file,
            blocked_file,
            state,
            done_ids,
            failed_ids,
            blocked_ids,
            stats.processed,
            campaign_ids[stats.processed - 1] if stats.processed > 0 else None,
        )
        print("\nInterrupted. State saved.")
        sys.exit(130)

    print()
    print(
        f"Summary: total={stats.total}, processed={stats.processed}, changed={stats.changed}, "
        f"unchanged={stats.unchanged}, skipped={stats.skipped}, blocked={stats.blocked}, failed={stats.failed}, "
        f"mode={'apply' if apply else 'dry-run'}"
    )
    print(f"State file: {state_file}")
    print(f"Failed IDs file: {failed_file}")
    print(f"Blocked IDs file: {blocked_file}")
    if stats.failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

