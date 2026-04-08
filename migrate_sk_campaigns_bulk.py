#!/usr/bin/env python3
"""
Bulk one-time SourceKnowledge migration:
- fetch all campaigns
- per campaign: GET full payload
- update campaign `name` and `trackingUrl`
- PUT full payload back (single request per campaign)

Default behavior:
- Prefixes handled: KLWL* and KLFIX
- Prefixes skipped: everything else (e.g. SEF)
- Mode: dry-run unless --apply is passed

Examples:
  python migrate_sk_campaigns_bulk.py --dry-run
  python migrate_sk_campaigns_bulk.py --apply
  python migrate_sk_campaigns_bulk.py --apply --cooldown-seconds 60
  python migrate_sk_campaigns_bulk.py --apply --start-page 1
"""
from __future__ import annotations

import json
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import SOURCEKNOWLEDGE_API_KEY  # noqa: E402


BASE_URL = "https://api.sourceknowledge.com/affiliate/v2"
REQUEST_TIMEOUT_SECONDS = 60
DEFAULT_COOLDOWN_SECONDS = 60
DEFAULT_STATE_FILE = "sk_migration_state.json"
DEFAULT_FAILED_IDS_FILE = "sk_migration_failed_ids.txt"
DEFAULT_BLOCKED_IDS_FILE = "sk_migration_blocked_ids.txt"

TRACKING_TEMPLATE_KL = (
    "https://shopli.city/raini?rain=https://trck.shopli.city/7FDKRK"
    "?external_id={clickid}&cost={adv_price}&sub_id_4={traffic_type}&sub_id_5={sub_id}"
    "&sub_id_2=XgeoX&sub_id_6=XbrandX&sub_id_1=XhpX&sub_id_3={oadest}"
)


@dataclass
class Stats:
    total: int = 0
    processed: int = 0
    changed: int = 0
    unchanged: int = 0
    skipped: int = 0
    blocked: int = 0
    failed: int = 0


def _load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"done_ids": [], "failed_ids": [], "last_index": 0, "last_campaign_id": None}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("state is not object")
        data.setdefault("done_ids", [])
        data.setdefault("failed_ids", [])
        data.setdefault("last_index", 0)
        data.setdefault("last_campaign_id", None)
        return data
    except Exception:
        return {"done_ids": [], "failed_ids": [], "last_index": 0, "last_campaign_id": None}


def _save_state(path: Path, state: dict[str, Any]) -> None:
    path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def _write_failed_ids(path: Path, failed_ids: set[int]) -> None:
    text = "\n".join(str(x) for x in sorted(failed_ids))
    if text:
        text += "\n"
    path.write_text(text, encoding="utf-8")


def _write_ids_file(path: Path, ids: set[int]) -> None:
    text = "\n".join(str(x) for x in sorted(ids))
    if text:
        text += "\n"
    path.write_text(text, encoding="utf-8")


def _persist_progress(
    *,
    state_file: Path,
    failed_ids_file: Path,
    blocked_ids_file: Path,
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
    _save_state(state_file, state)
    _write_failed_ids(failed_ids_file, failed_ids)
    _write_ids_file(blocked_ids_file, blocked_ids)


def _usage_error(message: str) -> None:
    print(f"Error: {message}")
    sys.exit(2)


def _headers() -> dict[str, str]:
    return {
        "accept": "application/json",
        "X-API-KEY": SOURCEKNOWLEDGE_API_KEY,
    }


def _request_with_retry(
    method: str,
    url: str,
    *,
    headers: dict[str, str],
    cooldown_seconds: int,
    json_body: dict[str, Any] | None = None,
) -> requests.Response:
    while True:
        try:
            resp = requests.request(
                method,
                url,
                headers=headers,
                json=json_body,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as e:
            print(f"  Network error: {e}. Cooling down {cooldown_seconds}s and retrying...")
            time.sleep(cooldown_seconds)
            continue
        if resp.status_code != 429:
            return resp
        print(f"  Rate-limited (429). Cooling down {cooldown_seconds}s and retrying...")
        time.sleep(cooldown_seconds)


def _parse_advertiser_name(advertiser_name: str) -> tuple[str, str, str] | None:
    # Expected format: {brand_name}-{geo}-{prefix}; brand may itself contain '-'
    parts = [p.strip() for p in advertiser_name.strip().split("-")]
    if len(parts) < 3:
        return None
    prefix = parts[-1]
    geo = parts[-2].lower()
    brand = "-".join(parts[:-2]).strip()
    if not brand or not geo or not prefix:
        return None
    return brand, geo, prefix


def _build_tracking_url(brand: str, geo: str, prefix: str) -> str:
    url = TRACKING_TEMPLATE_KL
    url = url.replace("XgeoX", quote(geo, safe=""))
    url = url.replace("XbrandX", quote(brand, safe=""))
    url = url.replace("XhpX", quote(prefix, safe=""))
    return url


def _build_campaign_name(brand: str, geo: str, prefix: str, campaign_id: int) -> str:
    # Includes encoded data + unique id for easy navigation/search in SK UI.
    return f"{brand}-{geo}-{prefix}-c{campaign_id}"


def _is_supported_prefix(prefix: str) -> bool:
    p = prefix.upper()
    return p in {"KLFIX", "KLFLEX", "KLTESTED"} or re.fullmatch(r"KLWL\d*", p) is not None


def _fallback_tracking_without_oadest(url: str) -> str:
    return url.replace("&sub_id_3={oadest}", "")


def list_all_campaign_ids(*, start_page: int, cooldown_seconds: int, only_active: bool) -> list[int]:
    ids: list[int] = []
    page = start_page
    while True:
        url = f"{BASE_URL}/campaigns?page={page}"
        resp = _request_with_retry("GET", url, headers=_headers(), cooldown_seconds=cooldown_seconds)
        if resp.status_code != 200:
            _usage_error(f"Failed to list campaigns page {page}: {resp.status_code} {resp.text[:300]}")
        data = resp.json()
        items = data.get("items", [])
        if not isinstance(items, list) or not items:
            break
        page_ids = []
        for x in items:
            if not isinstance(x, dict) or not str(x.get("id", "")).isdigit():
                continue
            if only_active and not bool(x.get("active")):
                continue
            page_ids.append(int(x["id"]))
        ids.extend(page_ids)
        print(f"Listed page {page}: +{len(page_ids)} campaigns (running total {len(ids)})")
        page += 1
    return ids


def get_campaign(campaign_id: int, *, cooldown_seconds: int) -> tuple[dict[str, Any] | None, str | None]:
    url = f"{BASE_URL}/campaigns/{campaign_id}"
    resp = _request_with_retry("GET", url, headers=_headers(), cooldown_seconds=cooldown_seconds)
    if resp.status_code != 200:
        return None, f"GET failed ({resp.status_code}): {resp.text[:240]}"
    data = resp.json()
    if not isinstance(data, dict):
        return None, "GET returned unexpected payload."
    return data, None


def put_campaign(campaign_id: int, payload: dict[str, Any], *, cooldown_seconds: int) -> tuple[bool, str | None]:
    url = f"{BASE_URL}/campaigns/{campaign_id}"
    resp = _request_with_retry("PUT", url, headers=_headers(), cooldown_seconds=cooldown_seconds, json_body=payload)
    if resp.status_code == 200:
        return False, None
    body = (resp.text or "")[:500]
    # Some campaigns reject {oadest} unless deep-linking is enabled; retry once without sub_id_3.
    if resp.status_code == 400 and "{oadest}" in body and "trackingUrl" in payload:
        fallback_payload = dict(payload)
        fallback_payload["trackingUrl"] = _fallback_tracking_without_oadest(str(payload.get("trackingUrl") or ""))
        print("  PUT 400 due to {oadest}; retrying once without sub_id_3...")
        resp2 = _request_with_retry("PUT", url, headers=_headers(), cooldown_seconds=cooldown_seconds, json_body=fallback_payload)
        if resp2.status_code == 200:
            return True, None
        return False, f"PUT failed ({resp2.status_code}) after fallback: {resp2.text[:240]}"
    return False, f"PUT failed ({resp.status_code}): {resp.text[:240]}"


def _fmt_eta(start_ts: float, done: int, total: int) -> str:
    if done <= 0 or total <= 0 or done > total:
        return "ETA n/a"
    elapsed = time.time() - start_ts
    rate = done / max(elapsed, 0.001)
    remain = total - done
    sec = int(remain / max(rate, 0.0001))
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return f"ETA {h:02d}:{m:02d}:{s:02d}"


def main() -> None:
    load_dotenv()
    if not SOURCEKNOWLEDGE_API_KEY:
        _usage_error("Missing SourceKnowledge API key. Set KEYSK in .env.")

    apply = False
    cooldown_seconds = DEFAULT_COOLDOWN_SECONDS
    start_page = 1
    progress_every = 25
    ids_file: Path | None = None
    state_file = Path(DEFAULT_STATE_FILE)
    failed_ids_file = Path(DEFAULT_FAILED_IDS_FILE)
    blocked_ids_file = Path(DEFAULT_BLOCKED_IDS_FILE)
    no_resume = False
    only_active = False

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
        if a == "--cooldown-seconds" and i + 1 < len(argv):
            try:
                cooldown_seconds = int(argv[i + 1])
            except ValueError:
                _usage_error(f"Invalid --cooldown-seconds value: {argv[i + 1]}")
            i += 2
            continue
        if a == "--start-page" and i + 1 < len(argv):
            try:
                start_page = int(argv[i + 1])
            except ValueError:
                _usage_error(f"Invalid --start-page value: {argv[i + 1]}")
            i += 2
            continue
        if a == "--progress-every" and i + 1 < len(argv):
            try:
                progress_every = int(argv[i + 1])
            except ValueError:
                _usage_error(f"Invalid --progress-every value: {argv[i + 1]}")
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
            failed_ids_file = Path(argv[i + 1].strip())
            i += 2
            continue
        if a == "--blocked-ids-file" and i + 1 < len(argv):
            blocked_ids_file = Path(argv[i + 1].strip())
            i += 2
            continue
        if a == "--no-resume":
            no_resume = True
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

    if cooldown_seconds <= 0:
        _usage_error("--cooldown-seconds must be > 0")
    print("SourceKnowledge bulk migration: campaign name + trackingUrl")
    print(f"Mode: {'APPLY' if apply else 'DRY-RUN'}")
    print(f"Rate-limit strategy: run-until-429, then cooldown {cooldown_seconds}s")
    if only_active:
        print("Filter: only active campaigns")
    if ids_file:
        if not ids_file.exists():
            _usage_error(f"--ids-file not found: {ids_file}")
        campaign_ids = [int(x.strip()) for x in ids_file.read_text(encoding="utf-8").splitlines() if x.strip().isdigit()]
        print(f"Loaded {len(campaign_ids)} campaign IDs from {ids_file}")
    else:
        print("Listing all campaigns...")
        campaign_ids = list_all_campaign_ids(start_page=start_page, cooldown_seconds=cooldown_seconds, only_active=only_active)
    if not campaign_ids:
        print("No campaigns found.")
        return

    state = _load_state(state_file)
    done_ids = set(int(x) for x in state.get("done_ids", []) if str(x).isdigit()) if not no_resume else set()
    failed_ids = set(int(x) for x in state.get("failed_ids", []) if str(x).isdigit()) if not no_resume else set()
    blocked_ids = set(int(x) for x in state.get("blocked_ids", []) if str(x).isdigit()) if not no_resume else set()
    if done_ids and not no_resume:
        before = len(campaign_ids)
        campaign_ids = [x for x in campaign_ids if x not in done_ids]
        print(f"Resume enabled: skipped {before - len(campaign_ids)} already-done campaigns from {state_file}")

    stats = Stats(total=len(campaign_ids))
    print(f"Total campaigns found: {stats.total}")
    print()
    start_ts = time.time()

    try:
        for idx, cid in enumerate(campaign_ids, start=1):
            stats.processed += 1
            campaign, err = get_campaign(cid, cooldown_seconds=cooldown_seconds)
            if err:
                stats.failed += 1
                failed_ids.add(cid)
                print(f"[{idx}/{stats.total}] {cid} ERROR: {err}")
                _persist_progress(
                    state_file=state_file,
                    failed_ids_file=failed_ids_file,
                    blocked_ids_file=blocked_ids_file,
                    state=state,
                    done_ids=done_ids,
                    failed_ids=failed_ids,
                    blocked_ids=blocked_ids,
                    idx=idx,
                    cid=cid,
                )
                continue

            adv = campaign.get("advertiser")
            advertiser_name = str(adv.get("name") if isinstance(adv, dict) else "").strip()
            if not advertiser_name:
                stats.skipped += 1
                done_ids.add(cid)
                print(f"[{idx}/{stats.total}] {cid} SKIP: missing advertiser.name")
                _persist_progress(
                    state_file=state_file,
                    failed_ids_file=failed_ids_file,
                    blocked_ids_file=blocked_ids_file,
                    state=state,
                    done_ids=done_ids,
                    failed_ids=failed_ids,
                    blocked_ids=blocked_ids,
                    idx=idx,
                    cid=cid,
                )
                continue

            parsed = _parse_advertiser_name(advertiser_name)
            if not parsed:
                stats.skipped += 1
                done_ids.add(cid)
                print(f"[{idx}/{stats.total}] {cid} SKIP: advertiser name format mismatch '{advertiser_name}'")
                _persist_progress(
                    state_file=state_file,
                    failed_ids_file=failed_ids_file,
                    blocked_ids_file=blocked_ids_file,
                    state=state,
                    done_ids=done_ids,
                    failed_ids=failed_ids,
                    blocked_ids=blocked_ids,
                    idx=idx,
                    cid=cid,
                )
                continue
            brand, geo, prefix = parsed
            if not _is_supported_prefix(prefix):
                stats.skipped += 1
                done_ids.add(cid)
                print(f"[{idx}/{stats.total}] {cid} SKIP: unsupported prefix '{prefix}'")
                _persist_progress(
                    state_file=state_file,
                    failed_ids_file=failed_ids_file,
                    blocked_ids_file=blocked_ids_file,
                    state=state,
                    done_ids=done_ids,
                    failed_ids=failed_ids,
                    blocked_ids=blocked_ids,
                    idx=idx,
                    cid=cid,
                )
                continue

            old_name = str(campaign.get("name") or "")
            old_tracking = str(campaign.get("trackingUrl") or "")
            new_name = _build_campaign_name(brand, geo, prefix, cid)
            new_tracking = _build_tracking_url(brand, geo, prefix)

            if old_name == new_name and old_tracking == new_tracking:
                stats.unchanged += 1
                done_ids.add(cid)
            else:
                stats.changed += 1
                if apply:
                    payload = dict(campaign)
                    payload["name"] = new_name
                    payload["trackingUrl"] = new_tracking
                    used_fallback, put_err = put_campaign(cid, payload, cooldown_seconds=cooldown_seconds)
                    if put_err:
                        if "PUT failed (403)" in put_err and "Access Denied" in put_err:
                            stats.blocked += 1
                            done_ids.add(cid)
                            blocked_ids.add(cid)
                            failed_ids.discard(cid)
                            print(f"[{idx}/{stats.total}] {cid} BLOCKED: {put_err}")
                            _persist_progress(
                                state_file=state_file,
                                failed_ids_file=failed_ids_file,
                                blocked_ids_file=blocked_ids_file,
                                state=state,
                                done_ids=done_ids,
                                failed_ids=failed_ids,
                                blocked_ids=blocked_ids,
                                idx=idx,
                                cid=cid,
                            )
                            continue
                        stats.failed += 1
                        failed_ids.add(cid)
                        print(f"[{idx}/{stats.total}] {cid} ERROR: {put_err}")
                        _persist_progress(
                            state_file=state_file,
                            failed_ids_file=failed_ids_file,
                            blocked_ids_file=blocked_ids_file,
                            state=state,
                            done_ids=done_ids,
                            failed_ids=failed_ids,
                            blocked_ids=blocked_ids,
                            idx=idx,
                            cid=cid,
                        )
                        continue
                    done_ids.add(cid)
                    failed_ids.discard(cid)
                    if used_fallback:
                        print(f"[{idx}/{stats.total}] {cid} Updated with fallback (no sub_id_3).")
                else:
                    done_ids.add(cid)

            _persist_progress(
                state_file=state_file,
                failed_ids_file=failed_ids_file,
                blocked_ids_file=blocked_ids_file,
                state=state,
                done_ids=done_ids,
                failed_ids=failed_ids,
                blocked_ids=blocked_ids,
                idx=idx,
                cid=cid,
            )

            if idx <= 5 or idx % max(progress_every, 1) == 0 or idx == stats.total:
                print(
                    f"[{idx}/{stats.total}] changed={stats.changed}, unchanged={stats.unchanged}, "
                    f"skipped={stats.skipped}, blocked={stats.blocked}, failed={stats.failed} | "
                    f"{_fmt_eta(start_ts, idx, stats.total)}"
                )
    except KeyboardInterrupt:
        _persist_progress(
            state_file=state_file,
            failed_ids_file=failed_ids_file,
            blocked_ids_file=blocked_ids_file,
            state=state,
            done_ids=done_ids,
            failed_ids=failed_ids,
            blocked_ids=blocked_ids,
            idx=stats.processed,
            cid=campaign_ids[stats.processed - 1] if stats.processed > 0 else None,
        )
        print("\nInterrupted. State saved for resume.")
        sys.exit(130)

    print()
    print("Bulk migration finished.")
    print(
        f"Summary: total={stats.total}, processed={stats.processed}, changed={stats.changed}, "
        f"unchanged={stats.unchanged}, skipped={stats.skipped}, blocked={stats.blocked}, failed={stats.failed}, "
        f"mode={'apply' if apply else 'dry-run'}"
    )
    print(f"State file: {state_file}")
    print(f"Failed IDs file: {failed_ids_file}")
    print(f"Blocked IDs file: {blocked_ids_file}")
    if stats.failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
