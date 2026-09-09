"""
Fan child Val_click conversions up to Keitaro hub campaign 94.

Child campaigns record ``status=click`` / ``conversion_type=Val_click`` with revenue.
Hub 94's click id is stored as the child's ``external_id`` (hub offer URL
``external_id={subid}``). Keitaro S2S did not copy those onto 94, so this job GETs
the same postback endpoint with ``subid=<hub click id>``, ``status=click``, ``payout=0``.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

from config import (
    DAILY_CONVERSION_POSTBACK_CLICK_STATUS,
    HUB_VAL_CLICK_POSTBACK_LOOKBACK_DAYS,
    HUB_VAL_CLICK_POSTBACK_STATE_PATH,
    HUB_VAL_CLICK_POSTBACK_STATUS,
    KEITARO_HUB_BLEND_TRAFFIC_VALUE,
    KEITARO_HUB_CAMPAIGN_ID,
)
from integrations.daily_conversion_postbacks import build_daily_postback_url, send_postback_get
from integrations.keitaro import KeitaroClient, KeitaroClientError
from integrations.keitaro_conversions import iter_conversion_log

logger = logging.getLogger(__name__)

_CHILD_LOG_COLUMNS = (
    "sub_id",
    "campaign",
    "campaign_id",
    "status",
    "conversion_type",
    "datetime",
    "external_id",
    "sub_id_15",
    "revenue",
)
_HUB_LOG_COLUMNS = ("sub_id", "campaign_id", "status", "datetime")
_SEND_DELAY_SEC = 0.05


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def default_date_range(
    *,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    lookback_days: Optional[int] = None,
) -> Tuple[date, date]:
    today = _utc_today()
    if date_to:
        end = date.fromisoformat(date_to.strip()[:10])
    else:
        end = today
    if date_from:
        start = date.fromisoformat(date_from.strip()[:10])
    else:
        days = int(lookback_days if lookback_days is not None else HUB_VAL_CLICK_POSTBACK_LOOKBACK_DAYS)
        start = end - timedelta(days=max(1, days) - 1)
    if start > end:
        start, end = end, start
    return start, end


def _state_path(path: Optional[str] = None) -> Path:
    return Path(path or HUB_VAL_CLICK_POSTBACK_STATE_PATH)


def load_state(path: Optional[str] = None) -> Dict[str, Any]:
    p = _state_path(path)
    if not p.is_file():
        return {"done": {}}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"done": {}}
    if not isinstance(data, dict):
        return {"done": {}}
    done = data.get("done")
    if not isinstance(done, dict):
        data["done"] = {}
    return data


def save_state(state: Dict[str, Any], path: Optional[str] = None) -> None:
    p = _state_path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(p)


def _prune_done(done: Dict[str, str], *, keep_from: date) -> Dict[str, str]:
    floor = keep_from.isoformat()
    return {sid: day for sid, day in done.items() if str(day or "")[:10] >= floor}


def _is_val_click_row(row: Dict[str, Any]) -> bool:
    ctype = str(row.get("conversion_type") or "").strip().lower()
    if ctype in ("val_click", "valclick"):
        return True
    status = str(row.get("status") or "").strip().lower()
    return status == "click" and not ctype


def _is_hub_fed_child(row: Dict[str, Any], hub_id: int) -> bool:
    try:
        cid = int(row.get("campaign_id") or 0)
    except (TypeError, ValueError):
        cid = 0
    if cid == int(hub_id):
        return False
    tag = str(row.get("sub_id_15") or "").strip().lower()
    want = (KEITARO_HUB_BLEND_TRAFFIC_VALUE or "domain").strip().lower()
    return tag == want


def _postback_status() -> str:
    return (HUB_VAL_CLICK_POSTBACK_STATUS or DAILY_CONVERSION_POSTBACK_CLICK_STATUS or "click").strip() or "click"


def _usable_hub_subid(value: str) -> bool:
    sid = (value or "").strip()
    if not sid:
        return False
    # Unresolved tracker macros, e.g. ``{clickid}`` / ``{subid}``.
    if "{" in sid or "}" in sid:
        return False
    return True


def collect_hub_existing_subids(
    client: KeitaroClient,
    *,
    date_from: date,
    date_to: date,
    hub_id: int,
    status: Optional[str] = None,
) -> Set[str]:
    st = (status or _postback_status()).strip()
    out: Set[str] = set()
    extra = [{"name": "campaign_id", "operator": "EQUALS", "expression": str(int(hub_id))}]
    for row in iter_conversion_log(
        client,
        date_from=date_from,
        date_to=date_to,
        status=st,
        columns=_HUB_LOG_COLUMNS,
        extra_filters=extra,
    ):
        sid = str(row.get("sub_id") or "").strip()
        if sid:
            out.add(sid)
    return out


def iter_child_val_clicks(
    client: KeitaroClient,
    *,
    date_from: date,
    date_to: date,
    hub_id: int,
) -> List[Dict[str, Any]]:
    """Child Val_click rows that carry a hub click id (``external_id``)."""
    rows: List[Dict[str, Any]] = []
    seen_hub: Set[str] = set()
    for row in iter_conversion_log(
        client,
        date_from=date_from,
        date_to=date_to,
        status="click",
        columns=_CHILD_LOG_COLUMNS,
    ):
        if not _is_val_click_row(row) or not _is_hub_fed_child(row, hub_id):
            continue
        hub_sid = str(row.get("external_id") or "").strip()
        if not _usable_hub_subid(hub_sid) or hub_sid in seen_hub:
            continue
        seen_hub.add(hub_sid)
        rows.append(row)
    return rows


def run_hub_val_click_postbacks(
    *,
    dry_run: bool = True,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    lookback_days: Optional[int] = None,
    limit: Optional[int] = None,
    hub_campaign_id: Optional[int] = None,
    state_path: Optional[str] = None,
    delay_sec: float = _SEND_DELAY_SEC,
) -> Dict[str, Any]:
    start, end = default_date_range(
        date_from=date_from, date_to=date_to, lookback_days=lookback_days
    )
    hub_id = int(hub_campaign_id or KEITARO_HUB_CAMPAIGN_ID)
    status = _postback_status()
    client = KeitaroClient()
    state = load_state(state_path)
    done: Dict[str, str] = dict(state.get("done") or {})
    prune_from = start - timedelta(days=1)
    done = _prune_done(done, keep_from=prune_from)

    child_rows = iter_child_val_clicks(client, date_from=start, date_to=end, hub_id=hub_id)
    # Conversion datetime on 94 is postback time, not the original click day — look through today.
    hub_log_to = max(end, _utc_today())
    try:
        already_on_hub = collect_hub_existing_subids(
            client, date_from=start, date_to=hub_log_to, hub_id=hub_id, status=status
        )
    except KeitaroClientError as e:
        logger.warning("Could not list hub %s existing %s conversions: %s", hub_id, status, e)
        already_on_hub = set()

    eligible: List[Dict[str, Any]] = []
    skipped_done = 0
    skipped_on_hub = 0
    retried_done = 0
    for row in child_rows:
        hub_sid = str(row.get("external_id") or "").strip()
        if hub_sid in already_on_hub:
            skipped_on_hub += 1
            done[hub_sid] = str(row.get("datetime") or end.isoformat())[:10]
            continue
        # ``done`` is only a resume hint. If 94 still lacks the conversion, send again.
        if hub_sid in done:
            retried_done += 1
        eligible.append(row)

    if limit is not None and int(limit) >= 0:
        eligible = eligible[: int(limit)]

    session = requests.Session()
    sent = 0
    failed = 0
    samples: List[str] = []
    checkpoint_every = 250
    for row in eligible:
        hub_sid = str(row.get("external_id") or "").strip()
        url = build_daily_postback_url(subid=hub_sid, payout="0", status=status)
        if len(samples) < 5:
            samples.append(url)
        code = send_postback_get(session, url, dry_run=dry_run, dry_run_log=dry_run)
        if dry_run or code < 400:
            sent += 1
            if not dry_run:
                done[hub_sid] = str(row.get("datetime") or end.isoformat())[:10]
        else:
            failed += 1
            logger.warning(
                "Hub val_click postback HTTP %s child=%s hub_subid=%s",
                code,
                row.get("campaign"),
                hub_sid[:40],
            )
        if not dry_run and delay_sec > 0:
            time.sleep(delay_sec)
        if not dry_run and sent and sent % checkpoint_every == 0:
            state["done"] = done
            save_state(state, state_path)
            logger.info("Hub val_click checkpoint sent=%s/%s failed=%s", sent, len(eligible), failed)

    state["done"] = done
    state["last_run"] = {
        "utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "dry_run": dry_run,
        "date_from": start.isoformat(),
        "date_to": end.isoformat(),
        "hub_campaign_id": hub_id,
        "child_val_clicks": len(child_rows),
        "eligible": len(eligible),
        "sent": sent,
        "failed": failed,
        "skipped_done": skipped_done,
        "retried_done": retried_done,
        "skipped_on_hub": skipped_on_hub,
        "hub_log_to": hub_log_to.isoformat(),
    }
    if not dry_run:
        save_state(state, state_path)

    summary = {
        "dry_run": dry_run,
        "date_from": start.isoformat(),
        "date_to": end.isoformat(),
        "hub_campaign_id": hub_id,
        "status": status,
        "payout": "0",
        "child_val_clicks": len(child_rows),
        "eligible": len(eligible),
        "sent": sent,
        "failed": failed,
        "skipped_done": skipped_done,
        "retried_done": retried_done,
        "skipped_on_hub": skipped_on_hub,
        "hub_log_to": hub_log_to.isoformat(),
        "sample_urls": samples,
    }
    logger.info(
        "Hub val_click fan-in %s %s..%s child=%s eligible=%s sent=%s failed=%s retried_done=%s skipped_on_hub=%s",
        "DRY-RUN" if dry_run else "APPLY",
        start,
        end,
        len(child_rows),
        len(eligible),
        sent,
        failed,
        retried_done,
        skipped_on_hub,
    )
    return summary


def cli_main(argv: Optional[List[str]] = None) -> int:
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ap = argparse.ArgumentParser(
        description="Copy child Val_click conversions onto hub campaign 94 with payout=0."
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Send postbacks and write resume state (default is dry-run).",
    )
    ap.add_argument("--from", dest="date_from", default="", help="Start date YYYY-MM-DD (UTC).")
    ap.add_argument("--to", dest="date_to", default="", help="End date YYYY-MM-DD (UTC).")
    ap.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Days ending today UTC when --from/--to omitted (default from env, 2).",
    )
    ap.add_argument("--limit", type=int, default=None, help="Max postbacks this run.")
    ap.add_argument("--hub-campaign-id", type=int, default=None, help="Override hub campaign id.")
    args = ap.parse_args(argv)
    result = run_hub_val_click_postbacks(
        dry_run=not args.apply,
        date_from=args.date_from.strip() or None,
        date_to=args.date_to.strip() or None,
        lookback_days=args.lookback_days,
        limit=args.limit,
        hub_campaign_id=args.hub_campaign_id,
    )
    print(json.dumps({k: v for k, v in result.items() if k != "sample_urls"}, indent=2))
    if result.get("sample_urls"):
        print("sample postback URLs:")
        for u in result["sample_urls"]:
            print(" ", u)
    if result.get("dry_run"):
        print("Dry-run complete. Re-run with --apply to send payout=0 val_click postbacks to hub 94.")
    return 1 if int(result.get("failed") or 0) else 0
