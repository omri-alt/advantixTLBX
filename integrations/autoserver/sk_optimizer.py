"""
SourceKnowledge exploration / WL optimizer (hourly).

Live API field verification (2026-04, GET/POST against api.sourceknowledge.com):
- Campaign GET ``/affiliate/v2/campaigns/{id}`` includes:
  - ``dailyBudget`` (float, e.g. ``25.0``) — daily spend cap; omit/null/0 treated as no cap for budget-reached logic.
  - No separate blacklist array is returned; stopping traffic from a sub-publisher uses
    ``POST .../bid-factors`` in batches (max 100) with ``bidFactor: 0``; single ``/bid-factor`` is fallback.
- Publisher stats GET ``/affiliate/v2/stats/campaigns/{id}/by-publisher?from=YYYY-MM-DD&to=YYYY-MM-DD&page=N``:
  - Each ``items[]`` entry includes ``subId``, ``clicks``, ``spend``, ``bidFactor``, ``winRate``, etc.
  - Paginate while ``hasMore`` is true (same pattern as ``integrations/overview_costs._sk_by_publisher_spend_campaign``).

Sheets (workbook ``config.SK_OPTIMIZER_SHEET_ID``):
- ``SKtrackExploration``: campaignId, campaignName, brand, geo, monUrl, monNetwork, wl, status,
  budgetReachedYesterday, lastBlacklisted, lastMonCheck, lastAction, logs
  (``monUrl`` matches EC ``trackExploration`` semantics — merchant/homepage URL for monetization probes.)
- ``SKtrackWL``: campaignId, campaignName, brand, geo, monUrl, monNetwork, status,
  budgetReachedYesterday, lastMonCheck, lastAction, logs

``monNetwork`` (case-insensitive): ``kl`` | ``feed1`` | ``feed2`` | ``feed5`` | ``kelkoo5`` | ``feed3`` | ``feed4`` | ``adexa`` | ``yadore``
| ``new`` | ``skip`` — Kelkoo feed1/feed2/feed5 use ``FEED1_API_KEY`` / ``FEED2_API_KEY`` / ``FEED5_API_KEY``; ``kl`` uses legacy Kelkoo key;
``feed3`` / ``feed4`` use Yadore deeplink / Adexa link monetizer checks. ``new`` / ``skip`` skip the unmon pause probe (not yet integrated or operator opt-out).

Once per UTC day (first hourly ``SKExplorationOptimizer`` run), ``status`` on every row is synced from
``GET /affiliate/v2/campaigns/{id}`` ``active`` (reactivated → ``active``; inactive → ``paused`` or keep ``paused-unmon``).
State: ``data/sk_daily_status_sync_state.json``.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote

import requests

from config import (
    ADEXA_SITE_ID,
    FEED1_API_KEY,
    FEED2_API_KEY,
    FEED5_API_KEY,
    SK_OPTIMIZER_SHEET_ID,
    SK_UNMON_SKIP_CAMPAIGN_IDS,
    SK_TOOLS_SPREADSHEET_ID,
)
from integrations.adexa import AdexaClientError, merchant_monetization_check
from integrations.autoserver import gdocs_as as gd
from integrations.autoserver import kl_as as kl
from integrations.autoserver import sk as sk
from integrations.autoserver.exploration_sheet_logs import append_exploration_log_row
from integrations.kelkoo_search import format_kelkoo_monetization_status, kelkoo_merchant_link_check
from integrations.yadore import YadoreClientError, deeplink

logger = logging.getLogger(__name__)

TAB_EXPLORATION = "SKtrackExploration"
TAB_WL = "SKtrackWL"

HEADERS_EXPLORATION = [
    "campaignId",
    "campaignName",
    "brand",
    "geo",
    "monUrl",
    "monNetwork",
    "wl",
    "status",
    "budgetReachedYesterday",
    "lastBlacklisted",
    "lastMonCheck",
    "lastAction",
    "logs",
    "skipUnmon",
]
HEADERS_WL = [
    "campaignId",
    "campaignName",
    "brand",
    "geo",
    "monUrl",
    "monNetwork",
    "skipUnmon",
    "status",
    "budgetReachedYesterday",
    "lastMonCheck",
    "lastAction",
    "logs",
]

_BID_DECAY_STATE_PATH = Path(__file__).resolve().parents[2] / "data" / "sk_bidfactor_decay_state.json"
_STATUS_SYNC_STATE_PATH = Path(__file__).resolve().parents[2] / "data" / "sk_daily_status_sync_state.json"
_SK_TOOLS_LOG_DISABLED_UNTIL: Optional[datetime] = None
_SK_UNMON_SKIP_CAMPAIGN_ID_SET = {int(x) for x in (SK_UNMON_SKIP_CAMPAIGN_IDS or ())}
# monNetwork values that skip unmon pause (no API probe; campaign stays active).
_MON_NETWORK_SKIP_UNMON = frozenset({"new", "skip"})

# SK bid-factor floor (exploration often uses ~0.205; halving below ~0.0101 returns HTTP 400).
SK_MIN_BID_FACTOR = float((os.getenv("SK_MIN_BID_FACTOR") or "0.0101").strip() or "0.0101")
_SK_BID_POST_DELAY_S = float((os.getenv("SK_BID_POST_DELAY_S") or "0.4").strip() or "0.4")
_SK_BID_429_MAX_ATTEMPTS = max(4, int((os.getenv("SK_BID_429_MAX_ATTEMPTS") or "8").strip() or "8"))
_SK_BID_DECAY_RETRY_ROUNDS = max(1, int((os.getenv("SK_BID_DECAY_RETRY_ROUNDS") or "3").strip() or "3"))
_SK_BID_TRANSIENT_RETRY_MINUTES = max(
    1, int((os.getenv("SK_BID_TRANSIENT_RETRY_MINUTES") or "5").strip() or "5")
)
_SK_BID_FACTORS_BATCH_SIZE = max(
    1, min(100, int((os.getenv("SK_BID_FACTORS_BATCH_SIZE") or "100").strip() or "100"))
)
# SK by-publisher stats: ``from`` must be within ~3 calendar months (~74d works; 85d returns HTTP 400).
_SK_STATS_MAX_LOOKBACK_DAYS = max(
    7, min(74, int((os.getenv("SK_STATS_MAX_LOOKBACK_DAYS") or "74").strip() or "74"))
)


def _utc_today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _utc_yesterday() -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=1)).strftime("%Y-%m-%d")


def _parse_campaign_start_date(camp_json: Optional[dict]) -> Optional[datetime.date]:
    if not isinstance(camp_json, dict):
        return None
    raw = str(camp_json.get("start") or "").strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        return datetime.fromisoformat(raw).date()
    except Exception:
        try:
            return datetime.strptime(raw[:10], "%Y-%m-%d").date()
        except Exception:
            return None


def _sk_stats_earliest_allowed(today: Optional[datetime.date] = None) -> datetime.date:
    """Earliest ``from`` date accepted by SK publisher stats (within last ~3 months)."""
    today = today or datetime.now(timezone.utc).date()
    return today - timedelta(days=_SK_STATS_MAX_LOOKBACK_DAYS)


def _clamp_sk_stats_range(d0: str, d1: str) -> Tuple[str, str]:
    """Clamp ``from``/``to`` so SK stats API does not return HTTP 400 invalid date range."""
    today = datetime.now(timezone.utc).date()
    earliest = _sk_stats_earliest_allowed(today)
    try:
        d1_date = datetime.strptime((d1 or "")[:10], "%Y-%m-%d").date()
    except ValueError:
        d1_date = today
    if d1_date > today:
        d1_date = today
    try:
        d0_date = datetime.strptime((d0 or "")[:10], "%Y-%m-%d").date()
    except ValueError:
        d0_date = earliest
    if d0_date < earliest:
        d0_date = earliest
    if d0_date > d1_date:
        d0_date = d1_date
    return d0_date.strftime("%Y-%m-%d"), d1_date.strftime("%Y-%m-%d")


def _sk_stats_start_date(camp_json: Optional[dict]) -> str:
    """
    Start date for SK by-publisher blacklist window:
    ``max(campaign_start, earliest_allowed)`` so old campaigns never request a range SK rejects.
    """
    today = datetime.now(timezone.utc).date()
    earliest = _sk_stats_earliest_allowed(today)
    campaign_start = _parse_campaign_start_date(camp_json)
    if campaign_start and campaign_start <= today:
        start = campaign_start if campaign_start >= earliest else earliest
        return start.strftime("%Y-%m-%d")
    return earliest.strftime("%Y-%m-%d")


def _append_logs_cell(existing: str, line: str, max_entries: int = 5) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    entry = f"{ts} {line}".strip()
    prev = [x.strip() for x in (existing or "").split("\n") if x.strip()]
    prev.append(entry)
    return "\n".join(prev[-max_entries:])


def _parse_wl(raw: Any) -> List[str]:
    s = (raw or "").strip() if isinstance(raw, str) else ""
    while "'" in s:
        s = s.replace("'", '"')
    if not s:
        return []
    try:
        v = json.loads(s)
        if isinstance(v, list):
            return [str(x) for x in v]
    except Exception:
        pass
    return []


def _norm_mon_network(raw: str) -> str:
    return (raw or "").strip().lower()


def _hp_from_tracking_url(tracking_url: str) -> Optional[str]:
    if not tracking_url:
        return None
    m = re.search(r"hp=([^&]+)", tracking_url)
    if not m:
        return None
    frag = unquote(m.group(1))
    if frag.lower().startswith("http"):
        return frag
    return f"https://{frag.lstrip('/')}"


def _monetization_for_network(mon_network: str, mon_url: str, geo: str) -> Tuple[Optional[bool], Optional[str]]:
    """
    Returns (is_monetized, err_tag).

    err_tag: ``None`` | ``error`` (inconclusive API) | ``skip_unmon`` (``new`` / ``skip`` networks).
    """
    net = _norm_mon_network(mon_network)
    if net in _MON_NETWORK_SKIP_UNMON:
        return True, "skip_unmon"
    url = (mon_url or "").strip()
    g = (geo or "").strip().lower()
    if g == "gb":
        g = "uk"
    try:
        if net in ("kl",):
            r = kl.check_monetization(url, g)
            if r == "error occured":
                return None, "error"
            if r is False:
                return False, None
            return True, None
        if net in ("feed1",):
            if not FEED1_API_KEY:
                return None, "error"
            st = format_kelkoo_monetization_status(kelkoo_merchant_link_check(url, g, FEED1_API_KEY))
            return st.startswith("monetized"), None
        if net in ("feed2",):
            if not FEED2_API_KEY:
                return None, "error"
            st = format_kelkoo_monetization_status(kelkoo_merchant_link_check(url, g, FEED2_API_KEY))
            return st.startswith("monetized"), None
        if net in ("feed5", "kelkoo5"):
            if not FEED5_API_KEY:
                return None, "error"
            st = format_kelkoo_monetization_status(kelkoo_merchant_link_check(url, g, FEED5_API_KEY))
            return st.startswith("monetized"), None
        if net in ("feed3", "yadore"):
            d = deeplink(url, g)
            return bool(d.get("found")), None
        if net in ("feed4", "adexa"):
            if not (ADEXA_SITE_ID or "").strip():
                return None, "error"
            res = merchant_monetization_check(url, g)
            return bool(res.get("found")), None
    except (AdexaClientError, YadoreClientError, requests.RequestException) as e:
        logger.warning("Monetization check failed (%s %s): %s", net, url[:40], e)
        return None, "error"
    except Exception as e:
        logger.warning("Monetization unexpected error (%s): %s", net, e)
        return None, "error"
    # Unknown network — do not pause
    return True, None


def _sk_headers() -> dict:
    return dict(sk.headers_sk)


def _sk_publisher_stats_page(campaign_id: int, d0: str, d1: str, page: int) -> Tuple[Optional[dict], Optional[str]]:
    url = f"https://api.sourceknowledge.com/affiliate/v2/stats/campaigns/{campaign_id}/by-publisher"
    try:
        r = requests.get(
            url,
            headers=_sk_headers(),
            params={"from": d0, "to": d1, "page": page},
            timeout=60,
        )
    except requests.RequestException as e:
        return None, str(e)
    if r.status_code != 200:
        return None, f"HTTP {r.status_code}: {(r.text or '')[:200]}"
    try:
        return r.json(), None
    except Exception as e:
        return None, str(e)


def _sk_aggregate_clicks_by_subid(
    campaign_id: int, d0: str, d1: str
) -> Tuple[Dict[str, int], Optional[str], str, str]:
    """Returns (clicks_by_subid, error, from_used, to_used)."""
    d0, d1 = _clamp_sk_stats_range(d0, d1)
    clicks: Dict[str, int] = {}
    page = 1
    while True:
        data, err = _sk_publisher_stats_page(campaign_id, d0, d1, page)
        if err and "date range is invalid" in err.lower() and page == 1:
            today = datetime.now(timezone.utc).date()
            shrunk = False
            for shrink_days in (60, 45, 30, 14, 7):
                d0_retry = (today - timedelta(days=shrink_days)).strftime("%Y-%m-%d")
                d0_retry, d1_retry = _clamp_sk_stats_range(d0_retry, d1)
                if d0_retry == d0:
                    continue
                logger.info(
                    "SK stats %s: retry from=%s (was %s) after invalid date range",
                    campaign_id,
                    d0_retry,
                    d0,
                )
                d0, d1 = d0_retry, d1_retry
                data, err = _sk_publisher_stats_page(campaign_id, d0, d1, page)
                shrunk = True
                if not err:
                    break
            if err and not shrunk:
                d0_fb, d1_fb = _clamp_sk_stats_range(d0, today.strftime("%Y-%m-%d"))
                if d0_fb != d0:
                    d0, d1 = d0_fb, d1_fb
                    data, err = _sk_publisher_stats_page(campaign_id, d0, d1, page)
        if err:
            return {}, err, d0, d1
        if not isinstance(data, dict):
            return {}, "bad json"
        items = data.get("items") or []
        for it in items:
            if not isinstance(it, dict):
                continue
            sid = str(it.get("subId") or "").strip()
            if not sid:
                continue
            try:
                c = int(it.get("clicks") or 0)
            except (TypeError, ValueError):
                c = 0
            clicks[sid] = clicks.get(sid, 0) + c
        if not data.get("hasMore"):
            break
        page += 1
        if page > 500:
            logger.warning("SK stats pagination cap hit for campaign %s", campaign_id)
            break
    return clicks, None, d0, d1


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _is_100_winrate(v: Any) -> bool:
    """
    Accept common formats from SK stats:
      - 100 / 100.0
      - 1 / 1.0
      - "100", "100%", "1", "1.0"
    """
    if isinstance(v, str):
        s = v.strip().replace("%", "")
        f = _to_float(s, default=-1.0)
    else:
        f = _to_float(v, default=-1.0)
    if f < 0:
        return False
    return abs(f - 100.0) < 1e-9 or abs(f - 1.0) < 1e-9


def _load_bid_decay_state() -> Dict[str, str]:
    p = _BID_DECAY_STATE_PATH
    try:
        if not p.exists():
            return {}
        data = json.loads(p.read_text(encoding="utf-8") or "{}")
        if isinstance(data, dict):
            return {str(k): str(v) for k, v in data.items()}
    except Exception as e:
        logger.warning("SK bid-decay state read failed: %s", e)
    return {}


def _save_bid_decay_state(state: Dict[str, str]) -> None:
    # Keep only recent days to avoid unbounded growth.
    keep_after = (datetime.now(timezone.utc).date() - timedelta(days=21)).strftime("%Y-%m-%d")
    pruned: Dict[str, str] = {}
    for k, v in state.items():
        parts = str(k).split("|", 2)
        if len(parts) != 3:
            continue
        d = parts[0]
        if d >= keep_after:
            pruned[k] = v
    p = _BID_DECAY_STATE_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(pruned, ensure_ascii=False, indent=2), encoding="utf-8")


def _bid_decay_state_blocks(lock_key: str, state: Dict[str, str], now: datetime) -> bool:
    """True if we should not POST again today (success, skip:*, or retry: not yet due)."""
    val = (state.get(lock_key) or "").strip()
    if not val:
        return False
    if val.startswith("retry:"):
        raw = val[6:].strip()
        try:
            if raw.endswith("Z"):
                raw = raw[:-1] + "+00:00"
            retry_at = datetime.fromisoformat(raw)
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            return now < retry_at.astimezone(timezone.utc)
        except Exception:
            return False
    return True


def _schedule_bid_decay_retry(state: Dict[str, str], lock_key: str, now: datetime) -> None:
    retry_at = now + timedelta(minutes=_SK_BID_TRANSIENT_RETRY_MINUTES)
    state[lock_key] = f"retry:{retry_at.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}"


def _post_bid_factor_with_retry(
    campaign_id: int,
    sub_id: str,
    bid_factor: float,
) -> Tuple[bool, str]:
    """
    POST bid-factor with 429 backoff. Returns (ok, reason).
    reason: ``ok`` | ``api_min`` | ``rate_limit`` | ``http_<code>`` | ``exception``.
    """
    wait_s = 1.0
    last_status: Optional[int] = None
    last_body = ""
    try:
        for _attempt in range(_SK_BID_429_MAX_ATTEMPTS):
            r = sk.post_bid_factor(campaign_id, sub_id, bid_factor)
            last_status = r.status_code
            last_body = (r.text or "")[:300]
            if r.status_code == 200:
                if _SK_BID_POST_DELAY_S > 0:
                    time.sleep(_SK_BID_POST_DELAY_S)
                return True, "ok"
            if r.status_code == 429:
                time.sleep(wait_s)
                wait_s = min(wait_s * 2.0, 30.0)
                continue
            if r.status_code == 400 and "lower than" in last_body.lower():
                return False, "api_min"
            break
    except Exception as e:
        logger.exception("SK bid-factor exception %s %s: %s", campaign_id, sub_id, e)
        return False, "exception"

    if last_status == 429:
        return False, "rate_limit"
    return False, f"http_{last_status or 0}"


def _post_bid_factors_bulk_chunk_with_retry(
    campaign_id: int,
    chunk: List[Tuple[str, float]],
) -> Tuple[List[str], List[Tuple[str, str]]]:
    """POST one bulk batch (≤100). On non-200/non-429, fall back to single POSTs."""
    if not chunk:
        return [], []
    payload = [{"subId": str(sid), "bidFactor": float(bf)} for sid, bf in chunk]
    wait_s = 1.0
    last_status: Optional[int] = None
    try:
        for _attempt in range(_SK_BID_429_MAX_ATTEMPTS):
            r = sk.post_bid_factors_bulk(campaign_id, payload)
            last_status = r.status_code
            if r.status_code == 200:
                if _SK_BID_POST_DELAY_S > 0:
                    time.sleep(_SK_BID_POST_DELAY_S)
                return [sid for sid, _ in chunk], []
            if r.status_code == 429:
                time.sleep(wait_s)
                wait_s = min(wait_s * 2.0, 30.0)
                continue
            break
    except Exception as e:
        logger.exception("SK bid-factors bulk exception %s (%s items): %s", campaign_id, len(chunk), e)
        return [], [(sid, "exception") for sid, _ in chunk]

    if last_status == 429:
        return [], [(sid, "rate_limit") for sid, _ in chunk]

    ok: List[str] = []
    failed: List[Tuple[str, str]] = []
    for sid, bf in chunk:
        success, reason = _post_bid_factor_with_retry(campaign_id, sid, bf)
        if success:
            ok.append(sid)
        else:
            failed.append((sid, reason))
    return ok, failed


def _post_bid_factors_bulk_with_retry(
    campaign_id: int,
    updates: List[Tuple[str, float]],
) -> Tuple[List[str], List[Tuple[str, str]]]:
    """Bulk bid-factor updates in chunks of ``_SK_BID_FACTORS_BATCH_SIZE`` (max 100)."""
    ok: List[str] = []
    failed: List[Tuple[str, str]] = []
    for i in range(0, len(updates), _SK_BID_FACTORS_BATCH_SIZE):
        chunk = updates[i : i + _SK_BID_FACTORS_BATCH_SIZE]
        chunk_ok, chunk_fail = _post_bid_factors_bulk_chunk_with_retry(campaign_id, chunk)
        ok.extend(chunk_ok)
        failed.extend(chunk_fail)
    return ok, failed


def _flush_bid_decay_pending(
    campaign_id: int,
    pending: List[Tuple[str, float, str]],
    state: Dict[str, str],
    now: datetime,
) -> Tuple[int, int]:
    """Retry rate-limited decay updates in the same run (not only next hour)."""
    ok = 0
    failed = 0
    queue = list(pending)
    for round_i in range(_SK_BID_DECAY_RETRY_ROUNDS):
        if not queue:
            break
        if round_i > 0:
            time.sleep(2.0)
        updates = [(sid, new_bf) for sid, new_bf, _lock in queue]
        ok_sids, fail_pairs = _post_bid_factors_bulk_with_retry(campaign_id, updates)
        ok_set = set(ok_sids)
        fail_map = {sid: reason for sid, reason in fail_pairs}
        next_queue: List[Tuple[str, float, str]] = []
        for sid, new_bf, lock_key in queue:
            if sid in ok_set:
                state[lock_key] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
                ok += 1
                continue
            reason = fail_map.get(sid, "http_0")
            if reason == "api_min":
                state[lock_key] = "skip:api_min"
                continue
            if reason in ("rate_limit", "exception") or str(reason).startswith("http_5"):
                next_queue.append((sid, new_bf, lock_key))
                continue
            failed += 1
            _schedule_bid_decay_retry(state, lock_key, now)
        queue = next_queue
    for _sid, _nbf, lock_key in queue:
        failed += 1
        _schedule_bid_decay_retry(state, lock_key, now)
    return ok, failed


def _stats_items_by_subid_today(
    campaign_id: int, d0: str, d1: str
) -> Tuple[Dict[str, Dict[str, Any]], Optional[str]]:
    """
    Aggregate clicks and keep latest bidFactor / winRate for each subId in [d0..d1].
    """
    d0, d1 = _clamp_sk_stats_range(d0, d1)
    out: Dict[str, Dict[str, Any]] = {}
    page = 1
    while True:
        data, err = _sk_publisher_stats_page(campaign_id, d0, d1, page)
        if err and "date range is invalid" in err.lower() and page == 1:
            today_s = datetime.now(timezone.utc).date().strftime("%Y-%m-%d")
            for shrink_days in (60, 45, 30, 14, 7):
                d0_retry = (
                    datetime.now(timezone.utc).date() - timedelta(days=shrink_days)
                ).strftime("%Y-%m-%d")
                d0_retry, d1_retry = _clamp_sk_stats_range(d0_retry, today_s)
                if d0_retry == d0:
                    continue
                d0, d1 = d0_retry, d1_retry
                data, err = _sk_publisher_stats_page(campaign_id, d0, d1, page)
                if not err:
                    break
        if err:
            return {}, err
        if not isinstance(data, dict):
            return {}, "bad json"
        items = data.get("items") or []
        for it in items:
            if not isinstance(it, dict):
                continue
            sid = str(it.get("subId") or "").strip()
            if not sid:
                continue
            clicks = int(_to_float(it.get("clicks"), default=0.0))
            prev = out.get(sid) or {"clicks": 0, "winRate": None, "bidFactor": None}
            prev["clicks"] = int(prev.get("clicks") or 0) + max(0, clicks)
            if it.get("winRate") is not None:
                prev["winRate"] = it.get("winRate")
            if it.get("bidFactor") is not None:
                prev["bidFactor"] = _to_float(it.get("bidFactor"), default=1.0)
            out[sid] = prev
        if not data.get("hasMore"):
            break
        page += 1
        if page > 500:
            logger.warning("SK stats pagination cap hit for campaign %s", campaign_id)
            break
    return out, None


def _apply_slow_exploration_bid_decay_once_daily(
    campaign_id: int,
    wl: List[str],
    today: str,
    campaign_name: str,
    per_sub: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Tuple[int, int]:
    """
    For non-WL sources with today's clicks, <30 clicks, 100% winRate:
    reduce bidFactor to (current_bidFactor * 0.5), once per source per campaign per day.
    Skips sources already at ``SK_MIN_BID_FACTOR`` (~0.0101). Retries 429 in-run + short retry window.
    Returns (updated_ok, failed_updates).

    Pass ``per_sub`` from a prior ``_stats_items_by_subid_today`` call to avoid a duplicate API fetch.
    """
    if per_sub is None:
        per_sub, err = _stats_items_by_subid_today(campaign_id, today, today)
    else:
        err = None
    if err:
        logger.warning("SK exploration bid-decay stats unavailable for %s: %s", campaign_id, err)
        _sk_tools_workbook_log(
            campaign_id,
            campaign_name,
            "SK exploration: bid-decay stats skipped",
            {"error": err, "from": today, "to": today},
        )
        return 0, 0

    state = _load_bid_decay_state()
    now = datetime.now(timezone.utc)
    ok = 0
    failed = 0
    skipped_min = 0
    pending: List[Tuple[str, float, str]] = []
    to_apply: List[Tuple[str, float, str]] = []
    min_bf = SK_MIN_BID_FACTOR

    for sid, info in per_sub.items():
        clicks = int(info.get("clicks") or 0)
        if clicks <= 0 or clicks >= 30:
            continue
        if sid in wl:
            continue
        if not _is_100_winrate(info.get("winRate")):
            continue

        lock_key = f"{today}|{campaign_id}|{sid}"
        if _bid_decay_state_blocks(lock_key, state, now):
            continue

        cur_bf = _to_float(info.get("bidFactor"), default=1.0)
        if cur_bf <= 0:
            continue
        if cur_bf <= min_bf + 1e-9:
            state[lock_key] = "skip:min"
            skipped_min += 1
            continue
        new_bf = round(cur_bf * 0.5, 4)
        if new_bf <= 0 or new_bf < min_bf:
            state[lock_key] = "skip:half_below_min"
            skipped_min += 1
            continue

        to_apply.append((sid, new_bf, lock_key))

    if to_apply:
        updates = [(sid, new_bf) for sid, new_bf, _lock in to_apply]
        ok_sids, fail_pairs = _post_bid_factors_bulk_with_retry(campaign_id, updates)
        ok_set = set(ok_sids)
        fail_map = {sid: reason for sid, reason in fail_pairs}
        for sid, new_bf, lock_key in to_apply:
            if sid in ok_set:
                state[lock_key] = now.strftime("%Y-%m-%dT%H:%M:%SZ")
                ok += 1
                continue
            reason = fail_map.get(sid, "http_0")
            if reason == "api_min":
                state[lock_key] = "skip:api_min"
                skipped_min += 1
                continue
            if reason in ("rate_limit", "exception"):
                pending.append((sid, new_bf, lock_key))
                continue
            failed += 1
            _schedule_bid_decay_retry(state, lock_key, now)

    extra_ok, extra_fail = _flush_bid_decay_pending(campaign_id, pending, state, now)
    ok += extra_ok
    failed += extra_fail

    _save_bid_decay_state(state)
    if ok or failed or skipped_min:
        _sk_tools_workbook_log(
            campaign_id,
            campaign_name,
            f"SK exploration: bid-decay {ok} updated, {failed} failed, {skipped_min} at min",
            {"bid_decay_ok": ok, "bid_decay_failed": failed, "bid_decay_skipped_min": skipped_min},
        )
    return ok, failed


def _sk_yesterday_spend_total(campaign_id: int) -> Tuple[float, Optional[str]]:
    y = _utc_yesterday()
    total = 0.0
    page = 1
    while True:
        data, err = _sk_publisher_stats_page(campaign_id, y, y, page)
        if err:
            return 0.0, err
        items = data.get("items") or [] if isinstance(data, dict) else []
        for it in items:
            if isinstance(it, dict) and it.get("spend") is not None:
                try:
                    total += float(it["spend"])
                except (TypeError, ValueError):
                    pass
        if not isinstance(data, dict) or not data.get("hasMore"):
            break
        page += 1
        if page > 500:
            break
    return total, None


def check_budget_reached_yesterday_SK(campaign_id: Any) -> str:
    """
    SK budget vs yesterday spend (confirmed field names on GET campaign + by-publisher stats):
    - Cap: ``dailyBudget`` on ``GET /affiliate/v2/campaigns/{id}`` (float).
    - Spend: sum of ``spend`` across all ``items`` pages for ``from=to=yesterday`` (UTC).
    Returns ``Yes`` / ``No`` / ``No limit``.
    """
    try:
        cid = int(str(campaign_id).strip())
    except (TypeError, ValueError):
        return "No"
    camp = sk.get_campaignById(cid)
    if not isinstance(camp, dict):
        return "No"
    raw_b = camp.get("dailyBudget")
    try:
        cap = float(raw_b) if raw_b is not None and str(raw_b).strip() != "" else 0.0
    except (TypeError, ValueError):
        cap = 0.0
    if cap is None or cap <= 0:
        return "No limit"
    spend, err = _sk_yesterday_spend_total(cid)
    if err:
        logger.warning("SK yesterday spend unavailable for %s: %s", cid, err)
        return "No"
    if spend >= cap:
        return "Yes"
    return "No"


def _resolve_mon_url(row: Dict[str, Any], camp_json: Optional[dict]) -> str:
    u = (row.get("monUrl") or row.get("monURL") or "").strip()
    if u:
        return u
    if camp_json and isinstance(camp_json, dict):
        hp = _hp_from_tracking_url(str(camp_json.get("trackingUrl") or ""))
        if hp:
            return hp
    return ""


def _sk_tools_workbook_log(
    camp_id: Any,
    camp_name: str,
    verify: str,
    response: Any = "",
) -> None:
    """Append one row to the SK tools workbook ``logs`` tab (shared with bulk opener)."""
    global _SK_TOOLS_LOG_DISABLED_UNTIL
    sid = (SK_TOOLS_SPREADSHEET_ID or "").strip()
    if not sid:
        return
    now = datetime.now(timezone.utc)
    if _SK_TOOLS_LOG_DISABLED_UNTIL and now < _SK_TOOLS_LOG_DISABLED_UNTIL:
        return
    try:
        append_exploration_log_row(
            sid,
            camp_id=str(camp_id or ""),
            camp_name=str(camp_name or ""),
            verify=str(verify or "")[:4000],
            response=response,
        )
    except Exception as e:
        msg = str(e)
        if "429" in msg or "Quota exceeded" in msg or "Rate Limit" in msg:
            _SK_TOOLS_LOG_DISABLED_UNTIL = now + timedelta(minutes=2)


def _blacklist_sources_sk(campaign_id: int, sub_ids: List[str]) -> List[str]:
    """Sets bidFactor 0 in bulk batches (SK equivalent of EC blacklist). Returns sub_ids that failed."""
    if not sub_ids:
        return []
    failed: List[str] = []
    pending = list(sub_ids)
    for round_i in range(_SK_BID_DECAY_RETRY_ROUNDS):
        if not pending:
            break
        if round_i > 0:
            time.sleep(2.0)
        updates = [(sid, 0.0) for sid in pending]
        ok_sids, fail_pairs = _post_bid_factors_bulk_with_retry(campaign_id, updates)
        ok_set = set(ok_sids)
        fail_map = {sid: reason for sid, reason in fail_pairs}
        next_pending: List[str] = []
        for sid in pending:
            if sid in ok_set:
                continue
            reason = fail_map.get(sid, "http_0")
            if reason in ("rate_limit", "exception"):
                next_pending.append(sid)
                continue
            logger.error("SK bid-factor 0 failed %s %s: %s", campaign_id, sid, reason)
            failed.append(sid)
        pending = next_pending
    for sid in pending:
        logger.error("SK bid-factor 0 rate-limited %s %s after retries", campaign_id, sid)
        failed.append(sid)
    return failed


def _load_status_sync_state() -> Dict[str, str]:
    p = _STATUS_SYNC_STATE_PATH
    try:
        if not p.exists():
            return {}
        data = json.loads(p.read_text(encoding="utf-8") or "{}")
        if isinstance(data, dict):
            return {str(k): str(v) for k, v in data.items()}
    except Exception as e:
        logger.warning("SK daily status-sync state read failed: %s", e)
    return {}


def _save_status_sync_state(state: Dict[str, str]) -> None:
    p = _STATUS_SYNC_STATE_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _status_sync_due(tab_key: str, today: str) -> bool:
    return _load_status_sync_state().get(tab_key) != today


def _mark_status_sync_done(tab_key: str, today: str) -> None:
    state = _load_status_sync_state()
    state[tab_key] = today
    _save_status_sync_state(state)


def _normalize_sheet_status(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in ("", "[]", "none", "null"):
        return ""
    return raw


def _sheet_status_from_sk_api(
    camp_json: Optional[dict],
    current_sheet_status: Any,
) -> Optional[str]:
    """
    Map SK campaign ``active`` to sheet ``status``.

    Reactivated campaigns (SK active) always become ``active``. When SK is inactive,
    keep ``paused-unmon`` if the optimizer set it; otherwise use ``paused``.
    """
    if not isinstance(camp_json, dict) or "active" not in camp_json:
        return None
    sk_active = bool(camp_json.get("active"))
    cur = _normalize_sheet_status(current_sheet_status)
    if sk_active:
        return "active"
    if cur == "paused-unmon":
        return "paused-unmon"
    return "paused"


def _apply_daily_status_sync(row: Dict[str, Any], camp_json: dict) -> bool:
    """Update row ``status`` from SK API. Returns True if the cell changed."""
    new_status = _sheet_status_from_sk_api(camp_json, row.get("status"))
    if new_status is None:
        return False
    cur = _normalize_sheet_status(row.get("status"))
    if cur == new_status:
        return False
    old_display = str(row.get("status") or "").strip() or "(empty)"
    row["status"] = new_status
    row["logs"] = _append_logs_cell(
        row.get("logs", ""),
        f"daily status sync: {old_display} -> {new_status}",
    )
    return True


def _row_active(row: Dict[str, Any]) -> bool:
    return str(row.get("status") or "").strip().lower() == "active"


def _is_truthy_cell(v: Any) -> bool:
    s = str(v or "").strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def _row_skip_unmon(row: Dict[str, Any]) -> bool:
    """
    Per-row operator override from sheet. Accept common header variants.
    """
    return (
        _is_truthy_cell(row.get("skipUnmon"))
        or _is_truthy_cell(row.get("skip_unmon"))
        or _is_truthy_cell(row.get("skip unmon"))
    )


def sync_sk_track_status_from_api(
    *,
    exploration: bool = True,
    wl: bool = True,
    mark_daily_done: bool = True,
) -> Dict[str, Any]:
    """
    Sync ``status`` on ``SKtrackExploration`` / ``SKtrackWL`` from SK campaign ``active``.

    Lightweight pass — no blacklist, bid-decay, or unmon checks. Writes the sheet after
    each tab when any row changed.
    """
    sheet_id = (SK_OPTIMIZER_SHEET_ID or "").strip()
    if not sheet_id:
        raise RuntimeError("SK_OPTIMIZER_SHEET_ID is not configured")

    today = _utc_today()
    out: Dict[str, Any] = {"date_utc": today, "tabs": {}}

    def _sync_tab(tab: str, headers: List[str], tab_key: str) -> Dict[str, Any]:
        gd.append_missing_headers_row1(sheet_id, tab, headers)
        rows = gd.read_sheet_withID(sheet_id, tab)
        changed = 0
        errors = 0
        processed = 0
        updates: List[Dict[str, Any]] = []
        for row in rows:
            cid_raw = row.get("campaignId") or row.get("campId")
            if not str(cid_raw or "").strip():
                continue
            try:
                cid = int(str(cid_raw).strip())
            except (TypeError, ValueError):
                errors += 1
                continue
            processed += 1
            try:
                camp_json = sk.get_campaignById(cid)
            except Exception as e:
                errors += 1
                logger.error("SK status sync GET %s failed: %s", cid, e)
                continue
            if isinstance(camp_json, dict) and _apply_daily_status_sync(row, camp_json):
                changed += 1
                updates.append(
                    {
                        "campaignId": cid,
                        "campaignName": str(row.get("campaignName") or row.get("campName") or ""),
                        "status": row.get("status"),
                    }
                )
        if changed:
            gd.create_or_update_sheet_from_dicts_withID(sheet_id, tab, rows)
        if mark_daily_done:
            _mark_status_sync_done(tab_key, today)
        summary = {"processed": processed, "changed": changed, "errors": errors, "updates": updates}
        _sk_tools_workbook_log("", tab, "SK daily status sync", summary)
        return summary

    if exploration:
        out["tabs"]["exploration"] = _sync_tab(TAB_EXPLORATION, HEADERS_EXPLORATION, "exploration")
    if wl:
        out["tabs"]["wl"] = _sync_tab(TAB_WL, HEADERS_WL, "wl")
    return out


def checkUnmonExploration_SK() -> None:
    sheet_id = (SK_OPTIMIZER_SHEET_ID or "").strip()
    if not sheet_id:
        raise RuntimeError("SK_OPTIMIZER_SHEET_ID is not configured")

    gd.append_missing_headers_row1(sheet_id, TAB_EXPLORATION, HEADERS_EXPLORATION)
    try:
        rows = gd.read_sheet_withID(sheet_id, TAB_EXPLORATION)
    except Exception as e:
        logger.error("SK optimizer: failed to read %s: %s", TAB_EXPLORATION, e)
        raise RuntimeError(f"read_sheet failed: {e}") from e

    today = _utc_today()
    status_sync_due = _status_sync_due("exploration", today)
    changed = False
    processed_rows = 0
    status_sync_changed = 0
    blacklisted_ok_total = 0
    blacklisted_fail_total = 0
    bid_decay_ok_total = 0
    bid_decay_fail_total = 0

    garbage_summary: Dict[str, int] = {}
    try:
        from integrations.autoserver.sk_garbage_sources import GarbagePassContext

        garbage_ctx = GarbagePassContext.begin(_blacklist_sources_sk)
    except Exception as e:
        garbage_ctx = None
        logger.exception("SK garbage-source init failed: %s", e)
        _sk_tools_workbook_log("", "", "SK garbage-source init error", str(e))

    for row in rows:
        cid_raw = row.get("campaignId") or row.get("campId")
        if not str(cid_raw or "").strip():
            continue
        try:
            cid = int(str(cid_raw).strip())
        except (TypeError, ValueError):
            row["logs"] = _append_logs_cell(row.get("logs", ""), f"skip invalid campaignId {cid_raw!r}")
            changed = True
            _sk_tools_workbook_log("", "", "SK exploration: skip invalid campaignId", cid_raw)
            continue

        camp_json: Optional[dict] = None
        try:
            camp_json = sk.get_campaignById(cid)
        except Exception as e:
            logger.error("SK GET campaign %s failed: %s", cid, e)
            row["logs"] = _append_logs_cell(row.get("logs", ""), f"GET campaign error: {e}")
            changed = True
            _sk_tools_workbook_log(cid, "", "SK exploration: GET campaign error", str(e))
            continue

        if status_sync_due and isinstance(camp_json, dict):
            if _apply_daily_status_sync(row, camp_json):
                status_sync_changed += 1
                changed = True

        row["budgetReachedYesterday"] = check_budget_reached_yesterday_SK(cid)
        changed = True
        processed_rows += 1

        skip_unmon_pause = (cid in _SK_UNMON_SKIP_CAMPAIGN_ID_SET) or _row_skip_unmon(row)
        if skip_unmon_pause:
            row["logs"] = _append_logs_cell(
                row.get("logs", ""),
                f"unmon pause skipped (skipUnmon/config) for campaign {cid}",
            )
            row["lastAction"] = "unmon-skip-config"
            changed = True

        wl = _parse_wl(row.get("wl"))

        did_blacklist = False
        cname_expl = str(camp_json.get("name") or "") if isinstance(camp_json, dict) else ""
        if _row_active(row):
            per_sub_today, stats_today_err = _stats_items_by_subid_today(cid, today, today)
            if stats_today_err:
                logger.warning(
                    "SK today stats unavailable for %s: %s", cid, stats_today_err
                )
                row["logs"] = _append_logs_cell(
                    row.get("logs", ""),
                    f"today stats skipped (bid-decay + garbage): {stats_today_err}",
                )
                changed = True
            else:
                dec_ok, dec_fail = _apply_slow_exploration_bid_decay_once_daily(
                    cid, wl, today, cname_expl, per_sub=per_sub_today
                )
                if garbage_ctx is not None and garbage_ctx.enabled:
                    today_clicks = {
                        sid: int(info.get("clicks") or 0)
                        for sid, info in per_sub_today.items()
                    }
                    garbage_ctx.process_campaign(cid, cname_expl, set(wl), today_clicks)
            if stats_today_err:
                dec_ok, dec_fail = 0, 0
            elif dec_ok or dec_fail:
                bid_decay_ok_total += dec_ok
                bid_decay_fail_total += dec_fail
                changed = True
                if dec_ok:
                    row["lastAction"] = "bid-decay"
                    row["logs"] = _append_logs_cell(
                        row.get("logs", ""),
                        f"bid-decay applied to {dec_ok} source(s) today (100% winRate, <30 clicks, non-WL)",
                    )
                if dec_fail:
                    row["logs"] = _append_logs_cell(
                        row.get("logs", ""),
                        f"bid-decay failed for {dec_fail} source(s)",
                    )

            stats_start = _sk_stats_start_date(camp_json)
            from_used, to_used = _clamp_sk_stats_range(stats_start, today)
            clicks_map, err, from_used, to_used = _sk_aggregate_clicks_by_subid(
                cid, stats_start, today
            )
            if err:
                logger.warning("SK blacklist-window stats unavailable for %s: %s", cid, err)
                row["logs"] = _append_logs_cell(row.get("logs", ""), f"blacklist-window stats skipped: {err}")
                _sk_tools_workbook_log(
                    cid,
                    cname_expl,
                    "SK exploration: blacklist-window stats skipped",
                    {"error": err, "from": from_used, "to": to_used},
                )
            else:
                to_block = [sid for sid, c in clicks_map.items() if c >= 30 and sid not in wl]
                if to_block:
                    bad = _blacklist_sources_sk(cid, to_block)
                    ok = [s for s in to_block if s not in bad]
                    n_ok = len(ok)
                    n_bad = len(bad)
                    blacklisted_ok_total += n_ok
                    blacklisted_fail_total += n_bad
                    logger.info(
                        "SK exploration campaign %s: blacklisted %s source(s), %s failed",
                        cid,
                        n_ok,
                        n_bad,
                    )
                    row["lastBlacklisted"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    row["lastAction"] = "blacklist"
                    did_blacklist = True
                    log_line = f"blacklisted {n_ok} source(s) on campaign {cid}"
                    if n_bad:
                        log_line += f", {n_bad} failed"
                    row["logs"] = _append_logs_cell(row.get("logs", ""), log_line)
                    verify = f"SK exploration: blacklisted {n_ok} source(s)"
                    if n_bad:
                        verify += f", {n_bad} failed"
                    _sk_tools_workbook_log(
                        cid,
                        cname_expl,
                        verify,
                        {
                            "blacklisted_count": n_ok,
                            "blacklist_failed_count": n_bad,
                            "from": from_used,
                            "to": to_used,
                        },
                    )
                    changed = True

            if not skip_unmon_pause:
                mon_url = _resolve_mon_url(row, camp_json if isinstance(camp_json, dict) else None)
                geo = str(row.get("geo") or "").strip()
                net = row.get("monNetwork") or ""
                mon_ok, err_t = _monetization_for_network(str(net), mon_url, geo)
                row["lastMonCheck"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                changed = True
                if err_t == "skip_unmon":
                    row["logs"] = _append_logs_cell(
                        row.get("logs", ""),
                        f"unmon check skipped (monNetwork={net})",
                    )
                    row["lastAction"] = "mon-skip-network"
                    _sk_tools_workbook_log(
                        cid, cname_expl, "SK exploration: unmon check skipped", f"net={net}"
                    )
                elif err_t == "error":
                    logger.warning("SK monetization inconclusive; not pausing %s", cid)
                    row["logs"] = _append_logs_cell(row.get("logs", ""), "mon check inconclusive (network error)")
                    row["lastAction"] = "mon-skip"
                    _sk_tools_workbook_log(
                        cid, cname_expl, "SK exploration: monetization check inconclusive", f"net={net}"
                    )
                elif mon_ok is False:
                    try:
                        sk.pause_campaign(cid)
                        row["status"] = "paused-unmon"
                        msg = f"Paused SK campaign {cid} (unmonetized, monNetwork={net})"
                        logger.info(msg)
                        row["logs"] = _append_logs_cell(row.get("logs", ""), msg)
                        row["lastAction"] = "pause-unmon"
                        _sk_tools_workbook_log(cid, cname_expl, "SK exploration: paused (unmonetized)", msg)
                    except Exception as e:
                        logger.error("pause_campaign %s: %s", cid, e)
                        row["logs"] = _append_logs_cell(row.get("logs", ""), f"pause error: {e}")
                        _sk_tools_workbook_log(cid, cname_expl, "SK exploration: pause error", str(e))
                elif not did_blacklist:
                    row["lastAction"] = "ok"

    if garbage_ctx is not None:
        try:
            garbage_summary = garbage_ctx.finish()
            if any(garbage_summary.values()):
                logger.info("SK garbage-source pass: %s", garbage_summary)
        except Exception as e:
            logger.exception("SK garbage-source finish failed: %s", e)
            _sk_tools_workbook_log("", "", "SK garbage-source finish error", str(e))

    if status_sync_due:
        _mark_status_sync_done("exploration", today)

    if changed and rows:
        gd.create_or_update_sheet_from_dicts_withID(sheet_id, TAB_EXPLORATION, rows)
    _sk_tools_workbook_log(
        "",
        "SKtrackExploration",
        "SK exploration run summary",
        {
            "rows_processed": processed_rows,
            "status_sync_due": status_sync_due,
            "status_sync_changed": status_sync_changed,
            "sources_bid_decayed": bid_decay_ok_total,
            "sources_bid_decay_failed": bid_decay_fail_total,
            "sources_blacklisted": blacklisted_ok_total,
            "sources_blacklist_failed": blacklisted_fail_total,
            "garbage_yellow_new": garbage_summary.get("yellow_new", 0),
            "garbage_red_new": garbage_summary.get("red_new", 0),
            "garbage_global_bl_ok": garbage_summary.get("global_blacklist_ok", 0),
            "garbage_global_bl_fail": garbage_summary.get("global_blacklist_fail", 0),
            "date_utc": today,
        },
    )


def checkUnmonWL_SK() -> None:
    sheet_id = (SK_OPTIMIZER_SHEET_ID or "").strip()
    if not sheet_id:
        raise RuntimeError("SK_OPTIMIZER_SHEET_ID is not configured")

    gd.append_missing_headers_row1(sheet_id, TAB_WL, HEADERS_WL)
    try:
        rows = gd.read_sheet_withID(sheet_id, TAB_WL)
    except Exception as e:
        logger.error("SK optimizer: failed to read %s: %s", TAB_WL, e)
        raise RuntimeError(f"read_sheet failed: {e}") from e

    today = _utc_today()
    status_sync_due = _status_sync_due("wl", today)
    changed = False
    status_sync_changed = 0
    for row in rows:
        cid_raw = row.get("campaignId") or row.get("campId")
        if not str(cid_raw or "").strip():
            continue
        try:
            cid = int(str(cid_raw).strip())
        except (TypeError, ValueError):
            row["logs"] = _append_logs_cell(row.get("logs", ""), f"skip invalid campaignId {cid_raw!r}")
            changed = True
            _sk_tools_workbook_log("", "", "SK WL: skip invalid campaignId", cid_raw)
            continue

        try:
            camp_json = sk.get_campaignById(cid)
        except Exception as e:
            logger.error("SK GET campaign %s failed: %s", cid, e)
            row["logs"] = _append_logs_cell(row.get("logs", ""), f"GET campaign error: {e}")
            changed = True
            _sk_tools_workbook_log(cid, "", "SK WL: GET campaign error", str(e))
            continue

        if status_sync_due and isinstance(camp_json, dict):
            if _apply_daily_status_sync(row, camp_json):
                status_sync_changed += 1
                changed = True

        row["budgetReachedYesterday"] = check_budget_reached_yesterday_SK(cid)
        changed = True

        if not _row_active(row):
            continue

        skip_unmon_pause = (cid in _SK_UNMON_SKIP_CAMPAIGN_ID_SET) or _row_skip_unmon(row)
        if skip_unmon_pause:
            row["logs"] = _append_logs_cell(
                row.get("logs", ""),
                f"unmon pause skipped (skipUnmon/config) for campaign {cid}",
            )
            row["lastAction"] = "unmon-skip-config"
            changed = True
            continue

        cname_wl = str(camp_json.get("name") or "") if isinstance(camp_json, dict) else ""
        mon_url = _resolve_mon_url(row, camp_json if isinstance(camp_json, dict) else None)
        geo = str(row.get("geo") or "").strip()
        net = row.get("monNetwork") or ""
        mon_ok, err_t = _monetization_for_network(str(net), mon_url, geo)
        row["lastMonCheck"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        changed = True
        if err_t == "skip_unmon":
            row["logs"] = _append_logs_cell(
                row.get("logs", ""),
                f"unmon check skipped (monNetwork={net})",
            )
            row["lastAction"] = "mon-skip-network"
            _sk_tools_workbook_log(cid, cname_wl, "SK WL: unmon check skipped", f"net={net}")
        elif err_t == "error":
            logger.warning("SK WL monetization inconclusive; not pausing %s", cid)
            row["logs"] = _append_logs_cell(row.get("logs", ""), "mon check inconclusive (network error)")
            row["lastAction"] = "mon-skip"
            _sk_tools_workbook_log(cid, cname_wl, "SK WL: monetization check inconclusive", f"net={net}")
        elif mon_ok is False:
            try:
                sk.pause_campaign(cid)
                row["status"] = "paused-unmon"
                msg = f"Paused SK campaign {cid} (unmonetized, monNetwork={net})"
                logger.info(msg)
                row["logs"] = _append_logs_cell(row.get("logs", ""), msg)
                row["lastAction"] = "pause-unmon"
                _sk_tools_workbook_log(cid, cname_wl, "SK WL: paused (unmonetized)", msg)
            except Exception as e:
                logger.error("pause_campaign %s: %s", cid, e)
                row["logs"] = _append_logs_cell(row.get("logs", ""), f"pause error: {e}")
                _sk_tools_workbook_log(cid, cname_wl, "SK WL: pause error", str(e))
        else:
            row["lastAction"] = "ok"

    if status_sync_due:
        _mark_status_sync_done("wl", today)

    if changed and rows:
        gd.create_or_update_sheet_from_dicts_withID(sheet_id, TAB_WL, rows)
    if status_sync_due or status_sync_changed:
        _sk_tools_workbook_log(
            "",
            "SKtrackWL",
            "SK WL run summary",
            {
                "status_sync_due": status_sync_due,
                "status_sync_changed": status_sync_changed,
                "date_utc": today,
            },
        )


def _normalize_exploration_sheet_row(raw: Dict[str, Any]) -> Dict[str, str]:
    """Map sheet row keys onto ``HEADERS_EXPLORATION`` (supports legacy ``campId`` / ``campName``)."""
    cid = raw.get("campaignId") or raw.get("campId") or ""
    cname = raw.get("campaignName") or raw.get("campName") or ""
    out: Dict[str, str] = {}
    for h in HEADERS_EXPLORATION:
        if h == "campaignId":
            out[h] = str(cid).strip()
        elif h == "campaignName":
            out[h] = str(cname).strip()
        else:
            v = raw.get(h, "")
            if v is None:
                out[h] = ""
            elif isinstance(v, (dict, list)):
                out[h] = json.dumps(v, ensure_ascii=False)
            else:
                out[h] = str(v)
    return out


def exploration_row_from_bulk_sheet_row(
    item: Dict[str, str],
    camp_json: Dict[str, Any],
    *,
    mon_network: str = "kl",
) -> Dict[str, str]:
    """
    Build one ``SKtrackExploration`` row after a bulk-open campaign create.

    ``item`` is a row dict from the bulk input tab (``brand``, ``geo``, ``url``, ``hpfb``, …).
    ``camp_json`` is the SK ``POST /campaigns`` response body.
    """
    cid = camp_json.get("id")
    name = str(camp_json.get("name") or "").strip()
    brand = str(item.get("brand") or "").strip()
    geo = str(item.get("geo") or "").strip().lower()[:2]
    url = str(item.get("url") or "").strip()
    if url and not url.lower().startswith("http"):
        url = f"https://{url.lstrip('/')}"
    active = bool(camp_json.get("active", True))
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    log_line = f"{ts} Added from SK bulk opener"
    row: Dict[str, str] = {
        "campaignId": str(cid) if cid is not None else "",
        "campaignName": name,
        "brand": brand,
        "geo": geo,
        "monUrl": url,
        "monNetwork": (mon_network or "kl").strip().lower(),
        "wl": "[]",
        "status": "active" if active else "paused",
        "budgetReachedYesterday": "",
        "lastBlacklisted": "",
        "lastMonCheck": "",
        "lastAction": "",
        "logs": log_line,
    }
    return {h: row.get(h, "") for h in HEADERS_EXPLORATION}


def append_sk_exploration_tracking_rows(rows: List[Dict[str, Any]]) -> tuple[int, str]:
    """
    Append rows to ``SKtrackExploration`` for campaign IDs not already present.
    ``rows`` may be partial dicts; each is normalized to ``HEADERS_EXPLORATION``.
    Returns ``(added_count, error_message)`` — ``error_message`` empty on success.
    """
    sheet_id = (SK_OPTIMIZER_SHEET_ID or "").strip()
    if not sheet_id:
        return 0, "SK_OPTIMIZER_SHEET_ID is not set"
    if not rows:
        return 0, ""
    try:
        gd.append_missing_headers_row1(sheet_id, TAB_EXPLORATION, HEADERS_EXPLORATION)
        data = gd.read_sheet_withID(sheet_id, TAB_EXPLORATION)
    except Exception as e:
        logger.exception("append_sk_exploration_tracking_rows: read failed")
        return 0, str(e)

    normalized = [_normalize_exploration_sheet_row(r) for r in data]
    existing_ids = {r["campaignId"] for r in normalized if r.get("campaignId")}

    added = 0
    for raw in rows:
        if all(h in raw for h in HEADERS_EXPLORATION):
            nr = {h: "" if raw.get(h) is None else str(raw.get(h, "")) for h in HEADERS_EXPLORATION}
        else:
            nr = _normalize_exploration_sheet_row(raw)
        cid = (nr.get("campaignId") or "").strip()
        if not cid or cid in existing_ids:
            continue
        normalized.append(nr)
        existing_ids.add(cid)
        added += 1

    if not added:
        return 0, ""

    try:
        gd.create_or_update_sheet_from_dicts_withID(sheet_id, TAB_EXPLORATION, normalized)
    except Exception as e:
        logger.exception("append_sk_exploration_tracking_rows: write failed")
        return 0, str(e)
    return added, ""
