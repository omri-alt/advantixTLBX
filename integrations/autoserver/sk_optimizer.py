"""
SourceKnowledge exploration / WL optimizer (hourly).

Live API field verification (2026-04, GET/POST against api.sourceknowledge.com):
- Campaign GET ``/affiliate/v2/campaigns/{id}`` includes:
  - ``dailyBudget`` (float, e.g. ``25.0``) — daily spend cap; omit/null/0 treated as no cap for budget-reached logic.
  - No separate blacklist array is returned; stopping traffic from a sub-publisher is done via
    ``POST /affiliate/v2/campaigns/{id}/bid-factor`` with ``{"subId": "<subId>", "bidFactor": 0}`` (200 OK).
- Publisher stats GET ``/affiliate/v2/stats/campaigns/{id}/by-publisher?from=YYYY-MM-DD&to=YYYY-MM-DD&page=N``:
  - Each ``items[]`` entry includes ``subId``, ``clicks``, ``spend``, ``bidFactor``, ``winRate``, etc.
  - Paginate while ``hasMore`` is true (same pattern as ``integrations/overview_costs._sk_by_publisher_spend_campaign``).

Sheets (workbook ``config.SK_OPTIMIZER_SHEET_ID``):
- ``SKtrackExploration``: campaignId, campaignName, brand, geo, monUrl, monNetwork, wl, status,
  budgetReachedYesterday, lastBlacklisted, lastMonCheck, lastAction, logs
  (``monUrl`` matches EC ``trackExploration`` semantics — merchant/homepage URL for monetization probes.)
- ``SKtrackWL``: campaignId, campaignName, brand, geo, monUrl, monNetwork, status,
  budgetReachedYesterday, lastMonCheck, lastAction, logs

``monNetwork`` (case-insensitive): ``kl`` | ``feed1`` | ``feed2`` | ``feed3`` | ``feed4`` | ``adexa`` | ``yadore``
— Kelkoo feed1/feed2 use ``FEED1_API_KEY`` / ``FEED2_API_KEY``; ``kl`` uses legacy Kelkoo key via ``kl_as.check_monetization``;
``feed3`` / ``feed4`` use Yadore deeplink / Adexa link monetizer checks.
"""
from __future__ import annotations

import json
import logging
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
    SK_OPTIMIZER_SHEET_ID,
    SK_UNMON_SKIP_CAMPAIGN_IDS,
    SK_TOOLS_SPREADSHEET_ID,
)
from integrations.adexa import AdexaClientError, links_merchant_check
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
]
HEADERS_WL = [
    "campaignId",
    "campaignName",
    "brand",
    "geo",
    "monUrl",
    "monNetwork",
    "status",
    "budgetReachedYesterday",
    "lastMonCheck",
    "lastAction",
    "logs",
]

_BID_DECAY_STATE_PATH = Path(__file__).resolve().parents[2] / "data" / "sk_bidfactor_decay_state.json"
_SK_TOOLS_LOG_DISABLED_UNTIL: Optional[datetime] = None
_SK_UNMON_SKIP_CAMPAIGN_ID_SET = {int(x) for x in (SK_UNMON_SKIP_CAMPAIGN_IDS or ())}


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


def _sk_stats_start_date(camp_json: Optional[dict]) -> str:
    """
    Start date for SK by-publisher checks:
    - if campaign start is within the last 90 days, use campaign start
    - otherwise use the last 90 days including today
    """
    today = datetime.now(timezone.utc).date()
    floor_90d = today - timedelta(days=89)
    campaign_start = _parse_campaign_start_date(camp_json)
    if campaign_start and campaign_start > floor_90d:
        return campaign_start.strftime("%Y-%m-%d")
    return floor_90d.strftime("%Y-%m-%d")


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
    Returns (is_monetized, None) on success, (None, 'error') on inconclusive API failure.
    """
    net = _norm_mon_network(mon_network)
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
        if net in ("feed3", "yadore"):
            d = deeplink(url, g)
            return bool(d.get("found")), None
        if net in ("feed4", "adexa"):
            if not (ADEXA_SITE_ID or "").strip():
                return None, "error"
            res = links_merchant_check(url, g)
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


def _sk_aggregate_clicks_by_subid(campaign_id: int, d0: str, d1: str) -> Tuple[Dict[str, int], Optional[str]]:
    clicks: Dict[str, int] = {}
    page = 1
    while True:
        data, err = _sk_publisher_stats_page(campaign_id, d0, d1, page)
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
    return clicks, None


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


def _stats_items_by_subid_today(campaign_id: int, d0: str, d1: str) -> Tuple[Dict[str, Dict[str, Any]], Optional[str]]:
    """
    Aggregate clicks and keep latest bidFactor / winRate for each subId in [d0..d1].
    """
    out: Dict[str, Dict[str, Any]] = {}
    page = 1
    while True:
        data, err = _sk_publisher_stats_page(campaign_id, d0, d1, page)
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
) -> Tuple[int, int]:
    """
    For non-WL sources with today's clicks, <30 clicks, 100% winRate:
    reduce bidFactor to (current_bidFactor * 0.5), once per source per campaign per day.
    Returns (updated_ok, failed_updates).
    """
    per_sub, err = _stats_items_by_subid_today(campaign_id, today, today)
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
    ok = 0
    failed = 0
    changed_sources: List[str] = []
    failed_sources: List[str] = []
    for sid, info in per_sub.items():
        clicks = int(info.get("clicks") or 0)
        if clicks <= 0 or clicks >= 30:
            continue
        if sid in wl:
            continue
        if not _is_100_winrate(info.get("winRate")):
            continue

        lock_key = f"{today}|{campaign_id}|{sid}"
        if lock_key in state:
            continue

        cur_bf = _to_float(info.get("bidFactor"), default=1.0)
        if cur_bf <= 0:
            continue
        new_bf = round(cur_bf * 0.5, 4)
        if new_bf <= 0:
            continue

        try:
            wait_s = 0.7
            r = None
            for _attempt in range(4):
                r = sk.post_bid_factor(campaign_id, sid, new_bf)
                if r.status_code == 200:
                    break
                if r.status_code == 429:
                    time.sleep(wait_s)
                    wait_s = min(wait_s * 2.0, 6.0)
                    continue
                break
            if r is not None and r.status_code == 200:
                state[lock_key] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                ok += 1
                if len(changed_sources) < 20:
                    changed_sources.append(sid)
            else:
                failed += 1
                if len(failed_sources) < 20:
                    failed_sources.append(sid)
        except Exception as e:
            failed += 1
            logger.exception("SK bid-decay exception %s %s: %s", campaign_id, sid, e)
            if len(failed_sources) < 20:
                failed_sources.append(sid)

    _save_bid_decay_state(state)
    if ok or failed:
        _sk_tools_workbook_log(
            campaign_id,
            campaign_name,
            "SK exploration: bid-decay summary",
            {
                "updated_ok": ok,
                "failed_updates": failed,
                "updated_sample_subIds": changed_sources,
                "failed_sample_subIds": failed_sources,
            },
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
    """Sets bidFactor 0 per sub (SK equivalent of EC blacklist). Returns sub_ids that failed."""
    failed: List[str] = []
    for sid in sub_ids:
        try:
            wait_s = 0.7
            ok = False
            last_status = None
            last_body = ""
            for _attempt in range(4):
                r = sk.post_bid_factor(campaign_id, sid, 0.0)
                last_status = r.status_code
                last_body = (r.text or "")[:200]
                if r.status_code == 200:
                    ok = True
                    break
                if r.status_code == 429:
                    time.sleep(wait_s)
                    wait_s = min(wait_s * 2.0, 6.0)
                    continue
                break
            if not ok:
                logger.error("SK bid-factor 0 failed %s %s: %s %s", campaign_id, sid, last_status, last_body)
                failed.append(sid)
        except Exception as e:
            logger.exception("SK bid-factor exception %s %s: %s", campaign_id, sid, e)
            failed.append(sid)
    return failed


def _row_active(row: Dict[str, Any]) -> bool:
    return str(row.get("status") or "").strip().lower() == "active"


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
    changed = False
    processed_rows = 0
    blacklisted_ok_total = 0
    blacklisted_fail_total = 0
    bid_decay_ok_total = 0
    bid_decay_fail_total = 0
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

        row["budgetReachedYesterday"] = check_budget_reached_yesterday_SK(cid)
        changed = True
        processed_rows += 1

        skip_unmon_pause = cid in _SK_UNMON_SKIP_CAMPAIGN_ID_SET
        if skip_unmon_pause:
            row["logs"] = _append_logs_cell(
                row.get("logs", ""),
                f"unmon pause skipped by config for campaign {cid}",
            )
            row["lastAction"] = "unmon-skip-config"
            changed = True

        wl = _parse_wl(row.get("wl"))

        did_blacklist = False
        cname_expl = str(camp_json.get("name") or "") if isinstance(camp_json, dict) else ""
        if _row_active(row):
            dec_ok, dec_fail = _apply_slow_exploration_bid_decay_once_daily(cid, wl, today, cname_expl)
            if dec_ok or dec_fail:
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
            clicks_map, err = _sk_aggregate_clicks_by_subid(cid, stats_start, today)
            if err:
                logger.warning("SK blacklist-window stats unavailable for %s: %s", cid, err)
                row["logs"] = _append_logs_cell(row.get("logs", ""), f"blacklist-window stats skipped: {err}")
                _sk_tools_workbook_log(
                    cid,
                    cname_expl,
                    "SK exploration: blacklist-window stats skipped",
                    {"error": err, "from": stats_start, "to": today},
                )
            else:
                to_block = [sid for sid, c in clicks_map.items() if c >= 30 and sid not in wl]
                if to_block:
                    bad = _blacklist_sources_sk(cid, to_block)
                    ok = [s for s in to_block if s not in bad]
                    blacklisted_ok_total += len(ok)
                    blacklisted_fail_total += len(bad)
                    for s in ok:
                        msg = f"Blacklisted source {s} on campaign {cid} ({clicks_map.get(s, 0)} clicks)"
                        logger.info(msg)
                        row["logs"] = _append_logs_cell(row.get("logs", ""), msg)
                        _sk_tools_workbook_log(
                            cid,
                            cname_expl,
                            f"SK exploration: blacklisted source {s}",
                            {"clicks_window": int(clicks_map.get(s, 0) or 0), "from": stats_start, "to": today, "bidFactor": 0},
                        )
                    row["lastBlacklisted"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    row["lastAction"] = "blacklist"
                    did_blacklist = True
                    if bad:
                        row["logs"] = _append_logs_cell(
                            row.get("logs", ""), f"bid-factor 0 failed for: {','.join(bad)}"
                        )
                        for s in bad:
                            _sk_tools_workbook_log(
                                cid,
                                cname_expl,
                                f"SK exploration: blacklist failed for source {s}",
                                {"clicks_window": int(clicks_map.get(s, 0) or 0), "from": stats_start, "to": today, "bidFactor": 0},
                            )
                    _sk_tools_workbook_log(
                        cid,
                        cname_expl,
                        "SK exploration: blacklist high-click publishers",
                        {"blacklisted": ok, "bid_factor_failed": bad},
                    )
                    changed = True

            if not skip_unmon_pause:
                mon_url = _resolve_mon_url(row, camp_json if isinstance(camp_json, dict) else None)
                geo = str(row.get("geo") or "").strip()
                net = row.get("monNetwork") or ""
                mon_ok, err_t = _monetization_for_network(str(net), mon_url, geo)
                row["lastMonCheck"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                changed = True
                if err_t == "error":
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

    if changed and rows:
        gd.create_or_update_sheet_from_dicts_withID(sheet_id, TAB_EXPLORATION, rows)
    _sk_tools_workbook_log(
        "",
        "SKtrackExploration",
        "SK exploration run summary",
        {
            "rows_processed": processed_rows,
            "sources_bid_decayed": bid_decay_ok_total,
            "sources_bid_decay_failed": bid_decay_fail_total,
            "sources_blacklisted": blacklisted_ok_total,
            "sources_blacklist_failed": blacklisted_fail_total,
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

    changed = False
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

        row["budgetReachedYesterday"] = check_budget_reached_yesterday_SK(cid)
        changed = True

        if not _row_active(row):
            continue

        skip_unmon_pause = cid in _SK_UNMON_SKIP_CAMPAIGN_ID_SET
        if skip_unmon_pause:
            row["logs"] = _append_logs_cell(
                row.get("logs", ""),
                f"unmon pause skipped by config for campaign {cid}",
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
        if err_t == "error":
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

    if changed and rows:
        gd.create_or_update_sheet_from_dicts_withID(sheet_id, TAB_WL, rows)


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
