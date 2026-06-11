"""
SK garbage-source detection (hourly bombardment) with yellow / red tickets.

Runs inside ``checkUnmonExploration_SK`` — reuses today's by-publisher stats already
fetched for bid-decay (no extra SK stats API call per campaign).

Detection: between hourly runs, a sub with >=100 clicks in the interval is flagged
unless it is on the campaign WL.

- Yellow: first campaign hit → log on ``SKgarbageSources``, bid 0 on campaign.
- Red: same source on a second campaign → update global control list 48365 (GET+PUT).
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import requests
from gspread.utils import rowcol_to_a1

from config import (
    SK_GARBAGE_CLICK_THRESHOLD,
    SK_GLOBAL_BLACKLIST_CONTROL_LIST_ID,
    SK_TOOLS_SPREADSHEET_ID,
)
from integrations.autoserver import gdocs_as as gd
from integrations.autoserver import sk as sk_mod

logger = logging.getLogger(__name__)

TAB_GARBAGE_LOG = "SKgarbageSources"
SK_API_BASE = "https://api.sourceknowledge.com/affiliate/v2"

HEADERS_GARBAGE_LOG = [
    "subId",
    "ticketStatus",
    "yellowTicketAt",
    "yellowCampaignId",
    "yellowCampaignName",
    "redTicketAt",
    "redCampaignId",
    "redCampaignName",
    "lastSeenAt",
    "lastIntervalClicks",
    "globalBlacklistAt",
    "globalBlacklistStatus",
    "notes",
]

_STATE_PATH = Path(__file__).resolve().parents[2] / "data" / "sk_garbage_click_snapshots.json"
_ENSURED_LOG_TAB: set[str] = set()


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sk_headers() -> dict:
    return dict(sk_mod.headers_sk)


def ensure_garbage_log_worksheet(spreadsheet_id: str) -> None:
    sid = (spreadsheet_id or "").strip()
    if not sid:
        return
    gd.ensure_worksheet_with_headers(sid, TAB_GARBAGE_LOG, HEADERS_GARBAGE_LOG)
    _ENSURED_LOG_TAB.add(sid)


def _load_click_snapshot() -> dict:
    if not _STATE_PATH.exists():
        return {"run_utc": "", "campaigns": {}}
    try:
        data = json.loads(_STATE_PATH.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            data.setdefault("campaigns", {})
            return data
    except Exception as e:
        logger.warning("garbage snapshot read failed: %s", e)
    return {"run_utc": "", "campaigns": {}}


def _save_click_snapshot(state: dict) -> None:
    _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _STATE_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8")
    tmp.replace(_STATE_PATH)


def _interval_clicks(
    current: Dict[str, int],
    previous: Dict[str, int],
) -> Dict[str, int]:
    """Clicks gained since the previous hourly snapshot (today totals)."""
    deltas: Dict[str, int] = {}
    for sid, cur in current.items():
        prev = int(previous.get(sid) or 0)
        delta = int(cur) - prev
        if delta > 0:
            deltas[sid] = delta
    return deltas


def _read_garbage_log(spreadsheet_id: str) -> Tuple[List[dict], Dict[str, dict]]:
    rows = gd.read_sheet_withID(spreadsheet_id, TAB_GARBAGE_LOG)
    by_sub: Dict[str, dict] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        sid = str(row.get("subId") or "").strip()
        if sid:
            by_sub[sid] = row
    return rows, by_sub


def _append_garbage_log_row(spreadsheet_id: str, row: dict) -> None:
    ws = gd.client.open_by_key(spreadsheet_id).worksheet(TAB_GARBAGE_LOG)
    values = [str(row.get(h) or "") for h in HEADERS_GARBAGE_LOG]
    ws.append_row(values, value_input_option="USER_ENTERED")


def _update_garbage_log_row(spreadsheet_id: str, row_index: int, row: dict) -> None:
    """``row_index`` is 1-based sheet row (header is row 1)."""
    ws = gd.client.open_by_key(spreadsheet_id).worksheet(TAB_GARBAGE_LOG)
    values = [str(row.get(h) or "") for h in HEADERS_GARBAGE_LOG]
    end = rowcol_to_a1(row_index, len(HEADERS_GARBAGE_LOG))
    ws.update(f"A{row_index}:{end}", [values])


def _get_control_list(list_id: int) -> Tuple[Optional[dict], Optional[str]]:
    url = f"{SK_API_BASE}/control-lists/{list_id}"
    try:
        r = requests.get(url, headers=_sk_headers(), timeout=60)
    except requests.RequestException as e:
        return None, str(e)
    if r.status_code == 429:
        time.sleep(60)
        try:
            r = requests.get(url, headers=_sk_headers(), timeout=60)
        except requests.RequestException as e:
            return None, str(e)
    if r.status_code != 200:
        return None, f"HTTP {r.status_code}: {(r.text or '')[:300]}"
    try:
        data = r.json()
    except Exception as e:
        return None, str(e)
    return data if isinstance(data, dict) else None, None


def _put_control_list(list_id: int, body: dict) -> Tuple[bool, str]:
    url = f"{SK_API_BASE}/control-lists/{list_id}"
    for attempt in range(3):
        try:
            r = requests.put(url, headers=_sk_headers(), json=body, timeout=60)
        except requests.RequestException as e:
            return False, str(e)
        if r.status_code == 429 and attempt < 2:
            time.sleep(60)
            continue
        if r.status_code == 200:
            return True, "ok"
        return False, f"HTTP {r.status_code}: {(r.text or '')[:300]}"
    return False, "rate_limited"


def append_to_global_blacklist(sub_id: str, *, list_id: Optional[int] = None) -> Tuple[bool, str]:
    """
    GET control list, append ``sub_id`` to ``subIds`` if missing, PUT update.
    """
    lid = int(list_id or SK_GLOBAL_BLACKLIST_CONTROL_LIST_ID)
    data, err = _get_control_list(lid)
    if err or not data:
        return False, err or "empty response"
    sub_ids = list(data.get("subIds") or [])
    if sub_id in sub_ids:
        return True, "already_listed"
    sub_ids.append(sub_id)
    body = {
        "name": data.get("name") or "Global block list",
        "resetBidFactors": True,
        "subIds": sub_ids,
        "campaigns": list(data.get("campaigns") or []),
    }
    ok, put_err = _put_control_list(lid, body)
    if ok:
        return True, "added"
    return False, put_err


class GarbagePassContext:
    """
    Per-hour garbage detection state. Created once at the start of
    ``checkUnmonExploration_SK``; ``process_campaign`` is called from the
    existing per-campaign loop using stats already fetched for bid-decay.
    """

    def __init__(self, blacklist_fn: Callable[[int, List[str]], List[str]]) -> None:
        self._blacklist_fn = blacklist_fn
        self.summary: Dict[str, int] = {
            "yellow_new": 0,
            "yellow_repeat": 0,
            "red_new": 0,
            "global_blacklist_ok": 0,
            "global_blacklist_fail": 0,
            "campaign_blacklist_ok": 0,
            "campaign_blacklist_fail": 0,
            "skipped_wl": 0,
            "skipped_no_baseline": 0,
        }
        self._tools_id = (SK_TOOLS_SPREADSHEET_ID or "").strip()
        self.enabled = bool(self._tools_id)
        self._threshold = max(1, int(SK_GARBAGE_CLICK_THRESHOLD))
        self._has_baseline = False
        self._prev_campaigns: Dict[str, Dict[str, int]] = {}
        self._new_snapshots: Dict[str, Dict[str, int]] = {}
        self._global_bl_added: Set[str] = set()
        self._log_rows: List[dict] = []
        self._log_by_sub: Dict[str, dict] = {}
        self._sub_to_row_index: Dict[str, int] = {}

    @classmethod
    def begin(cls, blacklist_fn: Callable[[int, List[str]], List[str]]) -> "GarbagePassContext":
        ctx = cls(blacklist_fn)
        if not ctx.enabled:
            logger.warning("SK_TOOLS_SPREADSHEET_ID not set; garbage detection disabled")
            return ctx
        try:
            ensure_garbage_log_worksheet(ctx._tools_id)
            ctx._log_rows, ctx._log_by_sub = _read_garbage_log(ctx._tools_id)
            for i, row in enumerate(ctx._log_rows, start=2):
                sid = str(row.get("subId") or "").strip()
                if sid:
                    ctx._sub_to_row_index[sid] = i
            state = _load_click_snapshot()
            ctx._has_baseline = bool(state.get("run_utc"))
            ctx._prev_campaigns = state.get("campaigns") or {}
        except Exception as e:
            logger.exception("garbage pass init failed: %s", e)
            ctx.enabled = False
        return ctx

    def process_campaign(
        self,
        campaign_id: int,
        campaign_name: str,
        wl: Set[str],
        today_clicks: Dict[str, int],
    ) -> None:
        """Evaluate one campaign using pre-fetched today click totals per subId."""
        if not self.enabled:
            return

        cid_s = str(campaign_id)
        self._new_snapshots[cid_s] = dict(today_clicks)
        if not self._has_baseline:
            self.summary["skipped_no_baseline"] += 1
            return

        prev = self._prev_campaigns.get(cid_s) or {}
        deltas = _interval_clicks(today_clicks, prev)
        flagged = [
            (sid, delta)
            for sid, delta in deltas.items()
            if delta >= self._threshold and sid not in wl
        ]
        self.summary["skipped_wl"] += sum(
            1 for sid, delta in deltas.items() if delta >= self._threshold and sid in wl
        )
        if not flagged:
            return

        to_blacklist = [sid for sid, _ in flagged]
        failed = self._blacklist_fn(campaign_id, to_blacklist)
        ok_set = {s for s in to_blacklist if s not in failed}
        self.summary["campaign_blacklist_ok"] += len(ok_set)
        self.summary["campaign_blacklist_fail"] += len(failed)

        now = _utc_now()
        for sid, delta in flagged:
            self._handle_ticket(sid, delta, campaign_id, campaign_name, now)

    def _handle_ticket(
        self,
        sid: str,
        delta: int,
        campaign_id: int,
        campaign_name: str,
        now: str,
    ) -> None:
        existing = self._log_by_sub.get(sid)
        yellow_cid = str(existing.get("yellowCampaignId") or "").strip() if existing else ""
        ticket = str(existing.get("ticketStatus") or "").strip().lower() if existing else ""

        if not existing:
            new_row = {
                "subId": sid,
                "ticketStatus": "yellow",
                "yellowTicketAt": now,
                "yellowCampaignId": str(campaign_id),
                "yellowCampaignName": campaign_name,
                "redTicketAt": "",
                "redCampaignId": "",
                "redCampaignName": "",
                "lastSeenAt": now,
                "lastIntervalClicks": str(delta),
                "globalBlacklistAt": "",
                "globalBlacklistStatus": "",
                "notes": (
                    f"Hourly bombardment: {delta} clicks in interval "
                    f"(threshold {self._threshold})"
                ),
            }
            _append_garbage_log_row(self._tools_id, new_row)
            self._log_by_sub[sid] = new_row
            self._sub_to_row_index[sid] = len(self._log_rows) + 2
            self._log_rows.append(new_row)
            self.summary["yellow_new"] += 1
            return

        if ticket == "red":
            existing["lastSeenAt"] = now
            existing["lastIntervalClicks"] = str(delta)
            existing["notes"] = (
                f"{existing.get('notes', '')}; seen again on {campaign_id} ({delta} clicks) {now}"
            ).strip("; ")
            idx = self._sub_to_row_index.get(sid)
            if idx:
                _update_garbage_log_row(self._tools_id, idx, existing)
            self.summary["yellow_repeat"] += 1
            return

        if yellow_cid and yellow_cid != str(campaign_id):
            existing["ticketStatus"] = "red"
            existing["redTicketAt"] = now
            existing["redCampaignId"] = str(campaign_id)
            existing["redCampaignName"] = campaign_name
            existing["lastSeenAt"] = now
            existing["lastIntervalClicks"] = str(delta)
            bl_status = ""
            bl_at = ""
            if sid not in self._global_bl_added:
                ok, msg = append_to_global_blacklist(sid)
                self._global_bl_added.add(sid)
                bl_at = now if ok else ""
                bl_status = msg
                if ok:
                    self.summary["global_blacklist_ok"] += 1
                else:
                    self.summary["global_blacklist_fail"] += 1
            else:
                bl_status = "deduped_same_run"
            existing["globalBlacklistAt"] = bl_at
            existing["globalBlacklistStatus"] = bl_status
            existing["notes"] = (
                f"{existing.get('notes', '')}; red ticket on campaign {campaign_id} "
                f"({delta} clicks) {now}; global BL: {bl_status}"
            ).strip("; ")
            idx = self._sub_to_row_index.get(sid)
            if idx:
                _update_garbage_log_row(self._tools_id, idx, existing)
            self.summary["red_new"] += 1
        else:
            existing["lastSeenAt"] = now
            existing["lastIntervalClicks"] = str(delta)
            existing["notes"] = (
                f"{existing.get('notes', '')}; repeat on {campaign_id} ({delta} clicks) {now}"
            ).strip("; ")
            idx = self._sub_to_row_index.get(sid)
            if idx:
                _update_garbage_log_row(self._tools_id, idx, existing)
            self.summary["yellow_repeat"] += 1

    def finish(self) -> Dict[str, int]:
        if self.enabled:
            state = _load_click_snapshot()
            state["run_utc"] = _utc_now()
            state["campaigns"] = self._new_snapshots
            _save_click_snapshot(state)
        return dict(self.summary)
