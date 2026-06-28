"""
Bulk raise SK campaign CPC from ``BulkBidRaising`` sheet, capping bid factors on boosted sources.

Sheet tab ``BulkBidRaising`` on ``SK_TOOLS_SPREADSHEET_ID`` (row 1 headers):
  - campaignId
  - newCampaignBid
  - maxBidPerSource  (max effective bid = campaign_cpc * bid_factor for sources with bidFactor > 1)

After raising campaign CPC, sources with bidFactor > 1 are checked; if
``newCampaignBid * bidFactor`` exceeds ``maxBidPerSource``, bid factor is lowered to
``maxBidPerSource / newCampaignBid`` (respecting SK minimum bid factor).
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from config import SK_TOOLS_SPREADSHEET_ID, SOURCEKNOWLEDGE_API_KEY
from integrations.autoserver import sk as sk_mod
from integrations.autoserver.sk_optimizer import (
    SK_MIN_BID_FACTOR,
    _post_bid_factors_bulk_with_retry,
    _sk_stats_start_date,
    _stats_items_by_subid_today,
)

logger = logging.getLogger(__name__)

TAB_BULK_BID_RAISING = "BulkBidRaising"
HEADERS_BULK_BID_RAISING = [
    "campaignId",
    "newCampaignBid",
    "maxBidPerSource",
]

SK_API_BASE = "https://api.sourceknowledge.com/affiliate/v2"
REQUEST_TIMEOUT = 60
COOLDOWN_SECONDS = 60
BID_FACTOR_GT = 1.0


@dataclass
class BulkBidRow:
    row_number: int
    campaign_id: int
    new_campaign_bid: float
    max_bid_per_source: float


@dataclass
class CampaignBidRaiseResult:
    campaign_id: int
    ok: bool
    dry_run: bool
    old_cpc: Optional[float] = None
    new_cpc: Optional[float] = None
    sources_checked: int = 0
    sources_capped: int = 0
    bid_updates: List[Tuple[str, float, float, float]] = field(default_factory=list)
    message: str = ""


def _credentials_path() -> Path:
    p = Path(__file__).resolve().parents[1] / "credentials.json"
    if not p.exists():
        raise FileNotFoundError(f"credentials.json not found at {p}")
    return p


def _headers() -> dict[str, str]:
    return {"accept": "application/json", "X-API-KEY": SOURCEKNOWLEDGE_API_KEY}


def _request(method: str, url: str, *, json_body: dict | None = None) -> requests.Response:
    while True:
        try:
            r = requests.request(
                method,
                url,
                headers=_headers(),
                json=json_body,
                timeout=REQUEST_TIMEOUT,
            )
        except requests.RequestException as e:
            logger.warning("SK network error: %s; cooldown %ss", e, COOLDOWN_SECONDS)
            time.sleep(COOLDOWN_SECONDS)
            continue
        if r.status_code == 429:
            logger.warning("SK 429; cooldown %ss", COOLDOWN_SECONDS)
            time.sleep(COOLDOWN_SECONDS)
            continue
        return r


def ensure_bulk_bid_raising_sheet(spreadsheet_id: Optional[str] = None) -> str:
    """Create or fix headers on ``BulkBidRaising`` tab. Returns spreadsheet id used."""
    from integrations.autoserver import gdocs_as as gd

    sid = (spreadsheet_id or SK_TOOLS_SPREADSHEET_ID or "").strip()
    if not sid:
        raise RuntimeError("SK_TOOLS_SPREADSHEET_ID is not set")
    gd.ensure_worksheet_with_headers(sid, TAB_BULK_BID_RAISING, HEADERS_BULK_BID_RAISING)
    return sid


def _read_sheet_rows(
    spreadsheet_id: str,
    tab_name: str = TAB_BULK_BID_RAISING,
) -> List[BulkBidRow]:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds = service_account.Credentials.from_service_account_file(str(_credentials_path()))
    service = build("sheets", "v4", credentials=creds).spreadsheets()
    quoted = tab_name.replace("'", "''")
    res = service.values().get(spreadsheetId=spreadsheet_id, range=f"'{quoted}'!A:Z").execute()
    values = res.get("values") or []
    if not values:
        return []

    header = [str(c or "").strip().lower() for c in values[0]]

    def col_idx(*names: str) -> int:
        for n in names:
            if n in header:
                return header.index(n)
        return -1

    i_cid = col_idx("campaignid", "campaign id", "id")
    i_bid = col_idx("newcampaignbid", "new campaign bid", "cpc", "bid")
    i_max = col_idx("maxbidpersource", "max bid per source", "maxbid", "max bid")
    if i_cid < 0 or i_bid < 0 or i_max < 0:
        raise ValueError(
            f"Tab {tab_name!r} must have campaignId, newCampaignBid, maxBidPerSource headers"
        )

    rows: List[BulkBidRow] = []
    for row_num, raw in enumerate(values[1:], start=2):
        if not raw:
            continue

        def cell(i: int) -> str:
            if i < 0 or i >= len(raw):
                return ""
            return str(raw[i] or "").strip()

        cid_s = cell(i_cid).replace(",", "")
        if not cid_s or not cid_s.isdigit():
            continue
        try:
            new_bid = float(cell(i_bid).replace(",", "") or "0")
            max_bid = float(cell(i_max).replace(",", "") or "0")
        except ValueError:
            continue
        if new_bid <= 0 or max_bid <= 0:
            continue
        rows.append(
            BulkBidRow(
                row_number=row_num,
                campaign_id=int(cid_s),
                new_campaign_bid=new_bid,
                max_bid_per_source=max_bid,
            )
        )
    return rows


def _get_campaign(campaign_id: int) -> Tuple[Optional[dict], Optional[str]]:
    camp = sk_mod.get_campaignById(campaign_id)
    if not isinstance(camp, dict):
        return None, "invalid campaign response"
    if camp.get("error"):
        return None, str(camp.get("error"))
    if camp.get("_http_status"):
        return None, f"HTTP {camp['_http_status']}"
    if camp.get("cpc") is None:
        return None, "campaign missing cpc"
    return camp, None


def _put_campaign_cpc(campaign_id: int, camp: dict, new_cpc: float) -> Tuple[bool, str]:
    payload = dict(camp)
    payload["cpc"] = float(new_cpc)
    url = f"{SK_API_BASE}/campaigns/{campaign_id}"
    r = _request("PUT", url, json_body=payload)
    if r.status_code == 200:
        return True, ""
    return False, f"PUT {r.status_code}: {(r.text or '')[:240]}"


def _sources_with_bid_factor_gt(
    campaign_id: int,
    camp_json: dict,
    *,
    min_bid_factor: float = BID_FACTOR_GT,
) -> Tuple[Dict[str, float], Optional[str]]:
    """Return ``{subId: bidFactor}`` for sources with bidFactor > min_bid_factor."""
    today = datetime.now(timezone.utc).date().strftime("%Y-%m-%d")
    d0 = _sk_stats_start_date(camp_json)
    stats, err = _stats_items_by_subid_today(campaign_id, d0, today)
    if err:
        return {}, err
    out: Dict[str, float] = {}
    for sid, info in stats.items():
        bf = float(info.get("bidFactor") if info.get("bidFactor") is not None else 1.0)
        if bf > min_bid_factor:
            out[sid] = bf
    return out, None


def _cap_bid_factor(new_cpc: float, max_effective: float) -> float:
    if new_cpc <= 0:
        raise ValueError("new_cpc must be positive")
    bf = max_effective / new_cpc
    return max(SK_MIN_BID_FACTOR, bf)


def process_campaign_bid_raise(
    row: BulkBidRow,
    *,
    dry_run: bool = True,
) -> CampaignBidRaiseResult:
    res = CampaignBidRaiseResult(
        campaign_id=row.campaign_id,
        ok=False,
        dry_run=dry_run,
        new_cpc=row.new_campaign_bid,
    )
    camp, err = _get_campaign(row.campaign_id)
    if err or not camp:
        res.message = err or "campaign not found"
        return res

    try:
        old_cpc = float(camp["cpc"])
    except (TypeError, ValueError):
        res.message = "invalid current cpc"
        return res
    res.old_cpc = old_cpc

    if abs(old_cpc - row.new_campaign_bid) < 1e-9:
        res.message = f"cpc already {old_cpc:.4f}; skipping bid update"
        cpc_for_cap = old_cpc
    elif dry_run:
        res.message = f"would raise cpc {old_cpc:.4f} -> {row.new_campaign_bid:.4f}"
        cpc_for_cap = row.new_campaign_bid
    else:
        ok, put_err = _put_campaign_cpc(row.campaign_id, camp, row.new_campaign_bid)
        if not ok:
            res.message = put_err
            return res
        res.message = f"raised cpc {old_cpc:.4f} -> {row.new_campaign_bid:.4f}"
        cpc_for_cap = row.new_campaign_bid

    sources, stats_err = _sources_with_bid_factor_gt(row.campaign_id, camp)
    if stats_err:
        res.message = f"{res.message}; stats error: {stats_err}".strip("; ")
        res.ok = not dry_run and "raised cpc" in res.message
        return res

    res.sources_checked = len(sources)
    updates: List[Tuple[str, float]] = []
    for sid, cur_bf in sources.items():
        effective = cpc_for_cap * cur_bf
        if effective <= row.max_bid_per_source + 1e-9:
            continue
        new_bf = _cap_bid_factor(cpc_for_cap, row.max_bid_per_source)
        new_effective = cpc_for_cap * new_bf
        res.bid_updates.append((sid, cur_bf, new_bf, new_effective))
        updates.append((sid, new_bf))

    res.sources_capped = len(updates)
    if updates:
        cap_note = (
            f"{len(updates)} source(s) would need bid-factor cap (max effective ${row.max_bid_per_source:.4f})"
            if dry_run
            else f"capping {len(updates)} source(s) to max effective ${row.max_bid_per_source:.4f}"
        )
        res.message = f"{res.message}; {cap_note}".strip("; ")
        if not dry_run:
            ok_sids, fail_pairs = _post_bid_factors_bulk_with_retry(row.campaign_id, updates)
            if fail_pairs:
                res.message += f"; {len(fail_pairs)} bid-factor update(s) failed"
                res.ok = len(ok_sids) > 0
            else:
                res.ok = True
        else:
            res.ok = True
    else:
        res.ok = True
        if res.sources_checked:
            res.message += f"; {res.sources_checked} boosted source(s) within max"
        else:
            res.message += "; no sources with bidFactor > 1 in stats window"

    return res


def run_bulk_bid_raising(
    *,
    spreadsheet_id: Optional[str] = None,
    tab_name: str = TAB_BULK_BID_RAISING,
    dry_run: bool = True,
    ensure_sheet: bool = True,
) -> Tuple[List[CampaignBidRaiseResult], str]:
    """
    Read sheet and process each row. Returns (per-campaign results, text log).
    """
    sid = (spreadsheet_id or SK_TOOLS_SPREADSHEET_ID or "").strip()
    if not sid:
        raise RuntimeError("SK_TOOLS_SPREADSHEET_ID is not set")
    if not SOURCEKNOWLEDGE_API_KEY:
        raise RuntimeError("SOURCEKNOWLEDGE_API_KEY / KEYSK is not set")

    lines: List[str] = []
    mode = "DRY RUN" if dry_run else "APPLY"
    lines.append(f"BulkBidRaising ({mode}) — tab {tab_name!r} on {sid}")

    if ensure_sheet:
        ensure_bulk_bid_raising_sheet(sid)
        lines.append("Ensured BulkBidRaising tab and headers.")

    rows = _read_sheet_rows(sid, tab_name)
    if not rows:
        lines.append("No data rows (need campaignId, newCampaignBid, maxBidPerSource).")
        return [], "\n".join(lines), True

    lines.append(f"Found {len(rows)} row(s) to process.\n")
    results: List[CampaignBidRaiseResult] = []
    failed = 0
    for i, row in enumerate(rows, start=1):
        lines.append(
            f"[{i}/{len(rows)}] campaign {row.campaign_id}: "
            f"new bid ${row.new_campaign_bid:.4f}, max/source ${row.max_bid_per_source:.4f}"
        )
        res = process_campaign_bid_raise(row, dry_run=dry_run)
        results.append(res)
        lines.append(f"  -> {res.message}")
        for sid, old_bf, new_bf, eff in res.bid_updates[:15]:
            lines.append(
                f"     sub {sid}: bf {old_bf:.4f} -> {new_bf:.4f} "
                f"(effective ${eff:.4f})"
            )
        if len(res.bid_updates) > 15:
            lines.append(f"     ... and {len(res.bid_updates) - 15} more")
        if not res.ok:
            failed += 1
        lines.append("")

    ok_count = sum(1 for r in results if r.ok)
    failed = len(results) - ok_count
    lines.append(f"Done: {ok_count} ok, {failed} failed.")
    return results, "\n".join(lines), failed == 0
