"""
Daily Blend CPC refresh.

Updates ``cpc_desktop`` / ``cpc_mobile`` on the Blend sheet using recent
Keitaro offer/device EPC where available, with potential-sheet CPCs as
fallback when a device has no recent tracker data.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from blend_sync_from_sheet import POTENTIAL_TAB_BY_FEED, SPREADSHEET_ID, get_sheets_service
from config import BLEND_CPC_REFRESH_LOOKBACK_DAYS, BLEND_CPC_REFRESH_STATE_PATH
from integrations.blend_legacy_review import (
    BlendReviewRow,
    DEFAULT_DATE_FROM,
    DEFAULT_DATE_TO,
    _column_letter,
    _epc_to_sheet,
    _parse_float,
    _quoted_sheet_name,
    _get_cell,
    ensure_review_headers,
    fetch_blend_offer_device_epc,
    load_blend_review_sheet,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PotentialFallback:
    desktop: str
    mobile: str
    source_tab: str


def _state_path() -> Path:
    raw = (BLEND_CPC_REFRESH_STATE_PATH or "").strip()
    p = Path(raw) if raw else (Path(__file__).resolve().parents[1] / "runtime" / "blend_cpc_refresh_state.json")
    return p if p.is_absolute() else Path(__file__).resolve().parents[1] / p


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def rolling_refresh_window(
    *,
    lookback_days: int = BLEND_CPC_REFRESH_LOOKBACK_DAYS,
    today: Optional[date] = None,
) -> Tuple[date, date]:
    base = today or datetime.now(timezone.utc).date()
    end = base - timedelta(days=1)
    days = max(1, int(lookback_days or 4))
    start = end - timedelta(days=days - 1)
    return start, end


def _normalize_brand_key(value: str) -> str:
    return (value or "").strip().lower()


def _load_potential_fallbacks(service, *, feed_tags: Iterable[str]) -> Dict[Tuple[str, str, str], PotentialFallback]:
    out: Dict[Tuple[str, str, str], PotentialFallback] = {}
    feeds = sorted({(f or "").strip().lower() for f in feed_tags if f})
    for feed_tag in feeds:
        tab = POTENTIAL_TAB_BY_FEED.get(feed_tag)
        if not tab:
            continue
        try:
            result = service.values().get(spreadsheetId=SPREADSHEET_ID, range=f"{tab}!A:Z").execute()
        except Exception as e:
            logger.warning("Blend CPC refresh: potential tab %s unavailable for %s: %s", tab, feed_tag, e)
            continue
        rows = result.get("values") or []
        if not rows:
            continue
        header = [str(c).strip() for c in rows[0]]
        idx = {name.lower(): i for i, name in enumerate(header)}
        for row in rows[1:]:
            merchant = _get_cell(row, idx, "merchant")
            geo = (_get_cell(row, idx, "geo_origin") or "").strip().lower()[:2]
            if not merchant or not geo:
                continue
            desktop = _get_cell(row, idx, "cpc_desktop")
            mobile = _get_cell(row, idx, "cpc_mobile")
            key = (feed_tag, geo, _normalize_brand_key(merchant))
            out[key] = PotentialFallback(desktop=desktop, mobile=mobile, source_tab=tab)
    return out


def _fallback_for_row(
    row: BlendReviewRow,
    fallback_map: Dict[Tuple[str, str, str], PotentialFallback],
) -> Optional[PotentialFallback]:
    keys = [
        (row.feed_tag, row.geo, _normalize_brand_key(row.brand_name)),
    ]
    for key in keys:
        fallback = fallback_map.get(key)
        if fallback:
            return fallback
    return None


def _build_cpc_updates(
    row: BlendReviewRow,
    *,
    epc_stats: Dict[Tuple[str, str], Any],
    fallback: Optional[PotentialFallback],
) -> Tuple[str, str, Dict[str, Any]]:
    stat_d = epc_stats.get((row.offer_name, "desktop"))
    stat_m = epc_stats.get((row.offer_name, "mobile"))

    fallback_d = (fallback.desktop if fallback else "").strip()
    fallback_m = (fallback.mobile if fallback else "").strip()

    current_d = (row.cpc_desktop_raw or "").strip()
    current_m = (row.cpc_mobile_raw or "").strip()

    if stat_d and getattr(stat_d, "denominator", 0) > 0:
        new_d = _epc_to_sheet(stat_d.epc)
        source_d = stat_d.epc_source
    elif fallback_d:
        new_d = fallback_d
        source_d = f"fallback:{fallback.source_tab}" if fallback else "fallback"
    else:
        new_d = current_d
        source_d = "existing" if current_d else ""

    if stat_m and getattr(stat_m, "denominator", 0) > 0:
        new_m = _epc_to_sheet(stat_m.epc)
        source_m = stat_m.epc_source
    elif fallback_m:
        new_m = fallback_m
        source_m = f"fallback:{fallback.source_tab}" if fallback else "fallback"
    else:
        new_m = current_m
        source_m = "existing" if current_m else ""

    return new_d, new_m, {
        "desktop_source": source_d,
        "mobile_source": source_m,
        "had_keitaro_desktop": bool(stat_d and getattr(stat_d, "denominator", 0) > 0),
        "had_keitaro_mobile": bool(stat_m and getattr(stat_m, "denominator", 0) > 0),
        "used_fallback_desktop": bool(not (stat_d and getattr(stat_d, "denominator", 0) > 0) and fallback_d),
        "used_fallback_mobile": bool(not (stat_m and getattr(stat_m, "denominator", 0) > 0) and fallback_m),
    }


def _batch_write_cpcs(service, header_index: Dict[str, int], updates: List[Tuple[int, str, str]]) -> None:
    if not updates:
        return
    cpc_d_idx = header_index.get("cpc_desktop")
    cpc_m_idx = header_index.get("cpc_mobile")
    if cpc_d_idx is None or cpc_m_idx is None:
        raise ValueError("Blend sheet missing cpc_desktop/cpc_mobile columns")
    quoted = _quoted_sheet_name()
    data = []
    for sheet_row, cpc_d, cpc_m in updates:
        data.append(
            {
                "range": f"'{quoted}'!{_column_letter(cpc_d_idx + 1)}{sheet_row}",
                "values": [[cpc_d]],
            }
        )
        data.append(
            {
                "range": f"'{quoted}'!{_column_letter(cpc_m_idx + 1)}{sheet_row}",
                "values": [[cpc_m]],
            }
        )
    service.values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"valueInputOption": "RAW", "data": data},
    ).execute()


def _write_state(payload: Dict[str, Any]) -> None:
    path = _state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def refresh_blend_cpcs(
    *,
    dry_run: bool = False,
    d_from: Optional[date] = None,
    d_to: Optional[date] = None,
) -> Dict[str, Any]:
    service = get_sheets_service()
    ensure_review_headers(service)
    sheet = load_blend_review_sheet(service, legacy_only=False)
    if not sheet.rows:
        payload = {
            "ok": True,
            "mode": "dry-run" if dry_run else "apply",
            "updated_utc": _utc_now_iso(),
            "date_window": None,
            "row_count": 0,
            "updated_rows": 0,
            "changed_rows": 0,
            "rows": [],
        }
        _write_state(payload)
        return payload

    window_start, window_end = rolling_refresh_window()
    start = d_from or window_start
    end = d_to or window_end
    stats = fetch_blend_offer_device_epc(d_from=start, d_to=end)
    fallback_map = _load_potential_fallbacks(service, feed_tags={row.feed_tag for row in sheet.rows})

    pending_updates: List[Tuple[int, str, str]] = []
    row_summaries: List[Dict[str, Any]] = []
    changed_rows = 0
    for row in sheet.rows:
        fallback = _fallback_for_row(row, fallback_map)
        new_d, new_m, meta = _build_cpc_updates(row, epc_stats=stats, fallback=fallback)
        changed = new_d != (row.cpc_desktop_raw or "").strip() or new_m != (row.cpc_mobile_raw or "").strip()
        if changed:
            changed_rows += 1
            pending_updates.append((row.sheet_row, new_d, new_m))
        row_summaries.append(
            {
                "sheet_row": row.sheet_row,
                "brand_name": row.brand_name,
                "geo": row.geo,
                "feed": row.feed_tag,
                "offer_name": row.offer_name,
                "cpc_desktop_before": row.cpc_desktop_raw,
                "cpc_mobile_before": row.cpc_mobile_raw,
                "cpc_desktop_after": new_d,
                "cpc_mobile_after": new_m,
                "changed": changed,
                **meta,
            }
        )

    if not dry_run:
        _batch_write_cpcs(service, sheet.header_index, pending_updates)

    payload = {
        "ok": True,
        "mode": "dry-run" if dry_run else "apply",
        "updated_utc": _utc_now_iso(),
        "date_window": {
            "from": start.isoformat(),
            "to": end.isoformat(),
            "lookback_days": (end - start).days + 1,
        },
        "row_count": len(sheet.rows),
        "changed_rows": changed_rows,
        "updated_rows": len(pending_updates) if not dry_run else 0,
        "rows": row_summaries,
    }
    _write_state(payload)
    return payload
