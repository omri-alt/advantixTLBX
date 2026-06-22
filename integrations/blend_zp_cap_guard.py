"""
Pause mapped Zeropark Blend campaigns once the Blend geo/device cap is filled.

The Google Sheet is a lightweight mapping table:
  - column A: geo
  - column B: device (desktop/mobile)
  - column C: Zeropark campaign id

The actual target clicks per geo/device come from the Blend sheet via
``integrations.blend_cap_progress.refresh_blend_cap_progress()``, so there is a
single source of truth for Blend click-cap math.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from config import (
    BLEND_SHEETS_SPREADSHEET_ID,
    KEYZP,
    ZEROPARK_BLEND_CAP_SHEET_NAME,
    ZEROPARK_BLEND_CAP_SPREADSHEET_ID,
)
from integrations.blend_cap_progress import refresh_blend_cap_progress
from integrations.zeropark import (
    ZeroparkClientError,
    list_campaign_rows_today,
    pause_campaign,
    resume_campaign,
)

logger = logging.getLogger(__name__)

_BLEND_ZP_NAME_RE = re.compile(r"^Blend-KL-([A-Za-z]{2})(?:-(Desktop|Mobile))?$", re.IGNORECASE)
_MAPPING_SHEET_HEADER = ["geo", "device", "campaign id"]


@dataclass(frozen=True)
class BlendZpCampaignMapping:
    geo: str
    device: str
    campaign_id: str


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _credentials_path() -> Path:
    path = _repo_root() / "credentials.json"
    if not path.exists():
        raise FileNotFoundError(f"credentials.json not found at {path}")
    return path


def _get_sheets_service():
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds = service_account.Credentials.from_service_account_file(str(_credentials_path()))
    return build("sheets", "v4", credentials=creds).spreadsheets()


def _normalize_geo(value: str) -> str:
    geo = (value or "").strip().lower()
    if not geo:
        return ""
    if geo == "gb":
        return "uk"
    return geo[:2]


def _normalize_device(value: str) -> str:
    raw = (value or "").strip().lower().replace("-", " ").replace("_", " ")
    if not raw:
        return ""
    if raw in ("all", "both", "any"):
        return "all"
    if "desktop" in raw:
        return "desktop"
    if raw in ("mobile", "phone", "mobile phone", "tablet", "smartphone"):
        return "mobile"
    return ""


def _looks_like_header(row: list[str]) -> bool:
    cells = [str(cell or "").strip().lower() for cell in row[:3]]
    return (
        any(cell in ("geo", "country") for cell in cells)
        or any("device" in cell for cell in cells)
        or any("campaign" in cell and "id" in cell for cell in cells)
    )


def _list_sheet_titles(service, spreadsheet_id: str) -> List[str]:
    meta = service.get(spreadsheetId=spreadsheet_id, fields="sheets(properties(title))").execute()
    return [
        str(s.get("properties", {}).get("title") or "")
        for s in meta.get("sheets", [])
        if s.get("properties", {}).get("title")
    ]


def _resolve_mapping_sheet_title(titles: List[str], preferred: str) -> Optional[str]:
    if preferred in titles:
        return preferred
    pref_norm = (preferred or "").replace(" ", "").lower()
    for title in titles:
        if title.replace(" ", "").lower() == pref_norm:
            return title
    for title in titles:
        tl = title.lower()
        if "zp" in tl and "blend" in tl and "campaign" in tl:
            return title
    return None


def _ensure_mapping_sheet_tab(service, spreadsheet_id: str, sheet_name: str) -> str:
    titles = _list_sheet_titles(service, spreadsheet_id)
    resolved = _resolve_mapping_sheet_title(titles, sheet_name)
    if resolved:
        return resolved
    service.batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]},
    ).execute()
    quoted = sheet_name.replace("'", "''")
    service.values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{quoted}'!A1:C1",
        valueInputOption="RAW",
        body={"values": [_MAPPING_SHEET_HEADER]},
    ).execute()
    logger.info("Created ZP Blend mapping tab %r on spreadsheet %s", sheet_name, spreadsheet_id)
    return sheet_name


def _parse_blend_zp_campaign_name(name: str) -> Optional[Tuple[str, str]]:
    m = _BLEND_ZP_NAME_RE.match((name or "").strip())
    if not m:
        return None
    geo = _normalize_geo(m.group(1))
    device_raw = m.group(2)
    device = device_raw.lower() if device_raw else "all"
    if not geo:
        return None
    return geo, device


def discover_blend_zp_mappings_from_api() -> List[BlendZpCampaignMapping]:
    """Infer geo/device/campaign-id rows from live Zeropark ``Blend-KL-*`` campaigns."""
    if not KEYZP:
        return []
    rows: List[BlendZpCampaignMapping] = []
    seen_ids: set[str] = set()
    for row in list_campaign_rows_today(KEYZP):
        details = row.get("details") if isinstance(row, dict) else None
        if not isinstance(details, dict):
            continue
        name = str(details.get("name") or "").strip()
        campaign_id = str(details.get("id") or "").strip()
        if not name.startswith("Blend-KL-") or not campaign_id:
            continue
        if "blendwl" in name.lower():
            continue
        parsed = _parse_blend_zp_campaign_name(name)
        if not parsed:
            continue
        if campaign_id in seen_ids:
            continue
        seen_ids.add(campaign_id)
        geo, device = parsed
        rows.append(BlendZpCampaignMapping(geo=geo, device=device, campaign_id=campaign_id))
    return rows


def _write_mapping_rows(
    service,
    spreadsheet_id: str,
    sheet_name: str,
    mappings: List[BlendZpCampaignMapping],
) -> None:
    quoted = sheet_name.replace("'", "''")
    values = [_MAPPING_SHEET_HEADER]
    for row in mappings:
        values.append([row.geo, row.device, row.campaign_id])
    service.values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{quoted}'!A1",
        valueInputOption="RAW",
        body={"values": values},
    ).execute()


def _read_mapping_rows(
    spreadsheet_id: str = ZEROPARK_BLEND_CAP_SPREADSHEET_ID,
    sheet_name: str = ZEROPARK_BLEND_CAP_SHEET_NAME,
    *,
    bootstrap_if_empty: bool = True,
) -> List[BlendZpCampaignMapping]:
    spreadsheet_id = (spreadsheet_id or BLEND_SHEETS_SPREADSHEET_ID).strip()
    service = _get_sheets_service()
    resolved_name = _ensure_mapping_sheet_tab(service, spreadsheet_id, sheet_name)
    quoted = resolved_name.replace("'", "''")
    result = service.values().get(
        spreadsheetId=spreadsheet_id,
        range=f"'{quoted}'!A:C",
    ).execute()
    values = result.get("values") or []
    rows: List[BlendZpCampaignMapping] = []
    for index, raw_row in enumerate(values):
        if index == 0 and _looks_like_header(raw_row):
            continue
        geo = _normalize_geo(str(raw_row[0]) if len(raw_row) > 0 else "")
        device = _normalize_device(str(raw_row[1]) if len(raw_row) > 1 else "")
        campaign_id = str(raw_row[2]).strip() if len(raw_row) > 2 else ""
        if not geo and not device and not campaign_id:
            continue
        if not (geo and device and campaign_id):
            logger.warning(
                "Skipping incomplete ZP Blend mapping row %s on %s: %r",
                index + 1,
                resolved_name,
                raw_row,
            )
            continue
        rows.append(BlendZpCampaignMapping(geo=geo, device=device, campaign_id=campaign_id))

    if not rows and bootstrap_if_empty:
        discovered = discover_blend_zp_mappings_from_api()
        if discovered:
            logger.info(
                "Bootstrapping %s with %s Blend-KL-* Zeropark campaign(s)",
                resolved_name,
                len(discovered),
            )
            _write_mapping_rows(service, spreadsheet_id, resolved_name, discovered)
            return discovered
    return rows


def load_mapping(
    spreadsheet_id: str = ZEROPARK_BLEND_CAP_SPREADSHEET_ID,
    sheet_name: str = ZEROPARK_BLEND_CAP_SHEET_NAME,
) -> Dict[Tuple[str, str], str]:
    mapping: Dict[Tuple[str, str], str] = {}
    for row in _read_mapping_rows(spreadsheet_id=spreadsheet_id, sheet_name=sheet_name):
        key = (row.geo, row.device)
        prev = mapping.get(key)
        if prev and prev != row.campaign_id:
            logger.warning(
                "Duplicate ZP Blend mapping for %s/%s; keeping first campaign %s and skipping %s",
                row.geo,
                row.device,
                prev,
                row.campaign_id,
            )
            continue
        mapping[key] = row.campaign_id
    return mapping


def _segment_rows_for_action(
    segments: Iterable[Dict[str, Any]],
    *,
    mode: str,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for segment in segments:
        try:
            target = int(segment.get("target_clicks") or 0)
            clicks = int(segment.get("clicks") or 0)
        except (TypeError, ValueError):
            continue
        if target <= 0:
            continue
        if mode == "pause_over_cap":
            if clicks < target:
                continue
        elif mode == "resume_under_cap":
            if clicks >= target:
                continue
        else:
            raise ValueError(f"Unsupported Blend ZP cap guard mode: {mode}")
        geo = _normalize_geo(str(segment.get("geo") or ""))
        device = _normalize_device(str(segment.get("device") or ""))
        if not (geo and device):
            continue
        out.append(
            {
                **segment,
                "geo": geo,
                "device": device,
                "target_clicks": target,
                "clicks": clicks,
            }
        )
    return out


def _extract_campaign_state(row: Dict[str, Any]) -> str:
    details = row.get("details") if isinstance(row, dict) else None
    if isinstance(details, dict):
        state = details.get("state")
        if isinstance(state, dict) and state.get("state") is not None:
            return str(state.get("state") or "").strip().upper()
        if state is not None:
            return str(state).strip().upper()
    for key in ("state", "status"):
        value = row.get(key) if isinstance(row, dict) else None
        if isinstance(value, dict) and value.get("state") is not None:
            return str(value.get("state") or "").strip().upper()
        if value is not None:
            return str(value).strip().upper()
    return ""


def _campaign_state_map(campaign_ids: Iterable[str]) -> Dict[str, str]:
    wanted = {str(cid).strip() for cid in campaign_ids if str(cid).strip()}
    if not wanted:
        return {}
    rows = list_campaign_rows_today(KEYZP)
    out: Dict[str, str] = {}
    for row in rows:
        details = row.get("details") if isinstance(row, dict) else None
        if not isinstance(details, dict):
            continue
        campaign_id = str(details.get("id") or "").strip()
        if not campaign_id or campaign_id not in wanted:
            continue
        out[campaign_id] = _extract_campaign_state(row)
    return out


def _state_is_paused(state: str) -> bool:
    s = (state or "").strip().upper()
    return s.startswith("PAUSED")


def _state_is_active(state: str) -> bool:
    return (state or "").strip().upper() == "ACTIVE"


def _build_action_record(
    segment: Dict[str, Any],
    campaign_id: Optional[str],
    campaign_state: str,
) -> Dict[str, Any]:
    return {
        "geo": segment["geo"],
        "device": segment["device"],
        "target_clicks": segment["target_clicks"],
        "clicks": segment["clicks"],
        "campaign_id": campaign_id,
        "campaign_state": campaign_state,
        "status": "pending",
    }


def run_blend_zp_cap_guard(
    *,
    dry_run: bool = False,
    reason: str = "manual",
    mode: str = "pause_over_cap",
    spreadsheet_id: str = ZEROPARK_BLEND_CAP_SPREADSHEET_ID,
    sheet_name: str = ZEROPARK_BLEND_CAP_SHEET_NAME,
) -> Dict[str, Any]:
    if not KEYZP:
        raise RuntimeError("KEYZP is not configured")

    progress = refresh_blend_cap_progress(reason=f"zp_cap_guard:{reason}")
    mapping = load_mapping(spreadsheet_id=spreadsheet_id, sheet_name=sheet_name)
    candidates = _segment_rows_for_action(progress.get("segments") or [], mode=mode)
    state_map = _campaign_state_map(mapping.values())

    actions: List[Dict[str, Any]] = []
    acted_ids: set[str] = set()
    performed = 0
    skipped_unmapped = 0
    errors: List[str] = list(progress.get("errors") or [])

    for segment in candidates:
        key = (segment["geo"], segment["device"])
        campaign_id = mapping.get(key)
        campaign_state = state_map.get(campaign_id or "", "")
        action = _build_action_record(segment, campaign_id, campaign_state)
        if not campaign_id:
            action["status"] = "unmapped"
            skipped_unmapped += 1
            actions.append(action)
            continue
        if campaign_id in acted_ids:
            action["status"] = "duplicate_campaign"
            actions.append(action)
            continue

        already_done = False
        if mode == "pause_over_cap" and _state_is_paused(campaign_state):
            action["status"] = "already_paused"
            already_done = True
        elif mode == "resume_under_cap" and _state_is_active(campaign_state):
            action["status"] = "already_active"
            already_done = True
        if already_done:
            acted_ids.add(campaign_id)
            actions.append(action)
            continue

        if dry_run:
            action["status"] = "would_pause" if mode == "pause_over_cap" else "would_resume"
            acted_ids.add(campaign_id)
            performed += 1
            actions.append(action)
            continue

        try:
            if mode == "pause_over_cap":
                pause_campaign(campaign_id, KEYZP)
                action["status"] = "paused"
            else:
                resume_campaign(campaign_id, KEYZP)
                action["status"] = "resumed"
            acted_ids.add(campaign_id)
            performed += 1
        except ZeroparkClientError as e:
            msg = f"{segment['geo']}/{segment['device']} {campaign_id}: {e}"
            if e.response_body:
                msg = f"{msg} | {e.response_body}"
            errors.append(msg)
            action["status"] = "error"
            action["error"] = str(e)
        actions.append(action)

    payload = {
        "mode": mode,
        "reason": reason,
        "dry_run": dry_run,
        "mapping_sheet": {
            "spreadsheet_id": spreadsheet_id,
            "sheet_name": sheet_name,
            "mappings": len(mapping),
        },
        "progress_status": progress.get("status"),
        "progress_updated_utc": progress.get("updated_utc"),
        "segments_seen": len(progress.get("segments") or []),
        "segments_matched": len(candidates),
        "segments_reached_cap": len(candidates) if mode == "pause_over_cap" else 0,
        "segments_under_cap": len(candidates) if mode == "resume_under_cap" else 0,
        "performed": performed,
        "paused": performed if mode == "pause_over_cap" else 0,
        "resumed": performed if mode == "resume_under_cap" else 0,
        "skipped_unmapped": skipped_unmapped,
        "actions": actions,
        "errors": errors,
    }
    logger.info(
        "Blend ZP cap guard (%s/%s): matched=%s performed=%s unmapped=%s errors=%s dry_run=%s",
        mode,
        reason,
        payload["segments_matched"],
        performed,
        skipped_unmapped,
        len(errors),
        dry_run,
    )
    return payload


def pause_all_mapped_blend_zp_campaigns(
    *,
    dry_run: bool = False,
    reason: str = "nightly_close",
    spreadsheet_id: str = ZEROPARK_BLEND_CAP_SPREADSHEET_ID,
    sheet_name: str = ZEROPARK_BLEND_CAP_SHEET_NAME,
) -> Dict[str, Any]:
    """Pause every Zeropark campaign listed on the Blend mapping sheet (nightly close)."""
    if not KEYZP:
        raise RuntimeError("KEYZP is not configured")

    mapping = load_mapping(spreadsheet_id=spreadsheet_id, sheet_name=sheet_name)
    campaign_ids = sorted({cid for cid in mapping.values() if str(cid).strip()})
    state_map = _campaign_state_map(campaign_ids)

    paused = 0
    already_paused = 0
    errors: List[str] = []
    actions: List[Dict[str, Any]] = []

    for campaign_id in campaign_ids:
        state = state_map.get(campaign_id, "")
        action: Dict[str, Any] = {"campaign_id": campaign_id, "state": state}
        if _state_is_paused(state):
            action["status"] = "already_paused"
            already_paused += 1
            actions.append(action)
            continue
        if dry_run:
            action["status"] = "would_pause"
            paused += 1
            actions.append(action)
            continue
        try:
            pause_campaign(campaign_id, KEYZP)
            action["status"] = "paused"
            paused += 1
        except ZeroparkClientError as e:
            msg = f"{campaign_id}: {e}"
            if e.response_body:
                msg = f"{msg} | {e.response_body}"
            errors.append(msg)
            action["status"] = "error"
            action["error"] = str(e)
        actions.append(action)

    payload = {
        "reason": reason,
        "dry_run": dry_run,
        "mapping_sheet": {
            "spreadsheet_id": spreadsheet_id,
            "sheet_name": sheet_name,
            "mappings": len(mapping),
            "campaign_ids": len(campaign_ids),
        },
        "paused": paused,
        "already_paused": already_paused,
        "errors": errors,
        "actions": actions,
    }
    logger.info(
        "Blend ZP nightly close (%s): campaigns=%s paused=%s already_paused=%s errors=%s",
        reason,
        len(campaign_ids),
        paused,
        already_paused,
        len(errors),
    )
    return payload
