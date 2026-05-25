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
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from config import (
    KEYZP,
    ZEROPARK_BLEND_CAP_SHEET_NAME,
    ZEROPARK_BLEND_CAP_SPREADSHEET_ID,
)
from integrations.blend_cap_progress import refresh_blend_cap_progress
from integrations.zeropark import ZeroparkClientError, pause_campaign

logger = logging.getLogger(__name__)


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


def _read_mapping_rows(
    spreadsheet_id: str = ZEROPARK_BLEND_CAP_SPREADSHEET_ID,
    sheet_name: str = ZEROPARK_BLEND_CAP_SHEET_NAME,
) -> List[BlendZpCampaignMapping]:
    service = _get_sheets_service()
    quoted = sheet_name.replace("'", "''")
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
                sheet_name,
                raw_row,
            )
            continue
        rows.append(BlendZpCampaignMapping(geo=geo, device=device, campaign_id=campaign_id))
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


def _campaign_targets_reached(segments: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    reached: List[Dict[str, Any]] = []
    for segment in segments:
        try:
            target = int(segment.get("target_clicks") or 0)
            clicks = int(segment.get("clicks") or 0)
        except (TypeError, ValueError):
            continue
        if target <= 0 or clicks < target:
            continue
        geo = _normalize_geo(str(segment.get("geo") or ""))
        device = _normalize_device(str(segment.get("device") or ""))
        if not (geo and device):
            continue
        reached.append(
            {
                **segment,
                "geo": geo,
                "device": device,
                "target_clicks": target,
                "clicks": clicks,
            }
        )
    return reached


def run_blend_zp_cap_guard(
    *,
    dry_run: bool = False,
    reason: str = "manual",
    spreadsheet_id: str = ZEROPARK_BLEND_CAP_SPREADSHEET_ID,
    sheet_name: str = ZEROPARK_BLEND_CAP_SHEET_NAME,
) -> Dict[str, Any]:
    if not KEYZP:
        raise RuntimeError("KEYZP is not configured")

    progress = refresh_blend_cap_progress(reason=f"zp_cap_guard:{reason}")
    mapping = load_mapping(spreadsheet_id=spreadsheet_id, sheet_name=sheet_name)
    reached = _campaign_targets_reached(progress.get("segments") or [])

    actions: List[Dict[str, Any]] = []
    paused_ids: set[str] = set()
    paused = 0
    skipped_unmapped = 0
    errors: List[str] = list(progress.get("errors") or [])

    for segment in reached:
        key = (segment["geo"], segment["device"])
        campaign_id = mapping.get(key)
        action = {
            "geo": segment["geo"],
            "device": segment["device"],
            "target_clicks": segment["target_clicks"],
            "clicks": segment["clicks"],
            "campaign_id": campaign_id,
            "status": "pending",
        }
        if not campaign_id:
            action["status"] = "unmapped"
            skipped_unmapped += 1
            actions.append(action)
            continue
        if campaign_id in paused_ids:
            action["status"] = "duplicate_campaign"
            actions.append(action)
            continue
        if dry_run:
            action["status"] = "would_pause"
            paused_ids.add(campaign_id)
            paused += 1
            actions.append(action)
            continue
        try:
            pause_campaign(campaign_id, KEYZP)
            action["status"] = "paused"
            paused_ids.add(campaign_id)
            paused += 1
        except ZeroparkClientError as e:
            msg = f"{segment['geo']}/{segment['device']} {campaign_id}: {e}"
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
        },
        "progress_status": progress.get("status"),
        "progress_updated_utc": progress.get("updated_utc"),
        "segments_seen": len(progress.get("segments") or []),
        "segments_reached_cap": len(reached),
        "paused": paused,
        "skipped_unmapped": skipped_unmapped,
        "actions": actions,
        "errors": errors,
    }
    logger.info(
        "Blend ZP cap guard (%s): reached=%s paused=%s unmapped=%s errors=%s dry_run=%s",
        reason,
        payload["segments_reached_cap"],
        paused,
        skipped_unmapped,
        len(errors),
        dry_run,
    )
    return payload
