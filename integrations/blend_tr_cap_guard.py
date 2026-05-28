"""
Pause mapped Trillion Blend RON campaigns once Blend geo/device click caps are filled.

Mapping is derived from Trillion ``list_campaigns`` under a folder (default: ``Blend``)
by parsing campaign names that embed geo/device tokens (for example:
``.de.01.mobile.nonadult`` -> geo=de, device=mobile).
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple

from config import KEYTR, TRILLION_BLEND_CAP_FOLDER
from integrations.blend_cap_progress import refresh_blend_cap_progress
from integrations.trillion import TrillionClientError, list_campaigns, update_ron_active

logger = logging.getLogger(__name__)


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
            raise ValueError(f"Unsupported Blend Trillion cap guard mode: {mode}")
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


def _detect_campaign_geo_device(campaign_name: str) -> Tuple[str, str]:
    # Expected naming similar to: .de.01.mobile.nonadult
    parts = [p.strip().lower() for p in str(campaign_name or "").split(".") if p.strip()]
    geo = ""
    device = ""
    for p in parts:
        if not geo and len(p) == 2 and p.isalpha():
            geo = _normalize_geo(p)
        if p in ("desktop", "mobile", "phone", "tablet", "smartphone"):
            device = _normalize_device(p)
    return geo, device


def _load_mapping_from_trillion(folder: str) -> Dict[Tuple[str, str], Dict[str, str]]:
    rows = list_campaigns(KEYTR, folder=folder)
    mapping: Dict[Tuple[str, str], Dict[str, str]] = {}
    for row in rows:
        name = str(row.get("Campaign") or "").strip()
        if not name:
            continue
        geo, device = _detect_campaign_geo_device(name)
        if not (geo and device):
            continue
        key = (geo, device)
        if key in mapping and mapping[key]["campaign"] != name:
            logger.warning(
                "Duplicate Trillion Blend mapping for %s/%s; keeping %s and skipping %s",
                geo,
                device,
                mapping[key]["campaign"],
                name,
            )
            continue
        mapping[key] = {
            "campaign": name,
            "status": str(row.get("Status") or "").strip(),
            "approval_status": str(row.get("Approval_status") or "").strip(),
        }
    return mapping


def _status_is_paused(status: str) -> bool:
    return (status or "").strip().lower() in ("stopped", "paused", "inactive")


def _status_is_active(status: str) -> bool:
    return (status or "").strip().lower() == "active"


def run_blend_tr_cap_guard(
    *,
    dry_run: bool = False,
    reason: str = "manual",
    mode: str = "pause_over_cap",
    folder: str = TRILLION_BLEND_CAP_FOLDER,
) -> Dict[str, Any]:
    if not KEYTR:
        raise RuntimeError("KEYTR is not configured")

    progress = refresh_blend_cap_progress(reason=f"tr_cap_guard:{reason}")
    candidates = _segment_rows_for_action(progress.get("segments") or [], mode=mode)
    mapping = _load_mapping_from_trillion(folder)

    actions: List[Dict[str, Any]] = []
    acted_campaigns: set[str] = set()
    performed = 0
    skipped_unmapped = 0
    errors: List[str] = list(progress.get("errors") or [])

    for segment in candidates:
        key = (segment["geo"], segment["device"])
        mapped = mapping.get(key) or {}
        campaign = mapped.get("campaign")
        status = mapped.get("status", "")
        approval_status = mapped.get("approval_status", "")
        action: Dict[str, Any] = {
            "geo": segment["geo"],
            "device": segment["device"],
            "target_clicks": segment["target_clicks"],
            "clicks": segment["clicks"],
            "campaign": campaign,
            "status_before": status,
            "approval_status": approval_status,
            "status": "pending",
        }
        if not campaign:
            action["status"] = "unmapped"
            skipped_unmapped += 1
            actions.append(action)
            continue
        if campaign in acted_campaigns:
            action["status"] = "duplicate_campaign"
            actions.append(action)
            continue

        already_done = False
        if mode == "pause_over_cap" and _status_is_paused(status):
            action["status"] = "already_paused"
            already_done = True
        elif mode == "resume_under_cap" and _status_is_active(status):
            action["status"] = "already_active"
            already_done = True
        if already_done:
            acted_campaigns.add(campaign)
            actions.append(action)
            continue

        if dry_run:
            action["status"] = "would_pause" if mode == "pause_over_cap" else "would_resume"
            acted_campaigns.add(campaign)
            performed += 1
            actions.append(action)
            continue

        try:
            update_ron_active(KEYTR, ron=campaign, active=(mode == "resume_under_cap"))
            action["status"] = "paused" if mode == "pause_over_cap" else "resumed"
            acted_campaigns.add(campaign)
            performed += 1
        except TrillionClientError as e:
            msg = f"{segment['geo']}/{segment['device']} {campaign}: {e}"
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
        "folder": folder,
        "mapping_campaigns": len(mapping),
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
        "Blend TR cap guard (%s/%s): matched=%s performed=%s unmapped=%s errors=%s dry_run=%s",
        mode,
        reason,
        payload["segments_matched"],
        performed,
        skipped_unmapped,
        len(errors),
        dry_run,
    )
    return payload
