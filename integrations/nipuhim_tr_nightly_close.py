"""
Nightly pause for Trillion RON campaigns that send traffic to Keitaro hub campaign 94.

After the nipuhim hub migration, Trillion ``Target_URL`` values point at the hub alias
(``trck.shopli.city/{alias}``, default alias from campaign 94 ``Domain`` / ``yPBBvXxR``).
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Set

from config import (
    KEYTR,
    KEITARO_HUB_CAMPAIGN_ID,
    TRILLION_HUB_CLOSE_ALIAS,
    TRILLION_HUB_CLOSE_FOLDERS,
)
from integrations.trillion import TrillionClientError, list_campaigns, update_ron_active

logger = logging.getLogger(__name__)


def _status_is_paused(status: str) -> bool:
    return (status or "").strip().lower() in ("stopped", "paused", "inactive")


def _status_is_active(status: str) -> bool:
    return (status or "").strip().lower() == "active"


def resolve_hub_close_alias(*, hub_campaign_id: Optional[int] = None) -> str:
    """Hub Keitaro alias used in Trillion target URLs (env override, else live campaign 94)."""
    alias = (TRILLION_HUB_CLOSE_ALIAS or "").strip()
    if alias:
        return alias
    cid = int(hub_campaign_id if hub_campaign_id is not None else KEITARO_HUB_CAMPAIGN_ID)
    try:
        from integrations.keitaro import KeitaroClient

        camp = KeitaroClient().get_campaign(cid)
        live = str((camp or {}).get("alias") or "").strip()
        if live:
            return live
    except Exception as e:
        logger.warning("Could not resolve hub alias from Keitaro campaign %s: %s", cid, e)
    return "yPBBvXxR"


def _folder_allowlist() -> Optional[Set[str]]:
    parts = [x.strip() for x in (TRILLION_HUB_CLOSE_FOLDERS or "").split(",") if x.strip()]
    return set(parts) if parts else None


def _target_url_matches_hub(url: str, alias: str) -> bool:
    if not url or not alias:
        return False
    needle = f"trck.shopli.city/{alias.strip().lower()}"
    return needle in url.lower()


def _index_hub_trillion_campaigns(
    rows: List[Dict[str, Any]],
    *,
    alias: str,
    folders: Optional[Set[str]] = None,
) -> List[Dict[str, Any]]:
    """Return unique RON campaigns whose target URL routes to the hub alias."""
    by_name: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        name = str(row.get("Campaign") or "").strip()
        if not name:
            continue
        folder = str(row.get("Folder") or "").strip()
        if folders is not None and folder not in folders:
            continue
        url = str(row.get("Target_URL") or "").strip()
        status = str(row.get("Status") or "").strip()
        if not _target_url_matches_hub(url, alias):
            continue
        entry = by_name.get(name)
        if not entry:
            entry = {
                "campaign": name,
                "folder": folder,
                "statuses": set(),
                "sample_url": url[:160],
            }
            by_name[name] = entry
        if status:
            entry["statuses"].add(status)
        if folder and not entry.get("folder"):
            entry["folder"] = folder

    out: List[Dict[str, Any]] = []
    for entry in by_name.values():
        statuses = entry.pop("statuses")
        entry["status"] = "Active" if any(_status_is_active(s) for s in statuses) else next(iter(statuses), "")
        entry["is_active"] = any(_status_is_active(s) for s in statuses)
        entry["is_paused"] = bool(statuses) and all(_status_is_paused(s) for s in statuses)
        out.append(entry)
    out.sort(key=lambda x: (str(x.get("folder") or ""), str(x.get("campaign") or "")))
    return out


def pause_trillion_hub_campaigns(
    *,
    dry_run: bool = False,
    reason: str = "nightly_close",
    hub_alias: Optional[str] = None,
    folders: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    """
    Pause every active Trillion campaign whose ``Target_URL`` hits the hub alias.

    Skips campaigns already paused/stopped. Does not resume anything.
    """
    if not KEYTR:
        raise RuntimeError("KEYTR is not configured")

    alias = (hub_alias or resolve_hub_close_alias()).strip()
    if not alias:
        raise RuntimeError("Hub alias is empty")

    allow_folders = folders if folders is not None else _folder_allowlist()
    rows = list_campaigns(KEYTR)
    matched = _index_hub_trillion_campaigns(rows, alias=alias, folders=allow_folders)

    paused = 0
    already_paused = 0
    errors: List[str] = []
    actions: List[Dict[str, Any]] = []

    for item in matched:
        campaign = str(item.get("campaign") or "").strip()
        action: Dict[str, Any] = {
            "campaign": campaign,
            "folder": item.get("folder"),
            "status_before": item.get("status"),
        }
        if item.get("is_paused") or not item.get("is_active"):
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
            update_ron_active(KEYTR, ron=campaign, active=False)
            action["status"] = "paused"
            paused += 1
        except TrillionClientError as e:
            msg = f"{campaign}: {e}"
            if e.response_body:
                msg = f"{msg} | {e.response_body}"
            errors.append(msg)
            action["status"] = "error"
            action["error"] = str(e)
        actions.append(action)

    payload = {
        "reason": reason,
        "dry_run": dry_run,
        "hub_alias": alias,
        "hub_campaign_id": KEITARO_HUB_CAMPAIGN_ID,
        "folders": sorted(allow_folders) if allow_folders else [],
        "trillion_rows_scanned": len(rows),
        "matched_campaigns": len(matched),
        "paused": paused,
        "already_paused": already_paused,
        "errors": errors,
        "actions": actions,
    }
    logger.info(
        "Trillion hub nightly close (%s): alias=%s matched=%s paused=%s already_paused=%s errors=%s dry_run=%s",
        reason,
        alias,
        len(matched),
        paused,
        already_paused,
        len(errors),
        dry_run,
    )
    if not dry_run:
        try:
            from integrations.domain_demand import archive_and_reset_domain_demand_for_new_day

            rollover = archive_and_reset_domain_demand_for_new_day(
                dry_run=False,
                reason=reason,
            )
            payload["domain_demand_rollover"] = {
                "status": rollover.get("status"),
                "archive_date": rollover.get("archive_date"),
                "new_day": rollover.get("new_day"),
                "archived_tabs": rollover.get("archived_tabs"),
            }
            for line in rollover.get("logs") or []:
                logger.info("Domain-demand rollover: %s", line)
        except Exception as e:
            logger.warning("Domain-demand nightly rollover failed: %s", e)
            payload["domain_demand_rollover_error"] = str(e)
    return payload
