"""
Clean NIPUHIM-feed* child campaigns: remove legacy cloned geo flows, move fallback last.

Cloned from HrQBXp (campaign 1), NIPUHIM children inherit ~30 country-name flows
(``Austria``, ``spain``, …) plus a ``fallback`` flow — all ranked above the
``{geo}_desktop`` / ``{geo}_mobile`` flows that nipuhim v2 sync actually uses.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from assistance import get_campaign_streams, parse_blend_stream_geo_channel
from integrations.keitaro import KeitaroClient, KeitaroClientError

logger = logging.getLogger(__name__)

_CHANNEL_ORDER = {"desktop": 0, "mobile": 1}


def classify_nipuhim_streams(
    streams: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Return (device, legacy_geo, fallback, other) stream lists."""
    device: List[Dict[str, Any]] = []
    legacy: List[Dict[str, Any]] = []
    fallback: List[Dict[str, Any]] = []
    other: List[Dict[str, Any]] = []

    for stream in streams:
        name = (stream.get("name") or "").strip()
        nlower = name.lower()
        stype = (stream.get("type") or "").lower()
        geo, channel = parse_blend_stream_geo_channel(name)

        if nlower == "fallback" or stype == "default":
            fallback.append(stream)
        elif channel in ("desktop", "mobile"):
            device.append(stream)
        elif channel == "legacy":
            legacy.append(stream)
        else:
            other.append(stream)

    return device, legacy, fallback, other


def _device_sort_key(stream: Dict[str, Any]) -> Tuple[str, int, int]:
    geo, channel = parse_blend_stream_geo_channel(stream.get("name") or "")
    return (
        geo or "",
        _CHANNEL_ORDER.get(channel or "", 9),
        int(stream.get("id") or 0),
    )


def _assign_stream_positions(
    client: KeitaroClient,
    streams: List[Dict[str, Any]],
    *,
    start: int,
    dry_run: bool,
    log,
) -> int:
    """Set sequential positions; returns count of updates."""
    updated = 0
    for idx, stream in enumerate(streams, start=start):
        sid = int(stream["id"])
        name = stream.get("name") or sid
        old_pos = stream.get("position")
        if int(old_pos or 0) == idx:
            continue
        if dry_run:
            log(f"would set {name!r} (id={sid}) position {old_pos} -> {idx}")
        else:
            client.update_stream(sid, {"position": idx})
            log(f"set {name!r} (id={sid}) position {old_pos} -> {idx}")
        updated += 1
    return updated


def _ensure_fallback_stream(
    client: KeitaroClient,
    campaign_id: int,
    *,
    position: int,
    dry_run: bool,
    log,
) -> Optional[Dict[str, Any]]:
    """Create empty catch-all fallback flow if missing."""
    if dry_run:
        log(f"would create fallback stream at position {position}")
        return None
    payload = {
        "campaign_id": int(campaign_id),
        "type": "regular",
        "name": "fallback",
        "schema": "landings",
        "action_type": "http",
        "state": "active",
        "weight": 100,
        "filter_or": False,
        "collect_clicks": True,
        "offer_selection": "before_click",
        "filters": [],
        "offers": [],
        "position": int(position),
    }
    created = client.create_stream(payload)
    log(f"created fallback stream id={created.get('id')} at position {position}")
    return created


def cleanup_nipuhim_campaign_streams(
    campaign_id: int,
    *,
    dry_run: bool = True,
    client: Optional[KeitaroClient] = None,
    label: str = "",
) -> Tuple[List[str], Dict[str, int]]:
    """
    Delete legacy country-name flows from a NIPUHIM-feed* campaign and reorder:
    device flows first (geo × desktop/mobile), fallback last.
    """
    client = client or KeitaroClient()
    cid = int(campaign_id)
    prefix = f"{label} " if label else ""
    logs: List[str] = []

    def log(msg: str) -> None:
        logs.append(f"{prefix}campaign {cid}: {msg}")

    streams = get_campaign_streams(cid, base_url=client.base_url, api_key=client.api_key)
    device, legacy, fallback, other = classify_nipuhim_streams(streams)

    stats = {
        "legacy_deleted": 0,
        "positions_updated": 0,
        "fallback_created": 0,
        "other_left": len(other),
    }

    if other:
        names = ", ".join((s.get("name") or "?") for s in other[:5])
        log(f"leaving {len(other)} unrecognized stream(s): {names}")

    device_sorted = sorted(device, key=_device_sort_key)
    fallback_sorted = sorted(fallback, key=lambda s: int(s.get("id") or 0))

    fb_before_device = False
    fb_wrong_position = False
    if fallback_sorted and device_sorted:
        fb_pos = int(fallback_sorted[0].get("position") or 0)
        min_dev = min(int(s.get("position") or 9999) for s in device_sorted)
        expected_fb_pos = len(device_sorted) + 1
        fb_before_device = fb_pos < min_dev
        fb_wrong_position = fb_pos != expected_fb_pos

    needs_work = bool(legacy) or not fallback_sorted or fb_before_device or fb_wrong_position
    if not needs_work:
        log(f"stream order already correct ({len(device_sorted)} device + {len(fallback_sorted)} fallback)")
        return logs, stats

    if not device_sorted:
        log("no device streams — skip reorder")
        return logs, stats

    all_streams = sorted(streams, key=lambda s: int(s.get("position") or 0))
    if dry_run:
        for stream in legacy:
            sid = int(stream["id"])
            name = stream.get("name") or sid
            log(f"would delete legacy flow {name!r} (id={sid})")
            stats["legacy_deleted"] += 1
        if not fallback_sorted:
            log("would create fallback stream at end")
            stats["fallback_created"] = 1
        ordered = device_sorted + fallback_sorted
        stats["positions_updated"] += _assign_stream_positions(
            client, ordered, start=1, dry_run=True, log=log
        )
        return logs, stats

    stats["positions_updated"] += _assign_stream_positions(
        client,
        all_streams,
        start=10000,
        dry_run=False,
        log=lambda m: log(f"temp {m}"),
    )

    for stream in legacy:
        sid = int(stream["id"])
        name = stream.get("name") or sid
        client.delete_stream(sid)
        log(f"deleted legacy flow {name!r} (id={sid})")
        stats["legacy_deleted"] += 1

    streams = get_campaign_streams(cid, base_url=client.base_url, api_key=client.api_key)
    device, legacy, fallback, other = classify_nipuhim_streams(streams)
    device_sorted = sorted(device, key=_device_sort_key)
    fallback_sorted = sorted(fallback, key=lambda s: int(s.get("id") or 0))

    if not fallback_sorted:
        _ensure_fallback_stream(client, cid, position=2000, dry_run=False, log=log)
        stats["fallback_created"] = 1
        streams = get_campaign_streams(cid, base_url=client.base_url, api_key=client.api_key)
        device, legacy, fallback, other = classify_nipuhim_streams(streams)
        device_sorted = sorted(device, key=_device_sort_key)
        fallback_sorted = sorted(fallback, key=lambda s: int(s.get("id") or 0))

    ordered = device_sorted + fallback_sorted
    stats["positions_updated"] += _assign_stream_positions(
        client,
        ordered,
        start=1,
        dry_run=False,
        log=log,
    )
    log(f"final order — {len(device_sorted)} device + {len(fallback_sorted)} fallback")

    return logs, stats


def nipuhim_campaign_needs_stream_cleanup(campaign_id: int) -> bool:
    """True when legacy geo flows exist, fallback is misplaced, or fallback is missing."""
    streams = get_campaign_streams(int(campaign_id))
    device, legacy, fallback, _ = classify_nipuhim_streams(streams)
    if legacy:
        return True
    if device and not fallback:
        return True
    if not fallback or not device:
        return False
    fb_pos = int(fallback[0].get("position") or 0)
    expected_fb_pos = len(device) + 1
    return fb_pos != expected_fb_pos


def cleanup_nipuhim_feed_campaigns(
    *,
    accounts: Tuple[int, ...] = (1, 2, 5),
    dry_run: bool = True,
) -> Dict[str, Any]:
    """Run stream cleanup on NIPUHIM-feed1/2/5 child campaigns."""
    from integrations.keitaro_child_campaigns import nipuhim_child_campaign_id_for_account

    all_logs: List[str] = []
    per_campaign: Dict[str, Any] = {}
    totals = {"legacy_deleted": 0, "positions_updated": 0}

    for account in accounts:
        cid, feed_key, _ = nipuhim_child_campaign_id_for_account(account)
        label = f"NIPUHIM-{feed_key}"
        try:
            logs, stats = cleanup_nipuhim_campaign_streams(
                cid, dry_run=dry_run, label=label
            )
        except KeitaroClientError as e:
            all_logs.append(f"{label}: ERROR {e}")
            per_campaign[feed_key] = {"error": str(e)}
            continue
        all_logs.extend(logs)
        per_campaign[feed_key] = {"campaign_id": cid, **stats}
        totals["legacy_deleted"] += stats["legacy_deleted"]
        totals["positions_updated"] += stats["positions_updated"]

    return {
        "dry_run": dry_run,
        "logs": all_logs,
        "campaigns": per_campaign,
        "totals": totals,
    }
