"""
Domain-demand guard: Trillion activate/pause + Keitaro flow weight equalization.

Uses ``summary_by_geo`` from the domain-demand bill (hub campaign 94 segments).
During the day: zero-weight filled offers, renormalize remaining demand, pause Trillion
when a geo×device segment is fully delivered. Rebalance always keeps **at least one**
offer with positive share on each flow so leftover clicks do not fall through to fallback
until Trillion is paused.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from config import (
    DOMAIN_DEMAND_TRILLION_PAUSE_FILL_PCT,
    KEYTR,
    KEITARO_HUB_CAMPAIGN_ID,
)
from integrations.domain_demand import build_domain_demand_payload, sync_domain_demand
from integrations.keitaro import KeitaroClient, KeitaroClientError
from integrations.keitaro_hub import (
    _hub_device_streams,
    _stream_hub_offer_weights,
    hub_active_feed_keys,
    load_hub_state,
    resolve_hub_weight_context,
    set_flow_offers_weighted_keep_zeros,
)
from integrations.nipuhim_tr_nightly_close import (
    _status_is_active,
    _status_is_paused,
    resolve_hub_close_alias,
)
from integrations.trillion import TrillionClientError, update_ron_active

logger = logging.getLogger(__name__)


def _slug(s: str, max_len: int = 48) -> str:
    from blend_sync_from_sheet import _slug as blend_slug

    return blend_slug(s, max_len=max_len)


def _pause_fill_pct() -> float:
    return float(DOMAIN_DEMAND_TRILLION_PAUSE_FILL_PCT or 98.0)


def _ensure_at_least_one_live_weight(
    offer_weights: Dict[int, float],
    zero_ids: List[int],
    *,
    preferred_offer_ids: Optional[List[int]] = None,
) -> Tuple[Dict[int, float], List[int], Optional[int]]:
    """
    Guarantee ≥1 offer keeps a positive weight so leftover clicks stay monetized.

    When every offer would be share=0 (caps filled), promote the best leftover candidate
    to weight 1.0 until Trillion for that segment is paused.
    """
    if offer_weights:
        return offer_weights, zero_ids, None

    ordered: List[int] = []
    seen: set[int] = set()
    for oid in list(preferred_offer_ids or []) + list(zero_ids):
        try:
            i = int(oid)
        except (TypeError, ValueError):
            continue
        if i in seen:
            continue
        seen.add(i)
        ordered.append(i)
    if not ordered:
        return offer_weights, zero_ids, None

    keep = ordered[0]
    new_zeros = [z for z in zero_ids if int(z) != keep]
    return {keep: 1.0}, new_zeros, keep


def _preferred_hub_leftover_ids(
    bill_rows: List[Dict[str, Any]],
    geo: str,
    channel: str,
    *,
    quality_brand_slugs: frozenset[str],
) -> List[int]:
    """Prefer leftover catch-all offers that had the most original demand for this segment."""
    from integrations.hub_blend_child_flows import hub_quality_offer_name

    state = load_hub_state()
    all_meta = {**(state.get("hub_offers") or {}), **(state.get("hub_quality_offers") or {})}
    demand_by_offer: Dict[str, float] = defaultdict(float)

    for row in bill_rows:
        if str(row.get("geo") or "").lower() != geo:
            continue
        if str(row.get("device") or "").lower() != channel:
            continue
        demand = max(0, int(row.get("demand_clicks") or 0))
        if demand <= 0:
            continue
        family = str(row.get("family") or "")
        feed = str(row.get("feed") or "")
        brand_slug = _slug(str(row.get("brand") or ""))
        if family == "nipuhim":
            offer_name = f"hub_nipuhim_{feed}"
        elif brand_slug in quality_brand_slugs:
            offer_name = hub_quality_offer_name(brand_slug)
        else:
            offer_name = f"hub_blend_{feed}"
        demand_by_offer[offer_name] += float(demand)

    ranked = sorted(demand_by_offer.items(), key=lambda kv: kv[1], reverse=True)
    out: List[int] = []
    for name, _ in ranked:
        meta = all_meta.get(name) or {}
        oid = meta.get("id")
        if oid is not None:
            out.append(int(oid))
    # Stable fallback: active nipuhim then blend feed offers.
    for prefix in ("hub_nipuhim_", "hub_blend_"):
        for name, meta in sorted(all_meta.items()):
            if not name.startswith(prefix):
                continue
            oid = meta.get("id")
            if oid is None:
                continue
            i = int(oid)
            if i not in out:
                out.append(i)
    return out


def _trillion_segment_map(folder: Optional[str] = None) -> Dict[Tuple[str, str], Dict[str, Any]]:
    from integrations.domain_demand import build_trillion_segment_map

    mapping, _logs = build_trillion_segment_map()
    if folder:
        # build_trillion_segment_map uses hub close folder config; mapping already hub-scoped.
        return mapping
    return mapping


def _segment_needs_traffic(seg: Dict[str, Any]) -> bool:
    demand = int(seg.get("demand_clicks") or 0)
    remaining = int(seg.get("remaining") or 0)
    return demand > 0 and remaining > 0


def _segment_should_pause(seg: Dict[str, Any]) -> bool:
    demand = int(seg.get("demand_clicks") or 0)
    if demand <= 0:
        return False
    hint = str(seg.get("trillion_hint") or "").upper()
    if hint == "PAUSE_SUGGESTED":
        return True
    fill = seg.get("fill_pct")
    if fill is not None and float(fill) >= _pause_fill_pct():
        return True
    remaining = int(seg.get("remaining") or 0)
    return remaining <= 0


def run_trillion_activate_for_demand(
    *,
    dry_run: bool = False,
    reason: str = "daily_activate",
    segments: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Resume Trillion hub campaigns for geo×device segments that still need clicks today."""
    if not KEYTR:
        raise RuntimeError("KEYTR is not configured")

    segs = segments
    if segs is None:
        payload = build_domain_demand_payload(rebuild_demand=True, reason=reason)
        segs = payload.get("summary_by_geo") or []

    tr_map = _trillion_segment_map()
    actions: List[Dict[str, Any]] = []
    resumed = 0
    errors: List[str] = []

    for seg in segs:
        if not _segment_needs_traffic(seg):
            continue
        geo = str(seg.get("geo") or "").lower()
        device = str(seg.get("device") or "").lower()
        campaign = str(seg.get("trillion_campaign") or tr_map.get((geo, device), {}).get("campaign") or "")
        action: Dict[str, Any] = {
            "geo": geo,
            "device": device,
            "campaign": campaign,
            "remaining": seg.get("remaining"),
            "demand_clicks": seg.get("demand_clicks"),
        }
        if not campaign:
            action["status"] = "unmapped"
            actions.append(action)
            continue
        status = str(tr_map.get((geo, device), {}).get("status") or seg.get("trillion_status") or "")
        if _status_is_active(status):
            action["status"] = "already_active"
            actions.append(action)
            continue
        if dry_run:
            action["status"] = "would_resume"
            resumed += 1
            actions.append(action)
            continue
        try:
            update_ron_active(KEYTR, ron=campaign, active=True)
            action["status"] = "resumed"
            resumed += 1
        except TrillionClientError as e:
            action["status"] = "error"
            action["error"] = str(e)
            errors.append(f"{geo}/{device} {campaign}: {e}")
        actions.append(action)

    return {
        "reason": reason,
        "dry_run": dry_run,
        "hub_alias": resolve_hub_close_alias(),
        "segments_seen": len(segs),
        "resumed": resumed,
        "actions": actions,
        "errors": errors,
    }


def run_trillion_pause_filled_segments(
    *,
    dry_run: bool = False,
    reason: str = "intraday_pause",
    segments: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Pause Trillion campaigns when geo×device demand is met (or over fill threshold)."""
    if not KEYTR:
        raise RuntimeError("KEYTR is not configured")

    segs = segments
    if segs is None:
        payload = build_domain_demand_payload(rebuild_demand=False, reason=reason)
        segs = payload.get("summary_by_geo") or []

    tr_map = _trillion_segment_map()
    actions: List[Dict[str, Any]] = []
    paused = 0
    errors: List[str] = []

    for seg in segs:
        if not _segment_should_pause(seg):
            continue
        geo = str(seg.get("geo") or "").lower()
        device = str(seg.get("device") or "").lower()
        campaign = str(seg.get("trillion_campaign") or tr_map.get((geo, device), {}).get("campaign") or "")
        action: Dict[str, Any] = {
            "geo": geo,
            "device": device,
            "campaign": campaign,
            "demand_clicks": seg.get("demand_clicks"),
            "delivered_clicks": seg.get("delivered_clicks"),
            "fill_pct": seg.get("fill_pct"),
        }
        if not campaign:
            action["status"] = "unmapped"
            actions.append(action)
            continue
        status = str(tr_map.get((geo, device), {}).get("status") or seg.get("trillion_status") or "")
        if _status_is_paused(status):
            action["status"] = "already_paused"
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
            action["status"] = "error"
            action["error"] = str(e)
            errors.append(f"{geo}/{device} {campaign}: {e}")
        actions.append(action)

    return {
        "reason": reason,
        "dry_run": dry_run,
        "segments_seen": len(segs),
        "paused": paused,
        "actions": actions,
        "errors": errors,
    }


def _hub_offer_weights_for_segment(
    bill_rows: List[Dict[str, Any]],
    geo: str,
    channel: str,
    *,
    quality_brand_slugs: frozenset[str],
) -> Tuple[Dict[str, float], List[int]]:
    """Map hub offer name -> remaining demand weight; return zero-offer ids to keep attached."""
    from integrations.hub_blend_child_flows import hub_quality_offer_name

    state = load_hub_state()
    hub_offers = state.get("hub_offers") or {}
    hub_quality = state.get("hub_quality_offers") or {}

    raw: Dict[str, float] = defaultdict(float)
    for row in bill_rows:
        if str(row.get("geo") or "").lower() != geo:
            continue
        if str(row.get("device") or "").lower() != channel:
            continue
        remaining = max(0, int(row.get("remaining") or 0))
        family = str(row.get("family") or "")
        feed = str(row.get("feed") or "")
        brand_slug = _slug(str(row.get("brand") or ""))
        if family == "nipuhim":
            offer_name = f"hub_nipuhim_{feed}"
        elif brand_slug in quality_brand_slugs:
            offer_name = hub_quality_offer_name(brand_slug)
        else:
            offer_name = f"hub_blend_{feed}"
        if remaining > 0:
            raw[offer_name] += float(remaining)

    offer_id_to_weight: Dict[int, float] = {}
    zero_ids: List[int] = []
    all_meta = {**hub_offers, **hub_quality}
    for offer_name, meta in all_meta.items():
        oid = meta.get("id")
        if oid is None:
            continue
        w = raw.get(offer_name, 0.0)
        if w > 0:
            offer_id_to_weight[int(oid)] = w
        else:
            zero_ids.append(int(oid))
    return offer_id_to_weight, zero_ids


def _quality_brand_slugs(client: KeitaroClient) -> frozenset[str]:
    from blend_sync_from_sheet import get_sheets_service, read_blend_rows
    from integrations.hub_blend_child_flows import group_blend_rows_by_quality_campaign

    try:
        rows = read_blend_rows(get_sheets_service())
    except Exception:
        return frozenset()
    _pool, groups, _logs = group_blend_rows_by_quality_campaign(rows, client)
    return frozenset(g.brand_slug for g in groups)


def restore_hub_stream_weights_from_click_caps(
    *,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Emergency / fallback: set campaign 94 geo flows from Blend clickCaps + Nipuhim equal split.

    Used when the demand bill is missing (so equalize cannot run) or after a bad zero-share write.
    """
    from integrations.hub_click_cap_weights import hub_offer_weights_from_caps

    client = KeitaroClient()
    state = load_hub_state()
    hub_id = int(state.get("hub_campaign_id") or KEITARO_HUB_CAMPAIGN_ID)
    offer_state = dict(state.get("hub_offers") or {})
    quality_state = dict(state.get("hub_quality_offers") or {})
    active = hub_active_feed_keys()
    wctx = resolve_hub_weight_context(use_click_caps=True)

    blend_feeds = frozenset((wctx.blend_feed_caps or {}).keys()) | frozenset(
        str(m.get("feed_key") or "")
        for m in offer_state.values()
        if str(m.get("hub_type") or "") == "blend"
    )
    blend_w = hub_offer_weights_from_caps(
        wctx.blend_feed_caps or {},
        {},
        active_feeds=blend_feeds,
        hub_types=("blend",),
    )
    nip_w = dict(wctx.weights)

    oid_w: Dict[int, float] = {}
    zeros: List[int] = []
    b_ids, b_z = _stream_hub_offer_weights(
        offer_state, blend_w, hub_types=("blend",), active_feeds=blend_feeds
    )
    n_ids, n_z = _stream_hub_offer_weights(
        offer_state, nip_w, hub_types=("nipuhim",), active_feeds=active
    )
    oid_w.update(b_ids)
    oid_w.update(n_ids)
    zeros.extend(b_z)
    zeros.extend(n_z)
    for meta in {**offer_state, **quality_state}.values():
        oid = meta.get("id")
        if oid is None:
            continue
        if int(oid) not in oid_w and int(oid) not in zeros:
            zeros.append(int(oid))

    logs = [
        f"click-cap restore: {len(oid_w)} weighted + {len(zeros)} zero-share "
        f"(blend_caps={dict(wctx.blend_feed_caps or {})}, source={wctx.source})"
    ]
    if not oid_w:
        return {
            "hub_streams_updated": 0,
            "logs": logs + ["ABORT: no positive click-cap weights"],
            "dry_run": dry_run,
            "status": "error",
        }

    updated = 0
    if dry_run:
        streams = _hub_device_streams(client, hub_id)
        logs.append(f"would update {len(streams)} hub stream(s)")
        return {
            "hub_streams_updated": 0,
            "logs": logs,
            "dry_run": True,
            "status": "dry_run",
            "offer_weights": oid_w,
        }

    for stream in _hub_device_streams(client, hub_id):
        sid = stream.get("id")
        if sid is None:
            continue
        try:
            set_flow_offers_weighted_keep_zeros(int(sid), oid_w, zero_offer_ids=zeros)
            updated += 1
            logs.append(f"hub {stream.get('name')}: restored")
        except KeitaroClientError as e:
            logs.append(f"hub {stream.get('name')}: error {e}")

    return {
        "hub_streams_updated": updated,
        "logs": logs,
        "dry_run": False,
        "status": "ok",
        "offer_weights": oid_w,
    }


def equalize_hub_stream_weights(
    *,
    dry_run: bool = False,
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Campaign 94: reweight hub offers by remaining bill demand per geo×device."""
    from assistance import parse_blend_stream_geo_channel

    client = KeitaroClient()
    data = payload or build_domain_demand_payload(rebuild_demand=False, reason="weight_equalize")
    if data.get("error") == "empty_bill_refused" or data.get("status") == "error":
        logs = list(data.get("logs") or []) + [
            "Bill missing — falling back to click-cap hub weight restore"
        ]
        fallback = restore_hub_stream_weights_from_click_caps(dry_run=dry_run)
        logs.extend(fallback.get("logs") or [])
        return {
            "hub_streams_updated": fallback.get("hub_streams_updated", 0),
            "logs": logs,
            "dry_run": dry_run,
            "skipped": False,
            "fallback": "click_caps",
            "status": fallback.get("status"),
        }
    bill_rows = data.get("bill") or []
    if not bill_rows:
        fallback = restore_hub_stream_weights_from_click_caps(dry_run=dry_run)
        return {
            "hub_streams_updated": fallback.get("hub_streams_updated", 0),
            "logs": ["Empty bill rows — click-cap restore"] + list(fallback.get("logs") or []),
            "dry_run": dry_run,
            "fallback": "click_caps",
            "status": fallback.get("status"),
        }
    hub_id = int(data.get("hub_campaign_id") or load_hub_state().get("hub_campaign_id") or KEITARO_HUB_CAMPAIGN_ID)
    quality_slugs = _quality_brand_slugs(client)

    logs: List[str] = []
    updated = 0
    skipped_all_zero = 0
    for stream in _hub_device_streams(client, hub_id):
        sname = stream.get("name") or ""
        sid = stream.get("id")
        if sid is None:
            continue
        geo, channel = parse_blend_stream_geo_channel(sname)
        if not geo or channel not in ("desktop", "mobile"):
            continue
        offer_weights, zero_ids = _hub_offer_weights_for_segment(
            bill_rows, geo, channel, quality_brand_slugs=quality_slugs
        )
        seg_rows = [
            r
            for r in bill_rows
            if str(r.get("geo") or "").lower() == geo
            and str(r.get("device") or "").lower() == channel
            and int(r.get("demand_clicks") or 0) > 0
        ]
        seg_remaining = sum(max(0, int(r.get("remaining") or 0)) for r in seg_rows)
        leftover_kept: Optional[int] = None
        if not offer_weights:
            if seg_remaining > 0:
                # Mapping bug / name mismatch — never wipe or invent a single leftover here.
                logs.append(
                    f"hub {sname}: skip (remaining={seg_remaining} but no mapped hub weights)"
                )
                continue
            preferred = _preferred_hub_leftover_ids(
                bill_rows, geo, channel, quality_brand_slugs=quality_slugs
            )
            offer_weights, zero_ids, leftover_kept = _ensure_at_least_one_live_weight(
                offer_weights, zero_ids, preferred_offer_ids=preferred
            )
            if not offer_weights:
                logs.append(f"hub {sname}: skip (no hub offers available for leftover)")
                continue
        if dry_run:
            extra = f" leftover_offer={leftover_kept}" if leftover_kept else ""
            logs.append(
                f"hub {sname}: would set {len(offer_weights)} weighted + "
                f"{len(zero_ids)} zero-share{extra}"
            )
            continue
        try:
            set_flow_offers_weighted_keep_zeros(int(sid), offer_weights, zero_offer_ids=zero_ids)
            updated += 1
            if leftover_kept is not None:
                skipped_all_zero += 1
                logs.append(
                    f"hub {sname}: leftover catch-all offer_id={leftover_kept} "
                    f"(+{len(zero_ids)} zero-share)"
                )
            else:
                logs.append(
                    f"hub {sname}: {len(offer_weights)} weighted + {len(zero_ids)} zero-share"
                )
        except KeitaroClientError as e:
            logs.append(f"hub {sname}: error {e}")

    return {
        "hub_streams_updated": updated,
        "logs": logs,
        "dry_run": dry_run,
        "segments_with_leftover": skipped_all_zero,
    }


def equalize_child_blend_offer_weights(
    *,
    dry_run: bool = False,
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """BLEND-feed* + Quality domain flows: weight=0 when click cap met, else proportional to remaining."""
    from assistance import parse_blend_stream_geo_channel, set_flow_offers_weighted
    from integrations.keitaro_child_campaigns import blend_child_campaign_id

    client = KeitaroClient()
    data = payload or build_domain_demand_payload(rebuild_demand=False, reason="weight_equalize")
    if data.get("error") == "empty_bill_refused" or data.get("status") == "error":
        return {
            "streams_updated": 0,
            "logs": ["Skipped child equalize: empty/missing demand bill"],
            "dry_run": dry_run,
            "skipped": True,
        }
    bill_rows = [r for r in (data.get("bill") or []) if str(r.get("family") or "") == "blend"]
    if not bill_rows:
        return {
            "streams_updated": 0,
            "logs": ["Skipped child equalize: no blend bill rows"],
            "dry_run": dry_run,
            "skipped": True,
        }

    offers_by_name = {(o.get("name") or "").strip(): int(o["id"]) for o in client.get_offers() if o.get("id")}

    # campaign_id -> stream_key -> offer_id -> remaining weight
    by_campaign: Dict[int, Dict[Tuple[str, str], Dict[int, float]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(float))
    )
    quality_slugs = _quality_brand_slugs(client)
    from integrations.hub_blend_child_flows import group_blend_rows_by_quality_campaign

    try:
        from blend_sync_from_sheet import get_sheets_service, read_blend_rows

        blend_rows = read_blend_rows(get_sheets_service())
        _pool, q_groups, _ = group_blend_rows_by_quality_campaign(blend_rows, client)
        quality_cid_by_slug = {g.brand_slug: int(g.campaign_id) for g in q_groups}
    except Exception:
        quality_cid_by_slug = {}

    for row in bill_rows:
        geo = str(row.get("geo") or "").lower()
        device = str(row.get("device") or "").lower()
        feed = str(row.get("feed") or "")
        brand = str(row.get("brand") or "")
        brand_slug = _slug(brand)
        remaining = max(0, int(row.get("remaining") or 0))
        offer_name = f"blend_{geo}_{_slug(feed, max_len=24)}_{_slug(brand)}"
        oid = offers_by_name.get(offer_name)
        if oid is None:
            continue
        if brand_slug in quality_slugs:
            cid = quality_cid_by_slug.get(brand_slug)
        else:
            try:
                cid = blend_child_campaign_id(feed)
            except Exception:
                continue
        if not cid:
            continue
        if remaining > 0:
            by_campaign[int(cid)][(geo, device)][int(oid)] += float(remaining)

    logs: List[str] = []
    streams_updated = 0
    for cid, stream_map in sorted(by_campaign.items()):
        is_quality_camp = cid in set(quality_cid_by_slug.values())
        for stream in client.get_streams(int(cid)):
            sname = str(stream.get("name") or "")
            is_domain_stream = sname.lower().endswith("_domain")
            if is_quality_camp and not is_domain_stream:
                continue
            if not is_quality_camp and is_domain_stream:
                continue
            geo, channel = parse_blend_stream_geo_channel(sname)
            if not geo or channel not in ("desktop", "mobile"):
                continue
            sid = stream.get("id")
            if sid is None:
                continue
            weights = dict(stream_map.get((geo, channel), {}))
            leftover_oid: Optional[int] = None
            if not weights:
                # Prefer the blend offer that had the highest original demand for this segment.
                demand_rank: List[Tuple[float, int]] = []
                for row in bill_rows:
                    if str(row.get("geo") or "").lower() != geo:
                        continue
                    if str(row.get("device") or "").lower() != channel:
                        continue
                    demand = max(0, int(row.get("demand_clicks") or 0))
                    if demand <= 0:
                        continue
                    feed = str(row.get("feed") or "")
                    brand = str(row.get("brand") or "")
                    oname = f"blend_{geo}_{_slug(feed, max_len=24)}_{_slug(brand)}"
                    oid = offers_by_name.get(oname)
                    if oid is not None:
                        demand_rank.append((float(demand), int(oid)))
                demand_rank.sort(key=lambda t: t[0], reverse=True)
                preferred = [oid for _, oid in demand_rank]
                # Also consider blend_* already on the stream.
                for slot in stream.get("offers") or []:
                    oidr = slot.get("offer_id")
                    if oidr is None:
                        continue
                    for on, oid in offers_by_name.items():
                        if oid == int(oidr) and on.startswith("blend_") and int(oid) not in preferred:
                            preferred.append(int(oid))
                            break
                weights, _ignored_zeros, leftover_oid = _ensure_at_least_one_live_weight(
                    {}, preferred, preferred_offer_ids=preferred
                )
            if dry_run:
                extra = f" leftover={leftover_oid}" if leftover_oid else ""
                logs.append(
                    f"campaign {cid} {geo}/{channel}: would weight {len(weights)} offer(s){extra}"
                )
                continue
            if weights:
                set_flow_offers_weighted(int(sid), weights)
                streams_updated += 1
                if leftover_oid is not None:
                    logs.append(
                        f"campaign {cid} {geo}/{channel}: leftover catch-all offer_id={leftover_oid}"
                    )
                else:
                    logs.append(f"campaign {cid} {geo}/{channel}: weighted {len(weights)} offer(s)")
            else:
                logs.append(f"campaign {cid} {geo}/{channel}: skip (no blend offers for leftover)")

    return {"streams_updated": streams_updated, "logs": logs, "dry_run": dry_run}


def run_domain_demand_guard(
    *,
    dry_run: bool = False,
    reason: str = "scheduled",
    rebuild_demand: bool = False,
    pause_trillion: bool = True,
    equalize_weights: bool = True,
) -> Dict[str, Any]:
    """Intraday: refresh delivered, equalize Keitaro weights, pause filled Trillion segments."""
    sync_result = sync_domain_demand(rebuild_demand=rebuild_demand, dry_run=dry_run, reason=reason)
    payload = {k: v for k, v in sync_result.items() if k != "write"}

    out: Dict[str, Any] = {
        "reason": reason,
        "dry_run": dry_run,
        "sync": sync_result.get("write"),
        "logs": list(sync_result.get("logs") or []),
    }

    if payload.get("error") == "empty_bill_refused" or payload.get("status") == "error":
        out["status"] = "error"
        out["error"] = payload.get("error") or "payload_error"
        out["logs"].append(
            "Demand bill missing — will restore hub weights from click caps; skipping Trillion pause"
        )
        if equalize_weights:
            hub_eq = equalize_hub_stream_weights(dry_run=dry_run, payload=payload)
            out["hub_equalize"] = hub_eq
            out["logs"].extend(hub_eq.get("logs") or [])
            if hub_eq.get("status") in ("ok", "dry_run"):
                out["status"] = hub_eq.get("status")
        return out

    if equalize_weights:
        hub_eq = equalize_hub_stream_weights(dry_run=dry_run, payload=payload)
        child_eq = equalize_child_blend_offer_weights(dry_run=dry_run, payload=payload)
        out["hub_equalize"] = hub_eq
        out["child_equalize"] = child_eq
        out["logs"].extend(hub_eq.get("logs") or [])
        out["logs"].extend(child_eq.get("logs") or [])

    if pause_trillion:
        pause_result = run_trillion_pause_filled_segments(
            dry_run=dry_run,
            reason=reason,
            segments=payload.get("summary_by_geo"),
        )
        out["trillion_pause"] = pause_result
        out["logs"].append(f"Trillion pause: {pause_result.get('paused')} segment(s)")

    out["status"] = "dry_run" if dry_run else "ok"
    return out


def run_daily_trillion_activate_step(
    *,
    dry_run: bool = False,
    reason: str = "daily_workflow",
    date_str: Optional[str] = None,
) -> Dict[str, Any]:
    """After domain-demand bill is built: resume Trillion for segments that need traffic."""
    payload = build_domain_demand_payload(
        date_str=date_str,
        rebuild_demand=False,
        reason=reason,
    )
    return run_trillion_activate_for_demand(
        dry_run=dry_run,
        reason=reason,
        segments=payload.get("summary_by_geo"),
    )
