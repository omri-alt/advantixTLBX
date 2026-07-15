"""
Hub blend routing: campaign 94 tags all child links with ``sub_id_15=domain``.

Child campaigns (BLEND-feed*, NIPUHIM-feed*, Quality merchant campaigns) need flows
that match that tag so blend offers receive hub traffic. Quality campaigns keep their
existing flows for direct quality traffic and add ``{geo}_{device}_domain`` for blend.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

from assistance import (
    ensure_domain_blend_stream,
    refresh_hub_child_domain_filters,
    set_flow_offers_weighted,
)
from config import (
    KEITARO_HUB_BLEND_DOMAIN_ENABLED,
    KEITARO_HUB_CAMPAIGN_ID,
    KEITARO_HUB_STATE_PATH,
    KEITARO_QUALITY_CAMPAIGN_GROUP,
)
from integrations.keitaro import KeitaroClient, KeitaroClientError
from integrations.hub_click_cap_weights import hub_offer_weights_from_caps
from integrations.keitaro_hub import (
    _create_campaign_link_offer,
    _hub_campaign_link_offer_body,
    _hub_device_streams,
    _hub_offer_needs_update,
    _stream_hub_offer_weights,
    ensure_hub_routing_geos,
    hub_active_feed_keys,
    hub_offer_click_url,
    load_hub_state,
    resolve_hub_weight_context,
    save_hub_state,
    set_flow_offers_weighted_keep_zeros,
)

logger = logging.getLogger(__name__)


def _slug(s: str, max_len: int = 48) -> str:
    from blend_sync_from_sheet import _slug as blend_slug

    return blend_slug(s, max_len=max_len)


def hub_quality_offer_name(brand_slug: str) -> str:
    return f"hub_quality_{brand_slug}"


@dataclass(frozen=True)
class QualityMerchantGroup:
    brand_name: str
    brand_slug: str
    geo: str
    campaign_id: int
    campaign_name: str
    campaign_alias: str
    rows: Tuple[Any, ...]


def build_quality_campaign_index(
    client: KeitaroClient,
    *,
    group: Optional[str] = None,
) -> Tuple[Dict[Tuple[str, str], Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """
    Return ((brand_slug, geo) -> campaign, alias_or_name_lower -> campaign).
    """
    from integrations.keitaro_feed_balance import parse_brand_geo_from_campaign_name

    want_group = (group or KEITARO_QUALITY_CAMPAIGN_GROUP or "Quality").strip()
    by_brand_geo: Dict[Tuple[str, str], Dict[str, Any]] = {}
    by_key: Dict[str, Dict[str, Any]] = {}
    offset = 0
    while True:
        batch = client.get_campaigns(offset=offset, limit=250)
        if not batch:
            break
        for camp in batch:
            if str(camp.get("group") or "").strip() != want_group:
                continue
            cid = int(camp.get("id") or 0)
            if not cid:
                continue
            name = str(camp.get("name") or "").strip()
            alias = str(camp.get("alias") or "").strip()
            brand_slug, geo = parse_brand_geo_from_campaign_name(name)
            entry = {
                "id": cid,
                "name": name,
                "alias": alias,
                "brand_slug": brand_slug,
                "geo": geo,
            }
            if brand_slug and geo:
                by_brand_geo[(brand_slug, geo)] = entry
            for key in (name.lower(), alias.lower(), f"{brand_slug}-{geo}" if brand_slug and geo else ""):
                if key:
                    by_key[key] = entry
        if len(batch) < 250:
            break
        offset += 250
    return by_brand_geo, by_key


def _resolve_quality_campaign_for_row(
    row: Any,
    by_brand_geo: Dict[Tuple[str, str], Dict[str, Any]],
    by_key: Dict[str, Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    explicit = (getattr(row, "quality_campaign", None) or "").strip()
    if explicit:
        key = explicit.lower()
        if key.isdigit():
            return {"id": int(key), "name": explicit, "alias": "", "brand_slug": "", "geo": row.geo}
        if key in by_key:
            return by_key[key]
        for camp in by_key.values():
            if camp.get("name", "").lower() == key or camp.get("alias", "").lower() == key:
                return camp
    brand_slug = _slug(row.brand_name)
    geo = (row.geo or "").strip().lower()
    return by_brand_geo.get((brand_slug, geo))


def group_blend_rows_by_quality_campaign(
    rows: List[Any],
    client: KeitaroClient,
) -> Tuple[List[Any], List[QualityMerchantGroup], List[str]]:
    """Split sheet rows into pool blend vs dedicated Quality merchant campaigns."""
    by_brand_geo, by_key = build_quality_campaign_index(client)
    logs: List[str] = []
    pool_rows: List[Any] = []
    groups: Dict[Tuple[int, str, str], List[Any]] = defaultdict(list)

    for row in rows:
        camp = _resolve_quality_campaign_for_row(row, by_brand_geo, by_key)
        if camp and camp.get("id"):
            key = (int(camp["id"]), _slug(row.brand_name), row.geo)
            groups[key].append(row)
        else:
            pool_rows.append(row)

    merchant_groups: List[QualityMerchantGroup] = []
    for (cid, brand_slug, geo), group_rows in sorted(groups.items()):
        camp = next(
            (c for c in by_brand_geo.values() if int(c["id"]) == cid),
            {"id": cid, "name": "", "alias": ""},
        )
        brand_name = group_rows[0].brand_name
        merchant_groups.append(
            QualityMerchantGroup(
                brand_name=brand_name,
                brand_slug=brand_slug,
                geo=geo,
                campaign_id=cid,
                campaign_name=str(camp.get("name") or ""),
                campaign_alias=str(camp.get("alias") or ""),
                rows=tuple(group_rows),
            )
        )
        logs.append(
            f"Quality blend {brand_name}/{geo} -> campaign {cid} "
            f"({camp.get('name') or '?'}) — {len(group_rows)} row(s)"
        )
    logs.append(f"Blend pool rows (BLEND-feed*): {len(pool_rows)}; Quality merchants: {len(merchant_groups)}")
    return pool_rows, merchant_groups, logs


def _channel_weight_for_row(row: Any, channel: str) -> float:
    from integrations.blend_device import blend_stream_weight_for_channel

    w = blend_stream_weight_for_channel(
        row.device_mode,
        channel,
        click_cap=row.click_cap,
        weight_desktop=row.weight_desktop,
        weight_mobile=row.weight_mobile,
    )
    return float(w) if w is not None and w > 0 else 0.0


def sync_quality_merchant_domain_flows(
    client: KeitaroClient,
    group: QualityMerchantGroup,
    *,
    dry_run: bool = False,
) -> List[str]:
    """Create ``{geo}_{device}_domain`` flows on a Quality campaign + attach blend offers."""
    from blend_sync_from_sheet import (
        _blend_keitaro_action_payload,
        _upsert_offer,
    )

    logs: List[str] = []
    cid = int(group.campaign_id)
    geo = group.geo

    offer_id_to_row: Dict[int, Any] = {}
    if not dry_run:
        for row in group.rows:
            oid = _upsert_offer(
                client,
                row.offer_name,
                _blend_keitaro_action_payload(row.geo, row.offer_url, row.feed_tag),
            )
            offer_id_to_row[int(oid)] = row

    for channel in ("desktop", "mobile"):
        stream_name = f"{geo}_{channel}_domain"
        if dry_run:
            active = sum(1 for row in group.rows if _channel_weight_for_row(row, channel) > 0)
            logs.append(
                f"  {group.campaign_name} {stream_name}: would ensure domain flow + {active} offer(s)"
            )
            continue

        stream = ensure_domain_blend_stream(cid, geo, channel, skip_if_exists=True)
        sid = stream.get("id")
        if sid is None:
            logs.append(f"  {group.campaign_name} {stream_name}: no stream id — skip")
            continue
        offer_weights = {
            oid: _channel_weight_for_row(row, channel) for oid, row in offer_id_to_row.items()
        }
        offer_weights = {k: v for k, v in offer_weights.items() if v > 0}
        if offer_weights:
            set_flow_offers_weighted(int(sid), offer_weights)
            logs.append(
                f"  {group.campaign_name} {stream_name}: {len(offer_weights)} blend offer(s) weighted"
            )
        else:
            logs.append(f"  {group.campaign_name} {stream_name}: no offers for {channel}")

    return logs


def ensure_hub_quality_offers(
    client: KeitaroClient,
    merchant_groups: List[QualityMerchantGroup],
    state: Dict[str, Any],
    *,
    dry_run: bool = False,
) -> Tuple[Dict[str, Any], List[str]]:
    """Hub campaign 94 offers that link directly to Quality merchant campaigns."""
    logs: List[str] = []
    offer_state: Dict[str, Any] = dict(state.get("hub_quality_offers") or {})
    offers_by_name = {(o.get("name") or "").strip(): o for o in client.get_offers()}

    for mg in merchant_groups:
        offer_name = hub_quality_offer_name(mg.brand_slug)
        cid = int(mg.campaign_id)
        alias = (mg.campaign_alias or "").strip() or None
        expected_url = hub_offer_click_url(client, cid, alias=alias)
        saved = offer_state.get(offer_name) or {}
        oid = saved.get("id")
        current = offers_by_name.get(offer_name) or {}
        if not oid and current.get("id"):
            oid = int(current["id"])
        if oid and (not current or int(current.get("id") or 0) != oid):
            for o in client.get_offers():
                if int(o.get("id") or 0) == oid:
                    current = o
                    break

        if oid:
            if _hub_offer_needs_update(current, expected_url):
                if dry_run:
                    logs.append(f"hub quality {offer_name}: would update id={oid}")
                else:
                    client.update_offer(oid, _hub_campaign_link_offer_body(client, cid, alias=alias))
                    logs.append(f"hub quality {offer_name}: updated id={oid}")
            else:
                logs.append(f"hub quality {offer_name}: reuse id={oid}")
        else:
            if dry_run:
                logs.append(f"hub quality {offer_name}: would create -> campaign {cid}")
            else:
                created = _create_campaign_link_offer(client, offer_name, cid, alias=alias)
                oid = int(created["id"])
                logs.append(f"hub quality {offer_name}: created id={oid} -> campaign {cid}")

        if oid:
            offer_state[offer_name] = {
                "id": int(oid),
                "child_campaign_id": cid,
                "brand_slug": mg.brand_slug,
                "brand_name": mg.brand_name,
                "geo": mg.geo,
                "click_url": expected_url,
            }

    state["hub_quality_offers"] = offer_state
    return state, logs


def _hub_quality_stream_weights(
    merchant_groups: List[QualityMerchantGroup],
    geo: str,
    channel: str,
    offer_state: Dict[str, Any],
) -> Dict[int, float]:
    """Per Quality merchant hub offer weight = device-weighted clickCap sum for geo."""
    weights: Dict[int, float] = {}
    for mg in merchant_groups:
        if mg.geo != geo:
            continue
        total = sum(_channel_weight_for_row(row, channel) for row in mg.rows)
        if total <= 0:
            continue
        meta = offer_state.get(hub_quality_offer_name(mg.brand_slug)) or {}
        oid = meta.get("id")
        if oid is not None:
            weights[int(oid)] = float(total)
    return weights


def wire_hub_with_blend_and_quality(
    client: KeitaroClient,
    state: Dict[str, Any],
    merchant_groups: List[QualityMerchantGroup],
    *,
    date_str: Optional[str] = None,
    nipuhim_max_offers_per_geo: int = 60,
    dry_run: bool = False,
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Campaign 94: attach hub_blend_*, hub_nipuhim_*, and hub_quality_* on geo device streams.

    All hub offer URLs already include ``sub_id_15=domain``; child campaigns apply matching filters.
    """
    logs: List[str] = []
    hub_id = int(state.get("hub_campaign_id") or KEITARO_HUB_CAMPAIGN_ID)
    offer_state = dict(state.get("hub_offers") or {})
    quality_offer_state = dict(state.get("hub_quality_offers") or {})

    wctx = resolve_hub_weight_context(
        date_str=date_str,
        nipuhim_max_offers_per_geo=nipuhim_max_offers_per_geo,
        use_click_caps=True,
    )
    logs.extend(wctx.logs)

    active_feeds = hub_active_feed_keys()
    blend_weight_names = hub_offer_weights_from_caps(
        wctx.blend_feed_caps,
        wctx.nipuhim_feed_caps,
        active_feeds=active_feeds,
        hub_types=("blend",),
    )
    nipuhim_weight_names = hub_offer_weights_from_caps(
        wctx.blend_feed_caps,
        wctx.nipuhim_feed_caps,
        active_feeds=active_feeds,
        hub_types=("nipuhim",),
    )

    ensure_res = ensure_hub_routing_geos(
        dry_run=dry_run, client=client, hub_campaign_id=hub_id
    )
    logs.extend(ensure_res.get("logs") or [])

    streams = _hub_device_streams(client, hub_id)
    if not streams:
        raise ValueError(f"Hub campaign {hub_id} has no geo desktop/mobile streams")

    for stream in streams:
        from assistance import parse_blend_stream_geo_channel

        sname = stream.get("name") or ""
        sid = stream.get("id")
        if sid is None:
            continue
        geo, channel = parse_blend_stream_geo_channel(sname)
        if not geo or channel not in ("desktop", "mobile"):
            continue

        blend_by_name = dict(blend_weight_names)
        nip_by_name = dict(nipuhim_weight_names)
        if wctx.weights_by_geo and geo in wctx.weights_by_geo:
            nip_by_name = {
                k: v
                for k, v in wctx.weights_by_geo[geo].items()
                if k.startswith("hub_nipuhim_")
            }

        offer_id_to_weight: Dict[int, float] = {}
        zero_ids: List[int] = []

        blend_ids, blend_zeros = _stream_hub_offer_weights(
            offer_state,
            blend_by_name,
            hub_types=("blend",),
            active_feeds=active_feeds,
        )
        nip_ids, nip_zeros = _stream_hub_offer_weights(
            offer_state,
            nip_by_name,
            hub_types=("nipuhim",),
            active_feeds=active_feeds,
        )
        offer_id_to_weight.update(blend_ids)
        offer_id_to_weight.update(nip_ids)
        zero_ids.extend(blend_zeros)
        zero_ids.extend(nip_zeros)

        quality_weights = _hub_quality_stream_weights(merchant_groups, geo, channel, quality_offer_state)
        offer_id_to_weight.update(quality_weights)

        if dry_run:
            logs.append(
                f"hub stream {sname}: would attach blend={len(blend_by_name)} nipuhim="
                f"{len(nip_by_name)} quality={len(quality_weights)} weighted offers"
            )
            continue

        set_flow_offers_weighted_keep_zeros(int(sid), offer_id_to_weight, zero_offer_ids=zero_ids)
        logs.append(
            f"hub stream {sname}: attached {len(offer_id_to_weight)} weighted + "
            f"{len(zero_ids)} zero-share hub offers"
        )

    state["hub_campaign_id"] = hub_id
    return state, logs


def refresh_all_hub_child_domain_filters(
    state: Dict[str, Any],
    *,
    dry_run: bool = False,
) -> List[str]:
    """Add ``sub_id_15=domain`` filter to BLEND-feed* and NIPUHIM-feed* device flows."""
    logs: List[str] = []
    child_state = state.get("child_campaigns") or {}
    seen: Set[int] = set()
    for key, meta in sorted(child_state.items()):
        cid = meta.get("id")
        if cid is None:
            continue
        cid = int(cid)
        if cid in seen:
            continue
        seen.add(cid)
        hub_type = str(meta.get("hub_type") or key.split("_", 1)[0])
        if dry_run:
            logs.append(f"child {key} (id={cid}): would refresh domain filters on device flows")
            continue
        n, errs = refresh_hub_child_domain_filters(cid)
        logs.append(f"child {key} ({hub_type}): refreshed domain filter on {n} flow(s)")
        for e in errs[:3]:
            logs.append(f"  {e}")
    return logs


def run_hub_blend_child_flows(
    *,
    date_str: Optional[str] = None,
    nipuhim_max_offers_per_geo: int = 60,
    dry_run: bool = False,
    state_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Daily step after blend_v2:

    1. Quality merchant domain flows + offers on their campaigns
    2. Hub quality bypass offers on campaign 94
    3. Wire hub_blend + hub_nipuhim + hub_quality weights on campaign 94
    4. ``sub_id_15=domain`` filters on BLEND-feed* / NIPUHIM-feed* child flows
    """
    if not KEITARO_HUB_BLEND_DOMAIN_ENABLED:
        return {"status": "skipped", "reason": "disabled", "logs": []}

    from blend_sync_from_sheet import get_sheets_service, read_blend_rows

    client = KeitaroClient()
    state = load_hub_state(state_path)
    logs: List[str] = []

    try:
        rows = read_blend_rows(get_sheets_service())
    except Exception as e:
        return {"status": "error", "error": f"Blend sheet: {e}", "logs": logs}

    pool_rows, merchant_groups, split_logs = group_blend_rows_by_quality_campaign(rows, client)
    logs.extend(split_logs)

    state, q_logs = ensure_hub_quality_offers(
        client, merchant_groups, state, dry_run=dry_run
    )
    logs.extend(q_logs)

    for mg in merchant_groups:
        logs.extend(sync_quality_merchant_domain_flows(client, mg, dry_run=dry_run))

    state, hub_logs = wire_hub_with_blend_and_quality(
        client,
        state,
        merchant_groups,
        date_str=date_str,
        nipuhim_max_offers_per_geo=nipuhim_max_offers_per_geo,
        dry_run=dry_run,
    )
    logs.extend(hub_logs)

    logs.extend(refresh_all_hub_child_domain_filters(state, dry_run=dry_run))

    if not dry_run:
        save_hub_state(state, state_path)

    return {
        "status": "dry_run" if dry_run else "ok",
        "dry_run": dry_run,
        "pool_blend_rows": len(pool_rows),
        "quality_merchants": len(merchant_groups),
        "logs": logs,
    }
