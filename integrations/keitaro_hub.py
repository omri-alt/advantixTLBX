"""
Keitaro hub campaign (Domain / id 94): route bought traffic to per-feed child campaigns.

Creates or reuses child campaigns (Blend + Nipuhim × kelkoo1/2/5, adexa, yadore, shopnomix),
hub campaign-link offers, and wires geo desktop/mobile streams on the hub.

Nipuhim children clone from Nipuh (HrQBXp): country flows with static PLA product URLs.
They are separate from KL-Main-feed* campaigns (dynamic oadest routing).
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from assistance import (
    ensure_blend_device_stream,
    find_campaign_by_alias_or_name,
    get_campaigns_data,
    parse_blend_stream_geo_channel,
    set_flow_offers_weighted_keep_zeros,
)
from config import (
    KEITARO_HUB_ACTIVE_FEEDS,
    KEITARO_HUB_BLEND_PCT,
    KEITARO_HUB_CAMPAIGN_ID,
    KEITARO_HUB_NIPUHIM_PCT,
    KEITARO_HUB_OFFER_ACTION_TYPE,
    KEITARO_HUB_RAIN_SHELL,
    KEITARO_HUB_STATE_PATH,
    KEITARO_HUB_TYPES,
    KEITARO_NIPUHIM_HUB_TEMPLATE_CAMPAIGN_ID,
)
from integrations.keitaro import KeitaroClient, KeitaroClientError

logger = logging.getLogger(__name__)

# kelkoo5 is the third Kelkoo account in this repo (user-facing "kelkoo3").
HUB_FEED_KEYS: Tuple[str, ...] = (
    "kelkoo1",
    "kelkoo2",
    "kelkoo5",
    "adexa",
    "yadore",
    "shopnomix",
)

# Per-type feed weights (sum 100). Kelkoo trio mirrors Nipuhim 65/25/10; others equal.
DEFAULT_FEED_WEIGHTS: Dict[str, int] = {
    "kelkoo1": 40,
    "kelkoo2": 15,
    "kelkoo5": 6,
    "adexa": 13,
    "yadore": 13,
    "shopnomix": 13,
}

HUB_TYPES: Tuple[str, ...] = ("blend", "nipuhim")


@dataclass(frozen=True)
class ChildCampaignSpec:
    hub_type: str  # blend | nipuhim
    feed_key: str
    name: str
    alias: str
    existing_campaign_id: Optional[int] = None
    existing_alias: Optional[str] = None
    clone_from_campaign_id: Optional[int] = None


CHILD_SPECS: Tuple[ChildCampaignSpec, ...] = (
    ChildCampaignSpec("blend", "kelkoo1", "BLEND-feed1", "blendFeed1", clone_from_campaign_id=2),
    ChildCampaignSpec("blend", "kelkoo2", "BLEND-feed2", "blendFeed2", clone_from_campaign_id=2),
    ChildCampaignSpec("blend", "kelkoo5", "BLEND-feed5", "blendFeed5", clone_from_campaign_id=2),
    ChildCampaignSpec("blend", "adexa", "BLEND-adexa", "blendAdexa", clone_from_campaign_id=2),
    ChildCampaignSpec("blend", "yadore", "BLEND-yadore", "blendYadore", clone_from_campaign_id=2),
    ChildCampaignSpec("blend", "shopnomix", "BLEND-shopnomix", "blendShopnomix", clone_from_campaign_id=2),
    # Nipuhim: clone Nipuh (HrQBXp) — per-country flows with static PLA product URLs.
    # Do NOT use KL-Main-feed* (dynamic oadest from traffic source).
    ChildCampaignSpec(
        "nipuhim",
        "kelkoo1",
        "NIPUHIM-feed1",
        "nipuhFeed1",
        clone_from_campaign_id=KEITARO_NIPUHIM_HUB_TEMPLATE_CAMPAIGN_ID,
    ),
    ChildCampaignSpec(
        "nipuhim",
        "kelkoo2",
        "NIPUHIM-feed2",
        "nipuhFeed2",
        clone_from_campaign_id=KEITARO_NIPUHIM_HUB_TEMPLATE_CAMPAIGN_ID,
    ),
    ChildCampaignSpec(
        "nipuhim",
        "kelkoo5",
        "NIPUHIM-feed5",
        "nipuhFeed5",
        clone_from_campaign_id=KEITARO_NIPUHIM_HUB_TEMPLATE_CAMPAIGN_ID,
    ),
    ChildCampaignSpec(
        "nipuhim",
        "adexa",
        "NIPUHIM-adexa",
        "nipuhAdexa",
        clone_from_campaign_id=KEITARO_NIPUHIM_HUB_TEMPLATE_CAMPAIGN_ID,
    ),
    ChildCampaignSpec(
        "nipuhim",
        "yadore",
        "NIPUHIM-yadore",
        "nipuhYadore",
        clone_from_campaign_id=KEITARO_NIPUHIM_HUB_TEMPLATE_CAMPAIGN_ID,
    ),
    ChildCampaignSpec(
        "nipuhim",
        "shopnomix",
        "NIPUHIM-shopnomix",
        "nipuhShopnomix",
        clone_from_campaign_id=KEITARO_NIPUHIM_HUB_TEMPLATE_CAMPAIGN_ID,
    ),
)


def _child_key(hub_type: str, feed_key: str) -> str:
    return f"{hub_type}_{feed_key}"


def hub_offer_name(hub_type: str, feed_key: str) -> str:
    return f"hub_{hub_type}_{feed_key}"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_hub_state(path: Optional[str] = None) -> Dict[str, Any]:
    p = Path(path or KEITARO_HUB_STATE_PATH)
    if not p.is_file():
        return {"hub_campaign_id": KEITARO_HUB_CAMPAIGN_ID, "child_campaigns": {}, "hub_offers": {}}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"hub_campaign_id": KEITARO_HUB_CAMPAIGN_ID, "child_campaigns": {}, "hub_offers": {}}


def save_hub_state(state: Dict[str, Any], path: Optional[str] = None) -> Path:
    p = Path(path or KEITARO_HUB_STATE_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at"] = _utc_now_iso()
    p.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return p


def _campaign_lookup(client: KeitaroClient) -> Dict[str, Dict[str, Any]]:
    by_alias: Dict[str, Dict[str, Any]] = {}
    by_name: Dict[str, Dict[str, Any]] = {}
    offset = 0
    while True:
        batch = client.get_campaigns(offset=offset, limit=100)
        if not batch:
            break
        for c in batch:
            alias = (c.get("alias") or "").strip().lower()
            name = (c.get("name") or "").strip().lower()
            if alias:
                by_alias[alias] = c
            if name:
                by_name[name] = c
        if len(batch) < 100:
            break
        offset += 100
    return {"alias": by_alias, "name": by_name}


def _saved_child_matches_spec(saved: Dict[str, Any], spec: ChildCampaignSpec) -> bool:
    """Reject stale state (e.g. nipuhim child still pointing at KL-Main-feed*)."""
    if not saved.get("id"):
        return False
    saved_name = (saved.get("name") or "").strip().lower()
    return saved_name == spec.name.strip().lower()


def _resolve_child_campaign(
    spec: ChildCampaignSpec,
    lookup: Dict[str, Dict[str, Dict[str, Any]]],
    state: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    key = _child_key(spec.hub_type, spec.feed_key)
    saved = (state.get("child_campaigns") or {}).get(key) or {}
    if _saved_child_matches_spec(saved, spec):
        return saved
    if spec.existing_campaign_id:
        return {"id": spec.existing_campaign_id, "alias": spec.existing_alias, "name": spec.name}
    if spec.existing_alias:
        found = lookup["alias"].get(spec.existing_alias.strip().lower())
        if found:
            return {"id": found.get("id"), "alias": found.get("alias"), "name": found.get("name")}
    if spec.name:
        found = lookup["name"].get(spec.name.strip().lower())
        if found:
            return {"id": found.get("id"), "alias": found.get("alias"), "name": found.get("name")}
    return None


def _clear_stream_offers(client: KeitaroClient, campaign_id: int) -> int:
    cleared = 0
    for stream in client.get_streams(int(campaign_id)):
        sid = stream.get("id")
        if sid is None or not (stream.get("offers") or []):
            continue
        client.update_stream(int(sid), {"offers": []})
        cleared += 1
    return cleared


def ensure_child_campaigns(
    *,
    dry_run: bool = True,
    client: Optional[KeitaroClient] = None,
    state: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Ensure 12 child campaigns exist. Clones Blend (id 2) or KL-Main-feed1 (id 3) when needed.
    Returns updated state fragment and log lines.
    """
    client = client or KeitaroClient()
    state = dict(state or load_hub_state())
    child_state: Dict[str, Any] = dict(state.get("child_campaigns") or {})
    logs: List[str] = []
    lookup = _campaign_lookup(client)

    for spec in CHILD_SPECS:
        key = _child_key(spec.hub_type, spec.feed_key)
        existing = _resolve_child_campaign(spec, lookup, state)
        if existing and existing.get("id"):
            child_state[key] = {
                "id": int(existing["id"]),
                "alias": existing.get("alias") or spec.alias,
                "name": existing.get("name") or spec.name,
                "feed_key": spec.feed_key,
                "hub_type": spec.hub_type,
            }
            logs.append(f"child {key}: reuse id={existing['id']} ({existing.get('name')})")
            continue

        if not spec.clone_from_campaign_id:
            raise ValueError(f"No existing or clone template for child {key}")

        if dry_run:
            logs.append(
                f"child {key}: would clone campaign {spec.clone_from_campaign_id} "
                f"-> {spec.name!r} alias={spec.alias!r}"
            )
            child_state[key] = {
                "id": None,
                "alias": spec.alias,
                "name": spec.name,
                "feed_key": spec.feed_key,
                "hub_type": spec.hub_type,
                "_pending": True,
            }
            continue

        cloned = client.clone_campaign(int(spec.clone_from_campaign_id))
        cid = cloned.get("id")
        if cid is None:
            raise KeitaroClientError(f"Clone did not return id for {key}: {cloned}")
        client.update_campaign(int(cid), {"name": spec.name, "alias": spec.alias})
        cleared = _clear_stream_offers(client, int(cid))
        child_state[key] = {
            "id": int(cid),
            "alias": spec.alias,
            "name": spec.name,
            "feed_key": spec.feed_key,
            "hub_type": spec.hub_type,
            "cloned_from": spec.clone_from_campaign_id,
        }
        logs.append(
            f"child {key}: created id={cid} name={spec.name!r} "
            f"(cloned {spec.clone_from_campaign_id}, cleared {cleared} streams)"
        )

    state["child_campaigns"] = child_state
    return state, logs


def hub_active_feed_keys() -> frozenset[str]:
    """Feed keys that receive non-zero hub traffic (from ``KEITARO_HUB_ACTIVE_FEEDS``)."""
    active = frozenset(k for k in KEITARO_HUB_ACTIVE_FEEDS if k in HUB_FEED_KEYS)
    if not active:
        raise ValueError(
            f"KEITARO_HUB_ACTIVE_FEEDS has no valid keys; expected subset of {HUB_FEED_KEYS}"
        )
    return active


def configured_hub_types() -> Tuple[str, ...]:
    """Child campaign families wired on hub campaign 94 (``blend``, ``nipuhim``, or both)."""
    valid = frozenset(HUB_TYPES)
    out = tuple(t for t in KEITARO_HUB_TYPES if t in valid)
    if not out:
        raise ValueError(
            f"KEITARO_HUB_TYPES has no valid entries; expected subset of {HUB_TYPES}"
        )
    return out


def _spec_on_hub(spec: ChildCampaignSpec) -> bool:
    return spec.hub_type in configured_hub_types() and spec.feed_key in hub_active_feed_keys()


def hub_feed_weights() -> Dict[str, int]:
    """Per-feed weight numerators; inactive feeds are 0 (still attached on hub at share 0)."""
    active = hub_active_feed_keys()
    return {fk: (DEFAULT_FEED_WEIGHTS[fk] if fk in active else 0) for fk in HUB_FEED_KEYS}


HUB_CHILD_CAMPAIGN_URL_MACROS = (
    "keyword={keyword}&cost={adv_price}&external_id={clickid}"
    "&sub_id_1={hp}&sub_id_2={geo}&sub_id_3={oadest}&sub_id_4={traffic_type}"
    "&sub_id_5={sub_id}&sub_id_6={brand}&sub_id_7={pubid}&sub_id_8={ctrl_fetch_dest}"
    "&sub_id_9={domain}&sub_id_10={ctrl_ab}&sub_id_11={campaignId}&sub_id_12={campaignName}"
    "&sub_id_13={ctrl_pm_key}&sub_id_15=domain"
)

_HUB_RAIN_PREFIX = "https://shopli.city/raini?rain="


def wrap_hub_child_click_url(inner_url: str) -> str:
    """
    Wrap child campaign click URL for traffic sources (SK/ZP/EC pattern).

    Inner URL keeps Keitaro macros unencoded, e.g.
    ``https://shopli.city/raini?rain=https://trck.shopli.city/alias?external_id={clickid}...``
    """
    inner = (inner_url or "").strip()
    if not inner:
        raise ValueError("empty inner click url")
    shell = (KEITARO_HUB_RAIN_SHELL or _HUB_RAIN_PREFIX).strip()
    if inner.startswith(shell) or inner.lower().startswith(_HUB_RAIN_PREFIX.lower()):
        return inner
    return f"{shell}{inner}"


def child_campaign_click_url(
    client: KeitaroClient,
    campaign_id: int,
    *,
    alias: Optional[str] = None,
    domain: Optional[str] = None,
) -> str:
    """Hub offer URL: child campaign tracker link with standard passthrough macros."""
    if not alias or not domain:
        camp = client.get_campaign(int(campaign_id))
        domain = (camp.get("domain") or "").strip().rstrip("/")
        alias = (camp.get("alias") or alias or "").strip()
    if not domain or not alias:
        raise ValueError(f"Campaign {campaign_id} missing tracker domain or alias")
    return f"{domain}/{alias}?{HUB_CHILD_CAMPAIGN_URL_MACROS}"


def hub_offer_action_type() -> str:
    """Keitaro offer action_type for hub → child campaign links (default ``double_meta``)."""
    return (KEITARO_HUB_OFFER_ACTION_TYPE or "double_meta").strip() or "double_meta"


def hub_offer_click_url(
    client: KeitaroClient,
    campaign_id: int,
    *,
    alias: Optional[str] = None,
    domain: Optional[str] = None,
) -> str:
    """Hub campaign 94 offer URL: direct child campaign tracker link (no shopli rain shell)."""
    return child_campaign_click_url(client, campaign_id, alias=alias, domain=domain)


def _normalize_hub_offer_url(url: str) -> str:
    """Compare hub offer URLs ignoring legacy raini wrapper."""
    from assistance import strip_nipuhim_rain_shell

    return strip_nipuhim_rain_shell((url or "").strip())


def _hub_offer_needs_update(
    current: Dict[str, Any],
    expected_url: str,
    *,
    expected_action_type: Optional[str] = None,
) -> bool:
    """True when hub offer URL or action_type diverges from the v2 nipuhim shape."""
    action_type = expected_action_type or hub_offer_action_type()
    current_url = (current.get("action_payload") or "").strip()
    if _normalize_hub_offer_url(current_url) != _normalize_hub_offer_url(expected_url):
        return True
    return (current.get("action_type") or "").strip() != action_type


def _hub_campaign_link_offer_body(
    client: KeitaroClient,
    child_campaign_id: int,
    *,
    alias: Optional[str] = None,
) -> Dict[str, Any]:
    click_url = hub_offer_click_url(client, int(child_campaign_id), alias=alias)
    return {
        "action_type": hub_offer_action_type(),
        "action_options": {"campaign_id": int(child_campaign_id)},
        "action_payload": click_url,
    }


def _nipuhim_equal_global_weights() -> Dict[str, float]:
    """Fallback hub weights: equal share across active Kelkoo feeds (e.g. 33.33% × 3)."""
    active = sorted(hub_active_feed_keys())
    if not active:
        return {}
    share = 100.0 / len(active)
    return {hub_offer_name("nipuhim", fk): share for fk in active}


def _hub_offer_weights_legacy() -> Dict[str, float]:
    hub_types = configured_hub_types()
    if hub_types == ("nipuhim",):
        return _nipuhim_equal_global_weights()

    type_weights = {
        "blend": max(0, int(KEITARO_HUB_BLEND_PCT)) if "blend" in hub_types else 0,
        "nipuhim": max(0, int(KEITARO_HUB_NIPUHIM_PCT)) if "nipuhim" in hub_types else 0,
    }
    type_total = sum(type_weights.values()) or 100
    feed_weights = hub_feed_weights()
    feed_total = sum(feed_weights.values()) or 0
    out: Dict[str, float] = {}
    for hub_type in hub_types:
        type_frac = type_weights.get(hub_type, 0) / type_total
        for feed_key in HUB_FEED_KEYS:
            fw = feed_weights[feed_key]
            if fw <= 0 or feed_total <= 0:
                out[hub_offer_name(hub_type, feed_key)] = 0.0
                continue
            feed_frac = fw / feed_total
            out[hub_offer_name(hub_type, feed_key)] = type_frac * feed_frac * 100.0
    return out


@dataclass
class HubWeightContext:
    weights: Dict[str, float]
    blend_feed_caps: Dict[str, float] = field(default_factory=dict)
    nipuhim_feed_caps: Dict[str, float] = field(default_factory=dict)
    weights_by_geo: Dict[str, Dict[str, float]] = field(default_factory=dict)
    nipuhim_feed_geos: Dict[str, frozenset[str]] = field(default_factory=dict)
    source: str = "legacy"
    logs: List[str] = field(default_factory=list)


def resolve_hub_weight_context(
    *,
    date_str: Optional[str] = None,
    nipuhim_max_offers_per_geo: int = 60,
    use_click_caps: bool = True,
    sheets_service: Any = None,
) -> HubWeightContext:
    """Hub stream weights from sheet click caps, else legacy fixed ratios."""
    logs: List[str] = []
    if not use_click_caps:
        logs.append("Hub weights: legacy fixed ratios (click caps disabled)")
        return HubWeightContext(weights=_hub_offer_weights_legacy(), source="legacy", logs=logs)

    from integrations.hub_click_cap_weights import (
        blend_feed_click_caps,
        hub_nipuhim_equal_weights_per_geo,
        hub_offer_weights_from_caps,
        nipuhim_feed_active_geos,
        nipuhim_feed_offer_slots,
    )

    blend_caps, blend_logs = blend_feed_click_caps(sheets_service=sheets_service)
    nipuhim_caps, nipuhim_logs = nipuhim_feed_offer_slots(
        date_str=date_str,
        max_offers_per_geo=nipuhim_max_offers_per_geo,
    )
    logs.extend(blend_logs)
    logs.extend(nipuhim_logs)

    active = hub_active_feed_keys()
    hub_types = configured_hub_types()

    if hub_types == ("nipuhim",):
        feed_geos, geo_logs = nipuhim_feed_active_geos(
            date_str=date_str,
            max_offers_per_geo=nipuhim_max_offers_per_geo,
        )
        logs.extend(geo_logs)
        weights_by_geo, geo_weight_logs = hub_nipuhim_equal_weights_per_geo(
            feed_geos,
            active_feeds=active,
        )
        logs.extend(geo_weight_logs)
        if weights_by_geo:
            logs.append(
                f"Hub weights: per-geo equal split across feeds with offers "
                f"({len(weights_by_geo)} geo(s))"
            )
            return HubWeightContext(
                weights=_nipuhim_equal_global_weights(),
                blend_feed_caps=blend_caps,
                nipuhim_feed_caps=nipuhim_caps,
                weights_by_geo=weights_by_geo,
                nipuhim_feed_geos=feed_geos,
                source="per_geo_equal",
                logs=logs,
            )
        logs.append("Hub weights: no per-geo offer data — equal 33% global fallback")
        return HubWeightContext(
            weights=_nipuhim_equal_global_weights(),
            blend_feed_caps=blend_caps,
            nipuhim_feed_caps=nipuhim_caps,
            nipuhim_feed_geos=feed_geos,
            source="equal_global_fallback",
            logs=logs,
        )

    weights = hub_offer_weights_from_caps(
        blend_caps,
        nipuhim_caps,
        active_feeds=active,
        hub_types=hub_types,
    )
    if weights:
        blend_sum = sum(blend_caps.get(fk, 0) for fk in active)
        nipuhim_sum = sum(nipuhim_caps.get(fk, 0) for fk in active)
        logs.append(
            f"Hub weights: click-cap based "
            f"(blend total={blend_sum:g}, nipuhim slots={nipuhim_sum:g})"
        )
        return HubWeightContext(
            weights=weights,
            blend_feed_caps=blend_caps,
            nipuhim_feed_caps=nipuhim_caps,
            source="click_caps",
            logs=logs,
        )

    logs.append("Hub weights: no click-cap data — using legacy fixed ratios")
    return HubWeightContext(
        weights=_hub_offer_weights_legacy(),
        blend_feed_caps=blend_caps,
        nipuhim_feed_caps=nipuhim_caps,
        source="legacy",
        logs=logs,
    )


def _hub_offer_weights() -> Dict[str, float]:
    return resolve_hub_weight_context().weights


def _create_campaign_link_offer(
    client: KeitaroClient,
    name: str,
    campaign_id: int,
    *,
    alias: Optional[str] = None,
) -> Dict[str, Any]:
    payload = {
        "name": name,
        "offer_type": "external",
        "affiliate_network_id": 0,
        "group_id": 0,
        "state": "active",
        "payout_value": 0,
        "payout_currency": "USD",
        "payout_type": "CPA",
        "payout_auto": True,
        "payout_upsell": True,
    }
    payload.update(_hub_campaign_link_offer_body(client, int(campaign_id), alias=alias))
    return client.create_offer(payload)


def ensure_hub_offers(
    *,
    dry_run: bool = True,
    client: Optional[KeitaroClient] = None,
    state: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], List[str]]:
    """Create or reuse hub campaign-link offers (one per child campaign)."""
    client = client or KeitaroClient()
    state = dict(state or load_hub_state())
    child_state = state.get("child_campaigns") or {}
    if not child_state:
        raise ValueError("child_campaigns missing — run ensure_child_campaigns first")

    offer_state: Dict[str, Any] = dict(state.get("hub_offers") or {})
    offers_by_name = {(o.get("name") or "").strip(): o for o in client.get_offers()}
    logs: List[str] = []

    for spec in CHILD_SPECS:
        if not _spec_on_hub(spec):
            continue
        key = _child_key(spec.hub_type, spec.feed_key)
        offer_name = hub_offer_name(spec.hub_type, spec.feed_key)
        child = child_state.get(key)
        if not child:
            raise ValueError(f"Missing child campaign for {key}")
        if not child.get("id"):
            if dry_run:
                logs.append(
                    f"hub offer {offer_name}: would create -> child {spec.name!r} (pending)"
                )
                continue
            raise ValueError(f"Missing child campaign id for {key}")
        child_id = int(child["id"])
        child_alias = (child.get("alias") or spec.alias or "").strip() or None
        expected_url = hub_offer_click_url(
            client, child_id, alias=child_alias
        )
        saved = offer_state.get(offer_name) or {}
        if saved.get("id"):
            oid = int(saved["id"])
            repoint = int(saved.get("child_campaign_id") or 0) != child_id
            current = offers_by_name.get(offer_name) or {}
            needs_update = _hub_offer_needs_update(current, expected_url)
            if repoint or needs_update:
                if dry_run:
                    if repoint:
                        logs.append(
                            f"hub offer {offer_name}: would repoint id={oid} "
                            f"from campaign {saved.get('child_campaign_id')} -> {child_id}"
                        )
                    else:
                        logs.append(
                            f"hub offer {offer_name}: would set click URL on id={oid} "
                            f"({expected_url})"
                        )
                else:
                    client.update_offer(
                        oid,
                        _hub_campaign_link_offer_body(
                            client, child_id, alias=child_alias
                        ),
                    )
                    if repoint:
                        logs.append(
                            f"hub offer {offer_name}: repointed id={oid} -> campaign {child_id} "
                            f"({expected_url})"
                        )
                    else:
                        logs.append(
                            f"hub offer {offer_name}: set click URL id={oid} ({expected_url})"
                        )
            else:
                logs.append(f"hub offer {offer_name}: reuse id={oid}")
            offer_state[offer_name] = {
                "id": oid,
                "child_campaign_id": child_id,
                "click_url": expected_url,
                "hub_type": spec.hub_type,
                "feed_key": spec.feed_key,
            }
            continue
        existing = offers_by_name.get(offer_name)
        if existing and existing.get("id"):
            oid = int(existing["id"])
            if _hub_offer_needs_update(existing, expected_url):
                if dry_run:
                    logs.append(
                        f"hub offer {offer_name}: would set click URL on id={oid} "
                        f"({expected_url})"
                    )
                else:
                    client.update_offer(
                        oid,
                        _hub_campaign_link_offer_body(
                            client, child_id, alias=child_alias
                        ),
                    )
                    logs.append(
                        f"hub offer {offer_name}: set click URL id={oid} ({expected_url})"
                    )
            else:
                logs.append(f"hub offer {offer_name}: found id={oid}")
            offer_state[offer_name] = {
                "id": oid,
                "child_campaign_id": child_id,
                "click_url": expected_url,
                "hub_type": spec.hub_type,
                "feed_key": spec.feed_key,
            }
            continue
        if dry_run:
            logs.append(
                f"hub offer {offer_name}: would create -> child campaign id={child['id']}"
            )
            continue
        created = _create_campaign_link_offer(
            client, offer_name, int(child["id"]), alias=child_alias
        )
        oid = created.get("id")
        if oid is None:
            raise KeitaroClientError(f"Create hub offer {offer_name} failed: {created}")
        offer_state[offer_name] = {
            "id": int(oid),
            "child_campaign_id": int(child["id"]),
            "click_url": expected_url,
            "hub_type": spec.hub_type,
            "feed_key": spec.feed_key,
        }
        logs.append(
            f"hub offer {offer_name}: created id={oid} -> campaign {child['id']} "
            f"({expected_url})"
        )

    state["hub_offers"] = offer_state
    return state, logs


def repair_all_hub_offer_urls(
    *,
    dry_run: bool = True,
    client: Optional[KeitaroClient] = None,
    state: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Set every hub_* offer (blend + nipuhim, on-hub or not) to direct child URL + double_meta.

    ``ensure_hub_offers`` only touches offers wired on campaign 94 today; this repairs all
    hub campaign-link offers that already exist so re-enabling blend feeds does not need manual fixes.
    """
    client = client or KeitaroClient()
    state = dict(state or load_hub_state())
    child_state = state.get("child_campaigns") or {}
    offer_state: Dict[str, Any] = dict(state.get("hub_offers") or {})
    offers_by_name = {(o.get("name") or "").strip(): o for o in client.get_offers()}
    logs: List[str] = []

    for spec in CHILD_SPECS:
        key = _child_key(spec.hub_type, spec.feed_key)
        offer_name = hub_offer_name(spec.hub_type, spec.feed_key)
        child = child_state.get(key)
        if not child or not child.get("id"):
            logs.append(f"hub offer {offer_name}: no child campaign — skip")
            continue

        child_id = int(child["id"])
        child_alias = (child.get("alias") or spec.alias or "").strip() or None
        expected_url = hub_offer_click_url(client, child_id, alias=child_alias)

        saved = offer_state.get(offer_name) or {}
        oid = saved.get("id")
        current = offers_by_name.get(offer_name) or {}
        if not oid and current.get("id"):
            oid = int(current["id"])
        if not oid:
            logs.append(f"hub offer {offer_name}: no Keitaro offer — skip")
            continue
        oid = int(oid)

        if not current or int(current.get("id") or 0) != oid:
            for o in client.get_offers():
                if int(o.get("id") or 0) == oid:
                    current = o
                    break

        on_hub = _spec_on_hub(spec)
        tag = "" if on_hub else " (off-hub)"

        if _hub_offer_needs_update(current, expected_url):
            if dry_run:
                logs.append(
                    f"hub offer {offer_name}{tag}: would set id={oid} -> "
                    f"{hub_offer_action_type()} + direct URL"
                )
            else:
                client.update_offer(
                    oid,
                    _hub_campaign_link_offer_body(client, child_id, alias=child_alias),
                )
                logs.append(
                    f"hub offer {offer_name}{tag}: set id={oid} -> "
                    f"{hub_offer_action_type()} + direct URL"
                )
        else:
            logs.append(f"hub offer {offer_name}{tag}: reuse id={oid}")

        offer_state[offer_name] = {
            "id": oid,
            "child_campaign_id": child_id,
            "click_url": expected_url,
            "hub_type": spec.hub_type,
            "feed_key": spec.feed_key,
        }

    state["hub_offers"] = offer_state
    return state, logs


def _hub_device_streams(client: KeitaroClient, hub_campaign_id: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for stream in client.get_streams(int(hub_campaign_id)):
        geo, channel = parse_blend_stream_geo_channel(stream.get("name") or "")
        if geo and channel in ("desktop", "mobile"):
            out.append(stream)
    return out


def _is_hub_fallback_stream(stream: Dict[str, Any]) -> bool:
    name = (stream.get("name") or "").strip().lower()
    stype = (stream.get("type") or "").strip().lower()
    if stype == "default":
        return True
    return name == "fallback" or name.startswith("fallback")


def _device_stream_sort_key(stream: Dict[str, Any]) -> Tuple[str, str, int]:
    geo, channel = parse_blend_stream_geo_channel(stream.get("name") or "")
    ch_rank = 0 if channel == "desktop" else 1
    return (geo or "", channel or "", ch_rank)


def reorder_hub_streams_fallback_last(
    client: KeitaroClient,
    hub_campaign_id: int,
    *,
    dry_run: bool = False,
) -> List[str]:
    """
    Keep geo desktop/mobile flows before the catch-all fallback.

    New streams are often appended after the empty-filter fallback; without a reorder,
    that fallback steals all unmatched (and newly added) geo traffic.

    Keitaro requires unique positions, so we park streams at high temporary positions
    first, then assign the final 1..N order.
    """
    logs: List[str] = []
    streams = list(client.get_streams(int(hub_campaign_id)))
    device: List[Dict[str, Any]] = []
    fallback: List[Dict[str, Any]] = []
    other: List[Dict[str, Any]] = []
    for s in streams:
        geo, channel = parse_blend_stream_geo_channel(s.get("name") or "")
        if geo and channel in ("desktop", "mobile"):
            device.append(s)
        elif _is_hub_fallback_stream(s):
            fallback.append(s)
        else:
            other.append(s)

    if not device:
        return ["hub reorder: no device streams"]

    device_sorted = sorted(device, key=_device_stream_sort_key)
    # Preserve relative order of non-device / fallback; fallback always last.
    ordered = device_sorted + other + fallback
    expected_pos = {int(s["id"]): idx for idx, s in enumerate(ordered, start=1) if s.get("id")}
    changes: List[Tuple[int, str, int, int]] = []
    for s in streams:
        sid = s.get("id")
        if sid is None:
            continue
        sid_i = int(sid)
        want = expected_pos.get(sid_i)
        if want is None:
            continue
        have = int(s.get("position") or 0)
        if have != want:
            changes.append((sid_i, str(s.get("name") or ""), have, want))

    if not changes:
        logs.append("hub reorder: positions already correct (device flows before fallback)")
        return logs

    if dry_run:
        logs.append(f"hub reorder: would update {len(changes)} stream position(s)")
        for sid_i, name, have, want in changes[:8]:
            logs.append(f"  {name} id={sid_i}: {have} -> {want}")
        return logs

    # Phase 1: park *all* ordered streams at unique high positions (avoids unique conflicts).
    park_base = 10000
    for i, s in enumerate(ordered):
        sid_i = int(s["id"])
        client.update_stream(sid_i, {"position": park_base + i})

    # Phase 2: assign final 1..N (device flows first, fallback last).
    for want, s in enumerate(ordered, start=1):
        sid_i = int(s["id"])
        name = str(s.get("name") or "")
        have = int(s.get("position") or 0)
        client.update_stream(sid_i, {"position": want})
        if have != want:
            logs.append(f"hub reorder: {name} id={sid_i} position {have} -> {want}")
    logs.append(
        f"hub reorder: {len(device_sorted)} device + {len(other)} other + "
        f"{len(fallback)} fallback (fallback last)"
    )
    return logs


def ensure_hub_device_streams_for_geos(
    geos: List[str],
    *,
    dry_run: bool = False,
    client: Optional[KeitaroClient] = None,
    hub_campaign_id: Optional[int] = None,
    refresh_filters: bool = True,
) -> Dict[str, Any]:
    """
    Ensure campaign 94 has ``{geo}_desktop`` / ``{geo}_mobile`` for each geo.

    Also repairs country filters (``uk`` → Keitaro ``GB``) when mismatched and
    pushes fallback last.
    """
    from assistance import (
        _geo_for_api,
        assert_blend_stream_filters_sane,
        set_blend_device_stream_filters,
    )
    from geos import SUPPORTED_GEOS, normalize_geo

    client = client or KeitaroClient()
    hub_id = int(hub_campaign_id or KEITARO_HUB_CAMPAIGN_ID)
    wanted: List[str] = []
    seen: set[str] = set()
    for raw in geos:
        g = normalize_geo(raw)
        if not g or g in seen:
            continue
        if SUPPORTED_GEOS and g not in SUPPORTED_GEOS:
            continue
        seen.add(g)
        wanted.append(g)

    logs: List[str] = []
    created: List[str] = []
    refreshed: List[str] = []

    existing_streams = list(client.get_streams(hub_id))
    by_name = {
        (s.get("name") or "").strip().lower(): s for s in existing_streams
    }

    if dry_run:
        for geo in wanted:
            for channel in ("desktop", "mobile"):
                key = f"{geo}_{channel}"
                cur = by_name.get(key)
                if cur:
                    if refresh_filters:
                        logs.append(f"hub stream {key}: would check/repair filters")
                        refreshed.append(key)
                else:
                    logs.append(f"hub stream {key}: would create")
                    created.append(key)
        logs.extend(reorder_hub_streams_fallback_last(client, hub_id, dry_run=True))
        return {
            "created": created,
            "refreshed": refreshed,
            "logs": logs,
            "dry_run": True,
            "geos": wanted,
        }

    for geo in wanted:
        for channel in ("desktop", "mobile"):
            key = f"{geo}_{channel}"
            cur = by_name.get(key)
            if cur:
                if refresh_filters:
                    geo_code = _geo_for_api(geo)
                    try:
                        assert_blend_stream_filters_sane(
                            cur.get("filters") or [], channel, geo_code=geo_code
                        )
                    except ValueError:
                        sid = cur.get("id")
                        if sid is not None:
                            set_blend_device_stream_filters(int(sid), geo, channel)
                            refreshed.append(key)
                            logs.append(
                                f"hub stream {key}: repaired filters id={sid}"
                            )
                continue
            result = ensure_blend_device_stream(
                hub_id, geo, channel, skip_if_exists=True
            )
            if result.get("_skipped"):
                if result.get("_filters_repaired"):
                    refreshed.append(key)
                    logs.append(f"hub stream {key}: repaired filters id={result.get('id')}")
            else:
                created.append(key)
                logs.append(f"hub stream {key}: created id={result.get('id')}")
                # keep local map current for later iterations
                by_name[key] = result

    logs.extend(reorder_hub_streams_fallback_last(client, hub_id, dry_run=False))
    return {
        "created": created,
        "refreshed": refreshed,
        "logs": logs,
        "dry_run": False,
        "geos": wanted,
    }


def ensure_hub_routing_geos(
    *,
    extra_geos: Optional[List[str]] = None,
    bill_rows: Optional[List[Dict[str, Any]]] = None,
    dry_run: bool = False,
    client: Optional[KeitaroClient] = None,
    hub_campaign_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Ensure hub streams for every supported geo we may route (bill + known inventory).

    Defaults to all ``SUPPORTED_GEOS`` so Trillion segments never land on empty-filter fallback
    just because a geo stream was never bootstrapped on campaign 94.
    """
    from geos import SUPPORTED_GEOS

    geos: List[str] = list(SUPPORTED_GEOS)
    for row in bill_rows or []:
        g = str(row.get("geo") or "").strip().lower()
        if g:
            geos.append(g)
    for g in extra_geos or []:
        if g:
            geos.append(str(g).strip().lower())
    return ensure_hub_device_streams_for_geos(
        geos,
        dry_run=dry_run,
        client=client,
        hub_campaign_id=hub_campaign_id,
        refresh_filters=True,
    )


def ensure_hub_child_device_streams(
    *,
    dry_run: bool = True,
    client: Optional[KeitaroClient] = None,
    state: Optional[Dict[str, Any]] = None,
    hub_campaign_id: Optional[int] = None,
) -> Tuple[List[str], List[str]]:
    """
    Ensure each new Blend child campaign has the same geo desktop/mobile streams as the hub.
    """
    client = client or KeitaroClient()
    state = state or load_hub_state()
    hub_id = int(hub_campaign_id or state.get("hub_campaign_id") or KEITARO_HUB_CAMPAIGN_ID)
    hub_streams = _hub_device_streams(client, hub_id)
    geos_channels = []
    for s in hub_streams:
        geo, ch = parse_blend_stream_geo_channel(s.get("name") or "")
        if geo and ch:
            geos_channels.append((geo, ch))

    logs: List[str] = []
    for spec in CHILD_SPECS:
        if spec.hub_type != "blend":
            continue
        key = _child_key(spec.hub_type, spec.feed_key)
        child = (state.get("child_campaigns") or {}).get(key)
        if not child:
            continue
        if not child.get("id"):
            if dry_run and child.get("_pending"):
                for geo, channel in geos_channels:
                    logs.append(f"child {key}: would ensure stream {geo}_{channel}")
            continue
        cid = int(child["id"])
        if child.get("cloned_from") is None and not spec.clone_from_campaign_id:
            continue
        for geo, channel in geos_channels:
            if dry_run:
                logs.append(f"child {key}: would ensure stream {geo}_{channel}")
                continue
            result = ensure_blend_device_stream(
                cid, geo, channel, skip_if_exists=True
            )
            if result.get("_skipped"):
                logs.append(f"child {key}: stream {geo}_{channel} exists id={result.get('id')}")
            else:
                logs.append(f"child {key}: created stream {geo}_{channel} id={result.get('id')}")

    return logs, geos_channels


def _stream_hub_offer_weights(
    offer_state: Dict[str, Any],
    weights_by_offer_name: Dict[str, float],
    *,
    hub_types: Tuple[str, ...],
    active_feeds: frozenset[str],
) -> Tuple[Dict[int, float], List[int]]:
    """Map hub offer names → Keitaro offer ids with positive or zero stream share."""
    offer_id_to_weight: Dict[int, float] = {}
    zero_offer_ids: List[int] = []
    for offer_name, meta in offer_state.items():
        if str(meta.get("hub_type") or "") not in hub_types:
            continue
        if str(meta.get("feed_key") or "") not in active_feeds:
            continue
        oid = meta.get("id")
        if oid is None:
            continue
        w = float(weights_by_offer_name.get(offer_name, 0.0))
        if w > 0:
            offer_id_to_weight[int(oid)] = w
        else:
            zero_offer_ids.append(int(oid))
    return offer_id_to_weight, zero_offer_ids


def wire_hub_streams(
    *,
    dry_run: bool = True,
    client: Optional[KeitaroClient] = None,
    state: Optional[Dict[str, Any]] = None,
    hub_campaign_id: Optional[int] = None,
    date_str: Optional[str] = None,
    nipuhim_max_offers_per_geo: int = 60,
    use_click_caps: bool = True,
    weight_context: Optional[HubWeightContext] = None,
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Attach hub campaign-link offers to every geo desktop/mobile stream on the hub campaign.
    Replaces existing offers on those streams.
    """
    client = client or KeitaroClient()
    state = dict(state or load_hub_state())
    hub_id = int(hub_campaign_id or state.get("hub_campaign_id") or KEITARO_HUB_CAMPAIGN_ID)
    offer_state: Dict[str, Any] = dict(state.get("hub_offers") or {})
    wctx = weight_context or resolve_hub_weight_context(
        date_str=date_str,
        nipuhim_max_offers_per_geo=nipuhim_max_offers_per_geo,
        use_click_caps=use_click_caps,
    )
    logs: List[str] = list(wctx.logs)
    hub_types = configured_hub_types()
    active_feeds = hub_active_feed_keys()
    per_geo = wctx.weights_by_geo or {}
    use_per_geo = bool(per_geo) and hub_types == ("nipuhim",)

    ensure_res = ensure_hub_routing_geos(
        dry_run=dry_run, client=client, hub_campaign_id=hub_id
    )
    logs.extend(ensure_res.get("logs") or [])

    if not use_per_geo:
        weights_by_name = wctx.weights
        offer_id_to_weight, zero_offer_ids = _stream_hub_offer_weights(
            offer_state,
            weights_by_name,
            hub_types=hub_types,
            active_feeds=active_feeds,
        )
        if dry_run and not offer_id_to_weight and not zero_offer_ids:
            streams = _hub_device_streams(client, hub_id)
            logs.append(
                f"hub streams: would attach {len(offer_id_to_weight)} weighted + "
                f"{len(zero_offer_ids)} zero-share hub offers per geo stream "
                f"({len(streams)} streams on campaign {hub_id})"
            )
            state["hub_campaign_id"] = hub_id
            state["active_feeds"] = sorted(hub_active_feed_keys())
            state["hub_types"] = list(configured_hub_types())
            state["weight_source"] = wctx.source
            state["blend_feed_caps"] = wctx.blend_feed_caps
            state["nipuhim_feed_caps"] = wctx.nipuhim_feed_caps
            state["nipuhim_feed_geos"] = {
                fk: sorted(geos) for fk, geos in (wctx.nipuhim_feed_geos or {}).items()
            }
            return state, logs

        if not offer_id_to_weight and not zero_offer_ids:
            raise ValueError("hub_offers missing — run ensure_hub_offers first")
        if not offer_id_to_weight:
            raise ValueError(
                "No hub offers with positive weight — check KEITARO_HUB_ACTIVE_FEEDS "
                f"(active: {sorted(hub_active_feed_keys())})"
            )

        streams = _hub_device_streams(client, hub_id)
        if not streams:
            raise ValueError(f"Hub campaign {hub_id} has no geo desktop/mobile streams")

        for stream in streams:
            sid = stream.get("id")
            sname = stream.get("name") or ""
            if sid is None:
                continue
            old_count = len(stream.get("offers") or [])
            if dry_run:
                logs.append(
                    f"hub stream {sname} (id={sid}): would attach {len(offer_id_to_weight)} weighted + "
                    f"{len(zero_offer_ids)} zero-share offers (replacing {old_count})"
                )
                continue
            set_flow_offers_weighted_keep_zeros(
                int(sid), offer_id_to_weight, zero_offer_ids=zero_offer_ids
            )
            logs.append(
                f"hub stream {sname} (id={sid}): attached {len(offer_id_to_weight)} weighted + "
                f"{len(zero_offer_ids)} zero-share hub offers (replaced {old_count})"
            )
    else:
        streams = _hub_device_streams(client, hub_id)
        if not streams:
            raise ValueError(f"Hub campaign {hub_id} has no geo desktop/mobile streams")

        zero_template = {hub_offer_name("nipuhim", fk): 0.0 for fk in sorted(active_feeds)}
        for stream in streams:
            sid = stream.get("id")
            sname = stream.get("name") or ""
            if sid is None:
                continue
            geo, _channel = parse_blend_stream_geo_channel(sname)
            weights_by_name = per_geo.get(geo or "", zero_template)
            offer_id_to_weight, zero_offer_ids = _stream_hub_offer_weights(
                offer_state,
                weights_by_name,
                hub_types=hub_types,
                active_feeds=active_feeds,
            )
            old_count = len(stream.get("offers") or [])
            if dry_run:
                active_parts = [
                    f"{n.split('_')[-1]}={weights_by_name.get(n, 0):.1f}%"
                    for n in sorted(weights_by_name)
                    if weights_by_name.get(n, 0) > 0
                ]
                logs.append(
                    f"hub stream {sname} (id={sid}): would attach "
                    f"{len(offer_id_to_weight)} weighted + {len(zero_offer_ids)} zero-share "
                    f"({', '.join(active_parts) or 'no feeds for geo'})"
                )
                continue
            if not offer_id_to_weight and not zero_offer_ids:
                logs.append(f"hub stream {sname} (id={sid}): skip (no hub offers in state)")
                continue
            set_flow_offers_weighted_keep_zeros(
                int(sid), offer_id_to_weight, zero_offer_ids=zero_offer_ids
            )
            logs.append(
                f"hub stream {sname} (id={sid}): attached {len(offer_id_to_weight)} weighted + "
                f"{len(zero_offer_ids)} zero-share hub offers (replaced {old_count})"
            )

    state["hub_campaign_id"] = hub_id
    state["active_feeds"] = sorted(hub_active_feed_keys())
    state["hub_types"] = list(configured_hub_types())
    state["weight_source"] = wctx.source
    state["blend_feed_caps"] = wctx.blend_feed_caps
    state["nipuhim_feed_caps"] = wctx.nipuhim_feed_caps
    state["nipuhim_feed_geos"] = {
        fk: sorted(geos) for fk, geos in (wctx.nipuhim_feed_geos or {}).items()
    }
    return state, logs


def run_hub_repair_offer_urls(
    *,
    dry_run: bool = True,
    state_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Ensure all hub campaign-link offers use direct child tracker URLs (double_meta)."""
    client = KeitaroClient()
    state = load_hub_state(state_path)
    state, logs = repair_all_hub_offer_urls(dry_run=dry_run, client=client, state=state)
    if not dry_run:
        save_hub_state(state, state_path)
    return {
        "dry_run": dry_run,
        "hub_offers": state.get("hub_offers") or {},
        "logs": logs,
    }


def run_hub_rewire_weights(
    *,
    dry_run: bool = True,
    hub_campaign_id: Optional[int] = None,
    state_path: Optional[str] = None,
    date_str: Optional[str] = None,
    nipuhim_max_offers_per_geo: int = 60,
    use_click_caps: bool = True,
) -> Dict[str, Any]:
    """Re-apply hub stream weights only (no child/offer creation)."""
    client = KeitaroClient()
    state = load_hub_state(state_path)
    state["hub_campaign_id"] = int(
        hub_campaign_id or state.get("hub_campaign_id") or KEITARO_HUB_CAMPAIGN_ID
    )
    wctx = resolve_hub_weight_context(
        date_str=date_str,
        nipuhim_max_offers_per_geo=nipuhim_max_offers_per_geo,
        use_click_caps=use_click_caps,
    )
    state, logs = wire_hub_streams(
        dry_run=dry_run,
        client=client,
        state=state,
        hub_campaign_id=state["hub_campaign_id"],
        weight_context=wctx,
    )
    if not dry_run:
        save_hub_state(state, state_path)
    return {
        "dry_run": dry_run,
        "hub_campaign_id": state["hub_campaign_id"],
        "active_feeds": sorted(hub_active_feed_keys()),
        "weights": wctx.weights,
        "weights_by_geo": wctx.weights_by_geo,
        "nipuhim_feed_geos": {
            fk: sorted(geos) for fk, geos in (wctx.nipuhim_feed_geos or {}).items()
        },
        "weight_source": wctx.source,
        "blend_feed_caps": wctx.blend_feed_caps,
        "nipuhim_feed_caps": wctx.nipuhim_feed_caps,
        "logs": logs,
    }


def run_hub_bootstrap(
    *,
    dry_run: bool = True,
    skip_child_streams: bool = False,
    hub_campaign_id: Optional[int] = None,
    state_path: Optional[str] = None,
    date_str: Optional[str] = None,
    nipuhim_max_offers_per_geo: int = 60,
    use_click_caps: bool = True,
) -> Dict[str, Any]:
    """Full bootstrap: children -> hub offers -> optional child streams -> wire hub."""
    client = KeitaroClient()
    state = load_hub_state(state_path)
    state["hub_campaign_id"] = int(
        hub_campaign_id or state.get("hub_campaign_id") or KEITARO_HUB_CAMPAIGN_ID
    )
    all_logs: List[str] = []
    wctx = resolve_hub_weight_context(
        date_str=date_str,
        nipuhim_max_offers_per_geo=nipuhim_max_offers_per_geo,
        use_click_caps=use_click_caps,
    )

    state, logs = ensure_child_campaigns(dry_run=dry_run, client=client, state=state)
    all_logs.extend(logs)

    state, logs = ensure_hub_offers(dry_run=dry_run, client=client, state=state)
    all_logs.extend(logs)

    if not skip_child_streams:
        stream_logs, _ = ensure_hub_child_device_streams(
            dry_run=dry_run, client=client, state=state, hub_campaign_id=state["hub_campaign_id"]
        )
        all_logs.extend(stream_logs)

    state, logs = wire_hub_streams(
        dry_run=dry_run,
        client=client,
        state=state,
        hub_campaign_id=state["hub_campaign_id"],
        weight_context=wctx,
    )
    all_logs.extend(logs)

    if not dry_run:
        save_hub_state(state, state_path)

    return {
        "dry_run": dry_run,
        "hub_campaign_id": state["hub_campaign_id"],
        "child_campaigns": state.get("child_campaigns") or {},
        "hub_offers": state.get("hub_offers") or {},
        "active_feeds": sorted(hub_active_feed_keys()),
        "weights": wctx.weights,
        "weights_by_geo": wctx.weights_by_geo,
        "nipuhim_feed_geos": {
            fk: sorted(geos) for fk, geos in (wctx.nipuhim_feed_geos or {}).items()
        },
        "weight_source": wctx.source,
        "blend_feed_caps": wctx.blend_feed_caps,
        "nipuhim_feed_caps": wctx.nipuhim_feed_caps,
        "logs": all_logs,
    }


def format_weights_table(
    weights: Dict[str, float],
    *,
    source: str = "",
    weights_by_geo: Optional[Dict[str, Dict[str, float]]] = None,
) -> str:
    active = sorted(hub_active_feed_keys())
    lines = [
        "Hub offer weights (% of traffic):",
        f"  Hub types: {', '.join(configured_hub_types())}",
        f"  Active feeds: {', '.join(active)}",
    ]
    if source:
        lines.append(f"  Source: {source}")
    if weights_by_geo:
        lines.append("  Per-geo (equal split among feeds with offers today):")
        for geo in sorted(weights_by_geo):
            parts = [
                f"{name.replace('hub_nipuhim_', '')}={weights_by_geo[geo][name]:.1f}%"
                for name in sorted(weights_by_geo[geo])
                if weights_by_geo[geo].get(name, 0) > 0
            ]
            lines.append(f"    {geo}: {', '.join(parts)}")
    else:
        for name in sorted(weights.keys()):
            w = weights[name]
            tag = "" if w > 0 else " (zero — inactive feed)"
            lines.append(f"  {name}: {w:.2f}%{tag}")
    return "\n".join(lines)
