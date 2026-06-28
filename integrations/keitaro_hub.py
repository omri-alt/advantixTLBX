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
from dataclasses import dataclass
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
    KEITARO_HUB_STATE_PATH,
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


def hub_feed_weights() -> Dict[str, int]:
    """Per-feed weight numerators; inactive feeds are 0 (still attached on hub at share 0)."""
    active = hub_active_feed_keys()
    return {fk: (DEFAULT_FEED_WEIGHTS[fk] if fk in active else 0) for fk in HUB_FEED_KEYS}


def _hub_offer_weights() -> Dict[str, float]:
    type_weights = {
        "blend": max(0, int(KEITARO_HUB_BLEND_PCT)),
        "nipuhim": max(0, int(KEITARO_HUB_NIPUHIM_PCT)),
    }
    type_total = sum(type_weights.values()) or 100
    feed_weights = hub_feed_weights()
    feed_total = sum(feed_weights.values()) or 0
    out: Dict[str, float] = {}
    for hub_type in HUB_TYPES:
        type_frac = type_weights[hub_type] / type_total
        for feed_key in HUB_FEED_KEYS:
            fw = feed_weights[feed_key]
            if fw <= 0 or feed_total <= 0:
                out[hub_offer_name(hub_type, feed_key)] = 0.0
                continue
            feed_frac = fw / feed_total
            out[hub_offer_name(hub_type, feed_key)] = type_frac * feed_frac * 100.0
    return out


def _create_campaign_link_offer(
    client: KeitaroClient,
    name: str,
    campaign_id: int,
) -> Dict[str, Any]:
    payload = {
        "name": name,
        "offer_type": "external",
        "action_type": "campaign",
        "action_options": {"campaign_id": int(campaign_id)},
        "affiliate_network_id": 0,
        "group_id": 0,
        "state": "active",
        "payout_value": 0,
        "payout_currency": "USD",
        "payout_type": "CPA",
        "payout_auto": True,
        "payout_upsell": True,
    }
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
        saved = offer_state.get(offer_name) or {}
        if saved.get("id"):
            oid = int(saved["id"])
            if int(saved.get("child_campaign_id") or 0) != child_id:
                if dry_run:
                    logs.append(
                        f"hub offer {offer_name}: would repoint id={oid} "
                        f"from campaign {saved.get('child_campaign_id')} -> {child_id}"
                    )
                else:
                    client.update_offer(
                        oid,
                        {
                            "action_type": "campaign",
                            "action_options": {"campaign_id": child_id},
                        },
                    )
                    logs.append(
                        f"hub offer {offer_name}: repointed id={oid} -> campaign {child_id}"
                    )
                offer_state[offer_name] = {
                    "id": oid,
                    "child_campaign_id": child_id,
                    "hub_type": spec.hub_type,
                    "feed_key": spec.feed_key,
                }
            else:
                logs.append(f"hub offer {offer_name}: reuse id={oid}")
            continue
        existing = offers_by_name.get(offer_name)
        if existing and existing.get("id"):
            offer_state[offer_name] = {
                "id": int(existing["id"]),
                "child_campaign_id": int(child["id"]),
                "hub_type": spec.hub_type,
                "feed_key": spec.feed_key,
            }
            logs.append(f"hub offer {offer_name}: found id={existing['id']}")
            continue
        if dry_run:
            logs.append(
                f"hub offer {offer_name}: would create -> child campaign id={child['id']}"
            )
            continue
        created = _create_campaign_link_offer(client, offer_name, int(child["id"]))
        oid = created.get("id")
        if oid is None:
            raise KeitaroClientError(f"Create hub offer {offer_name} failed: {created}")
        offer_state[offer_name] = {
            "id": int(oid),
            "child_campaign_id": int(child["id"]),
            "hub_type": spec.hub_type,
            "feed_key": spec.feed_key,
        }
        logs.append(f"hub offer {offer_name}: created id={oid} -> campaign {child['id']}")

    state["hub_offers"] = offer_state
    return state, logs


def _hub_device_streams(client: KeitaroClient, hub_campaign_id: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for stream in client.get_streams(int(hub_campaign_id)):
        geo, channel = parse_blend_stream_geo_channel(stream.get("name") or "")
        if geo and channel in ("desktop", "mobile"):
            out.append(stream)
    return out


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


def wire_hub_streams(
    *,
    dry_run: bool = True,
    client: Optional[KeitaroClient] = None,
    state: Optional[Dict[str, Any]] = None,
    hub_campaign_id: Optional[int] = None,
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Attach hub campaign-link offers to every geo desktop/mobile stream on the hub campaign.
    Replaces existing offers on those streams.
    """
    client = client or KeitaroClient()
    state = dict(state or load_hub_state())
    hub_id = int(hub_campaign_id or state.get("hub_campaign_id") or KEITARO_HUB_CAMPAIGN_ID)
    offer_state: Dict[str, Any] = dict(state.get("hub_offers") or {})
    weights_by_name = _hub_offer_weights()
    offer_id_to_weight: Dict[int, float] = {}
    zero_offer_ids: List[int] = []
    for offer_name, meta in offer_state.items():
        oid = meta.get("id")
        if oid is None:
            continue
        w = weights_by_name.get(offer_name, 0.0)
        if w > 0:
            offer_id_to_weight[int(oid)] = w
        else:
            zero_offer_ids.append(int(oid))

    if dry_run and not offer_id_to_weight and not zero_offer_ids:
        streams = _hub_device_streams(client, hub_id)
        logs: List[str] = []
        logs.append(
            f"hub streams: would attach {len(offer_id_to_weight)} weighted + "
            f"{len(zero_offer_ids)} zero-share hub offers per geo stream "
            f"({len(streams)} streams on campaign {hub_id})"
        )
        state["hub_campaign_id"] = hub_id
        state["active_feeds"] = sorted(hub_active_feed_keys())
        return state, logs

    if not offer_id_to_weight and not zero_offer_ids:
        raise ValueError("hub_offers missing — run ensure_hub_offers first")
    if not offer_id_to_weight:
        raise ValueError(
            "No hub offers with positive weight — check KEITARO_HUB_ACTIVE_FEEDS "
            f"(active: {sorted(hub_active_feed_keys())})"
        )

    logs: List[str] = []
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

    state["hub_campaign_id"] = hub_id
    state["active_feeds"] = sorted(hub_active_feed_keys())
    return state, logs


def run_hub_rewire_weights(
    *,
    dry_run: bool = True,
    hub_campaign_id: Optional[int] = None,
    state_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Re-apply hub stream weights only (no child/offer creation)."""
    client = KeitaroClient()
    state = load_hub_state(state_path)
    state["hub_campaign_id"] = int(
        hub_campaign_id or state.get("hub_campaign_id") or KEITARO_HUB_CAMPAIGN_ID
    )
    state, logs = wire_hub_streams(
        dry_run=dry_run,
        client=client,
        state=state,
        hub_campaign_id=state["hub_campaign_id"],
    )
    if not dry_run:
        save_hub_state(state, state_path)
    return {
        "dry_run": dry_run,
        "hub_campaign_id": state["hub_campaign_id"],
        "active_feeds": sorted(hub_active_feed_keys()),
        "weights": _hub_offer_weights(),
        "logs": logs,
    }


def run_hub_bootstrap(
    *,
    dry_run: bool = True,
    skip_child_streams: bool = False,
    hub_campaign_id: Optional[int] = None,
    state_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Full bootstrap: children -> hub offers -> optional child streams -> wire hub."""
    client = KeitaroClient()
    state = load_hub_state(state_path)
    state["hub_campaign_id"] = int(
        hub_campaign_id or state.get("hub_campaign_id") or KEITARO_HUB_CAMPAIGN_ID
    )
    all_logs: List[str] = []

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
        "weights": _hub_offer_weights(),
        "logs": all_logs,
    }


def format_weights_table(weights: Dict[str, float]) -> str:
    active = sorted(hub_active_feed_keys())
    lines = [
        "Hub offer weights (% of traffic):",
        f"  Active feeds: {', '.join(active)}",
    ]
    for name in sorted(weights.keys()):
        w = weights[name]
        tag = "" if w > 0 else " (zero — inactive feed)"
        lines.append(f"  {name}: {w:.2f}%{tag}")
    return "\n".join(lines)
