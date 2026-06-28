"""
Sync Blend sheet rows into per-feed BLEND-feed* Keitaro hub child campaigns.

Legacy ``blend_sync_from_sheet`` (campaign alias 9Xq9dSMh) is unchanged; this fills hub
children for pre-cutover / hub routing validation.
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Set, Tuple

from integrations.keitaro import KeitaroClient, KeitaroClientError
from integrations.keitaro_child_campaigns import blend_child_campaign_id

logger = logging.getLogger(__name__)

# Hub blend children exist for these feed keys (see integrations.keitaro_hub.CHILD_SPECS).
HUB_BLEND_FEED_KEYS: Tuple[str, ...] = (
    "kelkoo1",
    "kelkoo2",
    "kelkoo5",
    "adexa",
    "yadore",
    "shopnomix",
)


def sync_blend_feed_to_hub(
    feed_key: str,
    rows: List,
    *,
    only_geo: Optional[str] = None,
    client: Optional[KeitaroClient] = None,
) -> Tuple[int, int, int]:
    """
    Upsert offers and device streams for one feed on its BLEND-* hub child.

    Returns (created_offers, updated_offers, flows_created).
    """
    from blend_sync_from_sheet import (
        BlendRow,
        _blend_keitaro_action_payload,
        _get_offer_id_by_name,
        _streams_by_geo_channel,
        _sync_geo_device_streams,
        _upsert_offer,
        refresh_all_blend_device_stream_filters,
    )

    fk = (feed_key or "").strip().lower()
    campaign_id = blend_child_campaign_id(fk)
    client = client or KeitaroClient()

    feed_rows: List[BlendRow] = [
        r
        for r in rows
        if isinstance(r, BlendRow) and (r.feed_tag or "kelkoo1").strip().lower() == fk
    ]
    if only_geo:
        feed_rows = [r for r in feed_rows if r.geo == only_geo]

    print(
        f"Blend v2 sync: feed={fk} -> campaign_id={campaign_id} "
        f"({len(feed_rows)} sheet row(s))"
    )

    if not feed_rows:
        print(f"  No Blend rows for feed {fk!r}; skipping.")
        return 0, 0, 0

    rows_by_geo: Dict[str, List[BlendRow]] = {}
    for r in feed_rows:
        rows_by_geo.setdefault(r.geo, []).append(r)

    streams_map = _streams_by_geo_channel(campaign_id)
    all_offers_by_id: Dict[int, str] = {}
    try:
        for o in client.get_offers():
            oid = o.get("id")
            if oid is not None:
                all_offers_by_id[int(oid)] = (o.get("name") or "").strip()
    except KeitaroClientError as e:
        print(f"  Warning: could not list offers: {e}")

    created_offers = 0
    updated_offers = 0
    created_flows = 0

    for geo, geo_rows in sorted(rows_by_geo.items()):
        offer_id_to_row: Dict[int, BlendRow] = {}
        for r in geo_rows:
            name = r.offer_name
            before = _get_offer_id_by_name(client, name)
            action_payload = _blend_keitaro_action_payload(r.geo, r.offer_url, r.feed_tag)
            oid = _upsert_offer(client, name, action_payload)
            offer_id_to_row[oid] = r
            if before is None:
                created_offers += 1
            else:
                updated_offers += 1

        cf, _su = _sync_geo_device_streams(
            client,
            campaign_id,
            geo,
            offer_id_to_row,
            streams_map,
            all_offers_by_id,
        )
        created_flows += cf

    # Geos on this campaign with no rows for this feed: detach blend_* offers.
    for geo, ch_streams in streams_map.items():
        if geo in rows_by_geo:
            continue
        from blend_sync_from_sheet import _detach_blend_offers_from_stream, set_blend_device_stream_filters

        for channel, stream in ch_streams.items():
            if channel not in ("desktop", "mobile", "legacy"):
                continue
            if channel in ("desktop", "mobile"):
                sid = stream.get("id")
                if sid is not None:
                    try:
                        set_blend_device_stream_filters(int(sid), geo, channel)
                    except KeitaroClientError as e:
                        print(f"  Warning: could not refresh {geo}/{channel} filters: {e}")
            detached = _detach_blend_offers_from_stream(
                client, stream, all_offers_by_id=all_offers_by_id
            )
            if detached:
                print(f"  {geo}/{channel}: no {fk} rows — detached {detached} blend_* offer(s)")

    print(f"  Refreshing device filters on BLEND-{fk} ...")
    n_filters, filter_errors = refresh_all_blend_device_stream_filters(
        campaign_id, only_geo=only_geo
    )
    print(f"  Device filters refreshed on {n_filters} flow(s).")
    for err in filter_errors[:5]:
        print(f"  Warning: {err}")

    return created_offers, updated_offers, created_flows


def run_blend_v2_sync(
    service,
    *,
    only_geo: Optional[str] = None,
    feed_keys: Optional[List[str]] = None,
) -> bool:
    """
    Read Blend sheet and sync each feed to its BLEND-* hub child campaign.
  Returns True on success.
    """
    from blend_sync_from_sheet import (
        _suppress_auto_v_rows_without_mtd_sales,
        read_blend_rows,
    )

    rows = read_blend_rows(service, only_geo=only_geo)
    if not rows:
        print("Blend v2: no valid rows on Blend sheet.")
        return True

    rows_for_sync, suppressed = _suppress_auto_v_rows_without_mtd_sales(rows, service=service)
    if suppressed:
        print(
            f"Blend v2: suppressed {suppressed} auto='v' row(s) with 0 MTD sales "
            "(same gate as legacy sync)."
        )

    present_feeds: Set[str] = {
        (r.feed_tag or "kelkoo1").strip().lower() for r in rows_for_sync
    }
    if feed_keys:
        targets = [fk.strip().lower() for fk in feed_keys if fk.strip()]
    else:
        targets = [fk for fk in HUB_BLEND_FEED_KEYS if fk in present_feeds]

    if not targets:
        print("Blend v2: no hub feed keys to sync (no matching sheet rows).")
        return True

    print(f"Blend v2: syncing feeds {', '.join(targets)}")
    print()

    client = KeitaroClient()
    ok = True
    for fk in targets:
        if fk not in HUB_BLEND_FEED_KEYS:
            print(f"  Skip unknown hub blend feed {fk!r}")
            continue
        try:
            blend_child_campaign_id(fk)
        except ValueError as e:
            print(f"  Skip {fk}: {e}")
            continue
        try:
            c, u, f = sync_blend_feed_to_hub(
                fk,
                rows_for_sync,
                only_geo=only_geo,
                client=client,
            )
            print(f"  {fk}: created {c} offer(s), updated {u}, flows+{f}")
        except Exception as e:
            print(f"  {fk}: sync failed: {e}")
            ok = False
        print()

    return ok
