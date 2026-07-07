"""
Sync today's offers sheet into a per-feed NIPUHIM-feed* Keitaro campaign (geo × device flows).

Legacy ``HrQBXp`` sync is unchanged; this populates hub child campaigns for pre-cutover testing.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from assistance import (
    build_nipuhim_v2_action_payload,
    create_next_geo_offers,
    ensure_blend_device_stream,
    get_campaign_streams,
    get_geo_offers_sorted,
    parse_blend_stream_geo_channel,
    remove_offer_best_effort,
    set_flow_offers,
    update_offer_action_payload,
)
from config import (
    FEED1_KELKOO_ACCOUNT_ID,
    FEED5_KELKOO_ACCOUNT_ID,
    KELKOO_ACCOUNT_ID,
    KELKOO_ACCOUNT_ID_2,
)
from geos import is_supported_geo
from integrations.keitaro import KeitaroClientError
from integrations.keitaro_child_campaigns import nipuhim_child_campaign_id_for_account

logger = logging.getLogger(__name__)


def _device_stream_map(campaign_id: int) -> Dict[Tuple[str, str], Dict]:
    out: Dict[Tuple[str, str], Dict] = {}
    for stream in get_campaign_streams(int(campaign_id)):
        geo, channel = parse_blend_stream_geo_channel(stream.get("name") or "")
        if geo and channel in ("desktop", "mobile"):
            out[(geo, channel)] = stream
    return out


def _ensure_device_streams(campaign_id: int, geo: str) -> Dict[Tuple[str, str], Dict]:
    for channel in ("desktop", "mobile"):
        ensure_blend_device_stream(int(campaign_id), geo, channel, skip_if_exists=True)
    return _device_stream_map(campaign_id)


def _attach_offers_to_device_streams(
    campaign_id: int,
    geo: str,
    offer_ids: List[int],
    stream_map: Optional[Dict[Tuple[str, str], Dict]] = None,
) -> None:
    if not offer_ids:
        return
    smap = stream_map or _device_stream_map(campaign_id)
    for channel in ("desktop", "mobile"):
        stream = smap.get((geo, channel))
        if stream and stream.get("id") is not None:
            set_flow_offers(int(stream["id"]), offer_ids)


def sync_sheet_to_nipuhim_v2(
    sheet_name: str,
    account: int,
    *,
    max_offers: int = 60,
) -> int:
    """
    Read offers sheet and sync into ``NIPUHIM-feed*`` campaign (device flows only).
    Returns process exit code (0 = success).
    """
    from update_offers_from_sheet import read_sheet_today_offers, write_upload_status_to_sheet

    campaign_id, feed_key, feed_prefix = nipuhim_child_campaign_id_for_account(account)
    if account == 2:
        kelkoo_account_id = KELKOO_ACCOUNT_ID_2 or None
        feed_num = 2
    elif account == 5:
        kelkoo_account_id = FEED5_KELKOO_ACCOUNT_ID or KELKOO_ACCOUNT_ID
        feed_num = 5
    else:
        kelkoo_account_id = FEED1_KELKOO_ACCOUNT_ID or KELKOO_ACCOUNT_ID
        feed_num = 1

    print(
        f"Nipuhim v2 sync: sheet={sheet_name!r} account={account} "
        f"-> {feed_key} campaign_id={campaign_id} (device flows)"
    )

    from integrations.nipuhim_stream_cleanup import (
        cleanup_nipuhim_campaign_streams,
        nipuhim_campaign_needs_stream_cleanup,
    )

    if nipuhim_campaign_needs_stream_cleanup(campaign_id):
        print(f"  Cleaning legacy/fallback stream order on campaign {campaign_id} ...")
        cleanup_logs, stats = cleanup_nipuhim_campaign_streams(
            campaign_id, dry_run=False, label=feed_key
        )
        for line in cleanup_logs:
            print(f"    {line}")

    try:
        by_geo, rows_by_geo = read_sheet_today_offers(sheet_name, max_per_geo=max_offers)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        return 1
    except Exception as e:
        print(f"Error reading sheet: {e}")
        return 1

    if not by_geo:
        print("No data in sheet (columns A=country, D=Store Link).")
        return 0

    stream_map = _device_stream_map(campaign_id)
    print(f"Campaign id={campaign_id} device streams loaded: {len(stream_map)}")
    print(f"Geos in sheet: {list(by_geo.keys())}")
    print()

    updated_total = 0
    rows_updated: List[int] = []

    for geo, store_links in sorted(by_geo.items()):
        if not is_supported_geo(geo):
            print(f"  {geo}: unsupported geo — skip")
            continue

        need_count = min(len(store_links), max_offers)
        offers = get_geo_offers_sorted(geo, feed_prefix=feed_prefix)

        if len(offers) < need_count:
            new_count = need_count - len(offers)
            print(f"  {geo}: only {len(offers)} offers, creating {new_count} more ...")
            try:
                start_idx = len(offers)
                new_urls = store_links[start_idx : start_idx + new_count]
                new_ids = create_next_geo_offers(
                    geo,
                    count=new_count,
                    feed_prefix=feed_prefix,
                    account_id=kelkoo_account_id,
                    feed=feed_num if feed_num != 5 else 1,
                    product_urls=new_urls or None,
                )
                print(f"  {geo}: created {new_ids}")
                offers = get_geo_offers_sorted(geo, feed_prefix=feed_prefix)
                stream_map = _ensure_device_streams(campaign_id, geo)
                keep_ids = [int(o["id"]) for o in offers[:need_count]]
                _attach_offers_to_device_streams(
                    campaign_id, geo, keep_ids, stream_map=stream_map
                )
                print(
                    f"  {geo}: attached {len(keep_ids)} offers to "
                    f"{geo}_desktop + {geo}_mobile"
                )
            except (KeitaroClientError, ValueError) as e:
                print(f"  {geo}: ERROR creating/attaching offers: {e}")
                return 1

        if len(offers) > need_count:
            keep_offers = offers[:need_count]
            remove_offers = offers[need_count:]
            print(
                f"  {geo}: {len(offers)} offers > {need_count}, "
                f"detaching {len(remove_offers)} from device flows ..."
            )
            try:
                stream_map = _ensure_device_streams(campaign_id, geo)
                keep_ids = [int(o["id"]) for o in keep_offers]
                _attach_offers_to_device_streams(
                    campaign_id, geo, keep_ids, stream_map=stream_map
                )
                for offer in remove_offers:
                    oid = int(offer["id"])
                    if remove_offer_best_effort(oid):
                        print(f"    removed {offer.get('name')} id={oid}")
                    else:
                        print(
                            f"    warning: offer {offer.get('name')} id={oid} "
                            "not deleted (detached from v2 flows only)"
                        )
            except KeitaroClientError as e:
                print(f"  {geo}: ERROR trimming offers: {e}")
                return 1
            offers = keep_offers

        stream_map = _ensure_device_streams(campaign_id, geo)
        offer_ids = [int(o["id"]) for o in offers[:need_count]]
        if offer_ids:
            _attach_offers_to_device_streams(
                campaign_id, geo, offer_ids, stream_map=stream_map
            )

        if not offers:
            print(f"  {geo}: no offers — skip")
            continue

        to_update = min(len(store_links), len(offers), max_offers)
        if to_update == 0:
            continue

        print(f"  {geo}: updating {to_update} offer payloads ...")
        geo_row_numbers = rows_by_geo.get(geo, [])
        for i in range(to_update):
            offer = offers[i]
            link = store_links[i]
            if account == 5:
                new_payload = build_nipuhim_v2_action_payload(geo, link, feed=5)
            else:
                new_payload = build_nipuhim_v2_action_payload(
                    geo, link, account_id=kelkoo_account_id, feed=feed_num if feed_num != 5 else 1
                )
            try:
                update_offer_action_payload(int(offer["id"]), new_payload)
                print(f"    {offer.get('name')} id={offer['id']} -> {link[:50]}...")
                updated_total += 1
                if i < len(geo_row_numbers):
                    rows_updated.append(geo_row_numbers[i])
            except KeitaroClientError as e:
                print(f"    {offer.get('name')}: ERROR {e}")
                return 1

    if rows_updated:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            write_upload_status_to_sheet(sheet_name, rows_updated, ts)
            print(f"Sheet: wrote live + timestamp to {len(rows_updated)} rows.")
        except Exception as e:
            print(f"Warning: could not write sheet status: {e}")

    print()
    print(f"Nipuhim v2 done. Updated {updated_total} offers on campaign {campaign_id}.")
    return 0
