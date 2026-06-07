#!/usr/bin/env python3
"""
Read today's offers from Google Sheet and sync Keitaro.

Sequence:
  - Only geos that appear in the input sheet are processed (no other geos).
  - For each such geo, only the first N offers (column A=country, D=Store Link) are taken (default N=10;
    ``run_daily_workflow`` passes ``--max-offers 60`` to match the PLA sheet cap).
  - Keitaro is updated: ensure N offers per geo, attach to flow, set store links; excess offers
    are removed from the flow, then the script tries delete/archive (best-effort; sync still succeeds
    if the tracker returns 404 on those endpoints — offers stay as unattached orphans).
  - Writes 'live' and upload timestamp to columns G and H for updated rows.

Traffic split (Nipuhim):
  - ``--traffic-feed1-only`` / ``--traffic-feed2-only``: single-feed flows, equal share within the feed.
  - default ("both"): feed1/feed2/feed5 offers aggregate to ``FEED1_TRAFFIC_PCT`` / ``FEED2_TRAFFIC_PCT`` /
    ``FEED5_TRAFFIC_PCT`` (default 65% / 25% / 10%); each feed's offers split their bucket equally.
    Three-way split applies on geos present in today's ``_offers_1``, ``_offers_2``, and ``_offers_5`` tabs.

  python update_offers_from_sheet.py
  python update_offers_from_sheet.py --sheet "2026-03-10_offers"
  python update_offers_from_sheet.py --sheet "2026-03-10_offers" --max-offers 5
  python update_offers_from_sheet.py --sheet "2026-03-10_offers_2" --account 2   # second Kelkoo account
  python update_offers_from_sheet.py --sheet "2026-03-10_offers_5" --account 5   # feed5: merchant homepage URLs (Blend kelkoo5 format)

Requires credentials.json in project root (Google service account). Share the sheet with
the service account email (see credentials.json client_email).
"""
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv
load_dotenv()

from config import (
    KEITARO_BASE_URL,
    KEITARO_API_KEY,
    KEITARO_CAMPAIGN_ALIAS,
    KELKOO_ACCOUNT_ID,
    KELKOO_ACCOUNT_ID_2,
    FEED1_KELKOO_ACCOUNT_ID,
    FEED5_KELKOO_ACCOUNT_ID,
    FEED5_API_KEY,
)
from assistance import (
    add_country_flow,
    build_offer_action_payload,
    kelkoo_keitaro_action_payload,
    find_campaign_by_alias_or_name,
    get_campaigns_data,
    get_geo_offers_sorted,
    update_offer_action_payload,
    create_next_geo_offers,
    set_flow_offers,
    set_flow_offers_multi_feed_split,
    flow_name_to_geo,
    get_campaign_streams_by_alias,
    remove_offer_best_effort,
)
from geos import is_supported_geo
from integrations.keitaro import KeitaroClientError

# Sheet: https://docs.google.com/spreadsheets/d/1XUkQoWqnNRqaSEnFVRAV36-oi9ENrNWtH5Ct8M4vNuU/
SPREADSHEET_ID = "1XUkQoWqnNRqaSEnFVRAV36-oi9ENrNWtH5Ct8M4vNuU"
DEFAULT_SHEET_NAME = "2026-03-09_offers"
MAX_OFFERS_PER_GEO = 10

# Nipuhim 'both' mode traffic split. Sum must equal 100.
FEED1_TRAFFIC_PCT = 65
FEED2_TRAFFIC_PCT = 25
FEED5_TRAFFIC_PCT = 10
_OFFERS_SHEET_DATE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})_offers_([125])$")
COL_COUNTRY = 0   # A
COL_MERCHANT_ID = 1  # B
COL_STORE_LINK = 3  # D
# Columns we write back: G = live, H = offerUpload timestamp
COL_LIVE = 6              # G (0-based index)
COL_UPLOAD_TIMESTAMP = 7  # H (0-based index)


def _offers_date_from_sheet(sheet_name: str) -> str | None:
    m = _OFFERS_SHEET_DATE_RE.match((sheet_name or "").strip())
    return m.group(1) if m else None


def _daily_offers_sheet_names(date_str: str) -> list[str]:
    return [f"{date_str}_offers_1", f"{date_str}_offers_2", f"{date_str}_offers_5"]


def _traffic_split_summary(ids1: list, ids2: list, ids5: list | None = None) -> str:
    parts = [
        f"feed1={len(ids1)} @ {FEED1_TRAFFIC_PCT}%",
        f"feed2={len(ids2)} @ {FEED2_TRAFFIC_PCT}%",
    ]
    if ids5 is not None:
        parts.append(f"feed5={len(ids5)} @ {FEED5_TRAFFIC_PCT}%")
    return " / ".join(parts)


def get_credentials_path():
    p = Path(__file__).resolve().parent / "credentials.json"
    if not p.exists():
        raise FileNotFoundError(
            f"credentials.json not found at {p}. Copy your Google service account JSON there."
        )
    return str(p)


def read_sheet_today_offers(
    sheet_name: str,
    max_per_geo: int = MAX_OFFERS_PER_GEO,
    *,
    include_merchant_id: bool = False,
):
    """
    Read sheet: columns A (country) and D (Store Link). Return (by_geo, rows_by_geo)
    where by_geo[geo] = [link1, link2, ...] and rows_by_geo[geo] = [row1, row2, ...] (1-based).
    When ``include_merchant_id=True``, also returns merchant_ids_by_geo[geo] aligned with links.
    """
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError

    creds_path = get_credentials_path()
    creds = service_account.Credentials.from_service_account_file(creds_path)
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()
    range_name = f"'{sheet_name}'!A:D"
    try:
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=range_name).execute()
    except HttpError as e:
        # Missing sheet/tab often surfaces as "Unable to parse range: '<tab>'!A:D".
        msg = str(e)
        if "Unable to parse range" in msg:
            return {}, {}
        raise
    rows = result.get("values") or []
    by_geo = defaultdict(list)
    rows_by_geo = defaultdict(list)
    merchant_ids_by_geo = defaultdict(list)
    for row_idx, row in enumerate(rows):
        if len(row) <= max(COL_COUNTRY, COL_STORE_LINK):
            continue
        geo = (row[COL_COUNTRY] or "").strip().lower()
        link = (row[COL_STORE_LINK] or "").strip()
        if not geo or not link:
            continue
        if geo in ("country", "geo"):  # skip header
            continue
        if len(by_geo[geo]) < max_per_geo:
            row_num = row_idx + 1  # 1-based for Sheets API
            by_geo[geo].append(link)
            rows_by_geo[geo].append(row_num)
            if include_merchant_id:
                mid = (row[COL_MERCHANT_ID] or "").strip() if len(row) > COL_MERCHANT_ID else ""
                merchant_ids_by_geo[geo].append(mid)
    if include_merchant_id:
        return dict(by_geo), dict(rows_by_geo), dict(merchant_ids_by_geo)
    return dict(by_geo), dict(rows_by_geo)


def write_upload_status_to_sheet(
    sheet_name: str,
    row_numbers: list,
    timestamp: str,
) -> None:
    """
    Write 'live' and offerUpload timestamp to columns G and H for the given 1-based row numbers.
    Sets G1:H1 headers if needed, then G{row}:H{row} = ['live', timestamp] for each row.
    """
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    if not row_numbers:
        return
    creds_path = get_credentials_path()
    creds = service_account.Credentials.from_service_account_file(creds_path)
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()
    quoted = sheet_name.replace("'", "''")
    data = [
        {"range": f"'{quoted}'!G1:H1", "values": [["live", "offerUpload timestamp"]]},
    ]
    for row in sorted(row_numbers):
        data.append({"range": f"'{quoted}'!G{row}:H{row}", "values": [["live", timestamp]]})
    body = {"valueInputOption": "USER_ENTERED", "data": data}
    sheet.values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()


def main():
    if not KEITARO_BASE_URL or not KEITARO_API_KEY:
        print("Error: Set KEITARO_BASE_URL and KEITARO_API_KEY in .env")
        sys.exit(1)

    argv = sys.argv[1:]
    sheet_name = DEFAULT_SHEET_NAME
    max_offers = MAX_OFFERS_PER_GEO
    account = 1
    traffic_mode = "both"  # "both" | "feed1-only" | "feed2-only"
    i = 0
    while i < len(argv):
        if argv[i] == "--sheet" and i + 1 < len(argv):
            sheet_name = argv[i + 1]
            i += 2
            continue
        if argv[i] == "--max-offers" and i + 1 < len(argv):
            max_offers = int(argv[i + 1])
            i += 2
            continue
        if argv[i] == "--account" and i + 1 < len(argv):
            account = int(argv[i + 1])
            i += 2
            continue
        if argv[i] == "--traffic-feed1-only":
            traffic_mode = "feed1-only"
            i += 1
            continue
        if argv[i] == "--traffic-feed2-only":
            traffic_mode = "feed2-only"
            i += 1
            continue
        i += 1

    if account == 2:
        feed_prefix = "feed2"
        kelkoo_account_id = KELKOO_ACCOUNT_ID_2 or None
    elif account == 5:
        feed_prefix = "feed5"
        kelkoo_account_id = FEED5_KELKOO_ACCOUNT_ID or KELKOO_ACCOUNT_ID
    else:
        feed_prefix = "feed1"
        kelkoo_account_id = FEED1_KELKOO_ACCOUNT_ID or KELKOO_ACCOUNT_ID
    feed = 1 if account == 5 else account

    merchant_ids_by_geo: dict[str, list[str]] = {}
    merchant_url_by_geo_id: dict[tuple[str, str], str] = {}
    merchant_url_by_id: dict[str, str] = {}
    if account == 5 and not (FEED5_API_KEY or "").strip():
        print("Error: FEED5_API_KEY is required for --account 5 (feed5 Kelkoo sync).")
        sys.exit(1)

    print(f"Reading sheet: {sheet_name} (spreadsheet {SPREADSHEET_ID})")
    print(f"Max offers per geo: {max_offers}, feed: {feed_prefix} (account {account})")
    try:
        if account == 5:
            by_geo, rows_by_geo, merchant_ids_by_geo = read_sheet_today_offers(
                sheet_name, max_per_geo=max_offers, include_merchant_id=True
            )
        else:
            by_geo, rows_by_geo = read_sheet_today_offers(sheet_name, max_per_geo=max_offers)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading sheet: {e}")
        sys.exit(1)

    if account == 5 and by_geo:
        from workflows.monthly_log_monetization import build_merchant_geo_url_lookup

        sheet_geos = sorted(by_geo.keys())
        print(f"Feed5: loading merchant homepages for geos: {', '.join(sheet_geos)}")
        merchant_url_by_geo_id, merchant_url_by_id = build_merchant_geo_url_lookup(
            FEED5_API_KEY, geos=sheet_geos
        )

    if not by_geo:
        print("No data found in sheet (columns A = country, D = Store Link).")
        sys.exit(0)

    # In "both" mode, only split traffic on geos present on all today's Nipuhim offer tabs.
    split_geos_all: set[str] = set()
    split_geos_pair: set[str] = set()
    if traffic_mode == "both":
        date_key = _offers_date_from_sheet(sheet_name)
        if date_key:
            geos_per_sheet: list[set[str]] = []
            for tab in _daily_offers_sheet_names(date_key):
                try:
                    tab_by_geo, _ = read_sheet_today_offers(tab, max_per_geo=max_offers)
                    geos_per_sheet.append(
                        {
                            (g or "").strip().lower()[:2]
                            for g in tab_by_geo.keys()
                            if len((g or "").strip()) >= 2
                        }
                    )
                except Exception as e:
                    print(f"Warning: could not read offers sheet {tab!r} for split geos: {e}")
                    geos_per_sheet.append(set())
            if len(geos_per_sheet) >= 2 and geos_per_sheet[0] and geos_per_sheet[1]:
                split_geos_pair = geos_per_sheet[0] & geos_per_sheet[1]
            if len(geos_per_sheet) >= 3 and all(geos_per_sheet[:3]):
                split_geos_all = geos_per_sheet[0] & geos_per_sheet[1] & geos_per_sheet[2]
        else:
            print(
                "Warning: could not infer dated offers tabs for feed split; "
                "defaulting to feed-only routing on geos without multi-tab rows."
            )

    print(f"Geos in sheet: {list(by_geo.keys())}")
    if traffic_mode == "both":
        if split_geos_all:
            print(f"Three-feed split geos (offers_1+_2+_5): {sorted(split_geos_all)}")
        elif split_geos_pair:
            print(f"Two-feed split geos (offers_1+_2): {sorted(split_geos_pair)}")
        else:
            print("Multi-feed split geos: none (single-feed routing for geos in this run)")
    print()

    def feed_offer_ids_for_geo(geo: str):
        """Return (feed1_ids, feed2_ids, feed5_ids) for this geo."""
        o1 = get_geo_offers_sorted(geo, feed_prefix="feed1")
        o2 = get_geo_offers_sorted(geo, feed_prefix="feed2")
        o5 = get_geo_offers_sorted(geo, feed_prefix="feed5")
        ids1 = [int(o["id"]) for o in o1]
        ids2 = [int(o["id"]) for o in o2]
        ids5 = [int(o["id"]) for o in o5]
        if traffic_mode == "feed1-only":
            return ids1, [], []
        if traffic_mode == "feed2-only":
            return [], ids2, []
        g = (geo or "").strip().lower()[:2]
        if g in split_geos_all:
            return ids1, ids2, ids5
        if g in split_geos_pair:
            return ids1, ids2, []
        if account == 1:
            return ids1, [], []
        if account == 2:
            return [], ids2, []
        return [], [], ids5

    def flow_offer_ids_for_geo(geo: str):
        ids1, ids2, ids5 = feed_offer_ids_for_geo(geo)
        return ids1 + ids2 + ids5

    def apply_flow_offers(stream_id: int, feed1_ids, feed2_ids, feed5_ids=None) -> None:
        feed5_ids = feed5_ids or []
        n1, n2, n5 = len(feed1_ids), len(feed2_ids), len(feed5_ids)
        if traffic_mode == "feed1-only":
            if n1:
                set_flow_offers(stream_id, feed1_ids)
            return
        if traffic_mode == "feed2-only":
            if n2:
                set_flow_offers(stream_id, feed2_ids)
            return
        if n1 == 0 and n2 == 0 and n5 == 0:
            return
        buckets = []
        if n1:
            buckets.append((feed1_ids, FEED1_TRAFFIC_PCT))
        if n2:
            buckets.append((feed2_ids, FEED2_TRAFFIC_PCT))
        if n5:
            buckets.append((feed5_ids, FEED5_TRAFFIC_PCT))
        set_flow_offers_multi_feed_split(stream_id, buckets)

    # Resolve flows by geo so we can attach new offers when we create them
    campaign_alias = KEITARO_CAMPAIGN_ALIAS or "HrQBXp"
    try:
        streams = get_campaign_streams_by_alias(campaign_alias)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)
    stream_by_geo = {}
    for s in streams:
        g = flow_name_to_geo(s.get("name") or "")
        if g is not None:
            stream_by_geo[g] = s
    print(f"Campaign alias={campaign_alias}, flows resolved for {len(stream_by_geo)} geos")

    campaigns = get_campaigns_data()
    campaign = find_campaign_by_alias_or_name(
        campaigns, alias=campaign_alias, name=campaign_alias
    )
    if not campaign or campaign.get("id") is None:
        print(f"Error: campaign {campaign_alias!r} has no id")
        sys.exit(1)
    campaign_id = int(campaign["id"])

    def ensure_country_stream(geo: str) -> None:
        """If the campaign has no flow for this geo, create one and attach current offers."""
        if stream_by_geo.get(geo) is not None:
            return
        if not is_supported_geo(geo):
            print(
                f"  {geo}: no Keitaro flow and geo not in supported list — "
                "add a flow manually or extend geos.SUPPORTED_GEOS"
            )
            return
        offer_ids = flow_offer_ids_for_geo(geo)
        if not offer_ids:
            return
        try:
            created = add_country_flow(
                campaign_id,
                geo,
                geo,
                offer_ids=offer_ids,
                skip_if_exists=True,
            )
            stream_by_geo[geo] = created
            note = "matched existing" if created.get("_skipped") else "created"
            print(f"  {geo}: country flow id={created.get('id')} ({note})")
        except (KeitaroClientError, ValueError) as e:
            print(f"  {geo}: could not add country flow: {e}")

    print()

    updated_total = 0
    rows_updated = []  # 1-based row numbers we wrote to Keitaro (for sheet timestamp + live)
    for geo, store_links in sorted(by_geo.items()):
        need_count = min(len(store_links), max_offers)
        offers = get_geo_offers_sorted(geo, feed_prefix=feed_prefix)
        if len(offers) < need_count:
            new_count = need_count - len(offers)
            print(f"  {geo}: only {len(offers)} offers, creating {new_count} more (target {need_count}) ...")
            try:
                new_ids = create_next_geo_offers(
                    geo, count=new_count, feed_prefix=feed_prefix, account_id=kelkoo_account_id, feed=feed
                )
                print(f"  {geo}: created {new_ids}")
                # Refresh so we have only feed-prefixed offers (no old-format fr_product1 etc.)
                offers = get_geo_offers_sorted(geo, feed_prefix=feed_prefix)
                stream = stream_by_geo.get(geo)
                if stream:
                    ids1, ids2, ids5 = feed_offer_ids_for_geo(geo)
                    apply_flow_offers(int(stream["id"]), ids1, ids2, ids5)
                    total = len(ids1) + len(ids2) + len(ids5)
                    if traffic_mode == "both":
                        print(
                            f"  {geo}: flow id={stream['id']} now has {total} offers "
                            f"({_traffic_split_summary(ids1, ids2, ids5)})"
                        )
                    else:
                        print(f"  {geo}: flow id={stream['id']} now has {total} offers ({traffic_mode})")
                else:
                    ensure_country_stream(geo)
                    stream = stream_by_geo.get(geo)
                    if stream:
                        ids1, ids2, ids5 = feed_offer_ids_for_geo(geo)
                        if ids1 or ids2 or ids5:
                            apply_flow_offers(int(stream["id"]), ids1, ids2, ids5)
                            total = len(ids1) + len(ids2) + len(ids5)
                            if traffic_mode == "both":
                                print(
                                    f"  {geo}: flow id={stream['id']} now has {total} offers "
                                    f"({_traffic_split_summary(ids1, ids2, ids5)})"
                                )
                            else:
                                print(
                                    f"  {geo}: flow id={stream['id']} now has {total} offers ({traffic_mode})"
                                )
            except KeitaroClientError as e:
                print(f"  {geo}: ERROR creating/attaching offers: {e}")
                if e.response_body:
                    print(f"      {e.response_body[:300]}")
                sys.exit(1)
        # Drop excess offers: shrink flow first, then delete offers (archive endpoint 404s on some Keitaro builds).
        if len(offers) > need_count:
            keep_offers = offers[:need_count]
            remove_offers = offers[need_count:]
            print(
                f"  {geo}: {len(offers)} offers > {need_count}, "
                f"detaching {len(remove_offers)} from flow then deleting ..."
            )
            try:
                stream = stream_by_geo.get(geo)
                if stream:
                    keep_ids = [int(o["id"]) for o in keep_offers]
                    if traffic_mode == "both":
                        if feed_prefix == "feed1":
                            ids1 = keep_ids
                            ids2 = [
                                int(o["id"])
                                for o in get_geo_offers_sorted(geo, feed_prefix="feed2")
                            ]
                            ids5 = [
                                int(o["id"])
                                for o in get_geo_offers_sorted(geo, feed_prefix="feed5")
                            ]
                        elif feed_prefix == "feed2":
                            ids1 = [
                                int(o["id"])
                                for o in get_geo_offers_sorted(geo, feed_prefix="feed1")
                            ]
                            ids2 = keep_ids
                            ids5 = [
                                int(o["id"])
                                for o in get_geo_offers_sorted(geo, feed_prefix="feed5")
                            ]
                        else:
                            ids1 = [
                                int(o["id"])
                                for o in get_geo_offers_sorted(geo, feed_prefix="feed1")
                            ]
                            ids2 = [
                                int(o["id"])
                                for o in get_geo_offers_sorted(geo, feed_prefix="feed2")
                            ]
                            ids5 = keep_ids
                        apply_flow_offers(int(stream["id"]), ids1, ids2, ids5)
                        total = len(ids1) + len(ids2) + len(ids5)
                        print(
                            f"  {geo}: flow id={stream['id']} now has {total} offers "
                            f"({_traffic_split_summary(ids1, ids2, ids5)})"
                        )
                    else:
                        if feed_prefix == "feed1":
                            apply_flow_offers(int(stream["id"]), keep_ids, [], [])
                        elif feed_prefix == "feed2":
                            apply_flow_offers(int(stream["id"]), [], keep_ids, [])
                        else:
                            apply_flow_offers(int(stream["id"]), [], [], keep_ids)
                        print(f"  {geo}: flow id={stream['id']} now has {len(keep_ids)} offers")
                for offer in remove_offers:
                    oid = int(offer["id"])
                    if remove_offer_best_effort(oid):
                        print(f"    removed {offer.get('name')} id={oid}")
                    else:
                        print(
                            f"    warning: offer {offer.get('name')} id={oid} not deleted by API "
                            "(detached from flow only; remove manually in Keitaro if needed)"
                        )
            except KeitaroClientError as e:
                print(f"  {geo}: ERROR updating flow for trim: {e}")
                if e.response_body:
                    print(f"      {e.response_body[:300]}")
                sys.exit(1)
            offers = keep_offers
        ensure_country_stream(geo)
        stream = stream_by_geo.get(geo)
        if stream:
            ids1, ids2, ids5 = feed_offer_ids_for_geo(geo)
            if ids1 or ids2 or ids5:
                apply_flow_offers(int(stream["id"]), ids1, ids2, ids5)
        if not offers:
            print(f"  {geo}: no Keitaro offers found, skip")
            continue
        # Match by position: first link -> first offer, etc.
        to_update = min(len(store_links), len(offers), max_offers)
        if to_update == 0:
            continue
        print(f"  {geo}: updating {to_update} offers with store links ...")
        geo_row_numbers = rows_by_geo.get(geo, [])
        geo_merchant_ids = merchant_ids_by_geo.get(geo, [])
        for i in range(to_update):
            offer = offers[i]
            link = store_links[i]
            if account == 5:
                mid = geo_merchant_ids[i] if i < len(geo_merchant_ids) else ""
                target_url = (
                    merchant_url_by_geo_id.get((geo, mid))
                    or merchant_url_by_id.get(mid, "")
                    if mid
                    else ""
                )
                if not target_url:
                    print(
                        f"    {offer.get('name')}: skip — no merchant homepage for geo={geo} merchant_id={mid or '?'}"
                    )
                    continue
                new_payload = kelkoo_keitaro_action_payload(geo, target_url, "kelkoo5")
                log_url = target_url
            else:
                new_payload = build_offer_action_payload(
                    geo, link, account_id=kelkoo_account_id, feed=feed
                )
                log_url = link
            try:
                update_offer_action_payload(offer["id"], new_payload)
                print(f"    {offer.get('name')} id={offer['id']} -> {log_url[:50]}...")
                updated_total += 1
                if i < len(geo_row_numbers):
                    rows_updated.append(geo_row_numbers[i])
            except KeitaroClientError as e:
                print(f"    {offer.get('name')}: ERROR {e}")
                if e.response_body:
                    print(f"      {e.response_body[:300]}")
                sys.exit(1)
    # Write back to sheet: 'live' and offerUpload timestamp for each row we updated
    if rows_updated:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            write_upload_status_to_sheet(sheet_name, rows_updated, ts)
            print(f"Sheet: wrote 'live' and offerUpload timestamp to {len(rows_updated)} rows (G,H).")
        except Exception as e:
            print(f"Warning: could not write timestamp/live to sheet: {e}")
    print()
    print(f"Done. Updated {updated_total} offers.")


if __name__ == "__main__":
    main()
