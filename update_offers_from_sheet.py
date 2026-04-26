#!/usr/bin/env python3
"""
Read today's offers from Google Sheet and sync Keitaro.

Sequence:
  - Only geos that appear in the input sheet are processed (no other geos).
  - For each such geo, only the first N offers (column A=country, D=Store Link) are taken (default N=10;
    ``run_daily_workflow`` passes ``--max-offers 100`` to match the PLA sheet cap).
  - Keitaro is updated: ensure N offers per geo, attach to flow, set store links; excess offers
    are removed from the flow, then the script tries delete/archive (best-effort; sync still succeeds
    if the tracker returns 404 on those endpoints — offers stay as unattached orphans).
  - Writes 'live' and upload timestamp to columns G and H for updated rows.

  python update_offers_from_sheet.py
  python update_offers_from_sheet.py --sheet "2026-03-10_offers"
  python update_offers_from_sheet.py --sheet "2026-03-10_offers" --max-offers 5
  python update_offers_from_sheet.py --sheet "2026-03-10_offers_2" --account 2   # second Kelkoo account

Requires credentials.json in project root (Google service account). Share the sheet with
the service account email (see credentials.json client_email).
"""
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
)
from assistance import (
    add_country_flow,
    build_offer_action_payload,
    find_campaign_by_alias_or_name,
    get_campaigns_data,
    get_geo_offers_sorted,
    update_offer_action_payload,
    create_next_geo_offers,
    set_flow_offers,
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
COL_COUNTRY = 0   # A
COL_STORE_LINK = 3  # D
# Columns we write back: G = live, H = offerUpload timestamp
COL_LIVE = 6              # G (0-based index)
COL_UPLOAD_TIMESTAMP = 7  # H (0-based index)


def get_credentials_path():
    p = Path(__file__).resolve().parent / "credentials.json"
    if not p.exists():
        raise FileNotFoundError(
            f"credentials.json not found at {p}. Copy your Google service account JSON there."
        )
    return str(p)


def read_sheet_today_offers(sheet_name: str, max_per_geo: int = MAX_OFFERS_PER_GEO):
    """
    Read sheet: columns A (country) and D (Store Link). Return (by_geo, rows_by_geo)
    where by_geo[geo] = [link1, link2, ...] and rows_by_geo[geo] = [row1, row2, ...] (1-based).
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

    feed_prefix = "feed2" if account == 2 else "feed1"
    feed = account
    kelkoo_account_id = (FEED1_KELKOO_ACCOUNT_ID or KELKOO_ACCOUNT_ID) if account == 1 else (KELKOO_ACCOUNT_ID_2 or None)

    print(f"Reading sheet: {sheet_name} (spreadsheet {SPREADSHEET_ID})")
    print(f"Max offers per geo: {max_offers}, feed: {feed_prefix} (account {account})")
    try:
        by_geo, rows_by_geo = read_sheet_today_offers(sheet_name, max_per_geo=max_offers)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading sheet: {e}")
        sys.exit(1)

    if not by_geo:
        print("No data found in sheet (columns A = country, D = Store Link).")
        sys.exit(0)

    print(f"Geos in sheet: {list(by_geo.keys())}")
    print()

    def flow_offer_ids_for_geo(geo: str):
        """Offer IDs for this geo based on requested traffic mode."""
        o1 = get_geo_offers_sorted(geo, feed_prefix="feed1")
        o2 = get_geo_offers_sorted(geo, feed_prefix="feed2")
        if traffic_mode == "feed1-only":
            return [o["id"] for o in o1]
        if traffic_mode == "feed2-only":
            return [o["id"] for o in o2]
        # "both": feed1 offers first, then feed2.
        return [o["id"] for o in o1] + [o["id"] for o in o2]

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
                    # Set flow to both feed1 and feed2 offers for this geo (equalized traffic)
                    offer_ids = flow_offer_ids_for_geo(geo)
                    set_flow_offers(stream["id"], offer_ids)
                    if traffic_mode == "both":
                        print(f"  {geo}: flow id={stream['id']} now has {len(offer_ids)} offers (feed1+feed2)")
                    else:
                        print(f"  {geo}: flow id={stream['id']} now has {len(offer_ids)} offers ({traffic_mode})")
                else:
                    ensure_country_stream(geo)
                    stream = stream_by_geo.get(geo)
                    if stream:
                        offer_ids = flow_offer_ids_for_geo(geo)
                        if offer_ids:
                            set_flow_offers(stream["id"], offer_ids)
                            if traffic_mode == "both":
                                print(
                                    f"  {geo}: flow id={stream['id']} now has {len(offer_ids)} offers (feed1+feed2)"
                                )
                            else:
                                print(
                                    f"  {geo}: flow id={stream['id']} now has {len(offer_ids)} offers ({traffic_mode})"
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
                            other = get_geo_offers_sorted(geo, feed_prefix="feed2")
                            new_flow_ids = keep_ids + [int(o["id"]) for o in other]
                        else:
                            other = get_geo_offers_sorted(geo, feed_prefix="feed1")
                            new_flow_ids = [int(o["id"]) for o in other] + keep_ids
                    else:
                        new_flow_ids = keep_ids
                    set_flow_offers(int(stream["id"]), new_flow_ids)
                    print(f"  {geo}: flow id={stream['id']} now has {len(new_flow_ids)} offers")
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
        # Ensure flow has the requested offer set for this geo.
        stream = stream_by_geo.get(geo)
        if stream:
            offer_ids = flow_offer_ids_for_geo(geo)
            if offer_ids:
                set_flow_offers(stream["id"], offer_ids)
        if not offers:
            print(f"  {geo}: no Keitaro offers found, skip")
            continue
        # Match by position: first link -> first offer, etc.
        to_update = min(len(store_links), len(offers), max_offers)
        if to_update == 0:
            continue
        print(f"  {geo}: updating {to_update} offers with store links ...")
        geo_row_numbers = rows_by_geo.get(geo, [])
        for i in range(to_update):
            offer = offers[i]
            link = store_links[i]
            new_payload = build_offer_action_payload(geo, link, account_id=kelkoo_account_id, feed=feed)
            try:
                update_offer_action_payload(offer["id"], new_payload)
                print(f"    {offer.get('name')} id={offer['id']} -> {link[:50]}...")
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
