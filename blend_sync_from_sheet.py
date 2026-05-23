#!/usr/bin/env python3
"""
Sync Keitaro campaign "Blend" (alias 9Xq9dSMh) from Google Sheet.

Spreadsheet: ``BLEND_SHEETS_SPREADSHEET_ID`` from config (env override).
Tab: Blend

Expected columns (header row):
  - brandName
  - offerUrl
  - clickCap
  - geo
  - merchantId (optional; used by potentialBlends generation)

Behavior:
  1) Ensure a country flow exists for each geo present in the sheet (name = geo).
  2) Create/update offers per (geo, brandName) using name `blend_{geo}_{feed}_{slug(brandName)}`.
     Kelkoo: offerUrl wrapped like Nipuhim. Adexa: shopli ``raino`` → LinksMerchant (country + encoded URL).
     Yadore: shopli ``rainotest`` → ``/v2/d`` (projectId from env). Default: sheet merchant URL + market
     in ``url=`` / ``market=`` with ``placementId={subid}`` (Keitaro macros preserved in ``rain=``).
     Set ``BLEND_YADORE_OFFER_USE_SUB_MACROS=1`` for ``url={sub_id_3}&market={sub_id_2}&placementId={subid}``
     (campaign must pass those subs).
  3) Per geo, attach offers to ``{geo}_desktop`` and ``{geo}_mobile`` streams (device_type
     filters; tablet counts as mobile) with weights from clickCap / device_mode. Legacy rows
     (neither CPC >= BLEND_DEVICE_CPC_MIN) use full clickCap on both streams. Undivided
     legacy geo flows are zeroed when device streams exist.

Usage:
  python blend_sync_from_sheet.py
  python blend_sync_from_sheet.py --geo fr
  python blend_sync_from_sheet.py --dry-run   # log Keitaro prune removals only (no stream updates from prune)
"""
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv
load_dotenv()

from config import (
    ADEXA_SITE_ID,
    BLEND_SHEETS_SPREADSHEET_ID,
    BLEND_YADORE_OFFER_USE_SUB_MACROS,
    FEED1_API_KEY,
    FEED2_API_KEY,
    FEED5_API_KEY,
    FEED1_KELKOO_ACCOUNT_ID,
    FEED2_KELKOO_ACCOUNT_ID,
    FEED5_KELKOO_ACCOUNT_ID,
    KELKOO_ACCOUNT_ID,
    KELKOO_ACCOUNT_ID_2,
    YADORE_PROJECT_ID,
)
from assistance import (
    build_offer_action_payload,
    get_campaigns_data,
    find_campaign_by_alias_or_name,
    get_campaign_streams,
    flow_name_to_geo,
    parse_blend_stream_geo_channel,
    ensure_blend_device_stream,
    set_blend_device_stream_filters,
    set_flow_offers_weighted,
    set_flow_offers_weighted_keep_zeros,
)
from integrations.blend_device import (
    DEVICE_MODE_LEGACY,
    blend_stream_weight_for_channel,
    device_mode_from_sheet_row,
    normalize_device_mode,
)
from integrations.keitaro import KeitaroClient, KeitaroClientError
from integrations.kelkoo_search import kelkoo_merchant_link_check
from integrations.monetization_geo import geo_for_yadore
from workflows.kelkoo_daily import fetch_reports

SPREADSHEET_ID = BLEND_SHEETS_SPREADSHEET_ID
BLEND_SHEET_NAME = "Blend"
BLEND_CAMPAIGN_ALIAS = "9Xq9dSMh"

# potentialKelkoo* / Adexa / Yadore — same tab names as ``populate_blend_from_potential`` / ``blend_potential_merchants``.
POTENTIAL_TAB_BY_FEED: Dict[str, str] = {
    "kelkoo1": "potentialKelkoo1",
    "kelkoo2": "potentialKelkoo2",
    "kelkoo5": "potentialKelkoo5",
    "adexa": "potentialAdexa",
    "yadore": "potentialYadore",
}
KNOWN_BLEND_FEED_TAGS: Tuple[str, ...] = ("kelkoo1", "kelkoo2", "kelkoo5", "adexa", "yadore")

# Keitaro offer shells: inner URLs are percent-encoded as the ``rain`` query value.
BLEND_ADEXA_RAIN_SHELL = "https://shopli.city/raino?rain="
BLEND_YADORE_RAIN_SHELL = "https://shopli.city/rainotest?rain="
BLEND_YADORE_DEEPLINK_PROJECT_FALLBACK = "WAF4IibbRqGG"


def get_credentials_path() -> str:
    p = Path(__file__).resolve().parent / "credentials.json"
    if not p.exists():
        raise FileNotFoundError(f"credentials.json not found at {p}")
    return str(p)


def get_sheets_service():
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds = service_account.Credentials.from_service_account_file(get_credentials_path())
    return build("sheets", "v4", credentials=creds).spreadsheets()


def _slug(s: str, max_len: int = 48) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        return "unknown"
    return s[:max_len].rstrip("_")


@dataclass(frozen=True)
class BlendRow:
    brand_name: str
    offer_url: str
    click_cap: float
    geo: str
    auto_flag: str = "x"
    feed_tag: str = "kelkoo1"
    merchant_id: Optional[str] = None
    device_mode: str = DEVICE_MODE_LEGACY
    weight_desktop: float = 0.0
    weight_mobile: float = 0.0

    @property
    def offer_name(self) -> str:
        feed_slug = _slug(self.feed_tag, max_len=24)
        return f"blend_{self.geo}_{feed_slug}_{_slug(self.brand_name)}"


def _normalize_geo(g: str) -> str:
    return (g or "").strip().lower()[:2]


def _parse_click_cap(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def ensure_blend_sheet_headers(service) -> None:
    """
    Ensure the Blend tab has expected headers, and add merchantId if missing.
    Does not modify any data rows.
    """
    quoted = BLEND_SHEET_NAME.replace("'", "''")
    result = service.values().get(spreadsheetId=SPREADSHEET_ID, range=f"'{quoted}'!1:1").execute()
    rows = result.get("values") or [[]]
    header = [c.strip() for c in (rows[0] if rows else [])]
    if not header:
        header = ["brandName", "offerUrl", "clickCap", "geo", "merchantId"]
    required = ["brandName", "offerUrl", "clickCap", "geo"]
    for r in required:
        if r not in header:
            header.append(r)
    if "merchantId" not in header:
        header.append("merchantId")
    if "auto" not in header:
        header.append("auto")
    if "feed" not in header:
        header.append("feed")
    for col in ("device_mode", "weight_desktop", "weight_mobile", "cpc_desktop", "cpc_mobile"):
        if col not in header:
            header.append(col)
    service.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{quoted}'!A1",
        valueInputOption="RAW",
        body={"values": [header]},
    ).execute()


def read_blend_rows(service, only_geo: Optional[str] = None) -> List[BlendRow]:
    quoted = BLEND_SHEET_NAME.replace("'", "''")
    result = service.values().get(spreadsheetId=SPREADSHEET_ID, range=f"'{quoted}'!A:Z").execute()
    rows = result.get("values") or []
    if not rows:
        return []
    header = [str(c).strip() for c in rows[0]]
    idx = {name.strip().lower(): i for i, name in enumerate(header)}

    def get_cell(row: list, name: str) -> str:
        i = idx.get((name or "").strip().lower())
        if i is None or i >= len(row):
            return ""
        return str(row[i] or "").strip()

    out: List[BlendRow] = []
    for row in rows[1:]:
        brand = get_cell(row, "brandName")
        url = get_cell(row, "offerUrl")
        geo = _normalize_geo(get_cell(row, "geo"))
        cap = _parse_click_cap(get_cell(row, "clickCap"))
        auto_flag = (get_cell(row, "auto") or "x").strip().lower()
        feed_tag = (get_cell(row, "feed") or "kelkoo1").strip().lower()
        mid = get_cell(row, "merchantId") if "merchantId" in idx else ""
        mode_raw = get_cell(row, "device_mode")
        cpc_d = get_cell(row, "cpc_desktop") if "cpc_desktop" in idx else ""
        cpc_m = get_cell(row, "cpc_mobile") if "cpc_mobile" in idx else ""

        if not brand or not url or not geo or cap is None:
            continue
        if cap <= 0:
            continue
        if only_geo and geo != only_geo:
            continue
        mode, w_d, w_m = device_mode_from_sheet_row(mode_raw, cap, cpc_d, cpc_m)
        wd_cell = _parse_click_cap(get_cell(row, "weight_desktop"))
        wm_cell = _parse_click_cap(get_cell(row, "weight_mobile"))
        if wd_cell is not None and wm_cell is not None and normalize_device_mode(mode_raw or mode) == mode:
            if mode != DEVICE_MODE_LEGACY:
                w_d, w_m = wd_cell, wm_cell
        out.append(
            BlendRow(
                brand_name=brand,
                offer_url=url,
                click_cap=cap,
                geo=geo,
                auto_flag=auto_flag,
                feed_tag=feed_tag,
                merchant_id=mid or None,
                device_mode=mode,
                weight_desktop=w_d,
                weight_mobile=w_m,
            )
        )
    return out


def _kelkoo_api_key_for_feed_tag(feed_tag: str) -> Optional[str]:
    ft = (feed_tag or "").strip().lower()
    if ft == "kelkoo1":
        return FEED1_API_KEY
    if ft == "kelkoo2":
        return FEED2_API_KEY
    if ft == "kelkoo5":
        return FEED5_API_KEY
    return None


def _blend_merchant_url_https(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return "https://example.invalid/"
    if not re.match(r"^https?://", u, flags=re.IGNORECASE):
        u = "https://" + u.lstrip("/")
    return u


def _blend_adexa_action_payload(geo: str, merchant_url: str) -> str:
    """
    shopli ``raino`` → Adexa ``LinksMerchant.php`` with country (sheet geo → Adexa ISO2)
    and URL-encoded merchant URL; ``clickid={subid}`` for Keitaro.
    """
    site_id = (ADEXA_SITE_ID or "").strip()
    if not site_id:
        raise ValueError("Blend feed=adexa requires ADEXA_SITE_ID in .env for Keitaro offer URLs")
    g = _normalize_geo(geo)
    if len(g) != 2:
        raise ValueError(f"Invalid geo for Adexa offer: {geo!r}")
    # Lowercase country like Kelkoo Blend (e.g. uk, fr) — same as GetMerchant / bulk tooling.
    country = g
    m_enc = quote(_blend_merchant_url_https(merchant_url), safe="")
    inner = (
        "https://api.adexad.com/LinksMerchant.php"
        f"?siteID={quote(str(site_id), safe='')}&country={quote(str(country), safe='')}"
        f"&merchantUrl={m_enc}&clickid={{subid}}"
    )
    # Keep the inner Adexa URL readable like the working feed4 format; only merchantUrl stays encoded.
    return BLEND_ADEXA_RAIN_SHELL + quote(inner, safe=":/?&={}")


def _blend_yadore_action_payload(geo: str, merchant_url: str) -> str:
    """
    shopli ``rainotest`` → Yadore ``/v2/d`` with ``placementId={{subid}}`` (and optional
    ``url={{sub_id_3}}`` / ``market={{sub_id_2}}`` when ``BLEND_YADORE_OFFER_USE_SUB_MACROS``).

    The ``rain`` query value must not use ``quote(..., safe="")``: that encodes ``{`` / ``}`` and
    breaks Keitaro macros (Yadore then never sees the real placement id → report vs Keitaro mismatch).
    """
    pid = (YADORE_PROJECT_ID or "").strip() or BLEND_YADORE_DEEPLINK_PROJECT_FALLBACK
    pid_q = quote(str(pid), safe="")

    if BLEND_YADORE_OFFER_USE_SUB_MACROS:
        inner = (
            "https://api.yadore.com/v2/d"
            f"?url={{sub_id_3}}&market={{sub_id_2}}"
            f"&placementId={{subid}}&projectId={pid_q}&isCouponing=false"
        )
        return BLEND_YADORE_RAIN_SHELL + quote(inner, safe=":/?&={}")

    g = _normalize_geo(geo)
    if len(g) != 2:
        raise ValueError(f"Invalid geo for Yadore offer: {geo!r}")
    market = geo_for_yadore(g)
    m_enc = quote(_blend_merchant_url_https(merchant_url), safe="")
    inner = (
        "https://api.yadore.com/v2/d"
        f"?url={m_enc}&market={quote(str(market), safe='')}"
        f"&placementId={{subid}}&projectId={pid_q}&isCouponing=false"
    )
    return BLEND_YADORE_RAIN_SHELL + quote(inner, safe=":/?&={}%")


def _blend_keitaro_action_payload(geo: str, offer_url: str, feed_tag: str) -> str:
    """
    Kelkoo Blend rows: wrap offerUrl like Nipuhim (merchantUrl in permanentLinkGo / klk-merchant).
    Adexa/Yadore: shopli rain shells with sheet geo + offerUrl as merchant target; ``{subid}`` for click id.
    """
    ft = (feed_tag or "").strip().lower()
    if ft == "kelkoo1":
        acc = (FEED1_KELKOO_ACCOUNT_ID or "").strip() or KELKOO_ACCOUNT_ID
        return build_offer_action_payload(geo, offer_url, account_id=acc, feed=1)
    if ft == "kelkoo2":
        acc = (FEED2_KELKOO_ACCOUNT_ID or "").strip() or KELKOO_ACCOUNT_ID_2
        return build_offer_action_payload(geo, offer_url, account_id=acc, feed=2)
    if ft == "kelkoo5":
        acc = (FEED5_KELKOO_ACCOUNT_ID or "").strip() or KELKOO_ACCOUNT_ID
        return build_offer_action_payload(geo, offer_url, account_id=acc, feed=1)
    if ft == "adexa":
        return _blend_adexa_action_payload(geo, offer_url)
    if ft == "yadore":
        return _blend_yadore_action_payload(geo, offer_url)
    return offer_url


def _delete_rows_from_blend_sheet(service, row_numbers_1based: List[int]) -> None:
    if not row_numbers_1based:
        return
    meta = service.get(
        spreadsheetId=SPREADSHEET_ID,
        fields="sheets(properties(sheetId,title))",
    ).execute()
    sheet_id = None
    for s in meta.get("sheets", []):
        if s.get("properties", {}).get("title") == BLEND_SHEET_NAME:
            sheet_id = s.get("properties", {}).get("sheetId")
            break
    if sheet_id is None:
        raise RuntimeError("Could not find sheetId for Blend tab")

    sorted_rows = sorted(set(row_numbers_1based))
    # Group contiguous 1-based rows.
    groups: List[Tuple[int, int]] = []
    start = prev = sorted_rows[0]
    for rn in sorted_rows[1:]:
        if rn == prev + 1:
            prev = rn
        else:
            groups.append((start, prev))
            start = prev = rn
    groups.append((start, prev))

    requests = []
    for first_rn, last_rn in groups:
        # 0-based startIndex; endIndex is exclusive.
        # Delete rows [first..last] (1-based inclusive) => startIndex=first-1, endIndex=last.
        requests.append(
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": int(sheet_id),
                        "dimension": "ROWS",
                        "startIndex": int(first_rn - 1),
                        "endIndex": int(last_rn),
                    }
                }
            }
        )
    service.batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": requests}).execute()


def _filter_and_delete_non_monetized_auto_rows(
    service,
    only_geo: Optional[str],
) -> int:
    """
    For rows with auto='v': check Kelkoo monetization using `feed` (kelkoo1/kelkoo2).
    Delete rows that are not monetized so they do not receive Blend traffic.

    Note: month-to-date "0 sales" suppression is handled in-memory (for attaching offers),
    not by deleting sheet rows.
    """
    quoted = BLEND_SHEET_NAME.replace("'", "''")
    result = service.values().get(spreadsheetId=SPREADSHEET_ID, range=f"'{quoted}'!A:Z").execute()
    rows = result.get("values") or []
    if len(rows) < 2:
        return 0
    header = [str(c).strip() for c in rows[0]]
    idx = {name.strip().lower(): i for i, name in enumerate(header)}

    def get_cell(row: list, name: str) -> str:
        i = idx.get((name or "").strip().lower())
        if i is None or i >= len(row):
            return ""
        return str(row[i] or "").strip()

    to_delete: List[int] = []
    for row_i, row in enumerate(rows[1:], start=2):  # 1-based sheet rows
        geo = _normalize_geo(get_cell(row, "geo"))
        if not geo:
            continue
        if only_geo and geo != only_geo:
            continue

        cap = _parse_click_cap(get_cell(row, "clickCap"))
        if cap is None or cap <= 0:
            continue

        auto_flag = (get_cell(row, "auto") or "x").strip().lower()
        if auto_flag != "v":
            continue

        brand = get_cell(row, "brandName")
        url = get_cell(row, "offerUrl")
        feed_tag = (get_cell(row, "feed") or "").strip().lower()
        if not brand or not url:
            continue
        if not feed_tag:
            # Safety: do not run a default Kelkoo gate when feed is unspecified.
            continue

        api_key = _kelkoo_api_key_for_feed_tag(feed_tag)
        if not api_key:
            # Not a Kelkoo feed we can check yet.
            continue

        # Legacy monetization gate: if Kelkoo does not consider this merchant as monetizable, delete it.
        url_norm = (url or "").strip()
        # Kelkoo expects a URL-like merchantUrl parameter; normalize bare domains.
        if url_norm and not re.match(r"^https?://", url_norm, flags=re.IGNORECASE):
            url_norm = "https://" + url_norm.lstrip("/")
        res = kelkoo_merchant_link_check(url_norm, geo, api_key)
        if not res.get("found"):
            to_delete.append(row_i)

    if to_delete:
        _delete_rows_from_blend_sheet(service, to_delete)
    return len(to_delete)


def _suppress_auto_v_rows_without_mtd_sales(
    rows: List[BlendRow],
) -> Tuple[List[BlendRow], int]:
    """
    Month-start gating for Take-Down:
    - Keep monetized Blend rows in the sheet.
    - If an `auto='v'` merchant has 0 MTD sales, suppress it from being attached
      to Keitaro flows (so traffic is taken down) until MTD sales becomes > 0.
    """
    today = datetime.now(timezone.utc).date()

    month_start = today.replace(day=1).strftime("%Y-%m-%d")
    month_end = today.strftime("%Y-%m-%d")

    targets = [r for r in rows if (r.auto_flag or "").strip().lower() == "v" and r.merchant_id]
    if not targets:
        return rows, 0

    reports_cache: Dict[str, Optional[Dict[str, Dict[str, int]]]] = {}
    for r in targets:
        feed_tag = (r.feed_tag or "kelkoo1").strip().lower()
        if feed_tag in reports_cache:
            continue
        api_key = _kelkoo_api_key_for_feed_tag(feed_tag)
        if not api_key:
            # Can't verify; don't take down.
            reports_cache[feed_tag] = None
            continue
        try:
            reports_cache[feed_tag] = fetch_reports(api_key, month_start, month_end)
        except Exception:
            # Fail open: if we can't fetch, keep traffic attached to avoid accidental drop.
            reports_cache[feed_tag] = None

    kept: List[BlendRow] = []
    suppressed = 0
    for r in rows:
        if (r.auto_flag or "").strip().lower() != "v" or not r.merchant_id:
            kept.append(r)
            continue
        feed_tag = (r.feed_tag or "kelkoo1").strip().lower()
        perf_map = reports_cache.get(feed_tag)
        if perf_map is None:
            kept.append(r)
            continue
        sales = int((perf_map.get(str(r.merchant_id)) or {}).get("sales", 0) or 0)
        if sales <= 0:
            suppressed += 1
            continue
        kept.append(r)

    return kept, suppressed


def _get_campaign_id_by_alias(alias: str) -> int:
    campaigns = get_campaigns_data()
    c = find_campaign_by_alias_or_name(campaigns, alias=alias, name=alias)
    if not c or c.get("id") is None:
        raise ValueError(f"Campaign not found by alias/name {alias!r}")
    return int(c["id"])


def _streams_by_geo_channel(campaign_id: int) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """``geo -> {desktop|mobile|legacy: stream}``."""
    streams = get_campaign_streams(campaign_id)
    out: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for s in streams:
        geo, channel = parse_blend_stream_geo_channel(s.get("name") or "")
        if geo and channel:
            out.setdefault(geo, {})[channel] = s
    return out


def _build_channel_offer_weights(
    offer_id_to_row: Dict[int, BlendRow],
    channel: str,
) -> Dict[int, float]:
    weights: Dict[int, float] = {}
    for oid, row in offer_id_to_row.items():
        w = blend_stream_weight_for_channel(
            row.device_mode,
            channel,
            click_cap=row.click_cap,
            weight_desktop=row.weight_desktop,
            weight_mobile=row.weight_mobile,
        )
        if w is not None and w > 0:
            weights[oid] = w
    return weights


def _zero_blend_offers_on_stream(
    client: KeitaroClient,
    stream: Dict[str, Any],
    *,
    all_offers_by_id: Dict[int, str],
) -> None:
    """Set share=0 for blend_* offers on a legacy undivided geo stream."""
    sid = stream.get("id")
    if sid is None:
        return
    offers_payload: List[Dict[str, Any]] = []
    for slot in stream.get("offers") or []:
        oid_raw = slot.get("offer_id")
        if oid_raw is None:
            continue
        oid = int(oid_raw)
        name = all_offers_by_id.get(oid, "")
        if not name.startswith("blend_"):
            continue
        offers_payload.append({"offer_id": oid, "state": "active", "share": 0})
    if offers_payload:
        client.update_stream(int(sid), {"offers": offers_payload})


def _sync_geo_device_streams(
    client: KeitaroClient,
    campaign_id: int,
    geo: str,
    offer_id_to_row: Dict[int, BlendRow],
    streams_map: Dict[str, Dict[str, Dict[str, Any]]],
    all_offers_by_id: Dict[int, str],
) -> Tuple[int, int]:
    """Upsert desktop/mobile streams for one geo. Returns (flows_created, streams_updated)."""
    created_flows = 0
    streams_updated = 0
    geo_streams = streams_map.setdefault(geo, {})

    for channel in ("desktop", "mobile"):
        stream = geo_streams.get(channel)
        if not stream:
            stream = ensure_blend_device_stream(campaign_id, geo, channel, skip_if_exists=True)
            if stream.get("_skipped"):
                created_flows += 0
            else:
                created_flows += 1
            geo_streams[channel] = stream
            streams_map[geo] = geo_streams
        else:
            sid = stream.get("id")
            if sid is not None:
                try:
                    set_blend_device_stream_filters(int(sid), geo, channel)
                except KeitaroClientError as e:
                    print(f"  Warning: could not refresh {geo}/{channel} filters: {e}")

        offer_id_to_weight = _build_channel_offer_weights(offer_id_to_row, channel)
        sid = stream.get("id")
        if sid is None:
            continue
        sid_i = int(sid)
        active_ids = set(offer_id_to_weight.keys())
        existing_attached: Set[int] = set()
        for slot in stream.get("offers") or []:
            oidr = slot.get("offer_id")
            if oidr is not None:
                existing_attached.add(int(oidr))
        zero_keep_ids: List[int] = []
        for oid in existing_attached:
            if oid in active_ids:
                continue
            name = all_offers_by_id.get(oid, "")
            if name.startswith("blend_"):
                zero_keep_ids.append(oid)

        if offer_id_to_weight:
            if zero_keep_ids:
                set_flow_offers_weighted_keep_zeros(sid_i, offer_id_to_weight, zero_keep_ids)
            else:
                set_flow_offers_weighted(sid_i, offer_id_to_weight)
            streams_updated += 1
            print(
                f"  {geo}/{channel}: {len(offer_id_to_weight)} offer(s) weighted; "
                f"+{len(zero_keep_ids)} at share=0"
            )
        elif zero_keep_ids or existing_attached:
            _zero_blend_offers_on_stream(client, stream, all_offers_by_id=all_offers_by_id)
            streams_updated += 1
            print(f"  {geo}/{channel}: no active offers; zeroed blend_* on stream")

    legacy = geo_streams.get("legacy")
    if legacy:
        _zero_blend_offers_on_stream(client, legacy, all_offers_by_id=all_offers_by_id)
        print(f"  {geo}: legacy stream blend_* offers set to share=0 (device streams active)")

    return created_flows, streams_updated


def _get_offer_id_by_name(client: KeitaroClient, name: str) -> Optional[int]:
    for o in client.get_offers():
        if (o.get("name") or "").strip() == name:
            oid = o.get("id")
            return int(oid) if oid is not None else None
    return None


def _upsert_offer(client: KeitaroClient, name: str, url: str) -> int:
    oid = _get_offer_id_by_name(client, name)
    if oid is None:
        created = client.create_offer(
            {
                "name": name,
                "action_type": "http",
                "action_payload": url,
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
        )
        if created.get("id") is None:
            raise ValueError(f"Create offer {name} did not return id: {created}")
        return int(created["id"])
    client.update_offer(oid, {"action_payload": url})
    return oid


def _blend_feed_prefix(geo: str, feed_tag: str) -> str:
    """Keitaro offer name prefix ``blend_{geo}_{slug(feed)}_`` (must match ``BlendRow.offer_name``)."""
    g = _normalize_geo(geo)
    return f"blend_{g}_{_slug(feed_tag, max_len=24)}_"


def detect_blend_feed_tag_from_offer_name(geo: str, offer_name: str) -> Optional[str]:
    """Infer feed tag (``kelkoo1``, …) from a Keitaro Blend offer name for this flow geo."""
    name = (offer_name or "").strip()
    if not name.startswith("blend_"):
        return None
    g = _normalize_geo(geo)
    for ft in KNOWN_BLEND_FEED_TAGS:
        if name.startswith(_blend_feed_prefix(g, ft)):
            return ft
    return None


def load_potential_monetized_offer_rows_by_feed(service) -> Tuple[Dict[str, List[Dict[str, str]]], Set[str]]:
    """
    Read each potential sheet; return rows per feed with ``offer_name`` matching ``BlendRow`` naming,
    plus a set of feeds whose sheet **failed to load** (caller must not prune those feeds in Keitaro).
    """
    out: Dict[str, List[Dict[str, str]]] = {k: [] for k in POTENTIAL_TAB_BY_FEED}
    failed: Set[str] = set()

    for feed_tag, title in POTENTIAL_TAB_BY_FEED.items():
        quoted = title.replace("'", "''")
        try:
            vals = (
                service.values()
                .get(spreadsheetId=SPREADSHEET_ID, range=f"'{quoted}'!A:Z")
                .execute()
                .get("values")
                or []
            )
        except Exception as e:
            print(
                f"Warning: could not read potential sheet {title!r} for {feed_tag}: {e}. "
                f"Skipping Keitaro prune for this feed (will not remove offers based on missing data)."
            )
            failed.add(feed_tag)
            continue

        if len(vals) < 2:
            continue

        header = [str(c or "").strip().lower() for c in vals[0]]

        def col(name: str) -> int:
            try:
                return header.index(name)
            except ValueError:
                return -1

        i_mid = col("merchantid")
        i_name = col("merchant")
        i_geo = col("geo_origin")
        i_mon = col("kelkoo_monetization")
        if min(i_mid, i_name, i_geo, i_mon) < 0:
            print(
                f"Warning: potential sheet {title!r} missing required columns "
                f"(merchantId, merchant, geo_origin, kelkoo_monetization). Skipping prune for {feed_tag}."
            )
            failed.add(feed_tag)
            continue

        for row in vals[1:]:
            monet = str(row[i_mon] if i_mon < len(row) else "").strip().lower()
            if not monet.startswith("monetized"):
                continue
            geo = str(row[i_geo] if i_geo < len(row) else "").strip().lower()[:2]
            brand = str(row[i_name] if i_name < len(row) else "").strip()
            if len(geo) != 2 or not brand:
                continue
            br = BlendRow(
                brand_name=brand,
                offer_url="https://example.invalid/",
                click_cap=1.0,
                geo=geo,
                auto_flag="v",
                feed_tag=feed_tag,
                merchant_id=None,
            )
            out[feed_tag].append({"offer_name": br.offer_name, "geo": geo, "feed": feed_tag})

    return out, failed


def prune_unmonetized_from_keitaro(
    client: KeitaroClient,
    campaign_id: int,
    potential_sheets: Dict[str, List[Dict[str, str]]],
    *,
    feeds_sheet_load_failed: Set[str],
    only_geo: Optional[str] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    For Blend offers that are absent from the current monetized potential snapshot
    (or not ``monetized*`` in that sheet), set their share to 0 in the geo flow but
    keep them attached so operators can re-enable them later.
    Feeds listed in ``feeds_sheet_load_failed`` are never modified.

    Returns ``removed`` entries ``(offer_id, geo, feed, reason)`` (kept name for
    backward compatibility — these offers are zeroed, not detached), ``errors``,
    ``empty_flow_geos``.
    """
    monetized_by_feed: Dict[str, Set[str]] = {}
    for ft, rows in potential_sheets.items():
        monetized_by_feed[ft] = {str(r.get("offer_name") or "").strip() for r in rows if r.get("offer_name")}

    offer_names_by_id: Dict[int, str] = {}
    try:
        for o in client.get_offers():
            oid = o.get("id")
            if oid is not None:
                offer_names_by_id[int(oid)] = (o.get("name") or "").strip()
    except KeitaroClientError as e:
        return {"removed": [], "errors": [f"list_offers:{e}"], "empty_flow_geos": []}

    removed: List[Tuple[int, str, str, str]] = []
    errors: List[str] = []
    empty_flow_geos: List[str] = []

    try:
        streams = get_campaign_streams(campaign_id)
    except Exception as e:
        return {"removed": [], "errors": [f"get_streams:{e}"], "empty_flow_geos": []}

    for stream in streams:
        geo = flow_name_to_geo(stream.get("name") or "")
        if not geo:
            continue
        if only_geo and geo != only_geo:
            continue
        sid = stream.get("id")
        if sid is None:
            continue
        sid_i = int(sid)
        attached = list(stream.get("offers") or [])
        zero_ids: List[int] = []

        for slot in attached:
            oid_raw = slot.get("offer_id")
            if oid_raw is None:
                continue
            oid = int(oid_raw)
            name = offer_names_by_id.get(oid, "")
            if not name.startswith("blend_"):
                continue
            ft = detect_blend_feed_tag_from_offer_name(geo, name)
            if ft is None:
                continue
            if ft in feeds_sheet_load_failed:
                continue
            if name in (monetized_by_feed.get(ft) or set()):
                continue
            reason = "not monetized in potentialFeed sheet or absent from potential sheet"
            zero_ids.append(oid)
            removed.append((oid, geo, ft, reason))
            msg = (
                f"Blend prune: zero share for offer id={oid} geo={geo} feed={ft} "
                f"reason={reason!r} offer_name={name!r} (kept attached)"
            )
            if dry_run:
                print(f"[dry-run] {msg}")
            else:
                print(msg)

        if not zero_ids:
            continue

        if dry_run:
            continue

        # Build a payload that keeps every currently-attached offer, but forces
        # share=0 for the unmonetized ones. Other offers preserve their existing share.
        zero_set = set(zero_ids)
        offers_payload: List[Dict[str, Any]] = []
        non_zero_total = 0
        for s in attached:
            oidr = s.get("offer_id")
            if oidr is None:
                continue
            oidi = int(oidr)
            if oidi in zero_set:
                share = 0
            else:
                share = int(s.get("share") or 0)
                non_zero_total += share
            offers_payload.append({"offer_id": oidi, "state": "active", "share": share})

        # If zeroing leaves the flow with no positive shares, log a warning so we
        # notice (Keitaro will accept all-zero, but no traffic will be routed).
        if non_zero_total <= 0:
            empty_flow_geos.append(geo)
            print(
                f"WARNING: geo {geo} Blend flow has no positive shares after zeroing "
                f"{len(zero_ids)} unmonetized offer(s)."
            )

        try:
            client.update_stream(sid_i, {"offers": offers_payload})
        except Exception as e:
            err = f"geo={geo} stream_id={sid_i}: {e}"
            errors.append(err)
            print(f"ERROR: Blend prune failed to update stream: {err}")

    return {"removed": removed, "errors": errors, "empty_flow_geos": empty_flow_geos}


def run_blend_prune_unmonetized_keitaro(
    service,
    *,
    only_geo: Optional[str] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Load potential sheets + detach unmonetized / missing Blend offers from the Keitaro Blend campaign.
    Used by ``run_daily_workflow`` (step 7a½) and at the start of ``blend_sync_from_sheet`` main.
    """
    potential_rows, failed = load_potential_monetized_offer_rows_by_feed(service)
    campaign_id = _get_campaign_id_by_alias(BLEND_CAMPAIGN_ALIAS)
    client = KeitaroClient()
    summary = prune_unmonetized_from_keitaro(
        client,
        campaign_id,
        potential_rows,
        feeds_sheet_load_failed=failed,
        only_geo=only_geo,
        dry_run=dry_run,
    )
    summary["feeds_sheet_load_failed"] = sorted(failed)
    return summary


def main() -> None:
    argv = sys.argv[1:]
    only_geo: Optional[str] = None
    dry_run = "--dry-run" in argv
    i = 0
    while i < len(argv):
        if argv[i] == "--geo" and i + 1 < len(argv):
            only_geo = _normalize_geo(argv[i + 1])
            i += 2
            continue
        i += 1

    service = get_sheets_service()
    ensure_blend_sheet_headers(service)
    print("Blend Keitaro prune: potential sheets (monetized snapshot) vs campaign flows ...")
    pre_prune = run_blend_prune_unmonetized_keitaro(service, only_geo=only_geo, dry_run=dry_run)
    n_pr = len(pre_prune.get("removed") or [])
    if dry_run and n_pr:
        print(f"Blend prune (--dry-run): would detach {n_pr} offer(s) (see [dry-run] lines above).")
    elif n_pr:
        print(f"Blend prune: detached {n_pr} offer(s) from Keitaro flows.")
    if pre_prune.get("errors"):
        print(f"Blend prune: {len(pre_prune['errors'])} stream update error(s) (see logs above).")

    deleted = _filter_and_delete_non_monetized_auto_rows(service, only_geo=only_geo)
    if deleted:
        print(f"Auto monetization gate: deleted {deleted} non-monetized Blend rows (auto='v').")
    rows = read_blend_rows(service, only_geo=only_geo)
    if not rows:
        print("No valid rows found in Blend sheet.")
        return

    # Take-down without deleting sheet rows: on month start we suppress auto='v' merchants
    # with 0 MTD sales from being attached to Keitaro flows.
    rows_for_sync, suppressed = _suppress_auto_v_rows_without_mtd_sales(rows)
    if suppressed:
        print(f"MTD take-down: suppressed {suppressed} auto='v' Blend rows with 0 MTD sales (no sheet deletions).")

    campaign_id = _get_campaign_id_by_alias(BLEND_CAMPAIGN_ALIAS)
    print(f"Blend campaign id={campaign_id} alias={BLEND_CAMPAIGN_ALIAS}")

    rows_by_geo: Dict[str, List[BlendRow]] = {}
    for r in rows_for_sync:
        rows_by_geo.setdefault(r.geo, []).append(r)

    client = KeitaroClient()
    streams_map = _streams_by_geo_channel(campaign_id)

    created_offers = 0
    updated_offers = 0
    created_flows = 0

    # Build a quick lookup of all offer names → ids so we can identify
    # currently-attached "blend_" offers that aren't in the sheet (those should
    # remain attached with share=0 instead of being detached).
    all_offers_by_id: Dict[int, str] = {}
    try:
        for o in client.get_offers():
            oid = o.get("id")
            if oid is not None:
                all_offers_by_id[int(oid)] = (o.get("name") or "").strip()
    except KeitaroClientError as e:
        print(f"Warning: could not list offers for share-0 keep-alive logic: {e}")

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

    # Geos with no sheet rows: zero blend_* on all device + legacy streams for that geo.
    for geo, ch_streams in streams_map.items():
        if geo in rows_by_geo:
            continue
        for channel, stream in ch_streams.items():
            if channel not in ("desktop", "mobile", "legacy"):
                continue
            _zero_blend_offers_on_stream(client, stream, all_offers_by_id=all_offers_by_id)
            print(f"  {geo}/{channel}: no Blend rows — zeroed blend_* offers")

    # Archive stale Blend offers (offers named like "blend_...") that are not attached to any Blend flow.
    expected_names = {r.offer_name for r in rows}
    try:
        streams_after = get_campaign_streams(campaign_id)
        attached_offer_ids: set[int] = set()
        for s in streams_after:
            for o in s.get("offers") or []:
                oid = o.get("offer_id")
                if oid is not None:
                    attached_offer_ids.add(int(oid))
        for o in client.get_offers():
            name = (o.get("name") or "").strip()
            oid = o.get("id")
            if not name or oid is None:
                continue
            if not name.startswith("blend_"):
                continue
            if name in expected_names:
                continue
            if int(oid) in attached_offer_ids:
                continue
            client.archive_offer(int(oid))
    except KeitaroClientError as e:
        print(f"Warning: could not archive stale Blend offers: {e}")

    print()
    print(f"Done. Offers created={created_offers}, updated={updated_offers}; flows created={created_flows}.")


if __name__ == "__main__":
    try:
        main()
    except (KeitaroClientError, ValueError, FileNotFoundError) as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)

