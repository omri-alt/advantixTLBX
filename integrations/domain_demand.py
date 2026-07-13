"""
Domain-demand bill for Keitaro hub campaign 94.

Builds a daily click order from:
- Nipuhim: each active feed × geo on today's offers tabs (``DOMAIN_DEMAND_NIPUHIM_CLICKS_PER_GEO``).
- Blend: each sheet row with device-weighted ``clickCap``.

Refreshes delivered clicks from Keitaro child campaigns (hub state) and hub 94 totals.
Writes ``summary`` + ``bill`` tabs on ``DOMAIN_DEMAND_SHEET_ID``.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, FrozenSet, List, Optional, Tuple

from config import (
    DOMAIN_DEMAND_BILL_TAB,
    DOMAIN_DEMAND_NIPUHIM_CLICKS_PER_GEO,
    DOMAIN_DEMAND_SHEET_ID,
    DOMAIN_DEMAND_SUMMARY_BY_GEO_TAB,
    DOMAIN_DEMAND_SUMMARY_TAB,
    DOMAIN_DEMAND_TRILLION_PAUSE_FILL_PCT,
    KEITARO_API_KEY,
    KEITARO_HUB_ACTIVE_FEEDS,
    KEITARO_HUB_CAMPAIGN_ID,
)
from integrations.blend_device import blend_stream_weight_for_channel
from integrations.hub_click_cap_weights import (
    hub_offer_weights_from_caps,
    nipuhim_feed_active_geos,
)
from integrations.keitaro import KeitaroClient, KeitaroClientError
from integrations.keitaro_hub import load_hub_state

logger = logging.getLogger(__name__)

BILL_HEADER = [
    "updated_at",
    "date",
    "line_key",
    "family",
    "feed",
    "geo",
    "device",
    "brand",
    "merchant_id",
    "demand_clicks",
    "delivered_clicks",
    "remaining",
    "fill_pct",
    "child_campaign_id",
    "source",
]

SUMMARY_HEADER = [
    "updated_at",
    "date",
    "scope",
    "family",
    "feed",
    "demand_clicks",
    "delivered_clicks",
    "remaining",
    "fill_pct",
    "hub_weight_pct",
    "trillion_hint",
    "notes",
]

SUMMARY_BY_GEO_HEADER = [
    "updated_at",
    "date",
    "geo",
    "device",
    "demand_clicks",
    "delivered_clicks",
    "remaining",
    "fill_pct",
    "trillion_campaign",
    "trillion_status",
    "trillion_hint",
]


# Keitaro ``country`` grouping returns full English names (``country_code`` is null),
# so map names -> ISO2 for the geos we route. Unmapped names fall back to lowercased first-2.
_COUNTRY_NAME_TO_ISO: Dict[str, str] = {
    "austria": "at",
    "australia": "au",
    "belgium": "be",
    "switzerland": "ch",
    "czech republic": "cz",
    "czechia": "cz",
    "germany": "de",
    "denmark": "dk",
    "spain": "es",
    "finland": "fi",
    "france": "fr",
    "greece": "gr",
    "hungary": "hu",
    "ireland": "ie",
    "italy": "it",
    "mexico": "mx",
    "netherlands": "nl",
    "norway": "no",
    "poland": "pl",
    "portugal": "pt",
    "romania": "ro",
    "sweden": "se",
    "slovakia": "sk",
    "united kingdom": "uk",
    "great britain": "uk",
    "united states": "us",
    "united states of america": "us",
}


def _normalize_geo_value(country: str, country_code: Any = None) -> str:
    cc = str(country_code or "").strip().lower()
    if len(cc) == 2 and cc.isalpha():
        return "uk" if cc == "gb" else cc
    name = str(country or "").strip().lower()
    if name in _COUNTRY_NAME_TO_ISO:
        return _COUNTRY_NAME_TO_ISO[name]
    if len(name) == 2 and name.isalpha():
        return "uk" if name == "gb" else name
    return name[:2] if name else ""


def _normalize_device_value(device_type: str) -> Optional[str]:
    s = (device_type or "").strip().lower()
    if not s:
        return None
    if "desktop" in s or s in ("pc", "computer"):
        return "desktop"
    if any(t in s for t in ("mobile", "phone", "tablet", "smartphone", "smart phone")):
        return "mobile"
    return None


@dataclass(frozen=True)
class BillLine:
    line_key: str
    family: str
    feed: str
    geo: str
    device: str
    brand: str
    merchant_id: str
    demand: float
    child_campaign_id: Optional[int]
    source: str


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _calendar_day() -> str:
    from integrations.blend_cap_progress import _today_in_report_tz

    return _today_in_report_tz()


def _fill_pct(delivered: float, demand: float) -> Optional[float]:
    if demand <= 0:
        return None
    return round((delivered / demand) * 100.0, 1)


def _remaining(demand: float, delivered: float) -> int:
    return max(0, int(round(demand)) - int(round(delivered)))


def _child_index(state: Dict[str, Any]) -> Dict[Tuple[str, str], int]:
    out: Dict[Tuple[str, str], int] = {}
    for meta in (state.get("child_campaigns") or {}).values():
        if not isinstance(meta, dict):
            continue
        hub_type = str(meta.get("hub_type") or "").strip().lower()
        feed_key = str(meta.get("feed_key") or "").strip().lower()
        cid = meta.get("id")
        if hub_type and feed_key and cid is not None:
            out[(hub_type, feed_key)] = int(cid)
    return out


def build_nipuhim_demand_lines(
    *,
    date_str: Optional[str] = None,
    max_offers_per_geo: int = 60,
    clicks_per_geo: Optional[int] = None,
    active_feeds: Optional[FrozenSet[str]] = None,
) -> Tuple[List[BillLine], List[str]]:
    """One bill line per feed × geo × device (50/50 desktop/mobile split)."""
    feeds = active_feeds or frozenset(KEITARO_HUB_ACTIVE_FEEDS)
    cap = clicks_per_geo if clicks_per_geo is not None else DOMAIN_DEMAND_NIPUHIM_CLICKS_PER_GEO
    if cap <= 0:
        return [], ["Nipuhim demand: clicks per geo is 0 — skipped"]

    feed_geos, logs = nipuhim_feed_active_geos(
        date_str=date_str,
        max_offers_per_geo=max_offers_per_geo,
        feed_keys=tuple(sorted(feeds)),
    )
    half = cap / 2.0
    lines: List[BillLine] = []
    child_idx = _child_index(load_hub_state())

    for fk in sorted(feeds):
        geos = sorted(feed_geos.get(fk, frozenset()))
        cid = child_idx.get(("nipuhim", fk))
        for geo in geos:
            for device, demand in (("desktop", half), ("mobile", cap - half)):
                key = f"nipuhim|{fk}|{geo}|{device}"
                lines.append(
                    BillLine(
                        line_key=key,
                        family="nipuhim",
                        feed=fk,
                        geo=geo,
                        device=device,
                        brand="",
                        merchant_id="",
                        demand=float(demand),
                        child_campaign_id=cid,
                        source="nipuhim_offers_tab",
                    )
                )
    if not lines:
        logs.append("Nipuhim demand: no feed×geo lines")
    else:
        total = sum(l.demand for l in lines)
        logs.append(f"Nipuhim demand: {len(lines)} line(s), {int(total)} clicks total")
    return lines, logs


def build_blend_demand_lines() -> Tuple[List[BillLine], List[str]]:
    logs: List[str] = []
    try:
        from blend_sync_from_sheet import get_sheets_service, read_blend_rows
    except Exception as e:
        return [], [f"Blend demand: import failed: {e}"]

    try:
        rows = read_blend_rows(get_sheets_service())
    except Exception as e:
        return [], [f"Blend demand: sheet read failed: {e}"]

    child_idx = _child_index(load_hub_state())
    lines: List[BillLine] = []
    for row in rows:
        fk = (row.feed_tag or "kelkoo1").strip().lower()
        geo = (row.geo or "").strip().lower()
        brand = (row.brand_name or "").strip()
        if not geo or len(geo) != 2:
            continue
        cid = child_idx.get(("blend", fk))
        for channel in ("desktop", "mobile"):
            w = blend_stream_weight_for_channel(
                row.device_mode,
                channel,
                click_cap=row.click_cap,
                weight_desktop=row.weight_desktop,
                weight_mobile=row.weight_mobile,
            )
            if w is None or w <= 0:
                continue
            key = f"blend|{fk}|{geo}|{channel}|{brand}"
            lines.append(
                BillLine(
                    line_key=key,
                    family="blend",
                    feed=fk,
                    geo=geo,
                    device=channel,
                    brand=brand,
                    merchant_id=str(row.merchant_id or ""),
                    demand=float(w),
                    child_campaign_id=cid,
                    source="blend_sheet",
                )
            )
    if lines:
        total = sum(l.demand for l in lines)
        logs.append(f"Blend demand: {len(lines)} line(s), {int(total)} clicks total")
    else:
        logs.append("Blend demand: no rows with clickCap > 0")
    return lines, logs


def _keitaro_clicks_by_geo_device(campaign_id: int) -> Tuple[Dict[Tuple[str, str], int], Optional[str]]:
    """Clicks keyed by (geo, device) for a campaign — robust country/device normalization."""
    if not (KEITARO_API_KEY or "").strip():
        return {}, "KEITARO_API_KEY not set"
    from integrations.blend_cap_progress import (
        _keitaro_cap_report_payload,
        _row_clicks,
        _rows_from_report,
        click_metric_name,
    )

    payload = _keitaro_cap_report_payload(int(campaign_id))
    try:
        report = KeitaroClient().build_report(payload)
    except KeitaroClientError as e:
        return {}, str(e)
    except Exception as e:
        return {}, str(e)

    metric = click_metric_name()
    clicks: Dict[Tuple[str, str], int] = defaultdict(int)
    for row in _rows_from_report(report):
        lk = {str(k).lower(): v for k, v in row.items()}
        geo = _normalize_geo_value(lk.get("country", ""), lk.get("country_code"))
        device = _normalize_device_value(str(lk.get("device_type") or lk.get("device") or ""))
        if not geo or len(geo) != 2 or not device:
            continue
        clicks[(geo, device)] += _row_clicks(row, metric=metric)

    if not clicks:
        return {}, "Keitaro report returned no geo/device rows"
    return dict(clicks), None


def _hub_total_clicks(hub_campaign_id: int) -> Tuple[int, Optional[str]]:
    if not (KEITARO_API_KEY or "").strip():
        return 0, "KEITARO_API_KEY not set"
    from integrations.blend_cap_progress import (
        _row_clicks,
        _rows_from_report,
        click_metric_name,
        report_timezone,
    )

    metric = click_metric_name()
    payload = {
        "range": {"interval": "today", "timezone": report_timezone()},
        "grouping": [],
        "metrics": [metric, "clicks"],
        "filters": [
            {
                "name": "campaign_id",
                "operator": "IN_LIST",
                "expression": [int(hub_campaign_id)],
            }
        ],
        "limit": 10,
    }
    try:
        report = KeitaroClient().build_report(payload)
    except KeitaroClientError as e:
        return 0, str(e)
    except Exception as e:
        return 0, str(e)

    rows = _rows_from_report(report)
    if not rows:
        return 0, "Hub report returned no rows"
    total = 0
    for row in rows:
        total += _row_clicks(row, metric=metric)
    return total, None


def _hub_clicks_by_geo_device(hub_campaign_id: int) -> Tuple[Dict[Tuple[str, str], int], Optional[str]]:
    """Hub campaign 94 clicks grouped by (geo, device) — what Trillion actually delivered."""
    return _keitaro_clicks_by_geo_device(hub_campaign_id)


def _parse_trillion_segment_name(name: str) -> Optional[Tuple[str, str]]:
    """Parse ``.at.01.desktop.nonadult`` -> ('at', 'desktop')."""
    tokens = [t.strip().lower() for t in (name or "").split(".") if t.strip()]
    geo = ""
    device = ""
    for t in tokens:
        if not device and t in ("desktop", "mobile"):
            device = t
        elif not geo and len(t) == 2 and t.isalpha():
            geo = t
    if not geo or not device:
        return None
    if geo == "gb":
        geo = "uk"
    return geo, device


def build_trillion_segment_map() -> Tuple[Dict[Tuple[str, str], Dict[str, Any]], List[str]]:
    """
    Map (geo, device) -> Trillion hub campaign that serves that segment.

    Matches the same campaigns as the nightly close (folder allowlist + hub alias in URL).
    Value: ``{"campaign", "status", "is_active", "is_paused", "folder"}``.
    """
    logs: List[str] = []
    try:
        from config import KEYTR
        from integrations.nipuhim_tr_nightly_close import (
            _folder_allowlist,
            _index_hub_trillion_campaigns,
            resolve_hub_close_alias,
        )
        from integrations.trillion import list_campaigns
    except Exception as e:
        return {}, [f"Trillion map: import failed: {e}"]

    if not KEYTR:
        return {}, ["Trillion map: KEYTR not set — segments will have no campaign id"]

    try:
        alias = resolve_hub_close_alias()
        rows = list_campaigns(KEYTR)
        matched = _index_hub_trillion_campaigns(rows, alias=alias, folders=_folder_allowlist())
    except Exception as e:
        return {}, [f"Trillion map: list failed: {e}"]

    out: Dict[Tuple[str, str], Dict[str, Any]] = {}
    unparsed: List[str] = []
    for item in matched:
        name = str(item.get("campaign") or "").strip()
        seg = _parse_trillion_segment_name(name)
        if not seg:
            unparsed.append(name)
            continue
        out[seg] = {
            "campaign": name,
            "status": item.get("status"),
            "is_active": bool(item.get("is_active")),
            "is_paused": bool(item.get("is_paused")),
            "folder": item.get("folder"),
        }
    logs.append(f"Trillion map: {len(out)} geo×device segment(s) from {len(matched)} hub campaign(s)")
    if unparsed:
        logs.append(f"Trillion map: {len(unparsed)} unparsed name(s): {', '.join(sorted(unparsed)[:6])}")
    return out, logs


def _fetch_delivered_by_bucket(
    lines: List[BillLine],
) -> Tuple[Dict[Tuple[str, str, str, str], int], Dict[str, Any], List[str]]:
    """
    Return clicks keyed by (family, feed, geo, device) from each child campaign report.
  """
    meta: Dict[str, Any] = {"child_reports": {}}
    logs: List[str] = []
    delivered: Dict[Tuple[str, str, str, str], int] = defaultdict(int)

    campaigns: Dict[int, Tuple[str, str]] = {}
    for line in lines:
        if line.child_campaign_id is None:
            continue
        campaigns[int(line.child_campaign_id)] = (line.family, line.feed)

    for cid, (family, feed) in sorted(campaigns.items(), key=lambda x: x[0]):
        clicks, err = _keitaro_clicks_by_geo_device(cid)
        meta["child_reports"][str(cid)] = {
            "family": family,
            "feed": feed,
            "rows": len(clicks),
            "error": err,
        }
        if err:
            logs.append(f"Keitaro {family}/{feed} (id {cid}): {err}")
        for (geo, device), n in clicks.items():
            delivered[(family, feed, geo, device)] += int(n)

    return dict(delivered), meta, logs


def _allocate_blend_delivered(
    lines: List[BillLine],
    delivered_bucket: Dict[Tuple[str, str, str, str], int],
) -> Dict[str, float]:
    """Proportional split of geo×device×feed clicks across blend bill lines."""
    bucket_demand: Dict[Tuple[str, str, str, str], float] = defaultdict(float)
    by_bucket: Dict[Tuple[str, str, str, str], List[BillLine]] = defaultdict(list)
    for line in lines:
        if line.family != "blend":
            continue
        bk = (line.family, line.feed, line.geo, line.device)
        bucket_demand[bk] += line.demand
        by_bucket[bk].append(line)

    out: Dict[str, float] = {}
    for bk, group in by_bucket.items():
        total_d = bucket_demand[bk]
        got = float(delivered_bucket.get(bk, 0))
        if total_d <= 0:
            for line in group:
                out[line.line_key] = 0.0
            continue
        allocated = 0.0
        for i, line in enumerate(group):
            if i == len(group) - 1:
                share = max(0.0, got - allocated)
            else:
                share = got * (line.demand / total_d)
                allocated += share
            out[line.line_key] = share
    return out


def _line_delivered(
    line: BillLine,
    delivered_bucket: Dict[Tuple[str, str, str, str], int],
    blend_alloc: Dict[str, float],
) -> float:
    if line.family == "nipuhim":
        return float(delivered_bucket.get((line.family, line.feed, line.geo, line.device), 0))
    return float(blend_alloc.get(line.line_key, 0.0))


def build_domain_demand_payload(
    *,
    date_str: Optional[str] = None,
    max_offers_per_geo: int = 60,
    rebuild_demand: bool = True,
    reason: str = "manual",
) -> Dict[str, Any]:
    day = (date_str or _calendar_day()).strip()
    logs: List[str] = []
    nip_lines: List[BillLine] = []
    blend_lines: List[BillLine] = []

    if rebuild_demand:
        nip_lines, nip_logs = build_nipuhim_demand_lines(
            date_str=day,
            max_offers_per_geo=max_offers_per_geo,
        )
        blend_lines, blend_logs = build_blend_demand_lines()
        logs.extend(nip_logs)
        logs.extend(blend_logs)
    else:
        logs.append("Demand rebuild skipped (delivered-only refresh)")

    lines = nip_lines + blend_lines
    delivered_bucket, report_meta, k_logs = _fetch_delivered_by_bucket(lines)
    logs.extend(k_logs)

    blend_alloc = _allocate_blend_delivered(lines, delivered_bucket)
    hub_id = int(load_hub_state().get("hub_campaign_id") or KEITARO_HUB_CAMPAIGN_ID)
    hub_delivered, hub_err = _hub_total_clicks(hub_id)
    if hub_err:
        logs.append(f"Hub {hub_id} clicks: {hub_err}")

    hub_seg_clicks, hub_seg_err = _hub_clicks_by_geo_device(hub_id)
    if hub_seg_err:
        logs.append(f"Hub {hub_id} geo×device clicks: {hub_seg_err}")
    tr_map, tr_logs = build_trillion_segment_map()
    logs.extend(tr_logs)

    bill_rows: List[Dict[str, Any]] = []
    now = _utc_now_iso()
    for line in sorted(lines, key=lambda l: (l.family, l.feed, l.geo, l.device, l.brand)):
        got = _line_delivered(line, delivered_bucket, blend_alloc)
        demand = line.demand
        bill_rows.append(
            {
                "line_key": line.line_key,
                "family": line.family,
                "feed": line.feed,
                "geo": line.geo,
                "device": line.device,
                "brand": line.brand,
                "merchant_id": line.merchant_id,
                "demand_clicks": int(round(demand)),
                "delivered_clicks": int(round(got)),
                "remaining": _remaining(demand, got),
                "fill_pct": _fill_pct(got, demand),
                "child_campaign_id": line.child_campaign_id or "",
                "source": line.source,
            }
        )

    def _rollup(
        rows: List[Dict[str, Any]],
        *,
        family: Optional[str] = None,
        feed: Optional[str] = None,
    ) -> Tuple[float, float]:
        d = g = 0.0
        for r in rows:
            if family and r["family"] != family:
                continue
            if feed and r["feed"] != feed:
                continue
            d += float(r["demand_clicks"])
            g += float(r["delivered_clicks"])
        return d, g

    total_d, total_g = _rollup(bill_rows)
    pause_pct = DOMAIN_DEMAND_TRILLION_PAUSE_FILL_PCT
    trillion_hint = (
        "PAUSE_SUGGESTED"
        if total_d > 0 and (total_g / total_d) * 100.0 >= pause_pct
        else "OPEN"
    )

    blend_feed_caps: Dict[str, float] = defaultdict(float)
    nip_feed_caps: Dict[str, float] = defaultdict(float)
    for r in bill_rows:
        fk = r["feed"]
        if r["family"] == "blend":
            blend_feed_caps[fk] += float(r["demand_clicks"])
        else:
            nip_feed_caps[fk] += float(r["demand_clicks"])

    active_feeds = frozenset(set(blend_feed_caps) | set(nip_feed_caps) | set(KEITARO_HUB_ACTIVE_FEEDS))
    hub_weights = hub_offer_weights_from_caps(
        dict(blend_feed_caps),
        dict(nip_feed_caps),
        active_feeds=active_feeds,
        hub_types=("blend", "nipuhim"),
    )

    summary_rows: List[Dict[str, Any]] = []

    def _summary_row(
        scope: str,
        family: str,
        feed: str,
        demand: float,
        delivered: float,
        *,
        notes: str = "",
    ) -> Dict[str, Any]:
        hw = ""
        if scope == "family_feed" and family and feed:
            hw = round(hub_weights.get(f"hub_{family}_{feed}", 0.0), 2)
        hint = trillion_hint if scope == "hub_total" else ""
        return {
            "scope": scope,
            "family": family,
            "feed": feed,
            "demand_clicks": int(round(demand)),
            "delivered_clicks": int(round(delivered)),
            "remaining": _remaining(demand, delivered),
            "fill_pct": _fill_pct(delivered, demand),
            "hub_weight_pct": hw,
            "trillion_hint": hint,
            "notes": notes,
        }

    summary_rows.append(
        _summary_row(
            "hub_total",
            "",
            "",
            total_d,
            float(hub_delivered) if hub_delivered else total_g,
            notes=f"hub_campaign_id={hub_id}; child_sum={int(total_g)}",
        )
    )
    for fam in ("nipuhim", "blend"):
        d, g = _rollup(bill_rows, family=fam)
        summary_rows.append(_summary_row("family", fam, "", d, g))
        feeds_seen = sorted({r["feed"] for r in bill_rows if r["family"] == fam})
        for fk in feeds_seen:
            fd, fg = _rollup(bill_rows, family=fam, feed=fk)
            summary_rows.append(_summary_row("family_feed", fam, fk, fd, fg))

    # Per geo × device rollup — the actionable table for pausing Trillion segments.
    demand_by_seg: Dict[Tuple[str, str], float] = defaultdict(float)
    for r in bill_rows:
        demand_by_seg[(r["geo"], r["device"])] += float(r["demand_clicks"])

    # Actionable segments only: a mapped Trillion campaign or real demand (skip GeoIP noise).
    seg_keys = sorted(
        seg
        for seg in (set(demand_by_seg) | set(hub_seg_clicks) | set(tr_map))
        if seg in tr_map or demand_by_seg.get(seg, 0.0) > 0
    )
    summary_by_geo_rows: List[Dict[str, Any]] = []
    for geo, device in seg_keys:
        demand = demand_by_seg.get((geo, device), 0.0)
        delivered = float(hub_seg_clicks.get((geo, device), 0))
        tr = tr_map.get((geo, device)) or {}
        pct = _fill_pct(delivered, demand)
        if tr.get("is_paused") or (tr and not tr.get("is_active")):
            hint = "ALREADY_PAUSED"
        elif demand > 0 and pct is not None and pct >= pause_pct:
            hint = "PAUSE_SUGGESTED"
        else:
            hint = "OPEN"
        summary_by_geo_rows.append(
            {
                "geo": geo,
                "device": device,
                "demand_clicks": int(round(demand)),
                "delivered_clicks": int(round(delivered)),
                "remaining": _remaining(demand, delivered),
                "fill_pct": pct,
                "trillion_campaign": tr.get("campaign") or "",
                "trillion_status": tr.get("status") or "",
                "trillion_hint": hint,
            }
        )

    summary_by_geo_rows.sort(
        key=lambda r: (r["geo"], r["device"])
    )

    return {
        "updated_at": now,
        "date": day,
        "reason": reason,
        "logs": logs,
        "bill": bill_rows,
        "summary": summary_rows,
        "summary_by_geo": summary_by_geo_rows,
        "hub_campaign_id": hub_id,
        "hub_delivered_clicks": hub_delivered,
        "trillion_hint": trillion_hint,
        "hub_weights": hub_weights,
        "keitaro_meta": report_meta,
        "total_demand": int(round(total_d)),
        "total_delivered_child_sum": int(round(total_g)),
    }


def _get_sheets_service() -> Any:
    from blend_sync_from_sheet import get_sheets_service

    return get_sheets_service()


def _list_tabs(service: Any, spreadsheet_id: str) -> List[str]:
    meta = service.get(spreadsheetId=spreadsheet_id, fields="sheets(properties(title))").execute()
    return [
        str(s.get("properties", {}).get("title") or "")
        for s in meta.get("sheets", [])
        if s.get("properties", {}).get("title")
    ]


def _ensure_tab(service: Any, spreadsheet_id: str, tab: str, header: List[str]) -> None:
    titles = _list_tabs(service, spreadsheet_id)
    if tab not in titles:
        service.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab}}}]},
        ).execute()
    quoted = tab.replace("'", "''")
    service.values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{quoted}'!A1",
        valueInputOption="RAW",
        body={"values": [header]},
    ).execute()


def _write_tab(service: Any, spreadsheet_id: str, tab: str, header: List[str], rows: List[List[Any]]) -> None:
    _ensure_tab(service, spreadsheet_id, tab, header)
    quoted = tab.replace("'", "''")
    body = [header] + rows
    service.values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"'{quoted}'!A:Z",
    ).execute()
    service.values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{quoted}'!A1",
        valueInputOption="RAW",
        body={"values": body},
    ).execute()


def write_domain_demand_sheet(payload: Dict[str, Any], *, dry_run: bool = False) -> Dict[str, Any]:
    sheet_id = (DOMAIN_DEMAND_SHEET_ID or "").strip()
    if not sheet_id:
        return {"status": "error", "error": "DOMAIN_DEMAND_SHEET_ID not set"}

    now = payload.get("updated_at") or _utc_now_iso()
    day = payload.get("date") or _calendar_day()

    bill_values: List[List[Any]] = []
    for r in payload.get("bill") or []:
        bill_values.append(
            [
                now,
                day,
                r.get("line_key"),
                r.get("family"),
                r.get("feed"),
                r.get("geo"),
                r.get("device"),
                r.get("brand"),
                r.get("merchant_id"),
                r.get("demand_clicks"),
                r.get("delivered_clicks"),
                r.get("remaining"),
                r.get("fill_pct"),
                r.get("child_campaign_id"),
                r.get("source"),
            ]
        )

    summary_values: List[List[Any]] = []
    for r in payload.get("summary") or []:
        summary_values.append(
            [
                now,
                day,
                r.get("scope"),
                r.get("family"),
                r.get("feed"),
                r.get("demand_clicks"),
                r.get("delivered_clicks"),
                r.get("remaining"),
                r.get("fill_pct"),
                r.get("hub_weight_pct"),
                r.get("trillion_hint"),
                r.get("notes"),
            ]
        )

    geo_values: List[List[Any]] = []
    for r in payload.get("summary_by_geo") or []:
        geo_values.append(
            [
                now,
                day,
                r.get("geo"),
                r.get("device"),
                r.get("demand_clicks"),
                r.get("delivered_clicks"),
                r.get("remaining"),
                r.get("fill_pct"),
                r.get("trillion_campaign"),
                r.get("trillion_status"),
                r.get("trillion_hint"),
            ]
        )

    if dry_run:
        return {
            "status": "dry_run",
            "spreadsheet_id": sheet_id,
            "bill_rows": len(bill_values),
            "summary_rows": len(summary_values),
            "geo_rows": len(geo_values),
            "trillion_hint": payload.get("trillion_hint"),
        }

    service = _get_sheets_service()
    _write_tab(service, sheet_id, DOMAIN_DEMAND_SUMMARY_TAB, SUMMARY_HEADER, summary_values)
    _write_tab(service, sheet_id, DOMAIN_DEMAND_SUMMARY_BY_GEO_TAB, SUMMARY_BY_GEO_HEADER, geo_values)
    _write_tab(service, sheet_id, DOMAIN_DEMAND_BILL_TAB, BILL_HEADER, bill_values)
    return {
        "status": "ok",
        "spreadsheet_id": sheet_id,
        "bill_rows": len(bill_values),
        "summary_rows": len(summary_values),
        "geo_rows": len(geo_values),
        "trillion_hint": payload.get("trillion_hint"),
    }


def sync_domain_demand(
    *,
    date_str: Optional[str] = None,
    max_offers_per_geo: int = 60,
    rebuild_demand: bool = True,
    dry_run: bool = False,
    reason: str = "sync",
) -> Dict[str, Any]:
    payload = build_domain_demand_payload(
        date_str=date_str,
        max_offers_per_geo=max_offers_per_geo,
        rebuild_demand=rebuild_demand,
        reason=reason,
    )
    write_result = write_domain_demand_sheet(payload, dry_run=dry_run)
    return {**payload, "write": write_result}
