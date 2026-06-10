"""
Interactive review helpers for legacy Blend rows.

Reads the Blend sheet with worksheet row numbers, pulls Keitaro Blend campaign
offer/device stats for a custom date range, and writes confirmed device fields
back to the sheet one row at a time.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Tuple

from assistance import find_campaign_by_alias_or_name, get_campaigns_data
from blend_sync_from_sheet import (
    BLEND_CAMPAIGN_ALIAS,
    BLEND_SHEET_NAME,
    SPREADSHEET_ID,
    ensure_blend_sheet_headers,
    get_sheets_service,
)
from integrations.blend_device import (
    DEVICE_MODE_DESKTOP_ONLY,
    DEVICE_MODE_LEGACY,
    DEVICE_MODE_MOBILE_ONLY,
    classify_device_mode,
    device_mode_from_sheet_row,
    normalize_device_mode,
    resolve_blend_row_weights,
    split_click_cap_weights,
)
from integrations.keitaro import KeitaroClient, KeitaroClientError

DEFAULT_DATE_FROM = date(2026, 5, 21)
DEFAULT_DATE_TO = date(2026, 5, 23)

_REVIEW_WRITE_COLUMNS = (
    "device_mode",
    "weight_desktop",
    "weight_mobile",
    "cpc_desktop",
    "cpc_mobile",
)
_CLICK_KEYS_ORDER = (
    "valid_clicks",
    "clicks",
    "campaign_unique_clicks",
    "stream_unique_clicks",
    "global_unique_clicks",
)
_REVENUE_KEYS_ORDER = (
    "revenue",
    "campaign_revenue",
    "sale_revenue",
    "conversions_revenue",
    "sales_revenue",
    "payout",
    "earn",
)
_EPC_KEYS_ORDER = ("epc", "ecpc", "earn_per_click")


@dataclass(frozen=True)
class BlendReviewRow:
    sheet_row: int
    brand_name: str
    offer_url: str
    click_cap: float
    geo: str
    merchant_id: Optional[str]
    auto_flag: str
    feed_tag: str
    offer_name: str
    device_mode_raw: str
    weight_desktop_raw: str
    weight_mobile_raw: str
    cpc_desktop_raw: str
    cpc_mobile_raw: str
    current_device_mode: str
    current_weight_desktop: float
    current_weight_mobile: float


@dataclass(frozen=True)
class BlendReviewSheet:
    header: Tuple[str, ...]
    header_index: Dict[str, int]
    rows: List[BlendReviewRow]


@dataclass(frozen=True)
class OfferDeviceStat:
    offer_name: str
    channel: str
    revenue: float
    denominator: int
    denominator_key: str
    epc: float
    epc_source: str
    raw_clicks: Dict[str, int]


@dataclass(frozen=True)
class RowDecision:
    device_mode: str
    weight_desktop: float
    weight_mobile: float
    cpc_desktop: str
    cpc_mobile: str


def _quoted_sheet_name() -> str:
    return BLEND_SHEET_NAME.replace("'", "''")


def _normalize_geo(g: str) -> str:
    return (g or "").strip().lower()[:2]


def _slug(s: str, max_len: int = 48) -> str:
    import re

    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        return "unknown"
    return s[:max_len].rstrip("_")


def build_blend_offer_name(geo: str, feed_tag: str, brand_name: str) -> str:
    return f"blend_{_normalize_geo(geo)}_{_slug(feed_tag, max_len=24)}_{_slug(brand_name)}"


def _parse_float(v: Any) -> Optional[float]:
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


def _float_to_sheet(v: float) -> str:
    if float(v).is_integer():
        return str(int(v))
    return f"{float(v):.6f}".rstrip("0").rstrip(".")


def _epc_to_sheet(v: Optional[float]) -> str:
    if v is None:
        return ""
    return f"{float(v):.6f}".rstrip("0").rstrip(".")


def _column_letter(one_based_col: int) -> str:
    out = ""
    n = int(one_based_col)
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out


def _get_cell(row: list, idx: Dict[str, int], name: str) -> str:
    i = idx.get(name.strip().lower())
    if i is None or i >= len(row):
        return ""
    return str(row[i] or "").strip()


def _lower_keys(d: Dict[str, Any]) -> Dict[str, Any]:
    return {str(k).lower(): v for k, v in d.items()}


def _rows_from_report(report: Any) -> List[Dict[str, Any]]:
    if not isinstance(report, dict):
        return []
    for key in ("rows", "data", "result", "body"):
        value = report.get(key)
        if isinstance(value, list) and value and isinstance(value[0], dict):
            return [x for x in value if isinstance(x, dict)]
    return []


def _normalize_device_channel(device_type: str) -> Optional[str]:
    s = (device_type or "").strip().lower()
    if s == "desktop":
        return "desktop"
    if s in ("mobile phone", "mobile", "tablet", "smartphone"):
        return "mobile"
    return None


def _row_offer_name(row: Dict[str, Any]) -> str:
    lk = _lower_keys(row)
    for key in ("offer", "offer_name"):
        value = lk.get(key)
        if value is not None:
            return str(value).strip()
    return ""


def _row_device_type(row: Dict[str, Any]) -> str:
    lk = _lower_keys(row)
    for key in ("device_type", "device", "devicetype"):
        value = lk.get(key)
        if value is not None:
            return str(value).strip()
    return ""


def _row_float_by_keys(row: Dict[str, Any], keys: Iterable[str]) -> Optional[float]:
    lk = _lower_keys(row)
    for key in keys:
        if key in lk and lk[key] is not None:
            try:
                return float(lk[key])
            except (TypeError, ValueError):
                continue
    return None


def _row_clicks_map(row: Dict[str, Any]) -> Dict[str, int]:
    lk = _lower_keys(row)
    out: Dict[str, int] = {}
    for key in _CLICK_KEYS_ORDER:
        if key in lk and lk[key] is not None:
            try:
                out[key] = max(0, int(float(lk[key])))
            except (TypeError, ValueError):
                continue
    return out


def _resolve_blend_campaign_id() -> int:
    campaigns = get_campaigns_data()
    campaign = find_campaign_by_alias_or_name(
        campaigns, alias=BLEND_CAMPAIGN_ALIAS, name=BLEND_CAMPAIGN_ALIAS
    )
    if not campaign or campaign.get("id") is None:
        raise ValueError(f"Blend campaign not found (alias {BLEND_CAMPAIGN_ALIAS!r})")
    return int(campaign["id"])


def load_blend_review_sheet(
    service,
    *,
    only_geo: Optional[str] = None,
    legacy_only: bool = True,
    start_row: int = 2,
    limit: Optional[int] = None,
) -> BlendReviewSheet:
    quoted = _quoted_sheet_name()
    result = service.values().get(spreadsheetId=SPREADSHEET_ID, range=f"'{quoted}'!A:Z").execute()
    all_rows = result.get("values") or []
    if not all_rows:
        return BlendReviewSheet(header=tuple(), header_index={}, rows=[])
    header = tuple(str(c).strip() for c in all_rows[0])
    idx = {name.lower(): i for i, name in enumerate(header)}

    out: List[BlendReviewRow] = []
    start = max(2, int(start_row or 2))
    for sheet_row, row in enumerate(all_rows[1:], start=2):
        if sheet_row < start:
            continue
        brand = _get_cell(row, idx, "brandName")
        offer_url = _get_cell(row, idx, "offerUrl")
        geo = _normalize_geo(_get_cell(row, idx, "geo"))
        click_cap = _parse_float(_get_cell(row, idx, "clickCap"))
        if not brand or not offer_url or not geo or click_cap is None or click_cap <= 0:
            continue
        if only_geo and geo != _normalize_geo(only_geo):
            continue
        auto_flag = (_get_cell(row, idx, "auto") or "x").strip().lower()
        feed_tag = (_get_cell(row, idx, "feed") or "kelkoo1").strip().lower()
        merchant_id = _get_cell(row, idx, "merchantId") or None
        device_mode_raw = _get_cell(row, idx, "device_mode")
        weight_desktop_raw = _get_cell(row, idx, "weight_desktop")
        weight_mobile_raw = _get_cell(row, idx, "weight_mobile")
        cpc_desktop_raw = _get_cell(row, idx, "cpc_desktop")
        cpc_mobile_raw = _get_cell(row, idx, "cpc_mobile")
        current_mode, current_w_d, current_w_m = resolve_blend_row_weights(
            device_mode_raw,
            click_cap,
            cpc_desktop_raw,
            cpc_mobile_raw,
            weight_desktop_raw=weight_desktop_raw,
            weight_mobile_raw=weight_mobile_raw,
        )
        if legacy_only and current_mode != DEVICE_MODE_LEGACY:
            continue
        out.append(
            BlendReviewRow(
                sheet_row=sheet_row,
                brand_name=brand,
                offer_url=offer_url,
                click_cap=click_cap,
                geo=geo,
                merchant_id=merchant_id,
                auto_flag=auto_flag,
                feed_tag=feed_tag,
                offer_name=build_blend_offer_name(geo, feed_tag, brand),
                device_mode_raw=device_mode_raw,
                weight_desktop_raw=weight_desktop_raw,
                weight_mobile_raw=weight_mobile_raw,
                cpc_desktop_raw=cpc_desktop_raw,
                cpc_mobile_raw=cpc_mobile_raw,
                current_device_mode=current_mode,
                current_weight_desktop=current_w_d,
                current_weight_mobile=current_w_m,
            )
        )
        if limit is not None and len(out) >= int(limit):
            break
    return BlendReviewSheet(header=header, header_index=idx, rows=out)


def ensure_review_headers(service) -> BlendReviewSheet:
    ensure_blend_sheet_headers(service)
    return load_blend_review_sheet(service, legacy_only=False)


def _report_payloads_offer_device(campaign_id: int, d_from: date, d_to: date) -> List[Dict[str, Any]]:
    range_payloads = [
        {"from": f"{d_from.isoformat()} 00:00:00", "to": f"{d_to.isoformat()} 23:59:59"},
        {"from": d_from.isoformat(), "to": d_to.isoformat()},
        {
            "interval": "custom",
            "from": f"{d_from.isoformat()} 00:00:00",
            "to": f"{d_to.isoformat()} 23:59:59",
        },
    ]
    filters = [
        [
            {"name": "campaign_id", "operator": "EQUALS", "expression": campaign_id},
            {"name": "is_bot", "operator": "EQUALS", "expression": 0},
        ],
        [{"name": "campaign_id", "operator": "EQUALS", "expression": campaign_id}],
    ]
    groupings = [
        ["offer", "device_type"],
        ["offer", "device_type", "campaign_id"],
        ["offer", "device_type", "campaign"],
        ["offer_id", "offer", "device_type"],
    ]
    metrics = [
        ["revenue", "epc", "clicks", "campaign_unique_clicks", "stream_unique_clicks", "global_unique_clicks"],
        ["revenue", "clicks", "campaign_unique_clicks", "stream_unique_clicks", "global_unique_clicks"],
        ["revenue", "clicks", "epc"],
    ]
    payloads: List[Dict[str, Any]] = []
    for range_payload in range_payloads:
        for grouping in groupings:
            for metric_set in metrics:
                for filter_set in filters:
                    payloads.append(
                        {
                            "range": range_payload,
                            "grouping": grouping,
                            "metrics": metric_set,
                            "filters": filter_set,
                        }
                    )
    return payloads


def fetch_blend_offer_device_epc(
    *,
    d_from: date = DEFAULT_DATE_FROM,
    d_to: date = DEFAULT_DATE_TO,
) -> Dict[Tuple[str, str], OfferDeviceStat]:
    campaign_id = _resolve_blend_campaign_id()
    client = KeitaroClient()
    aggregated: Dict[Tuple[str, str], Dict[str, Any]] = {}
    last_err: Optional[str] = None

    chosen_source = "revenue_over_clicks"
    for payload in _report_payloads_offer_device(campaign_id, d_from, d_to):
        try:
            report = client.build_report(payload)
        except KeitaroClientError as e:
            last_err = str(e)
            continue
        except Exception as e:
            last_err = str(e)
            continue
        rows = _rows_from_report(report)
        if not rows:
            continue
        current: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for row in rows:
            offer_name = _row_offer_name(row)
            channel = _normalize_device_channel(_row_device_type(row))
            if not offer_name or not channel:
                continue
            key = (offer_name, channel)
            current.setdefault(
                key,
                {
                    "offer_name": offer_name,
                    "channel": channel,
                    "revenue": 0.0,
                    "epc_metric_sum": 0.0,
                    "epc_metric_count": 0,
                    "raw_clicks": {k: 0 for k in _CLICK_KEYS_ORDER},
                },
            )
            bucket = current[key]
            revenue = _row_float_by_keys(row, _REVENUE_KEYS_ORDER)
            if revenue is not None:
                bucket["revenue"] += revenue
            epc_metric = _row_float_by_keys(row, _EPC_KEYS_ORDER)
            if epc_metric is not None:
                bucket["epc_metric_sum"] += epc_metric
                bucket["epc_metric_count"] += 1
            for click_key, click_value in _row_clicks_map(row).items():
                bucket["raw_clicks"][click_key] += click_value
        if current:
            if any(str(f.get("name")).lower() == "is_bot" for f in (payload.get("filters") or [])):
                chosen_source = "revenue_over_clicks_bot_filtered"
            else:
                chosen_source = "revenue_over_clicks"
            aggregated = current
            break
    if not aggregated:
        raise RuntimeError(last_err or "Keitaro report returned no offer/device rows")

    out: Dict[Tuple[str, str], OfferDeviceStat] = {}
    for key, bucket in aggregated.items():
        raw_clicks = {
            click_key: int(bucket["raw_clicks"].get(click_key, 0))
            for click_key in _CLICK_KEYS_ORDER
            if int(bucket["raw_clicks"].get(click_key, 0)) > 0
        }
        revenue = float(bucket.get("revenue") or 0.0)
        denominator_key = ""
        denominator = 0
        for click_key in _CLICK_KEYS_ORDER:
            candidate = int(bucket["raw_clicks"].get(click_key, 0))
            if candidate > 0:
                denominator_key = click_key
                denominator = candidate
                break
        epc_source = chosen_source
        epc = 0.0
        if denominator > 0:
            epc = revenue / float(denominator)
        elif int(bucket.get("epc_metric_count") or 0) > 0:
            epc = float(bucket.get("epc_metric_sum") or 0.0) / float(bucket.get("epc_metric_count") or 1)
            epc_source = "epc_metric_average"
        out[key] = OfferDeviceStat(
            offer_name=str(bucket["offer_name"]),
            channel=str(bucket["channel"]),
            revenue=revenue,
            denominator=denominator,
            denominator_key=denominator_key,
            epc=epc,
            epc_source=epc_source,
            raw_clicks=raw_clicks,
        )
    return out


def suggest_row_decision(
    row: BlendReviewRow,
    stats_by_offer_device: Dict[Tuple[str, str], OfferDeviceStat],
) -> RowDecision:
    stat_d = stats_by_offer_device.get((row.offer_name, "desktop"))
    stat_m = stats_by_offer_device.get((row.offer_name, "mobile"))
    epc_d = float(stat_d.epc) if stat_d and stat_d.denominator > 0 else 0.0
    epc_m = float(stat_m.epc) if stat_m and stat_m.denominator > 0 else 0.0
    has_d = bool(stat_d and stat_d.denominator > 0)
    has_m = bool(stat_m and stat_m.denominator > 0)
    mode = classify_device_mode(epc_d, epc_m, has_desktop=has_d, has_mobile=has_m)
    w_d, w_m = split_click_cap_weights(
        row.click_cap,
        mode,
        desktop_cpc=epc_d,
        mobile_cpc=epc_m,
    )
    return RowDecision(
        device_mode=mode,
        weight_desktop=w_d,
        weight_mobile=w_m,
        cpc_desktop=_epc_to_sheet(epc_d) if has_d else "",
        cpc_mobile=_epc_to_sheet(epc_m) if has_m else "",
    )


def update_blend_review_row(
    service,
    sheet: BlendReviewSheet,
    row: BlendReviewRow,
    decision: RowDecision,
    *,
    dry_run: bool = False,
) -> Dict[str, str]:
    idx = sheet.header_index
    missing = [col for col in _REVIEW_WRITE_COLUMNS if col not in idx]
    if missing:
        raise ValueError(f"Blend sheet missing review columns: {missing}")

    updates = {
        "device_mode": normalize_device_mode(decision.device_mode),
        "weight_desktop": _float_to_sheet(decision.weight_desktop),
        "weight_mobile": _float_to_sheet(decision.weight_mobile),
        "cpc_desktop": str(decision.cpc_desktop or "").strip(),
        "cpc_mobile": str(decision.cpc_mobile or "").strip(),
    }
    if dry_run:
        return updates

    data = []
    quoted = _quoted_sheet_name()
    for col_name, value in updates.items():
        col_idx = idx[col_name] + 1
        data.append(
            {
                "range": f"'{quoted}'!{_column_letter(col_idx)}{row.sheet_row}",
                "values": [[value]],
            }
        )
    service.values().batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"valueInputOption": "RAW", "data": data},
    ).execute()
    return updates


def get_review_service():
    return get_sheets_service()
