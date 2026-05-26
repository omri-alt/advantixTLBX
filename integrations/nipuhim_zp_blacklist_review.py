"""
Reviewable Zeropark target blacklist candidates from yesterday's Nipuhim traffic.

Flow:
1. Read yesterday's dated Nipuhim offers sheets to reconstruct which merchant sat on
   each Keitaro offer slot (feed1_geo_productN / feed2_geo_productN).
2. Query Keitaro for yesterday's Nipuhim campaign grouped by country, sub_id_5, offer.
3. Keep only countries with >50 clicks yesterday and targets with >10 clicks.
4. For mapped merchants, check whether the merchant is still monetized on Kelkoo.
5. Flag targets whose monetized-offer val-click / clicks ratio is below 0.3.

The output is meant for manual review in the UI. It does not write Zeropark blacklists.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from assistance import find_campaign_by_alias_or_name, get_campaigns_data
from config import (
    FEED1_API_KEY,
    FEED2_API_KEY,
    FEED2_MERCHANTS_GEOS,
    KEITARO_CAMPAIGN_ALIAS,
    KELKOO_SHEETS_SPREADSHEET_ID,
)
from integrations.keitaro import KeitaroClient, KeitaroClientError
from integrations.kelkoo_search import format_kelkoo_monetization_status, kelkoo_merchant_link_check
from workflows.kelkoo_daily import build_merchant_id_to_name_from_feed, read_offers_sheet_rows
from workflows.monthly_log_monetization import build_merchant_geo_url_lookup, resolve_merchant_url

logger = logging.getLogger(__name__)

DEFAULT_MIN_COUNTRY_CLICKS = 50
DEFAULT_MIN_TARGET_CLICKS = 10
DEFAULT_MIN_MONETIZED_CLICKS = 10
DEFAULT_MAX_VALID_RATIO = 0.3
DEFAULT_LINK_CHECK_DELAY_SEC = 0.1
_VALID_CLICK_KEYS = ("valid_clicks", "custom_conversion_7")

_OFFER_SLOT_RE = re.compile(r"^(?:(feed(?P<feed>[12]))_)?(?P<geo>[a-z]{2})_product(?P<slot>\d+)$", re.I)


@dataclass(frozen=True)
class NipuhimOfferSlot:
    offer_name: str
    geo: str
    feed_num: int
    slot_index: int
    merchant_id: str
    product_title: str
    store_link: str


def default_analysis_date() -> date:
    return datetime.now(timezone.utc).date() - timedelta(days=1)


def parse_analysis_date(raw: str) -> date:
    text = (raw or "").strip()
    if not text:
        return default_analysis_date()
    return datetime.strptime(text, "%Y-%m-%d").date()


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _snapshot_path() -> Path:
    rel = (os.getenv("NIPUHIM_OFFER_SLOT_SNAPSHOT_PATH") or "runtime/nipuhim_offer_slots.json").strip()
    return _repo_root() / rel


def _credentials_path() -> Path:
    path = _repo_root() / "credentials.json"
    if not path.exists():
        raise FileNotFoundError(f"credentials.json not found at {path}")
    return path


def _get_sheets_service():
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds = service_account.Credentials.from_service_account_file(str(_credentials_path()))
    return build("sheets", "v4", credentials=creds).spreadsheets()


def _normalize_geo(raw: Any) -> str:
    value = str(raw or "").strip().lower()
    if len(value) < 2:
        return ""
    geo = value[:2]
    if geo == "gb":
        return "uk"
    return geo


def _lower_keys(row: Dict[str, Any]) -> Dict[str, Any]:
    return {str(k).lower(): v for k, v in row.items()}


def _rows_from_report(report: Any) -> List[Dict[str, Any]]:
    if not isinstance(report, dict):
        return []
    for key in ("rows", "data", "result", "body"):
        value = report.get(key)
        if isinstance(value, list) and value and isinstance(value[0], dict):
            return [x for x in value if isinstance(x, dict)]
    return []


def _row_text_by_keys(row: Dict[str, Any], keys: Iterable[str]) -> str:
    lk = _lower_keys(row)
    for key in keys:
        value = lk.get(key)
        if value is not None:
            text = str(value).strip()
            if text:
                return text
    return ""


def _row_int_by_keys(row: Dict[str, Any], keys: Iterable[str]) -> int:
    lk = _lower_keys(row)
    for key in keys:
        value = lk.get(key)
        if value is None:
            continue
        try:
            return max(0, int(float(value)))
        except (TypeError, ValueError):
            continue
    return 0


def _resolve_nipuhim_campaign_id() -> Tuple[str, int]:
    alias = (KEITARO_CAMPAIGN_ALIAS or "HrQBXp").strip()
    campaigns = get_campaigns_data()
    campaign = find_campaign_by_alias_or_name(campaigns, alias=alias, name=alias)
    if not campaign or campaign.get("id") is None:
        raise ValueError(f"Nipuhim campaign not found (alias {alias!r})")
    return alias, int(campaign["id"])


def _report_payloads(campaign_id: int, run_day: date) -> List[Dict[str, Any]]:
    day_str = run_day.isoformat()
    filters = [[{"name": "campaign_id", "operator": "EQUALS", "expression": campaign_id}]]
    ranges = [
        {"interval": "yesterday", "timezone": "America/Danmarkshavn"},
        {"interval": "yesterday", "timezone": "UTC"},
        {"from": f"{day_str} 00:00:00", "to": f"{day_str} 23:59:59"},
        {"from": day_str, "to": day_str},
    ]
    groupings = [
        ["country", "sub_id_5", "offer", "offer_id"],
        ["country", "sub_id_5", "offer"],
        ["country", "sub_id_5", "offer_id", "offer"],
    ]
    metrics = [
        ["clicks", "valid_clicks"],
        ["clicks", "custom_conversion_7"],
        ["clicks", "valid_clicks", "campaign_unique_clicks", "stream_unique_clicks", "global_unique_clicks"],
        ["clicks", "custom_conversion_7", "campaign_unique_clicks", "stream_unique_clicks", "global_unique_clicks"],
    ]
    payloads: List[Dict[str, Any]] = []
    for range_payload in ranges:
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


def _fetch_nipuhim_country_target_offer_rows(run_day: date) -> Tuple[str, int, List[Dict[str, Any]]]:
    alias, campaign_id = _resolve_nipuhim_campaign_id()
    client = KeitaroClient()
    offer_name_by_id: Dict[int, str] = {}
    for offer in client.get_offers():
        oid = offer.get("id")
        if oid is None:
            continue
        try:
            offer_name_by_id[int(oid)] = str(offer.get("name") or "").strip()
        except (TypeError, ValueError):
            continue

    last_err: Optional[str] = None
    for payload in _report_payloads(campaign_id, run_day):
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

        aggregated: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        saw_valid_clicks_field = False
        valid_click_metric = ""
        for row in rows:
            geo = _normalize_geo(_row_text_by_keys(row, ("country", "country_code", "geo")))
            target = _row_text_by_keys(row, ("sub_id_5", "target"))
            offer_name = _row_text_by_keys(row, ("offer", "offer_name"))
            offer_id = _row_int_by_keys(row, ("offer_id",))
            if not offer_name and offer_id > 0:
                offer_name = offer_name_by_id.get(offer_id, "")
            clicks = _row_int_by_keys(row, ("clicks",))
            valid_clicks = _row_int_by_keys(row, _VALID_CLICK_KEYS)
            lk = _lower_keys(row)
            for key_name in _VALID_CLICK_KEYS:
                if key_name in lk:
                    saw_valid_clicks_field = True
                    if not valid_click_metric:
                        valid_click_metric = key_name
                    break
            if not geo or not target or not offer_name or clicks <= 0:
                continue
            key = (geo, target, offer_name)
            bucket = aggregated.setdefault(
                key,
                {
                    "geo": geo,
                    "target": target,
                    "offer_name": offer_name,
                    "offer_id": offer_id,
                    "clicks": 0,
                    "valid_clicks": 0,
                    "valid_click_metric": valid_click_metric,
                },
            )
            bucket["clicks"] += clicks
            bucket["valid_clicks"] += valid_clicks
            if offer_id > 0 and not bucket.get("offer_id"):
                bucket["offer_id"] = offer_id
            if valid_click_metric and not bucket.get("valid_click_metric"):
                bucket["valid_click_metric"] = valid_click_metric

        if aggregated:
            if not saw_valid_clicks_field:
                raise RuntimeError("Keitaro report returned rows without a val-click metric")
            return alias, campaign_id, list(aggregated.values())

    raise RuntimeError(last_err or "Keitaro report returned no country/sub_id_5/offer rows")

def _offer_slots_from_sheet_day(run_day: date) -> Dict[str, NipuhimOfferSlot]:
    spreadsheet_id = (KELKOO_SHEETS_SPREADSHEET_ID or "").strip()
    if not spreadsheet_id:
        raise RuntimeError("KELKOO_SHEETS_SPREADSHEET_ID is not configured")
    service = _get_sheets_service()
    day_str = run_day.isoformat()
    slots: Dict[str, NipuhimOfferSlot] = {}
    for feed_num in (1, 2):
        sheet_name = f"{day_str}_offers_{feed_num}"
        rows = read_offers_sheet_rows(service, spreadsheet_id, sheet_name)
        if not rows:
            logger.info("Nipuhim ZP review: no rows in %s", sheet_name)
            continue
        geo_positions: Dict[str, int] = defaultdict(int)
        for row in rows:
            geo = _normalize_geo(row.get("Country"))
            merchant_id = str(row.get("Merchant ID") or "").strip()
            if not geo or not merchant_id:
                continue
            geo_positions[geo] += 1
            slot_index = geo_positions[geo]
            offer_name = f"feed{feed_num}_{geo}_product{slot_index}"
            slots[offer_name] = NipuhimOfferSlot(
                offer_name=offer_name,
                geo=geo,
                feed_num=feed_num,
                slot_index=slot_index,
                merchant_id=merchant_id,
                product_title=str(row.get("Product Title") or "").strip(),
                store_link=str(row.get("Store Link") or "").strip(),
            )
    return slots


def _snapshot_payload_from_slots(slots: Dict[str, NipuhimOfferSlot]) -> List[Dict[str, Any]]:
    return [
        {
            "offer_name": slot.offer_name,
            "geo": slot.geo,
            "feed_num": slot.feed_num,
            "slot_index": slot.slot_index,
            "merchant_id": slot.merchant_id,
            "product_title": slot.product_title,
            "store_link": slot.store_link,
        }
        for slot in sorted(slots.values(), key=lambda item: (item.feed_num, item.geo, item.slot_index))
    ]


def _slots_from_snapshot_items(items: List[Dict[str, Any]]) -> Dict[str, NipuhimOfferSlot]:
    out: Dict[str, NipuhimOfferSlot] = {}
    for item in items or []:
        offer_name = str(item.get("offer_name") or "").strip()
        geo = _normalize_geo(item.get("geo"))
        merchant_id = str(item.get("merchant_id") or "").strip()
        try:
            feed_num = int(item.get("feed_num") or 0)
            slot_index = int(item.get("slot_index") or 0)
        except (TypeError, ValueError):
            continue
        if not offer_name or not geo or feed_num not in (1, 2) or slot_index <= 0 or not merchant_id:
            continue
        out[offer_name] = NipuhimOfferSlot(
            offer_name=offer_name,
            geo=geo,
            feed_num=feed_num,
            slot_index=slot_index,
            merchant_id=merchant_id,
            product_title=str(item.get("product_title") or "").strip(),
            store_link=str(item.get("store_link") or "").strip(),
        )
    return out


def snapshot_offer_slots_for_day(run_day: Optional[date] = None, *, keep_days: int = 45) -> Dict[str, Any]:
    run_day = run_day or datetime.now(timezone.utc).date()
    slots = _offer_slots_from_sheet_day(run_day)
    path = _snapshot_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        current = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    except Exception:
        current = {}
    days = dict(current.get("days") or {})
    days[run_day.isoformat()] = _snapshot_payload_from_slots(slots)
    ordered_days = sorted(days.keys(), reverse=True)
    trimmed = {day: days[day] for day in ordered_days[: max(1, int(keep_days))]}
    payload = {
        "saved_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "days": trimmed,
    }
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    return {
        "path": str(path),
        "date": run_day.isoformat(),
        "offer_slots": len(slots),
    }


def _slots_from_snapshot_day(run_day: date) -> Dict[str, NipuhimOfferSlot]:
    path = _snapshot_path()
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    day_items = (payload.get("days") or {}).get(run_day.isoformat()) or []
    return _slots_from_snapshot_items(day_items)


def _load_offer_slots_for_day(run_day: date) -> Tuple[Dict[str, NipuhimOfferSlot], str, str]:
    exact_slots = _offer_slots_from_sheet_day(run_day)
    if exact_slots:
        return exact_slots, "sheet", run_day.isoformat()

    snap_slots = _slots_from_snapshot_day(run_day)
    if snap_slots:
        return snap_slots, "snapshot", run_day.isoformat()

    today = datetime.now(timezone.utc).date()
    if run_day != today:
        fallback_slots = _offer_slots_from_sheet_day(today)
        if fallback_slots:
            return fallback_slots, "current_sheet_fallback", today.isoformat()
    return {}, "missing", ""


def _match_offer_slot(
    offer_name: str,
    slots_by_offer_name: Dict[str, NipuhimOfferSlot],
) -> Optional[NipuhimOfferSlot]:
    name = str(offer_name or "").strip()
    if not name:
        return None
    if name in slots_by_offer_name:
        return slots_by_offer_name[name]
    m = _OFFER_SLOT_RE.match(name)
    if not m:
        return None
    geo = _normalize_geo(m.group("geo"))
    slot_index = int(m.group("slot"))
    feed_s = m.group("feed")
    if feed_s:
        return slots_by_offer_name.get(f"feed{feed_s}_{geo}_product{slot_index}")
    for feed_num in (1, 2):
        hit = slots_by_offer_name.get(f"feed{feed_num}_{geo}_product{slot_index}")
        if hit is not None:
            return hit
    return None


def _merchant_context_map(
    keys: Iterable[Tuple[int, str, str]],
    *,
    delay_sec: float = DEFAULT_LINK_CHECK_DELAY_SEC,
) -> Dict[Tuple[int, str, str], Dict[str, str]]:
    needed = {(int(feed_num), _normalize_geo(geo), str(mid).strip()) for feed_num, geo, mid in keys if mid}
    if not needed:
        return {}

    by_feed: Dict[int, set[str]] = defaultdict(set)
    for feed_num, geo, _mid in needed:
        by_feed[feed_num].add(geo)

    out: Dict[Tuple[int, str, str], Dict[str, str]] = {}
    for feed_num, geos in sorted(by_feed.items()):
        api_key = (FEED1_API_KEY if feed_num == 1 else FEED2_API_KEY) or ""
        feed_geos = sorted(geos)
        lookup_geos = list(FEED2_MERCHANTS_GEOS) if feed_num == 2 and FEED2_MERCHANTS_GEOS else feed_geos
        name_lookup: Dict[str, str] = {}
        by_geo_id: Dict[Tuple[str, str], str] = {}
        by_id: Dict[str, str] = {}
        if api_key:
            try:
                name_lookup = build_merchant_id_to_name_from_feed(api_key, lookup_geos)
            except Exception as e:
                logger.warning("Nipuhim ZP review: merchant name lookup feed%s failed: %s", feed_num, e)
            try:
                by_geo_id, by_id = build_merchant_geo_url_lookup(api_key, lookup_geos)
            except Exception as e:
                logger.warning("Nipuhim ZP review: merchant URL lookup feed%s failed: %s", feed_num, e)

        for key in sorted(k for k in needed if k[0] == feed_num):
            _feed_num, geo, merchant_id = key
            merchant_name = name_lookup.get(merchant_id, "")
            if not api_key:
                status = "missing_api_key"
            else:
                url = resolve_merchant_url(geo.upper(), merchant_id, by_geo_id, by_id)
                if not url:
                    status = "no_merchant_url"
                else:
                    try:
                        status = format_kelkoo_monetization_status(
                            kelkoo_merchant_link_check(url, geo, api_key)
                        )
                    except Exception as e:
                        status = f"error ({e})"
                    if delay_sec > 0:
                        time.sleep(delay_sec)
            out[key] = {
                "merchant_name": merchant_name,
                "monetization_status": status,
            }
    return out


def _is_monetized_status(status: str) -> bool:
    return str(status or "").strip().lower().startswith("monetized")


def build_nipuhim_zp_blacklist_review(
    *,
    run_day: Optional[date] = None,
    min_country_clicks: int = DEFAULT_MIN_COUNTRY_CLICKS,
    min_target_clicks: int = DEFAULT_MIN_TARGET_CLICKS,
    min_monetized_clicks: int = DEFAULT_MIN_MONETIZED_CLICKS,
    max_valid_ratio: float = DEFAULT_MAX_VALID_RATIO,
) -> Dict[str, Any]:
    run_day = run_day or default_analysis_date()
    alias, campaign_id, rows = _fetch_nipuhim_country_target_offer_rows(run_day)
    offer_slots, slot_mapping_source, slot_mapping_date = _load_offer_slots_for_day(run_day)
    warnings: List[str] = []
    if not offer_slots:
        warnings.append(
            "No exact or fallback Nipuhim offer-slot mapping was available, so merchants could not be matched."
        )
    elif slot_mapping_source == "current_sheet_fallback":
        warnings.append(
            f"Exact {run_day.isoformat()} offer slots were unavailable; merchant matching is using current day "
            f"slots from {slot_mapping_date} and may be approximate."
        )

    geo_clicks: Dict[str, int] = defaultdict(int)
    target_clicks: Dict[Tuple[str, str], int] = defaultdict(int)
    for row in rows:
        geo = row["geo"]
        target = row["target"]
        clicks = int(row["clicks"])
        geo_clicks[geo] += clicks
        target_clicks[(geo, target)] += clicks

    review_geos = {geo for geo, clicks in geo_clicks.items() if clicks > int(min_country_clicks)}
    review_target_keys = {
        (geo, target)
        for (geo, target), clicks in target_clicks.items()
        if geo in review_geos and clicks > int(min_target_clicks)
    }

    merchant_keys = set()
    for row in rows:
        key = (row["geo"], row["target"])
        if key not in review_target_keys:
            continue
        slot = _match_offer_slot(str(row["offer_name"]), offer_slots)
        if slot:
            merchant_keys.add((slot.feed_num, slot.geo, slot.merchant_id))

    merchant_context = _merchant_context_map(merchant_keys)

    by_geo_target: Dict[Tuple[str, str], Dict[str, Any]] = {}
    mapped_offer_rows = 0
    unmapped_offer_rows = 0
    monetized_offer_rows = 0

    for row in rows:
        geo = row["geo"]
        target = row["target"]
        if (geo, target) not in review_target_keys:
            continue
        slot = _match_offer_slot(str(row["offer_name"]), offer_slots)
        detail = {
            "offer_name": row["offer_name"],
            "offer_id": row.get("offer_id") or "",
            "clicks": int(row["clicks"]),
            "valid_clicks": int(row["valid_clicks"]),
            "valid_ratio": (
                round(float(row["valid_clicks"]) / float(row["clicks"]), 4)
                if int(row["clicks"]) > 0
                else None
            ),
            "mapped": bool(slot),
            "feed": slot.feed_num if slot else None,
            "merchant_id": slot.merchant_id if slot else "",
            "merchant_name": "",
            "monetization_status": "",
            "product_title": slot.product_title if slot else "",
        }
        if slot:
            mapped_offer_rows += 1
            ctx = merchant_context.get((slot.feed_num, slot.geo, slot.merchant_id), {})
            detail["merchant_name"] = str(ctx.get("merchant_name") or "")
            detail["monetization_status"] = str(ctx.get("monetization_status") or "")
            if _is_monetized_status(detail["monetization_status"]):
                monetized_offer_rows += 1
        else:
            unmapped_offer_rows += 1

        bucket = by_geo_target.setdefault(
            (geo, target),
            {
                "geo": geo,
                "target": target,
                "country_clicks": int(geo_clicks.get(geo, 0)),
                "target_clicks": 0,
                "target_valid_clicks": 0,
                "monetized_clicks": 0,
                "monetized_valid_clicks": 0,
                "mapped_offer_rows": 0,
                "unmapped_offer_rows": 0,
                "offers": [],
                "merchant_labels": set(),
            },
        )
        bucket["target_clicks"] += int(row["clicks"])
        bucket["target_valid_clicks"] += int(row["valid_clicks"])
        if slot:
            bucket["mapped_offer_rows"] += 1
            if _is_monetized_status(detail["monetization_status"]):
                bucket["monetized_clicks"] += int(row["clicks"])
                bucket["monetized_valid_clicks"] += int(row["valid_clicks"])
                merchant_label = detail["merchant_name"] or detail["merchant_id"]
                if merchant_label:
                    bucket["merchant_labels"].add(str(merchant_label))
        else:
            bucket["unmapped_offer_rows"] += 1
        bucket["offers"].append(detail)

    countries: List[Dict[str, Any]] = []
    blacklist_candidates_count = 0
    for geo in sorted(review_geos):
        targets: List[Dict[str, Any]] = []
        for (row_geo, target), bucket in by_geo_target.items():
            if row_geo != geo:
                continue
            monetized_clicks = int(bucket["monetized_clicks"])
            monetized_valid_clicks = int(bucket["monetized_valid_clicks"])
            total_clicks = int(bucket["target_clicks"])
            total_valid_clicks = int(bucket["target_valid_clicks"])
            total_ratio = round(total_valid_clicks / total_clicks, 4) if total_clicks > 0 else None
            monetized_ratio = (
                round(monetized_valid_clicks / monetized_clicks, 4)
                if monetized_clicks > 0
                else None
            )
            eligible = monetized_clicks >= int(min_monetized_clicks)
            candidate = bool(
                eligible
                and monetized_ratio is not None
                and monetized_ratio < float(max_valid_ratio)
            )
            if candidate:
                blacklist_candidates_count += 1
            offers = sorted(
                bucket["offers"],
                key=lambda item: (
                    0 if _is_monetized_status(item.get("monetization_status") or "") else 1,
                    -(item.get("clicks") or 0),
                    str(item.get("offer_name") or ""),
                ),
            )
            targets.append(
                {
                    "target": target,
                    "country_clicks": bucket["country_clicks"],
                    "target_clicks": total_clicks,
                    "target_valid_clicks": total_valid_clicks,
                    "target_valid_ratio": total_ratio,
                    "monetized_clicks": monetized_clicks,
                    "monetized_valid_clicks": monetized_valid_clicks,
                    "monetized_valid_ratio": monetized_ratio,
                    "eligible": eligible,
                    "candidate": candidate,
                    "mapped_offer_rows": int(bucket["mapped_offer_rows"]),
                    "unmapped_offer_rows": int(bucket["unmapped_offer_rows"]),
                    "merchant_labels": sorted(bucket["merchant_labels"]),
                    "offers": offers,
                }
            )
        targets.sort(
            key=lambda item: (
                0 if item["candidate"] else 1,
                item["monetized_valid_ratio"] if item["monetized_valid_ratio"] is not None else 999,
                -item["monetized_clicks"],
                -item["target_clicks"],
                item["target"],
            )
        )
        candidate_targets = [item["target"] for item in targets if item["candidate"]]
        countries.append(
            {
                "geo": geo,
                "country_clicks": int(geo_clicks.get(geo, 0)),
                "target_count": len(targets),
                "candidate_count": len(candidate_targets),
                "candidate_targets": candidate_targets,
                "candidate_copy_block": "\n".join(candidate_targets),
                "targets": targets,
            }
        )

    countries.sort(key=lambda item: (-item["candidate_count"], -item["country_clicks"], item["geo"]))

    return {
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "analysis_date": run_day.isoformat(),
        "campaign_alias": alias,
        "campaign_id": campaign_id,
        "valid_click_metric": str((rows[0].get("valid_click_metric") if rows else "") or ""),
        "slot_mapping_source": slot_mapping_source,
        "slot_mapping_date": slot_mapping_date,
        "warnings": warnings,
        "thresholds": {
            "min_country_clicks": int(min_country_clicks),
            "min_target_clicks": int(min_target_clicks),
            "min_monetized_clicks": int(min_monetized_clicks),
            "max_valid_ratio": float(max_valid_ratio),
        },
        "summary": {
            "report_rows": len(rows),
            "countries_with_clicks": len(geo_clicks),
            "countries_reviewed": len(review_geos),
            "targets_reviewed": len(review_target_keys),
            "blacklist_candidates": blacklist_candidates_count,
            "mapped_offer_rows": mapped_offer_rows,
            "unmapped_offer_rows": unmapped_offer_rows,
            "monetized_offer_rows": monetized_offer_rows,
            "offer_slots_loaded": len(offer_slots),
        },
        "countries": countries,
    }
