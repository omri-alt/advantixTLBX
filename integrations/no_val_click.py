"""
Yesterday Nipuhim / Blend offers that received clicks but no Val_clicks.

Nipuhim: skip that merchant at today's pick and take the next ranked one.
Blend: keep the row, color it light red on the Blend sheet.
"""
from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from config import (
    BLEND_SHEETS_SPREADSHEET_ID,
    FEED5_API_KEY,
    KELKOO_SHEETS_SPREADSHEET_ID,
    NO_VAL_CLICK_ENABLED,
    NO_VAL_CLICK_MIN_CLICKS,
)
from integrations.keitaro import KeitaroClient, KeitaroClientError
from integrations.keitaro_child_campaigns import blend_child_campaign_id, nipuhim_child_campaign_id
from workflows.kelkoo_daily import (
    COLOR_LIGHT_RED,
    _geo_key_from_offer_country_cell,
    _normalize_merchant_id_from_sheet,
    read_offers_sheet_rows,
)

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
_VALID_CLICK_KEYS = ("valid_clicks", "custom_conversion_7")
_NIPUHIM_OFFER_RE = re.compile(
    r"^feed(?P<feed>\d+)_(?P<geo>[a-z]{2})_product(?P<slot>\d+)$",
    re.I,
)
_NIPUHIM_FEEDS = (1, 2, 5)
_BLEND_FEED_KEYS = ("kelkoo1", "kelkoo2", "kelkoo5", "kelkoo4", "adexa", "yadore", "shopnomix")
COLOR_WHITE = {"red": 1.0, "green": 1.0, "blue": 1.0}

_cache: Dict[str, Any] = {}


def yesterday_utc(run_date: Optional[date] = None) -> date:
    day = run_date or datetime.now(timezone.utc).date()
    return day - timedelta(days=1)


def _last_path() -> Path:
    return ROOT / "runtime" / "no_val_click_last.json"


def _credentials_path() -> Path:
    path = ROOT / "credentials.json"
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
        if value is None:
            continue
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


def _offer_name_by_id(client: KeitaroClient) -> Dict[int, str]:
    out: Dict[int, str] = {}
    try:
        offers = client.get_offers()
    except Exception:
        logger.exception("no-val-click: failed to list Keitaro offers")
        return out
    for offer in offers or []:
        oid = offer.get("id")
        if oid is None:
            continue
        try:
            out[int(oid)] = str(offer.get("name") or "").strip()
        except (TypeError, ValueError):
            continue
    return out


def _report_payloads(campaign_id: int, run_day: date) -> List[Dict[str, Any]]:
    day_str = run_day.isoformat()
    filters = [[{"name": "campaign_id", "operator": "EQUALS", "expression": campaign_id}]]
    ranges = [
        {"from": f"{day_str} 00:00:00", "to": f"{day_str} 23:59:59"},
        {"from": day_str, "to": day_str},
    ]
    groupings = [
        ["offer", "offer_id"],
        ["offer"],
    ]
    metrics = [
        ["clicks", "valid_clicks"],
        ["clicks", "custom_conversion_7"],
    ]
    payloads: List[Dict[str, Any]] = []
    for range_payload in ranges:
        for grouping in groupings:
            for metric_set in metrics:
                payloads.append(
                    {
                        "range": range_payload,
                        "grouping": grouping,
                        "metrics": metric_set,
                        "filters": filters,
                    }
                )
    return payloads


def _fetch_campaign_offer_rows(
    client: KeitaroClient,
    campaign_id: int,
    run_day: date,
    offer_names: Dict[int, str],
) -> List[Dict[str, Any]]:
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
        aggregated: Dict[str, Dict[str, Any]] = {}
        saw_valid = False
        for row in rows:
            offer_name = _row_text_by_keys(row, ("offer", "offer_name"))
            offer_id = _row_int_by_keys(row, ("offer_id",))
            if not offer_name and offer_id > 0:
                offer_name = offer_names.get(offer_id, "")
            clicks = _row_int_by_keys(row, ("clicks",))
            valid_clicks = _row_int_by_keys(row, _VALID_CLICK_KEYS)
            lk = _lower_keys(row)
            if any(k in lk for k in _VALID_CLICK_KEYS):
                saw_valid = True
            if not offer_name or clicks <= 0:
                continue
            bucket = aggregated.setdefault(
                offer_name,
                {
                    "offer_name": offer_name,
                    "offer_id": offer_id,
                    "clicks": 0,
                    "valid_clicks": 0,
                },
            )
            bucket["clicks"] += clicks
            bucket["valid_clicks"] += valid_clicks
            if offer_id > 0 and not bucket.get("offer_id"):
                bucket["offer_id"] = offer_id
        if aggregated:
            if not saw_valid:
                logger.warning(
                    "no-val-click: campaign %s report had clicks but no Val_click metric; skipping",
                    campaign_id,
                )
                return []
            return list(aggregated.values())
    if last_err:
        logger.warning("no-val-click: campaign %s report failed: %s", campaign_id, last_err)
    return []


def _nipuhim_slots_from_sheet(run_day: date) -> Dict[str, Dict[str, Any]]:
    spreadsheet_id = (KELKOO_SHEETS_SPREADSHEET_ID or "").strip()
    if not spreadsheet_id:
        raise RuntimeError("KELKOO_SHEETS_SPREADSHEET_ID is not configured")
    service = _get_sheets_service()
    day_str = run_day.isoformat()
    feeds = [1, 2]
    if (FEED5_API_KEY or "").strip():
        feeds.append(5)
    slots: Dict[str, Dict[str, Any]] = {}
    for feed_num in feeds:
        sheet_name = f"{day_str}_offers_{feed_num}"
        rows = read_offers_sheet_rows(service, spreadsheet_id, sheet_name)
        if not rows:
            logger.info("no-val-click: no rows in %s", sheet_name)
            continue
        geo_positions: Dict[str, int] = defaultdict(int)
        for row in rows:
            geo = _geo_key_from_offer_country_cell(row.get("Country"))
            if geo == "gb":
                geo = "uk"
            merchant_id = _normalize_merchant_id_from_sheet(row.get("Merchant ID"))
            if not geo or not merchant_id:
                continue
            geo_positions[geo] += 1
            slot_index = geo_positions[geo]
            offer_name = f"feed{feed_num}_{geo}_product{slot_index}"
            slots[offer_name] = {
                "offer_name": offer_name,
                "geo": geo,
                "feed_num": feed_num,
                "slot_index": slot_index,
                "merchant_id": merchant_id,
            }
    return slots


def analyze_yesterday(
    *,
    run_date: Optional[date] = None,
    min_clicks: Optional[int] = None,
    persist: bool = False,
    scope: str = "all",
) -> Dict[str, Any]:
    """
    Inspect yesterday's Nipuhim (feeds 1/2/5) and/or Blend child campaigns.

    ``scope``: ``all`` | ``nipuhim`` | ``blend``.
    """
    run_day = yesterday_utc(run_date)
    min_clicks = int(min_clicks or NO_VAL_CLICK_MIN_CLICKS)
    want_n = scope in ("all", "nipuhim")
    want_b = scope in ("all", "blend")
    cache_key = f"{run_day.isoformat()}:{min_clicks}:{scope}"
    if cache_key in _cache:
        return _cache[cache_key]

    client = KeitaroClient()
    offer_names = _offer_name_by_id(client)
    slots: Dict[str, Dict[str, Any]] = {}
    nipuhim_offers: List[Dict[str, Any]] = []
    merchant_agg: Dict[Tuple[int, str, str], Dict[str, Any]] = {}
    if want_n:
        slots = _nipuhim_slots_from_sheet(run_day)
        for feed_num in _NIPUHIM_FEEDS:
            if feed_num == 5 and not (FEED5_API_KEY or "").strip():
                continue
            feed_key = f"kelkoo{feed_num}"
            try:
                cid = nipuhim_child_campaign_id(feed_key)
            except Exception as e:
                logger.warning("no-val-click: skip NIPUHIM-%s (%s)", feed_key, e)
                continue
            for row in _fetch_campaign_offer_rows(client, cid, run_day, offer_names):
                name = str(row.get("offer_name") or "").strip()
                slot = slots.get(name)
                if not slot:
                    m = _NIPUHIM_OFFER_RE.match(name)
                    if m:
                        try:
                            parsed_feed = int(m.group("feed"))
                        except (TypeError, ValueError):
                            parsed_feed = feed_num
                        slot = {
                            "offer_name": name,
                            "geo": _normalize_geo(m.group("geo")),
                            "feed_num": parsed_feed,
                            "slot_index": int(m.group("slot") or 0),
                            "merchant_id": "",
                        }
                if not slot:
                    continue
                geo = slot["geo"]
                mid = str(slot.get("merchant_id") or "").strip()
                item = {
                    **row,
                    "geo": geo,
                    "feed_num": int(slot["feed_num"]),
                    "merchant_id": mid,
                    "campaign_id": cid,
                }
                nipuhim_offers.append(item)
                if not mid or not geo:
                    continue
                key = (int(slot["feed_num"]), geo, mid)
                bucket = merchant_agg.setdefault(
                    key,
                    {
                        "feed_num": key[0],
                        "geo": geo,
                        "merchant_id": mid,
                        "clicks": 0,
                        "valid_clicks": 0,
                        "offers": [],
                    },
                )
                bucket["clicks"] += int(row.get("clicks") or 0)
                bucket["valid_clicks"] += int(row.get("valid_clicks") or 0)
                bucket["offers"].append(name)

    dead_merchants: List[Dict[str, Any]] = []
    skip_keys: List[List[Any]] = []
    skip_set: Set[Tuple[int, str, str]] = set()
    for key, bucket in merchant_agg.items():
        if int(bucket["clicks"]) >= min_clicks and int(bucket["valid_clicks"]) <= 0:
            dead_merchants.append(bucket)
            skip_set.add(key)
            skip_keys.append([key[0], key[1], key[2]])

    blend_offers: List[Dict[str, Any]] = []
    dead_blend_names: List[str] = []
    if want_b:
        for feed_key in _BLEND_FEED_KEYS:
            try:
                cid = blend_child_campaign_id(feed_key)
            except Exception:
                continue
            for row in _fetch_campaign_offer_rows(client, cid, run_day, offer_names):
                item = {**row, "feed_key": feed_key, "campaign_id": cid}
                blend_offers.append(item)
                if int(row.get("clicks") or 0) >= min_clicks and int(row.get("valid_clicks") or 0) <= 0:
                    dead_blend_names.append(str(row.get("offer_name") or "").strip())

    dead_blend_names = sorted({n for n in dead_blend_names if n})
    payload: Dict[str, Any] = {
        "saved_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "analysis_date": run_day.isoformat(),
        "min_clicks": min_clicks,
        "scope": scope,
        "nipuhim_dead_merchants": dead_merchants,
        "nipuhim_skip_keys": skip_keys,
        "blend_dead_offer_names": dead_blend_names,
        "nipuhim_offer_rows": len(nipuhim_offers),
        "blend_offer_rows": len(blend_offers),
        "slot_count": len(slots),
    }
    if persist:
        path = _last_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        existing: Dict[str, Any] = {}
        try:
            if path.exists():
                existing = json.loads(path.read_text(encoding="utf-8")) or {}
        except Exception:
            existing = {}
        merged = dict(existing)
        merged.update(
            {
                "saved_utc": payload["saved_utc"],
                "analysis_date": payload["analysis_date"],
                "min_clicks": min_clicks,
            }
        )
        scopes = set(str(x) for x in (merged.get("scopes") or []))
        scopes.add(scope)
        merged["scopes"] = sorted(scopes)
        if want_n:
            merged["nipuhim_dead_merchants"] = dead_merchants
            merged["nipuhim_skip_keys"] = skip_keys
            merged["nipuhim_offer_rows"] = len(nipuhim_offers)
            merged["slot_count"] = len(slots)
        if want_b:
            merged["blend_dead_offer_names"] = dead_blend_names
            merged["blend_offer_rows"] = len(blend_offers)
        path.write_text(json.dumps(merged, ensure_ascii=True, indent=2, default=str), encoding="utf-8")
        payload["path"] = str(path)

    payload["_skip_set"] = skip_set
    payload["_blend_dead"] = set(dead_blend_names)
    _cache[cache_key] = payload
    logger.info(
        "no-val-click %s scope=%s: %s dead Nipuhim merchants, %s dead Blend offers (min_clicks=%s)",
        run_day.isoformat(),
        scope,
        len(skip_set),
        len(dead_blend_names),
        min_clicks,
    )
    return payload


def nipuhim_skip_set(
    *,
    run_date: Optional[date] = None,
    min_clicks: Optional[int] = None,
) -> Set[Tuple[int, str, str]]:
    if not NO_VAL_CLICK_ENABLED:
        return set()
    try:
        payload = analyze_yesterday(
            run_date=run_date, min_clicks=min_clicks, persist=True, scope="nipuhim"
        )
    except Exception:
        logger.exception("no-val-click: Nipuhim analysis failed; not skipping merchants")
        return set()
    skip = payload.get("_skip_set")
    if isinstance(skip, set):
        return skip
    out: Set[Tuple[int, str, str]] = set()
    for item in payload.get("nipuhim_skip_keys") or []:
        try:
            out.add((int(item[0]), str(item[1]).lower(), str(item[2])))
        except (TypeError, ValueError, IndexError):
            continue
    return out


def apply_nipuhim_skips(
    chosen: Dict[str, List[str]],
    ranked: Dict[str, List[str]],
    feed_num: int,
    skip_keys: Set[Tuple[int, str, str]],
    *,
    target_n: int,
    protected_by_geo: Optional[Dict[str, Set[str]]] = None,
) -> Tuple[Dict[str, List[str]], List[str]]:
    """
    Drop dead merchants from ``chosen`` and backfill from ``ranked``.

    ``protected_by_geo`` keeps manual ``--merchant-override`` ids even if they were dead yesterday.
    """
    if not skip_keys or not chosen:
        return chosen, []
    protected_by_geo = protected_by_geo or {}
    target_n = max(1, int(target_n))
    out: Dict[str, List[str]] = {}
    notes: List[str] = []
    for geo, mids in chosen.items():
        g = (geo or "").strip().lower()[:2]
        protected = protected_by_geo.get(g) or set()
        kept: List[str] = []
        skipped: List[str] = []
        seen: Set[str] = set()
        for raw in mids or []:
            mid = _normalize_merchant_id_from_sheet(raw)
            if not mid or mid in seen:
                continue
            if (feed_num, g, mid) in skip_keys and mid not in protected:
                skipped.append(mid)
                continue
            seen.add(mid)
            kept.append(mid)
        if skipped and len(kept) < target_n:
            for cand in ranked.get(g) or ranked.get(geo) or []:
                mid = _normalize_merchant_id_from_sheet(cand)
                if not mid or mid in seen:
                    continue
                if (feed_num, g, mid) in skip_keys and mid not in protected:
                    continue
                seen.add(mid)
                kept.append(mid)
                if len(kept) >= target_n:
                    break
        if target_n and len(kept) > target_n:
            kept = kept[:target_n]
        out[g] = kept
        if skipped:
            notes.append(
                f"Feed{feed_num} {g}: skipped no-Val-click merchant(s) {skipped!r}; "
                f"now {kept!r}"
            )
    return out, notes


def color_blend_dead_rows(
    *,
    run_date: Optional[date] = None,
    dry_run: bool = False,
    min_clicks: Optional[int] = None,
) -> Dict[str, Any]:
    """Color Blend-sheet rows whose Keitaro offers had clicks and 0 Val_clicks yesterday."""
    if not NO_VAL_CLICK_ENABLED:
        return {"status": "disabled", "colored": 0, "cleared": 0}
    spreadsheet_id = (BLEND_SHEETS_SPREADSHEET_ID or "").strip()
    if not spreadsheet_id:
        return {"status": "error", "error": "BLEND_SHEETS_SPREADSHEET_ID is not configured"}
    try:
        payload = analyze_yesterday(
            run_date=run_date, min_clicks=min_clicks, persist=True, scope="blend"
        )
    except Exception as e:
        logger.exception("no-val-click: Blend analysis failed; not coloring")
        return {"status": "error", "error": str(e)}
    dead_names: Set[str] = payload.get("_blend_dead") or set(payload.get("blend_dead_offer_names") or [])
    from blend_sync_from_sheet import BLEND_SHEET_NAME, BlendRow, _normalize_geo as blend_geo, _parse_click_cap

    service = _get_sheets_service()
    quoted = BLEND_SHEET_NAME.replace("'", "''")
    result = (
        service.values()
        .get(spreadsheetId=spreadsheet_id, range=f"'{quoted}'!A:Z")
        .execute()
    )
    rows = result.get("values") or []
    if len(rows) < 2:
        return {"status": "ok", "colored": 0, "cleared": 0, "analysis_date": payload.get("analysis_date")}

    header = [str(c).strip() for c in rows[0]]
    idx = {name.strip().lower(): i for i, name in enumerate(header)}

    def get_cell(row: list, name: str) -> str:
        i = idx.get((name or "").strip().lower())
        if i is None or i >= len(row):
            return ""
        return str(row[i] or "").strip()

    meta = service.get(
        spreadsheetId=spreadsheet_id,
        fields="sheets(properties(sheetId,title))",
    ).execute()
    sheet_id = None
    for s in meta.get("sheets", []):
        if s.get("properties", {}).get("title") == BLEND_SHEET_NAME:
            sheet_id = s["properties"]["sheetId"]
            break
    if sheet_id is None:
        return {"status": "error", "error": f"Sheet {BLEND_SHEET_NAME!r} not found"}

    colored_names: List[str] = []
    requests_batch: List[Dict[str, Any]] = []
    for i, row in enumerate(rows[1:], start=1):
        brand = get_cell(row, "brandName")
        url = get_cell(row, "offerUrl")
        geo = blend_geo(get_cell(row, "geo"))
        cap = _parse_click_cap(get_cell(row, "clickCap")) or 1.0
        if not brand or not geo:
            continue
        feed_tag = (get_cell(row, "feed") or "kelkoo1").strip().lower()
        br = BlendRow(
            brand_name=brand,
            offer_url=url or "https://placeholder.invalid/",
            click_cap=cap,
            geo=geo,
            feed_tag=feed_tag,
        )
        is_dead = br.offer_name in dead_names
        if is_dead:
            colored_names.append(br.offer_name)
        color = COLOR_LIGHT_RED if is_dead else COLOR_WHITE
        requests_batch.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": i,
                        "endRowIndex": i + 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": max(len(header), len(row)),
                    },
                    "cell": {"userEnteredFormat": {"backgroundColor": color}},
                    "fields": "userEnteredFormat.backgroundColor",
                }
            }
        )

    if dry_run:
        return {
            "status": "dry_run",
            "colored": len(colored_names),
            "cleared": max(0, len(requests_batch) - len(colored_names)),
            "offer_names": colored_names,
            "analysis_date": payload.get("analysis_date"),
        }

    CHUNK = 400
    for start in range(0, len(requests_batch), CHUNK):
        service.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests_batch[start : start + CHUNK]},
        ).execute()
    return {
        "status": "ok",
        "colored": len(colored_names),
        "cleared": max(0, len(requests_batch) - len(colored_names)),
        "offer_names": colored_names,
        "analysis_date": payload.get("analysis_date"),
    }
