"""
Blend daily click-cap progress by geo × device (desktop / mobile).

Reads targets from the Blend sheet (device-weighted clickCap) and today's clicks from
Keitaro ``report/build`` on the Blend campaign — same shape as the admin UI report:
``interval: today`` + timezone, ``campaign_id IN_LIST``, grouping ``country`` + ``device_type``.

``BlendTrCapGuard`` / ``BlendZpCapGuard`` refresh this on each run (default every 20 minutes)
and pause traffic sources when clicks meet the device-weighted cap.
"""
from __future__ import annotations

import json
import logging
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from integrations.blend_device import blend_stream_weight_for_channel
from integrations.keitaro import KeitaroClient, KeitaroClientError

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
BLEND_CAMPAIGN_ALIAS = "9Xq9dSMh"

_refresh_lock = threading.Lock()
_refresh_running = False


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def report_timezone() -> str:
    from config import BLEND_CAP_REPORT_TIMEZONE

    tz = (BLEND_CAP_REPORT_TIMEZONE or "America/Danmarkshavn").strip()
    return tz or "America/Danmarkshavn"


def click_metric_name() -> str:
    from config import BLEND_CAP_CLICK_METRIC

    return (BLEND_CAP_CLICK_METRIC or "clicks").strip().lower()


def _today_utc() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _today_in_report_tz() -> str:
    try:
        from zoneinfo import ZoneInfo

        return datetime.now(ZoneInfo(report_timezone())).date().isoformat()
    except Exception:
        return _today_utc()


def cache_path() -> Path:
    from config import BLEND_CAP_PROGRESS_CACHE_PATH

    raw = (BLEND_CAP_PROGRESS_CACHE_PATH or "").strip()
    if raw:
        p = Path(raw)
        return p if p.is_absolute() else ROOT / p
    return ROOT / "runtime" / "blend_cap_progress.json"


def refresh_interval_hours() -> float:
    from config import BLEND_CAP_PROGRESS_INTERVAL_HOURS

    try:
        h = float(BLEND_CAP_PROGRESS_INTERVAL_HOURS)
    except (TypeError, ValueError):
        h = 3.0
    return max(0.5, min(24.0, h))


def _progress_level(clicks: float, target: float) -> str:
    """green / yellow / red / none (no target)."""
    if target <= 0:
        return "none"
    pct = clicks / target
    if pct >= 0.9:
        return "green"
    if pct >= 0.5:
        return "yellow"
    return "red"


def _normalize_geo_from_country(country: str) -> str:
    g = (country or "").strip().lower()
    if len(g) > 2:
        g = g[:2]
    if g == "gb":
        return "uk"
    return g


def _normalize_device_channel(device_type: str) -> Optional[str]:
    s = (device_type or "").strip().lower()
    if s == "desktop":
        return "desktop"
    if s in ("mobile phone", "mobile", "tablet", "smartphone"):
        return "mobile"
    return None


def _rows_from_report(report: Any) -> List[dict]:
    if not isinstance(report, dict):
        return []
    for k in ("rows", "data", "result", "body"):
        v = report.get(k)
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return [x for x in v if isinstance(x, dict)]
    return []


def _row_clicks(row: dict, *, metric: Optional[str] = None) -> int:
    lk = {str(k).lower(): v for k, v in row.items()}
    primary = (metric or click_metric_name()).strip().lower()
    fallbacks = [primary]
    for k in ("clicks", "campaign_unique_clicks", "stream_unique_clicks", "global_unique_clicks"):
        if k not in fallbacks:
            fallbacks.append(k)
    for k in fallbacks:
        if k in lk and lk[k] is not None:
            try:
                return max(0, int(float(lk[k])))
            except (TypeError, ValueError):
                continue
    return 0


def _row_country(row: dict) -> str:
    lk = {str(k).lower(): v for k, v in row.items()}
    for k in ("country", "country_code", "geo", "region"):
        if k in lk and lk[k] is not None:
            return _normalize_geo_from_country(str(lk[k]))
    return ""


def _row_device_type(row: dict) -> str:
    lk = {str(k).lower(): v for k, v in row.items()}
    for k in ("device_type", "device", "devicetype"):
        if k in lk and lk[k] is not None:
            return str(lk[k]).strip()
    return ""


def _resolve_blend_campaign_id(client: KeitaroClient) -> int:
    from assistance import find_campaign_by_alias_or_name, get_campaigns_data

    campaigns = get_campaigns_data()
    c = find_campaign_by_alias_or_name(campaigns, alias=BLEND_CAMPAIGN_ALIAS, name=BLEND_CAMPAIGN_ALIAS)
    if not c or c.get("id") is None:
        raise ValueError(f"Blend campaign not found (alias {BLEND_CAMPAIGN_ALIAS!r})")
    return int(c["id"])


def _targets_from_blend_sheet() -> Tuple[Dict[Tuple[str, str], float], int, Optional[str]]:
    """Return ((geo, channel) -> target clicks, row_count, error)."""
    try:
        from blend_sync_from_sheet import get_sheets_service, read_blend_rows
    except Exception as e:
        return {}, 0, f"Blend sheet: {e}"

    try:
        service = get_sheets_service()
        rows = read_blend_rows(service)
    except Exception as e:
        return {}, 0, f"Blend sheet read: {e}"

    targets: Dict[Tuple[str, str], float] = defaultdict(float)
    for row in rows:
        for channel in ("desktop", "mobile"):
            w = blend_stream_weight_for_channel(
                row.device_mode,
                channel,
                click_cap=row.click_cap,
                weight_desktop=row.weight_desktop,
                weight_mobile=row.weight_mobile,
            )
            if w is not None and w > 0:
                targets[(row.geo, channel)] += float(w)
    return dict(targets), len(rows), None


def _keitaro_cap_report_payload(campaign_id: int) -> Dict[str, Any]:
    """Match Keitaro admin report: today + timezone, Blend campaign, country × device_type."""
    metric = click_metric_name()
    metrics = [metric]
    if metric != "clicks":
        metrics.append("clicks")
    if metric != "campaign_unique_clicks":
        metrics.append("campaign_unique_clicks")
    return {
        "range": {"interval": "today", "timezone": report_timezone()},
        "grouping": ["country", "device_type"],
        "metrics": metrics,
        "filters": [
            {
                "name": "campaign_id",
                "operator": "IN_LIST",
                "expression": [int(campaign_id)],
            }
        ],
        "limit": 500,
    }


def _clicks_from_keitaro_today(
    campaign_id: int,
) -> Tuple[Dict[Tuple[str, str], int], Optional[str], Dict[str, Any]]:
    from config import KEITARO_API_KEY

    meta = {
        "campaign_id": campaign_id,
        "report_timezone": report_timezone(),
        "click_metric": click_metric_name(),
        "calendar_day": _today_in_report_tz(),
    }
    if not (KEITARO_API_KEY or "").strip():
        return {}, "KEITARO_API_KEY not set", meta

    client = KeitaroClient()
    payload = _keitaro_cap_report_payload(campaign_id)
    meta["report_payload"] = payload
    try:
        report = client.build_report(payload)
    except KeitaroClientError as e:
        return {}, str(e), meta
    except Exception as e:
        return {}, str(e), meta

    clicks: Dict[Tuple[str, str], int] = defaultdict(int)
    metric = click_metric_name()
    for row in _rows_from_report(report):
        geo = _row_country(row)
        ch = _normalize_device_channel(_row_device_type(row))
        if not geo or len(geo) != 2 or not ch:
            continue
        clicks[(geo, ch)] += _row_clicks(row, metric=metric)

    if not clicks:
        return {}, "Keitaro report returned no geo/device rows", meta
    return dict(clicks), None, meta


def build_cap_progress_payload(*, reason: str = "scheduled") -> Dict[str, Any]:
    day = _today_in_report_tz()
    targets, blend_rows, sheet_err = _targets_from_blend_sheet()
    campaign_err: Optional[str] = None
    clicks: Dict[Tuple[str, str], int] = {}
    report_meta: Dict[str, Any] = {}
    campaign_id: Optional[int] = None
    try:
        client = KeitaroClient()
        campaign_id = _resolve_blend_campaign_id(client)
        clicks, campaign_err, report_meta = _clicks_from_keitaro_today(campaign_id)
    except Exception as e:
        campaign_err = str(e)

    keys = sorted(set(targets.keys()) | set(clicks.keys()))
    segments: List[Dict[str, Any]] = []
    for geo, channel in keys:
        target = int(round(targets.get((geo, channel), 0.0)))
        got = int(clicks.get((geo, channel), 0))
        pct = round((got / target) * 100.0, 1) if target > 0 else None
        segments.append(
            {
                "geo": geo,
                "device": channel,
                "target_clicks": target,
                "clicks": got,
                "remaining": max(0, target - got),
                "fill_pct": pct,
                "level": _progress_level(float(got), float(target)),
            }
        )

    segments.sort(key=lambda s: (-(s.get("target_clicks") or 0), s.get("geo") or "", s.get("device") or ""))

    errors: List[str] = []
    if sheet_err:
        errors.append(sheet_err)
    if campaign_err:
        errors.append(campaign_err)

    now = _utc_now_iso()
    interval_h = refresh_interval_hours()
    return {
        "calendar_day": day,
        "calendar_day_utc": day,
        "report_timezone": report_meta.get("report_timezone") or report_timezone(),
        "click_metric": report_meta.get("click_metric") or click_metric_name(),
        "campaign_id": campaign_id or report_meta.get("campaign_id"),
        "updated_utc": now,
        "next_refresh_utc": (
            datetime.now(timezone.utc) + timedelta(hours=interval_h)
        ).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "refresh_interval_hours": interval_h,
        "reason": reason,
        "blend_rows": blend_rows,
        "campaign_alias": BLEND_CAMPAIGN_ALIAS,
        "keitaro_report": report_meta,
        "segments": segments,
        "summary": {
            "segments": len(segments),
            "green": sum(1 for s in segments if s.get("level") == "green"),
            "yellow": sum(1 for s in segments if s.get("level") == "yellow"),
            "red": sum(1 for s in segments if s.get("level") == "red"),
            "none": sum(1 for s in segments if s.get("level") == "none"),
        },
        "errors": errors,
        "status": "error" if errors and not segments else ("partial" if errors else "ok"),
    }


def write_cache(payload: Dict[str, Any]) -> Path:
    path = cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    wrapped = {"saved_utc": payload.get("updated_utc") or _utc_now_iso(), "data": payload}
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(wrapped, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)
    return path


def read_cache() -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    path = cache_path()
    if not path.is_file():
        return None, None
    try:
        wrapped = json.loads(path.read_text(encoding="utf-8"))
        data = wrapped.get("data")
        if not isinstance(data, dict):
            return None, None
        return data, str(wrapped.get("saved_utc") or "")
    except Exception as e:
        logger.warning("Blend cap progress cache read failed: %s", e)
        return None, None


def cache_is_stale(data: Optional[Dict[str, Any]], saved_utc: Optional[str]) -> bool:
    if not data:
        return True
    if data.get("calendar_day") != _today_in_report_tz() and data.get("calendar_day_utc") != _today_in_report_tz():
        return True
    if not saved_utc:
        return True
    try:
        saved = datetime.strptime(str(saved_utc).strip(), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return True
    age_h = (datetime.now(timezone.utc) - saved).total_seconds() / 3600.0
    return age_h >= refresh_interval_hours()


def refresh_blend_cap_progress(*, reason: str = "manual") -> Dict[str, Any]:
    global _refresh_running
    with _refresh_lock:
        if _refresh_running:
            cached, saved = read_cache()
            if cached:
                return {**cached, "refresh_skipped": True, "message": "refresh already running"}
        _refresh_running = True
    try:
        payload = build_cap_progress_payload(reason=reason)
        write_cache(payload)
        logger.info(
            "Blend cap progress refreshed (%s): %s segments, status=%s",
            reason,
            len(payload.get("segments") or []),
            payload.get("status"),
        )
        return payload
    finally:
        with _refresh_lock:
            _refresh_running = False


def get_api_payload(*, allow_background_refresh: bool = True) -> Dict[str, Any]:
    """Return cached progress; optionally kick off background refresh if stale."""
    data, saved = read_cache()
    stale = cache_is_stale(data, saved)
    out: Dict[str, Any] = {
        "cache_saved_utc": saved,
        "stale": stale,
    }
    if data:
        out.update(data)
    else:
        out.update(
            {
                "status": "missing",
                "calendar_day_utc": _today_utc(),
                "segments": [],
                "errors": ["No cached data yet"],
            }
        )

    if stale and allow_background_refresh:
        def _bg() -> None:
            try:
                refresh_blend_cap_progress(reason="stale_api")
            except Exception as e:
                logger.exception("Background blend cap refresh failed: %s", e)

        threading.Thread(target=_bg, daemon=True, name="blend-cap-progress").start()
        out["refresh_queued"] = True
    else:
        out["refresh_queued"] = False

    return out
