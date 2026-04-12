"""
Daily conversion postbacks to Keitaro: Kelkoo (per geo), Adexa (single report), Yadore (single report).

State file avoids double-firing after partial runs (see ``DAILY_CONVERSION_POSTBACK_STATE_PATH``).
"""
from __future__ import annotations

import copy
import csv
import logging
import re
from datetime import date, datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlencode, urlsplit, urlunsplit

import requests

from config import (
    ADEXA_API_KEY,
    ADEXA_SITE_ID,
    DAILY_CONVERSION_POSTBACK_BASE,
    DAILY_CONVERSION_POSTBACK_CLICK_STATUS,
    DAILY_CONVERSION_POSTBACK_SALE_STATUS,
    DAILY_CONVERSION_POSTBACK_STATE_PATH,
    FEED1_API_KEY,
    FEED2_API_KEY,
    KELKOO_RAW_REPORT_GEOS,
)
from integrations.adexa import AdexaClientError, fetch_stats_raw
from integrations.daily_conversion_postback_state import (
    default_flat_run_state,
    default_geo_state,
    load_state,
    reset_source_date,
    save_state_atomic,
)
from integrations.yadore import YadoreClientError, fetch_conversion_detail

logger = logging.getLogger(__name__)

KELKOO_RAW_REPORT_URL = "https://api.kelkoogroup.net/publisher/reports/v1/raw"


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def default_report_date_str() -> str:
    return (_utc_today() - timedelta(days=1)).isoformat()


def _bucket(data: Dict[str, Any], source_key: str, report_date: str) -> Dict[str, Any]:
    sources = data.setdefault("sources", {})
    src = sources.setdefault(source_key, {})
    return src.setdefault(report_date, {})


def build_daily_postback_url(*, subid: str, payout: str, status: str) -> str:
    base = (DAILY_CONVERSION_POSTBACK_BASE or "").strip().rstrip("/")
    if not base:
        raise ValueError("DAILY_CONVERSION_POSTBACK_BASE / LATE_SALES_POSTBACK_BASE is empty")
    parts = urlsplit(base)
    if parts.query or ("?" in (parts.path or "")):
        raise ValueError("postback base must be URL without query string")
    q = urlencode({"subid": subid, "payout": str(payout), "status": status})
    return urlunsplit((parts.scheme, parts.netloc, parts.path, q, parts.fragment))


def send_postback_get(
    session: requests.Session,
    url: str,
    *,
    dry_run: bool,
    timeout: float = 45.0,
) -> int:
    if dry_run:
        logger.debug("DRY-RUN GET %s", url[:220])
        return 200
    r = session.get(url, timeout=timeout)
    return int(r.status_code)


def kelkoo_api_key_for(feed: str) -> str:
    if feed == "kelkoo1":
        return (FEED1_API_KEY or "").strip()
    if feed == "kelkoo2":
        return (FEED2_API_KEY or "").strip()
    raise ValueError(f"Unknown Kelkoo feed: {feed}")


def fetch_kelkoo_raw_tsv(
    country: str,
    report_date: str,
    api_key: str,
    session: requests.Session,
    timeout: float = 120.0,
) -> tuple[int, str]:
    """Returns (http_status, body_text)."""
    url = (
        f"{KELKOO_RAW_REPORT_URL}?country={country.lower()}"
        f"&start={report_date}&end={report_date}"
    )
    headers = {"Authorization": f"Bearer {api_key}", "Accept": "text/plain, */*"}
    r = session.get(url, headers=headers, timeout=timeout)
    return int(r.status_code), r.text or ""


def _parse_kelkoo_row(row: Dict[str, str]) -> Optional[Tuple[str, bool, bool, str, str]]:
    click_id = str(row.get("publisherClickId") or "").strip()
    lead_valid = (row.get("leadValid") or "").lower() == "true"
    sale = (row.get("sale") or "").lower() == "true"
    sale_value_usd = (row.get("saleValueInUsd") or "0").strip() or "0"
    cpc = (row.get("leadEstimatedRevenueInUsd") or "0").strip() or "0"
    if not click_id or not lead_valid:
        return None
    return (click_id, lead_valid, sale, sale_value_usd, cpc)


def run_kelkoo_feed_postbacks(
    feed: str,
    report_date: str,
    *,
    state_path: Path,
    geos: Sequence[str],
    only_geo: Optional[str],
    dry_run: bool,
    no_resume: bool,
    session: requests.Session,
) -> Dict[str, Any]:
    api_key = kelkoo_api_key_for(feed)
    if not api_key:
        return {"ok": False, "error": f"Missing API key for {feed}"}

    source_key = feed
    summary: Dict[str, Any] = {"feed": feed, "report_date": report_date, "geos": {}}

    snap: Optional[Dict[str, Any]] = copy.deepcopy(load_state(state_path)) if dry_run else None

    def _state_root() -> Dict[str, Any]:
        return snap if snap is not None else load_state(state_path)

    def commit(mutator: Callable[[Dict[str, Any]], None]) -> None:
        if snap is not None:
            mutator(snap)
            return
        d = load_state(state_path)
        mutator(d)
        save_state_atomic(state_path, d)

    def _ensure_geos(d: Dict[str, Any]) -> None:
        _bucket(d, source_key, report_date).setdefault("geos", {})

    commit(_ensure_geos)
    if dry_run:
        logger.info("Kelkoo %s dry-run: postback URLs at DEBUG; resume state is not written to disk.", feed)

    geo_list = [g.strip().lower() for g in geos if g.strip()]
    if only_geo:
        og = only_geo.strip().lower()
        geo_list = [g for g in geo_list if g == og]
    if not geo_list:
        return {"ok": False, "error": "No geos to process"}

    for geo in geo_list:
        gsum: Dict[str, Any] = {"geo": geo, "rows": 0, "postbacks_sent": 0, "skipped_done": False}
        summary["geos"][geo] = gsum

        def read_gs() -> Dict[str, Any]:
            data = _state_root()
            return _bucket(data, source_key, report_date).setdefault("geos", {}).setdefault(geo, default_geo_state())

        gs0 = read_gs()
        if not no_resume and gs0.get("status") == "done":
            gsum["skipped_done"] = True
            logger.info("%s %s %s: already done, skipping", feed, report_date, geo)
            continue

        status, body = fetch_kelkoo_raw_tsv(geo, report_date, api_key, session)
        if status != 200:
            logger.error("Kelkoo raw %s %s HTTP %s", geo, report_date, status)

            def _fail(d: Dict[str, Any]) -> None:
                g = _bucket(d, source_key, report_date).setdefault("geos", {}).setdefault(geo, default_geo_state())
                g["fetch_http_status"] = status
                g["fetch_ok"] = False
                g["fetch_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                g["status"] = "error"
                g["last_error"] = (body or "")[:500]

            commit(_fail)
            gsum["error"] = f"HTTP {status}"
            continue

        def _fetch_ok(d: Dict[str, Any]) -> None:
            b = _bucket(d, source_key, report_date)
            geos_map = b.setdefault("geos", {})
            if no_resume:
                geos_map[geo] = default_geo_state()
            g = geos_map.setdefault(geo, default_geo_state())
            g["fetch_http_status"] = status
            g["fetch_ok"] = True
            g["fetch_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            g.setdefault("status", "partial")

        commit(_fetch_ok)

        reader = csv.DictReader(StringIO(body), delimiter="\t")
        rows: List[Dict[str, str]] = []
        for row in reader:
            if isinstance(row, dict):
                rows.append({str(k): str(v) if v is not None else "" for k, v in row.items()})

        total = len(rows)
        gsum["rows"] = total

        for idx, row in enumerate(rows):
            gs_live = read_gs()
            next_idx = 0 if no_resume else int(gs_live.get("next_row_index") or 0)
            row_stage = None if no_resume else gs_live.get("row_stage")

            if idx < next_idx:
                continue

            parsed = _parse_kelkoo_row(row)
            if parsed is None:

                def _skip_invalid(d: Dict[str, Any]) -> None:
                    g = _bucket(d, source_key, report_date).setdefault("geos", {}).setdefault(geo, default_geo_state())
                    g["next_row_index"] = idx + 1
                    g["row_stage"] = None
                    g["rows_in_file"] = total
                    g["status"] = "partial" if idx + 1 < total else "done"
                    if g["status"] == "done":
                        g["completed_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

                commit(_skip_invalid)
                continue

            click_id, _lv, is_sale, sale_value_usd, cpc = parsed

            if row_stage == "after_click" and idx == next_idx:
                url_sale = build_daily_postback_url(
                    subid=click_id,
                    payout=str(sale_value_usd),
                    status=DAILY_CONVERSION_POSTBACK_SALE_STATUS,
                )
                send_postback_get(session, url_sale, dry_run=dry_run)

                def _resume_sale(d: Dict[str, Any]) -> None:
                    g = _bucket(d, source_key, report_date).setdefault("geos", {}).setdefault(geo, default_geo_state())
                    g["next_row_index"] = idx + 1
                    g["row_stage"] = None
                    g["postbacks_sent"] = int(g.get("postbacks_sent") or 0) + 1
                    g["rows_in_file"] = total
                    g["status"] = "partial" if idx + 1 < total else "done"
                    if g["status"] == "done":
                        g["completed_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

                commit(_resume_sale)
                continue

            if is_sale:
                url_click = build_daily_postback_url(
                    subid=click_id,
                    payout=str(cpc),
                    status=DAILY_CONVERSION_POSTBACK_CLICK_STATUS,
                )
                sc = send_postback_get(session, url_click, dry_run=dry_run)
                if sc >= 400:
                    logger.warning("Kelkoo click postback HTTP %s for %s", sc, click_id[:20])

                def _after_click_sale(d: Dict[str, Any]) -> None:
                    g = _bucket(d, source_key, report_date).setdefault("geos", {}).setdefault(geo, default_geo_state())
                    g["next_row_index"] = idx
                    g["row_stage"] = "after_click"
                    g["postbacks_sent"] = int(g.get("postbacks_sent") or 0) + 1
                    g["rows_in_file"] = total

                commit(_after_click_sale)

                url_sale = build_daily_postback_url(
                    subid=click_id,
                    payout=str(sale_value_usd),
                    status=DAILY_CONVERSION_POSTBACK_SALE_STATUS,
                )
                send_postback_get(session, url_sale, dry_run=dry_run)

                def _after_full_sale(d: Dict[str, Any]) -> None:
                    g = _bucket(d, source_key, report_date).setdefault("geos", {}).setdefault(geo, default_geo_state())
                    g["next_row_index"] = idx + 1
                    g["row_stage"] = None
                    g["postbacks_sent"] = int(g.get("postbacks_sent") or 0) + 1
                    g["rows_in_file"] = total
                    g["status"] = "partial" if idx + 1 < total else "done"
                    if g["status"] == "done":
                        g["completed_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

                commit(_after_full_sale)
            else:
                url_click = build_daily_postback_url(
                    subid=click_id,
                    payout=str(cpc),
                    status=DAILY_CONVERSION_POSTBACK_CLICK_STATUS,
                )
                send_postback_get(session, url_click, dry_run=dry_run)

                def _after_lead(d: Dict[str, Any]) -> None:
                    g = _bucket(d, source_key, report_date).setdefault("geos", {}).setdefault(geo, default_geo_state())
                    g["next_row_index"] = idx + 1
                    g["row_stage"] = None
                    g["postbacks_sent"] = int(g.get("postbacks_sent") or 0) + 1
                    g["rows_in_file"] = total
                    g["status"] = "partial" if idx + 1 < total else "done"
                    if g["status"] == "done":
                        g["completed_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

                commit(_after_lead)

        gsum["postbacks_sent"] = int(read_gs().get("postbacks_sent") or 0)

        def _finalize_geo(d: Dict[str, Any]) -> None:
            g = _bucket(d, source_key, report_date).setdefault("geos", {}).setdefault(geo, default_geo_state())
            g["rows_in_file"] = total
            if int(g.get("next_row_index") or 0) >= total and g.get("row_stage") is None:
                g["status"] = "done"
                g["completed_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        commit(_finalize_geo)

    summary["ok"] = True
    return summary


def _adexa_stat_actions(stat: Dict[str, Any]) -> Optional[Tuple[str, str, bool, str]]:
    cid = str(stat.get("publisherClickId") or stat.get("publisherClickID") or "").strip()
    if not cid:
        return None
    cpc = str(stat.get("commission") or "0").strip() or "0"
    try:
        sale_count = int(stat.get("saleCount") or 0)
    except (TypeError, ValueError):
        sale_count = 0
    sale_val = str(stat.get("saleValue") or stat.get("saleValueUsd") or "0").strip() or "0"
    is_sale = sale_count > 0
    return (cid, cpc, is_sale, sale_val)


def _yadore_click_actions(click: Dict[str, Any]) -> Optional[Tuple[str, str, bool, str]]:
    cid = None
    for k in ("publisherClickId", "publisherClickID", "clickId", "trackingId", "tracking_id", "subId", "id"):
        v = click.get(k)
        if v is not None and str(v).strip():
            cid = str(v).strip()
            break
    if not cid:
        return None
    cpc = "0"
    for k in ("commission", "cpc", "estimatedCpc", "revenue", "payout", "clickCommission"):
        v = click.get(k)
        if v is not None and str(v).strip():
            if isinstance(v, dict):
                amt = v.get("amount")
                if amt is not None:
                    cpc = str(amt).strip()
                    break
            else:
                cpc = str(v).strip()
                break
    sale_val = None
    for k in ("saleValue", "saleValueUsd", "conversionValue", "orderValue", "conversionRevenue"):
        v = click.get(k)
        if v is not None and str(v).strip():
            sale_val = str(v).strip()
            break
    if not sale_val:
        sale_val = "0"
    try:
        sc = int(click.get("saleCount") or click.get("conversions") or 0)
    except (TypeError, ValueError):
        sc = 0
    try:
        is_sale = sc > 0 or float(sale_val) > 0
    except ValueError:
        is_sale = sc > 0
    return (cid, cpc, is_sale, sale_val)


def run_flat_report_postbacks(
    source_key: str,
    report_date: str,
    items: List[Dict[str, Any]],
    row_parser: Callable[[Dict[str, Any]], Optional[Tuple[str, str, bool, str]]],
    *,
    state_path: Path,
    dry_run: bool,
    no_resume: bool,
    session: requests.Session,
) -> Dict[str, Any]:
    summary: Dict[str, Any] = {"source": source_key, "report_date": report_date, "items": len(items), "sent": 0}

    snap: Optional[Dict[str, Any]] = copy.deepcopy(load_state(state_path)) if dry_run else None

    def _state_root() -> Dict[str, Any]:
        return snap if snap is not None else load_state(state_path)

    def commit(mutator: Callable[[Dict[str, Any]], None]) -> None:
        if snap is not None:
            mutator(snap)
            return
        d = load_state(state_path)
        mutator(d)
        save_state_atomic(state_path, d)

    def read_flat() -> Dict[str, Any]:
        data = _state_root()
        b = _bucket(data, source_key, report_date)
        return b.setdefault("flat", default_flat_run_state())

    if dry_run:
        logger.info("%s dry-run: postback URLs at DEBUG; resume state is not written to disk.", source_key)

    f0 = read_flat()
    if not no_resume and f0.get("status") == "done":
        logger.info("%s %s: flat run already done, skipping", source_key, report_date)
        summary["skipped_done"] = True
        summary["ok"] = True
        return summary

    if no_resume:

        def _reset_flat(d: Dict[str, Any]) -> None:
            b = _bucket(d, source_key, report_date)
            b["flat"] = default_flat_run_state()

        commit(_reset_flat)

    def write_flat(**kwargs: Any) -> None:
        def _w(d: Dict[str, Any]) -> None:
            b = _bucket(d, source_key, report_date)
            fl = b.setdefault("flat", default_flat_run_state())
            for k, v in kwargs.items():
                if v is not None:
                    fl[k] = v
            fl["last_updated_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        commit(_w)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    write_flat(total_items=len(items), fetch_at_utc=now, status="partial")

    total = len(items)
    if total == 0:
        write_flat(
            next_index=0,
            row_stage=None,
            status="done",
            completed_at_utc=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        summary["ok"] = True
        return summary

    for idx in range(total):
        fl = read_flat()
        next_idx = 0 if no_resume else int(fl.get("next_index") or 0)
        row_stage = None if no_resume else fl.get("row_stage")

        if idx < next_idx:
            continue

        stat = items[idx]
        parsed = row_parser(stat)
        if parsed is None:
            write_flat(
                next_index=idx + 1,
                row_stage=None,
                status="partial" if idx + 1 < total else "done",
                completed_at_utc=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if idx + 1 >= total else None,
            )
            continue

        click_id, cpc, is_sale, sale_val = parsed

        if row_stage == "after_click" and idx == next_idx:
            url_sale = build_daily_postback_url(
                subid=click_id,
                payout=str(sale_val),
                status=DAILY_CONVERSION_POSTBACK_SALE_STATUS,
            )
            send_postback_get(session, url_sale, dry_run=dry_run)
            pb = int(read_flat().get("postbacks_sent") or 0) + 1
            write_flat(
                next_index=idx + 1,
                row_stage=None,
                postbacks_sent=pb,
                status="partial" if idx + 1 < total else "done",
                completed_at_utc=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if idx + 1 >= total else None,
            )
            continue

        if is_sale:
            url_click = build_daily_postback_url(
                subid=click_id,
                payout=str(cpc),
                status=DAILY_CONVERSION_POSTBACK_CLICK_STATUS,
            )
            send_postback_get(session, url_click, dry_run=dry_run)
            pb1 = int(read_flat().get("postbacks_sent") or 0) + 1
            write_flat(next_index=idx, row_stage="after_click", postbacks_sent=pb1, status="partial")

            url_sale = build_daily_postback_url(
                subid=click_id,
                payout=str(sale_val),
                status=DAILY_CONVERSION_POSTBACK_SALE_STATUS,
            )
            send_postback_get(session, url_sale, dry_run=dry_run)
            pb2 = int(read_flat().get("postbacks_sent") or 0) + 1
            write_flat(
                next_index=idx + 1,
                row_stage=None,
                postbacks_sent=pb2,
                status="partial" if idx + 1 < total else "done",
                completed_at_utc=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if idx + 1 >= total else None,
            )
        else:
            url_click = build_daily_postback_url(
                subid=click_id,
                payout=str(cpc),
                status=DAILY_CONVERSION_POSTBACK_CLICK_STATUS,
            )
            send_postback_get(session, url_click, dry_run=dry_run)
            pb = int(read_flat().get("postbacks_sent") or 0) + 1
            write_flat(
                next_index=idx + 1,
                row_stage=None,
                postbacks_sent=pb,
                status="partial" if idx + 1 < total else "done",
                completed_at_utc=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if idx + 1 >= total else None,
            )

    summary["sent"] = int(read_flat().get("postbacks_sent") or 0)
    summary["ok"] = True
    return summary


def run_adexa_postbacks(
    report_date: str,
    *,
    state_path: Path,
    dry_run: bool,
    no_resume: bool,
    session: requests.Session,
) -> Dict[str, Any]:
    if not (ADEXA_SITE_ID or "").strip() or not (ADEXA_API_KEY or "").strip():
        return {"ok": False, "error": "ADEXA_SITE_ID or ADEXA_API_KEY missing"}
    try:
        stats = fetch_stats_raw(report_date, report_date)
    except AdexaClientError as e:
        return {"ok": False, "error": str(e)}
    return run_flat_report_postbacks(
        "adexa",
        report_date,
        stats,
        _adexa_stat_actions,
        state_path=state_path,
        dry_run=dry_run,
        no_resume=no_resume,
        session=session,
    )


def run_yadore_postbacks(
    report_date: str,
    *,
    state_path: Path,
    dry_run: bool,
    no_resume: bool,
    session: requests.Session,
) -> Dict[str, Any]:
    try:
        clicks = fetch_conversion_detail(report_date)
    except YadoreClientError as e:
        return {"ok": False, "error": str(e)}
    return run_flat_report_postbacks(
        "yadore",
        report_date,
        clicks,
        _yadore_click_actions,
        state_path=state_path,
        dry_run=dry_run,
        no_resume=no_resume,
        session=session,
    )


def run_daily_conversion_postbacks_batch(
    *,
    report_date: str,
    only: str,
    only_geo: Optional[str],
    dry_run: bool,
    no_resume: bool,
    reset_sources: Optional[Sequence[str]],
) -> Dict[str, Any]:
    """
    Run Kelkoo / Adexa / Yadore postbacks; returns structured output for CLI and Flask UI.
    """
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", report_date):
        return {
            "ok": False,
            "exit_code": 2,
            "error": f"Invalid report_date {report_date!r} (use YYYY-MM-DD)",
            "results": [],
            "report_date": report_date,
        }

    state_path = Path(DAILY_CONVERSION_POSTBACK_STATE_PATH)
    if reset_sources:
        for s in reset_sources:
            reset_source_date(state_path, s.strip().lower(), report_date)
            logger.info("Reset state for %s @ %s", s, report_date)

    session = requests.Session()
    only_l = only.strip().lower()
    targets: List[str]
    if only_l in ("", "all"):
        targets = ["kelkoo1", "kelkoo2", "adexa", "yadore"]
    else:
        targets = [only_l]

    rc = 0
    results: List[Dict[str, Any]] = []
    geos = list(KELKOO_RAW_REPORT_GEOS) if KELKOO_RAW_REPORT_GEOS else list(
        (
            "ae",
            "at",
            "au",
            "be",
            "br",
            "ca",
            "ch",
            "cz",
            "de",
            "es",
            "fi",
            "fr",
            "gr",
            "hk",
            "hu",
            "id",
            "ie",
            "in",
            "it",
            "jp",
            "kr",
            "mx",
            "my",
            "nb",
            "nl",
            "no",
            "nz",
            "ph",
            "pl",
            "pt",
            "ro",
            "se",
            "sg",
            "sk",
            "tr",
            "uk",
            "us",
            "vn",
            "dk",
        )
    )

    for t in targets:
        if t in ("kelkoo1", "kelkoo2"):
            out = run_kelkoo_feed_postbacks(
                t,
                report_date,
                state_path=state_path,
                geos=geos,
                only_geo=only_geo,
                dry_run=dry_run,
                no_resume=no_resume,
                session=session,
            )
            results.append({"target": t, "summary": out})
            if not out.get("ok"):
                rc = 1
        elif t == "adexa":
            out = run_adexa_postbacks(
                report_date,
                state_path=state_path,
                dry_run=dry_run,
                no_resume=no_resume,
                session=session,
            )
            results.append({"target": t, "summary": out})
            if not out.get("ok"):
                rc = 1
        elif t == "yadore":
            out = run_yadore_postbacks(
                report_date,
                state_path=state_path,
                dry_run=dry_run,
                no_resume=no_resume,
                session=session,
            )
            results.append({"target": t, "summary": out})
            if not out.get("ok"):
                rc = 1
        else:
            logger.error("Unknown --only %r", t)
            rc = 2
            results.append({"target": t, "summary": {"ok": False, "error": f"unknown target {t!r}"}})

    out: Dict[str, Any] = {
        "ok": rc == 0,
        "exit_code": rc,
        "results": results,
        "report_date": report_date,
        "state_path": str(state_path),
    }
    try:
        from integrations.daily_postbacks_run_history import record_last_run

        for row in results:
            tid = str(row.get("target") or "").strip().lower()
            if not tid:
                continue
            summ = row.get("summary") or {}
            record_last_run(
                tid,
                report_date,
                dry_run=dry_run,
                ok=bool(summ.get("ok")),
                summary=summ,
                batch_exit_code=rc,
            )
    except Exception:
        logger.exception("daily postbacks: could not write last-run history")

    return out


def run_daily_conversion_postbacks_main(
    *,
    report_date: str,
    only: str,
    only_geo: Optional[str],
    dry_run: bool,
    no_resume: bool,
    reset_sources: Optional[Sequence[str]],
) -> int:
    batch = run_daily_conversion_postbacks_batch(
        report_date=report_date,
        only=only,
        only_geo=only_geo,
        dry_run=dry_run,
        no_resume=no_resume,
        reset_sources=reset_sources,
    )
    if batch.get("error"):
        logger.error("%s", batch["error"])
        print(batch["error"])
    for row in batch.get("results") or []:
        print(row.get("summary"))
    return int(batch.get("exit_code") or 1)
