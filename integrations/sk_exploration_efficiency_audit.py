"""
SK exploration buying-efficiency audit: lifetime, per-geo, yesterday, and weekly WoW.

Writes two tabs on the SK tools workbook:
- ``SKbuyingEfficiencyCampaigns`` — per-campaign metrics
- ``SKbuyingEfficiencySummary`` — portfolio, geo, yesterday, and weekly sections

Triggered from the SK console UI (background thread) or ``scripts/sk_exploration_efficiency_audit.py``.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import requests

from config import (
    SK_GARBAGE_SUBIDS_CSV,
    SK_GLOBAL_BLACKLIST_CONTROL_LIST_ID,
    SK_OPTIMIZER_SHEET_ID,
    SK_TOOLS_SPREADSHEET_ID,
    SOURCEKNOWLEDGE_API_KEY,
)
from integrations.autoserver import gdocs_as as gd
from integrations.overview_costs import SK_API_BASE, _sk_headers
from integrations.sk_exploration_costs import fetch_exploration_campaign_ids

logger = logging.getLogger(__name__)

TAB_CAMPAIGNS = "SKbuyingEfficiencyCampaigns"
TAB_SUMMARY = "SKbuyingEfficiencySummary"

HEADERS_CAMPAIGNS = [
    "campaign_id",
    "campaign_name",
    "brand",
    "geo",
    "status",
    "mon_network",
    "start_date",
    "lifetime_total_spend",
    "lifetime_garbage_spend",
    "lifetime_effective_spend",
    "lifetime_garbage_pct",
    "yesterday_total_spend",
    "yesterday_garbage_spend",
    "yesterday_effective_spend",
    "yesterday_garbage_pct",
    "publisher_count",
    "error",
    "report_as_of_utc",
]

HEADERS_SUMMARY = [
    "section",
    "label",
    "week_start",
    "week_end",
    "total_spend",
    "garbage_spend",
    "effective_spend",
    "garbage_pct",
    "effective_pct",
    "campaigns_with_spend",
    "wow_garbage_spend_delta",
    "wow_garbage_pct_delta",
    "wow_trend",
    "notes",
]

_ROOT = Path(__file__).resolve().parents[1]
STATE_PATH = _ROOT / "runtime" / "sk_exploration_efficiency_audit.json"
LOCK_PATH = STATE_PATH.with_suffix(".lock")
_AUDIT_LOG_PATH = STATE_PATH.parent / "sk_exploration_efficiency_audit.log"
_AUDIT_SCRIPT = _ROOT / "scripts" / "sk_exploration_efficiency_audit.py"

_HTTP_TIMEOUT = max(30, int((os.getenv("SK_EFFICIENCY_AUDIT_TIMEOUT") or "45").strip() or "45"))
_MAX_WORKERS = max(1, int((os.getenv("SK_AUDIT_WORKERS") or "6").strip() or "6"))
_WEEKLY_LOOKBACK = max(
    4, int((os.getenv("SK_EFFICIENCY_WEEKLY_LOOKBACK_WEEKS") or "20").strip() or "20")
)

_REFRESH_LOCK = threading.Lock()
_REFRESH_RUNNING = False
_AUDIT_PROC: Optional[subprocess.Popen] = None
_STALE_RUNNING_HOURS = max(
    1, int((os.getenv("SK_EFFICIENCY_AUDIT_STALE_HOURS") or "3").strip() or "3")
)


def _release_audit_lock() -> None:
    try:
        LOCK_PATH.unlink(missing_ok=True)
    except OSError:
        pass


def _try_acquire_audit_lock() -> bool:
    """Cross-process mutex so Gunicorn workers do not spawn duplicate audits."""
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        try:
            os.write(fd, f"{os.getpid()}\n".encode())
        finally:
            os.close(fd)
        return True
    except FileExistsError:
        return False


def _parse_utc_iso(raw: str) -> Optional[datetime]:
    s = (raw or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None


def _is_stale_running(state: dict) -> bool:
    if (state.get("status") or "").strip().lower() != "running":
        return False
    started = _parse_utc_iso(str(state.get("started_at") or ""))
    if started is None:
        return True
    return datetime.now(timezone.utc) - started > timedelta(hours=_STALE_RUNNING_HOURS)


def _subprocess_audit_running() -> bool:
    global _AUDIT_PROC
    proc = _AUDIT_PROC
    if proc is not None and proc.poll() is None:
        return True
    if proc is not None and proc.poll() is not None:
        _AUDIT_PROC = None
    return False


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _utc_today() -> date:
    return datetime.now(timezone.utc).date()


def _utc_yesterday() -> str:
    return (_utc_today() - timedelta(days=1)).strftime("%Y-%m-%d")


def _pct(garbage: float, total: float) -> float:
    return round((garbage / total * 100), 2) if total > 0 else 0.0


def load_garbage_sub_ids() -> Tuple[Set[str], str]:
    """Prefer global control list subIds; optional CSV fallback."""
    from integrations.autoserver.sk_garbage_sources import _get_control_list

    lid = int(SK_GLOBAL_BLACKLIST_CONTROL_LIST_ID)
    data, err = _get_control_list(lid)
    if data and not err:
        subs = {str(s).strip() for s in (data.get("subIds") or []) if str(s).strip()}
        if subs:
            return subs, f"control_list:{lid}"

    csv_path = (SK_GARBAGE_SUBIDS_CSV or "").strip()
    if csv_path:
        import csv

        path = Path(csv_path)
        if path.is_file():
            out: Set[str] = set()
            with open(path, newline="", encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                next(reader, None)
                for row in reader:
                    if row and row[0].strip():
                        out.add(row[0].strip().strip('"'))
            if out:
                return out, f"csv:{path.name}"

    if err:
        raise RuntimeError(f"Could not load garbage sub IDs: {err}")
    raise RuntimeError("No garbage sub IDs from control list or CSV")


def fetch_all_campaigns(api_key: str) -> Dict[int, dict]:
    headers = _sk_headers(api_key)
    out: Dict[int, dict] = {}
    page = 1
    while page <= 100:
        r = requests.get(
            f"{SK_API_BASE}/campaigns",
            headers=headers,
            params={"page": page},
            timeout=_HTTP_TIMEOUT,
        )
        if r.status_code == 429:
            time.sleep(60)
            continue
        r.raise_for_status()
        data = r.json()
        for item in data.get("items") or []:
            try:
                out[int(item["id"])] = item
            except (KeyError, TypeError, ValueError):
                pass
        if not data.get("hasMore"):
            break
        page += 1
        time.sleep(0.05)
    return out


def campaign_spend_breakdown(
    api_key: str,
    campaign_id: int,
    start: str,
    end: str,
    garbage: Set[str],
) -> Tuple[float, float, int, Optional[str]]:
    headers = _sk_headers(api_key)
    total = 0.0
    garbage_spend = 0.0
    sub_count = 0
    page = 1
    while page <= 200:
        try:
            r = requests.get(
                f"{SK_API_BASE}/stats/campaigns/{campaign_id}/by-publisher",
                headers=headers,
                params={"from": start, "to": end, "page": page},
                timeout=_HTTP_TIMEOUT,
            )
        except requests.RequestException as e:
            return total, garbage_spend, sub_count, str(e)
        if r.status_code == 429:
            time.sleep(60)
            continue
        if r.status_code != 200:
            return total, garbage_spend, sub_count, f"HTTP {r.status_code}"
        try:
            data = r.json()
        except Exception as e:
            return total, garbage_spend, sub_count, str(e)
        if isinstance(data, dict) and data.get("error"):
            return total, garbage_spend, sub_count, str(data["error"])
        for it in data.get("items") or []:
            if not isinstance(it, dict):
                continue
            sub_count += 1
            try:
                spend = float(it.get("spend") or 0)
            except (TypeError, ValueError):
                spend = 0.0
            total += spend
            if str(it.get("subId") or "") in garbage:
                garbage_spend += spend
        if not data.get("hasMore"):
            break
        page += 1
        time.sleep(0.03)
    return round(total, 4), round(garbage_spend, 4), sub_count, None


def iso_weeks_between(start: date, end: date) -> List[Tuple[str, str, str]]:
    cur = start - timedelta(days=start.weekday())
    out: List[Tuple[str, str, str]] = []
    while cur <= end:
        wk_end = cur + timedelta(days=6)
        if wk_end >= start:
            label = f"{cur.isocalendar().year}-W{cur.isocalendar().week:02d}"
            out.append((label, cur.isoformat(), min(wk_end, end).isoformat()))
        cur += timedelta(days=7)
    return out


def _aggregate_geo(rows: List[dict], prefix: str) -> List[dict]:
    """``prefix`` is ``lifetime`` or ``yesterday``."""
    t_key = f"{prefix}_total_spend"
    g_key = f"{prefix}_garbage_spend"
    e_key = f"{prefix}_effective_spend"
    by_geo: Dict[str, Dict[str, float]] = {}
    for r in rows:
        geo = str(r.get("geo") or "?").strip().upper() or "?"
        bucket = by_geo.setdefault(
            geo, {"total": 0.0, "garbage": 0.0, "effective": 0.0, "n": 0}
        )
        bucket["total"] += float(r.get(t_key) or 0)
        bucket["garbage"] += float(r.get(g_key) or 0)
        bucket["effective"] += float(r.get(e_key) or 0)
        if float(r.get(t_key) or 0) > 0:
            bucket["n"] += 1
    out: List[dict] = []
    for geo, v in sorted(by_geo.items(), key=lambda x: -x[1]["total"]):
        total, garb, eff = v["total"], v["garbage"], v["effective"]
        out.append(
            {
                "section": f"by_geo_{prefix}",
                "label": geo,
                "week_start": "",
                "week_end": "",
                "total_spend": round(total, 2),
                "garbage_spend": round(garb, 2),
                "effective_spend": round(eff, 2),
                "garbage_pct": _pct(garb, total),
                "effective_pct": _pct(eff, total),
                "campaigns_with_spend": int(v["n"]),
                "wow_garbage_spend_delta": "",
                "wow_garbage_pct_delta": "",
                "wow_trend": "",
                "notes": "",
            }
        )
    return out


def _portfolio_row(section: str, rows: List[dict], prefix: str) -> dict:
    t_key = f"{prefix}_total_spend"
    g_key = f"{prefix}_garbage_spend"
    e_key = f"{prefix}_effective_spend"
    total = sum(float(r.get(t_key) or 0) for r in rows)
    garb = sum(float(r.get(g_key) or 0) for r in rows)
    eff = sum(float(r.get(e_key) or 0) for r in rows)
    with_spend = sum(1 for r in rows if float(r.get(t_key) or 0) > 0)
    return {
        "section": section,
        "label": "ALL",
        "week_start": "",
        "week_end": "",
        "total_spend": round(total, 2),
        "garbage_spend": round(garb, 2),
        "effective_spend": round(eff, 2),
        "garbage_pct": _pct(garb, total),
        "effective_pct": _pct(eff, total),
        "campaigns_with_spend": with_spend,
        "wow_garbage_spend_delta": "",
        "wow_garbage_pct_delta": "",
        "wow_trend": "",
        "notes": "",
    }


def _build_weekly_rows(
    api_key: str,
    campaign_jobs: List[Tuple[int, str]],
    starts: Dict[int, str],
    garbage: Set[str],
    progress_cb: Optional[Callable[[str], None]] = None,
) -> List[dict]:
    today = _utc_today()
    if not campaign_jobs:
        return []
    min_start = min(date.fromisoformat(s) for s in starts.values() if s)
    lookback_start = today - timedelta(weeks=_WEEKLY_LOOKBACK)
    range_start = max(min_start, lookback_start)
    weeks = iso_weeks_between(range_start, today)

    weekly: Dict[str, Dict[str, float]] = {
        label: {"total_spend": 0.0, "garbage_spend": 0.0, "campaigns_with_spend": 0}
        for label, _, _ in weeks
    }

    jobs: List[Tuple[int, str, str, str]] = []
    cids = {cid for cid, _ in campaign_jobs}
    for label, wk_start, wk_end in weeks:
        wk_end_d = date.fromisoformat(wk_end)
        for cid in cids:
            camp_start = starts.get(cid, wk_start)
            if camp_start and date.fromisoformat(camp_start) > wk_end_d:
                continue
            jobs.append((cid, label, wk_start, wk_end))

    done = 0

    def _work(job: Tuple[int, str, str, str]) -> Tuple[str, float, float]:
        cid, label, wk_start, wk_end = job
        total, garb, _, _ = campaign_spend_breakdown(api_key, cid, wk_start, wk_end, garbage)
        return label, total, garb

    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        futures = [pool.submit(_work, j) for j in jobs]
        for fut in as_completed(futures):
            done += 1
            if progress_cb and (done % 200 == 0 or done == len(jobs)):
                progress_cb(f"weekly {done}/{len(jobs)}")
            try:
                label, total, garb = fut.result()
            except Exception:
                continue
            weekly[label]["total_spend"] += total
            weekly[label]["garbage_spend"] += garb
            if total > 0:
                weekly[label]["campaigns_with_spend"] += 1

    rows: List[dict] = []
    prev_garbage_pct: Optional[float] = None
    prev_garbage: Optional[float] = None
    prev_effective: Optional[float] = None

    for label, wk_start, wk_end in weeks:
        w = weekly[label]
        total = round(w["total_spend"], 2)
        garb = round(w["garbage_spend"], 2)
        effective = round(total - garb, 2)
        garb_pct = _pct(garb, total)

        wow_garbage_delta: Any = ""
        wow_garbage_pct_delta: Any = ""
        wow_trend = ""
        if prev_garbage is not None and prev_garbage > 0:
            wow_garbage_delta = round(garb - prev_garbage, 2)
        if prev_garbage_pct is not None and total > 0:
            wow_garbage_pct_delta = round(garb_pct - prev_garbage_pct, 2)
            if wow_garbage_pct_delta < -1:
                wow_trend = "improving"
            elif wow_garbage_pct_delta > 1:
                wow_trend = "worsening"
            else:
                wow_trend = "flat"

        rows.append(
            {
                "section": "weekly",
                "label": label,
                "week_start": wk_start,
                "week_end": wk_end,
                "total_spend": total,
                "garbage_spend": garb,
                "effective_spend": effective,
                "garbage_pct": garb_pct,
                "effective_pct": _pct(effective, total),
                "campaigns_with_spend": int(w["campaigns_with_spend"]),
                "wow_garbage_spend_delta": wow_garbage_delta,
                "wow_garbage_pct_delta": wow_garbage_pct_delta,
                "wow_trend": wow_trend,
                "notes": "",
            }
        )
        if total > 0:
            prev_garbage_pct = garb_pct
            prev_garbage = garb
            prev_effective = effective
    return rows


def _write_state(patch: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    cur: dict = {}
    if STATE_PATH.exists():
        try:
            cur = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            cur = {}
    cur.update(patch)
    tmp = STATE_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(cur, ensure_ascii=True, indent=2), encoding="utf-8")
    tmp.replace(STATE_PATH)


def load_state() -> dict:
    if not STATE_PATH.exists():
        return {"status": "idle"}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"status": "idle"}


def ensure_audit_worksheets(spreadsheet_id: str) -> None:
    sid = (spreadsheet_id or "").strip()
    if not sid:
        return
    gd.ensure_worksheet_with_headers(sid, TAB_CAMPAIGNS, HEADERS_CAMPAIGNS)
    gd.ensure_worksheet_with_headers(sid, TAB_SUMMARY, HEADERS_SUMMARY)


def run_efficiency_audit() -> Dict[str, Any]:
    """Full audit; writes both SK tools tabs. Raises on fatal error."""
    if not _try_acquire_audit_lock():
        state = load_state()
        if not _is_stale_running(state):
            raise RuntimeError("Buying efficiency audit is already running")
        _release_audit_lock()
        if not _try_acquire_audit_lock():
            raise RuntimeError("Could not acquire audit lock")

    try:
        return _run_efficiency_audit_body()
    finally:
        _release_audit_lock()


def _run_efficiency_audit_body() -> Dict[str, Any]:
    api_key = (SOURCEKNOWLEDGE_API_KEY or "").strip()
    tools_id = (SK_TOOLS_SPREADSHEET_ID or "").strip()
    if not api_key:
        raise RuntimeError("SOURCEKNOWLEDGE_API_KEY / KEYSK not set")
    if not tools_id:
        raise RuntimeError("SK_TOOLS_SPREADSHEET_ID is not set")

    started = time.time()
    as_of = _utc_now()
    today_s = _utc_today().isoformat()
    yesterday_s = _utc_yesterday()

    def progress(msg: str) -> None:
        _write_state({"status": "running", "progress": msg, "started_at": as_of})

    progress("loading garbage sub IDs")
    garbage, garbage_source = load_garbage_sub_ids()

    progress("reading SKtrackExploration")
    sheet_rows = gd.read_sheet_withID(SK_OPTIMIZER_SHEET_ID, "SKtrackExploration")
    all_campaigns = fetch_all_campaigns(api_key)

    jobs: List[dict] = []
    starts: Dict[int, str] = {}
    for row in sheet_rows:
        if not isinstance(row, dict):
            continue
        cid_raw = row.get("campaignId") or row.get("campId")
        try:
            cid = int(str(cid_raw).strip())
        except (TypeError, ValueError):
            continue
        camp = all_campaigns.get(cid, {})
        start = str(camp.get("start") or "")[:10] or today_s
        starts[cid] = start
        jobs.append(
            {
                "campaign_id": cid,
                "campaign_name": row.get("campaignName") or row.get("campName") or camp.get("name") or "",
                "brand": row.get("brand") or "",
                "geo": row.get("geo") or "",
                "status": row.get("status") or "",
                "mon_network": row.get("monNetwork") or "",
                "start_date": start,
            }
        )

    campaign_results: List[dict] = []
    errors = 0
    done = 0

    def _campaign_work(job: dict) -> dict:
        cid = job["campaign_id"]
        life_t, life_g, subs, err = campaign_spend_breakdown(
            api_key, cid, job["start_date"], today_s, garbage
        )
        y_t, y_g, _, y_err = campaign_spend_breakdown(api_key, cid, yesterday_s, yesterday_s, garbage)
        err_msg = err or y_err or ""
        life_e = round(life_t - life_g, 4)
        y_e = round(y_t - y_g, 4)
        return {
            **job,
            "lifetime_total_spend": life_t,
            "lifetime_garbage_spend": life_g,
            "lifetime_effective_spend": life_e,
            "lifetime_garbage_pct": _pct(life_g, life_t),
            "yesterday_total_spend": y_t,
            "yesterday_garbage_spend": y_g,
            "yesterday_effective_spend": y_e,
            "yesterday_garbage_pct": _pct(y_g, y_t),
            "publisher_count": subs,
            "error": err_msg,
            "report_as_of_utc": as_of,
        }

    progress(f"campaigns 0/{len(jobs)}")
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        futures = {pool.submit(_campaign_work, j): j for j in jobs}
        for fut in as_completed(futures):
            done += 1
            if done % 25 == 0 or done == len(jobs):
                progress(f"campaigns {done}/{len(jobs)}")
            try:
                row = fut.result()
            except Exception as e:
                job = futures[fut]
                row = {
                    **job,
                    "lifetime_total_spend": 0,
                    "lifetime_garbage_spend": 0,
                    "lifetime_effective_spend": 0,
                    "lifetime_garbage_pct": 0,
                    "yesterday_total_spend": 0,
                    "yesterday_garbage_spend": 0,
                    "yesterday_effective_spend": 0,
                    "yesterday_garbage_pct": 0,
                    "publisher_count": 0,
                    "error": str(e),
                    "report_as_of_utc": as_of,
                }
                errors += 1
            if row.get("error"):
                errors += 1
            campaign_results.append(row)

    campaign_results.sort(key=lambda r: float(r.get("lifetime_total_spend") or 0), reverse=True)

    progress("weekly aggregation")
    weekly_rows = _build_weekly_rows(
        api_key,
        [(j["campaign_id"], j["start_date"]) for j in jobs],
        starts,
        garbage,
        progress_cb=progress,
    )

    summary_rows: List[dict] = [
        {
            "section": "meta",
            "label": "report",
            "week_start": "",
            "week_end": "",
            "total_spend": "",
            "garbage_spend": "",
            "effective_spend": "",
            "garbage_pct": "",
            "effective_pct": "",
            "campaigns_with_spend": len(jobs),
            "wow_garbage_spend_delta": "",
            "wow_garbage_pct_delta": "",
            "wow_trend": "",
            "notes": (
                f"as_of={as_of}; garbage_source={garbage_source}; "
                f"garbage_subs={len(garbage)}; api_errors={errors}; "
                f"weekly_lookback_weeks={_WEEKLY_LOOKBACK}"
            ),
        },
        _portfolio_row("portfolio_lifetime", campaign_results, "lifetime"),
        _portfolio_row("portfolio_yesterday", campaign_results, "yesterday"),
    ]
    summary_rows.extend(_aggregate_geo(campaign_results, "lifetime"))
    summary_rows.extend(_aggregate_geo(campaign_results, "yesterday"))
    summary_rows.extend(weekly_rows)

    progress("writing Google Sheets")
    ensure_audit_worksheets(tools_id)
    gd.create_or_update_sheet_from_dicts_withId(tools_id, TAB_CAMPAIGNS, campaign_results)
    gd.create_or_update_sheet_from_dicts_withId(tools_id, TAB_SUMMARY, summary_rows)

    life_total = float(summary_rows[1]["total_spend"])
    life_garb = float(summary_rows[1]["garbage_spend"])
    y_total = float(summary_rows[2]["total_spend"])
    y_garb = float(summary_rows[2]["garbage_spend"])

    result = {
        "status": "ready",
        "as_of_utc": as_of,
        "finished_at_utc": _utc_now(),
        "fetch_seconds": round(time.time() - started, 1),
        "campaigns_audited": len(campaign_results),
        "api_errors": errors,
        "garbage_sub_count": len(garbage),
        "garbage_source": garbage_source,
        "lifetime_total_spend": life_total,
        "lifetime_garbage_pct": _pct(life_garb, life_total),
        "yesterday_total_spend": y_total,
        "yesterday_garbage_pct": _pct(y_garb, y_total),
        "sheets": {
            "workbook_id": tools_id,
            "campaigns_tab": TAB_CAMPAIGNS,
            "summary_tab": TAB_SUMMARY,
        },
        "error": None,
        "progress": "done",
    }
    _write_state(result)
    logger.info("SK efficiency audit done in %.1fs (%s campaigns)", result["fetch_seconds"], len(jobs))
    return result


def refresh_running() -> bool:
    state = load_state()
    if (state.get("status") or "").strip().lower() == "running" and not _is_stale_running(state):
        return True
    with _REFRESH_LOCK:
        return _subprocess_audit_running() or _REFRESH_RUNNING


def _spawn_audit_subprocess() -> bool:
    """Start ``scripts/sk_exploration_efficiency_audit.py`` (lock acquired inside child)."""
    global _AUDIT_PROC
    if not _AUDIT_SCRIPT.is_file():
        raise RuntimeError(f"Audit script missing: {_AUDIT_SCRIPT}")

    _AUDIT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    log_f = open(_AUDIT_LOG_PATH, "a", encoding="utf-8")
    try:
        _AUDIT_PROC = subprocess.Popen(
            [sys.executable, str(_AUDIT_SCRIPT)],
            cwd=str(_ROOT),
            stdout=log_f,
            stderr=subprocess.STDOUT,
            env=os.environ.copy(),
        )
    except Exception:
        log_f.close()
        raise
    else:
        log_f.close()
    return True


def _run_refresh() -> None:
    global _REFRESH_RUNNING
    try:
        run_efficiency_audit()
    except Exception as e:
        logger.exception("SK efficiency audit failed")
        _write_state(
            {
                "status": "error",
                "error": str(e),
                "finished_at_utc": _utc_now(),
                "progress": "failed",
            }
        )
    finally:
        with _REFRESH_LOCK:
            _REFRESH_RUNNING = False


def queue_refresh() -> bool:
    """Queue a full audit (subprocess when possible). Returns False if already running."""
    global _REFRESH_RUNNING
    state = load_state()
    if (state.get("status") or "").strip().lower() == "running":
        if _is_stale_running(state):
            _write_state(
                {
                    "status": "error",
                    "error": "Previous audit run timed out (stale running state)",
                    "finished_at_utc": _utc_now(),
                    "progress": "failed",
                }
            )
            _release_audit_lock()
        else:
            return False

    with _REFRESH_LOCK:
        if _subprocess_audit_running() or _REFRESH_RUNNING:
            return False

    try:
        if _spawn_audit_subprocess():
            _write_state(
                {
                    "status": "running",
                    "progress": "starting",
                    "started_at": _utc_now(),
                    "error": None,
                }
            )
            return True
    except Exception as e:
        logger.warning("Audit subprocess spawn failed (%s); falling back to in-process thread", e)

    with _REFRESH_LOCK:
        if _REFRESH_RUNNING:
            return False
        _REFRESH_RUNNING = True
    t = threading.Thread(target=_run_refresh, name="sk-efficiency-audit", daemon=True)
    t.start()
    return True


def payload_for_api(*, force_refresh: bool = False) -> Dict[str, Any]:
    state = load_state()
    if force_refresh:
        started = queue_refresh()
        state = load_state()
        if started and state.get("status") not in ("ready", "running"):
            return {
                "status": "building",
                "refresh_running": True,
                "message": "Buying efficiency audit started (typically 15–25 min)…",
            }
        out = dict(state)
        out["refresh_running"] = refresh_running() or started
        out["cached"] = state.get("status") == "ready" and not started
        if started or out["refresh_running"]:
            out["status"] = "running" if state.get("status") == "running" else "building"
        if started and not out.get("message"):
            out["message"] = "Buying efficiency audit started (typically 15–25 min)…"
        return out

    if state.get("status") == "running" or refresh_running():
        out = dict(state)
        out["status"] = "running"
        out["refresh_running"] = True
        return out

    if state.get("status") == "ready":
        out = dict(state)
        out["refresh_running"] = False
        out["cached"] = True
        return out

    return {
        "status": state.get("status") or "idle",
        "refresh_running": refresh_running(),
        "message": "No audit snapshot yet. Click Run audit.",
    }
