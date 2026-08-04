"""
Scheduled Kelkoo daily conversion postbacks (09:00 Asia/Jerusalem by default).

1. Probe each feed+geo raw report (HTTP OK + parseable TSV = ready).
2. Run postbacks for ready geos immediately.
3. If some geos are still missing, schedule a retry in one hour (capped attempts).
4. Append each attempt to ``data/kelkoo_daily_postbacks_run_log.json``.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence
from zoneinfo import ZoneInfo

import requests

logger = logging.getLogger(__name__)

_started = False
_run_lock = threading.Lock()
_FALLBACK_GEOS = (
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


def _pending_path() -> Path:
    from config import KELKOO_DAILY_POSTBACK_PENDING_PATH

    p = Path(KELKOO_DAILY_POSTBACK_PENDING_PATH)
    return p if p.is_absolute() else (Path(__file__).resolve().parents[1] / p)


def _load_pending() -> Dict[str, Any]:
    p = _pending_path()
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.warning("Kelkoo daily pending read failed %s: %s", p, e)
        return {}


def _save_pending(data: Dict[str, Any]) -> None:
    p = _pending_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)


def _clear_pending() -> None:
    p = _pending_path()
    if p.exists():
        try:
            p.unlink()
        except Exception:
            _save_pending({})


def default_kelkoo_postback_geos() -> List[str]:
    from config import KELKOO_RAW_REPORT_GEOS

    if KELKOO_RAW_REPORT_GEOS:
        return [g.strip().lower() for g in KELKOO_RAW_REPORT_GEOS if str(g).strip()]
    return list(_FALLBACK_GEOS)


def enabled_kelkoo_postback_feeds() -> List[str]:
    from config import KELKOO_POSTBACK_FEED_TAGS, kelkoo_api_key_for_postback_tag

    out: List[str] = []
    for tag in KELKOO_POSTBACK_FEED_TAGS:
        if (kelkoo_api_key_for_postback_tag(tag) or "").strip():
            out.append(tag)
    return out


def probe_kelkoo_geo_report(
    country: str,
    report_date: str,
    api_key: str,
    session: requests.Session,
    *,
    timeout: float = 90.0,
) -> Dict[str, Any]:
    """
    Check whether Kelkoo raw report data is available for one geo.

    ``ready`` — HTTP 200 and parseable TSV (0 data rows still counts as ready).
    ``not_ready`` — HTTP error, empty body, network failure, or non-TSV payload.
    """
    from integrations.daily_conversion_postbacks import fetch_kelkoo_raw_tsv

    geo = country.strip().lower()
    try:
        status, body = fetch_kelkoo_raw_tsv(geo, report_date, api_key, session, timeout=timeout)
    except Exception as e:
        return {
            "status": "not_ready",
            "geo": geo,
            "http_status": None,
            "reason": f"request error: {e}"[:400],
        }

    if status != 200:
        return {
            "status": "not_ready",
            "geo": geo,
            "http_status": status,
            "reason": f"HTTP {status}",
            "body_preview": (body or "")[:200],
        }

    text = body or ""
    stripped = text.strip()
    if not stripped:
        return {
            "status": "not_ready",
            "geo": geo,
            "http_status": 200,
            "reason": "empty body",
        }

    first = stripped.splitlines()[0]
    head = stripped[:300].lower()
    if "<html" in head or first.lstrip().startswith("{"):
        return {
            "status": "not_ready",
            "geo": geo,
            "http_status": 200,
            "reason": "non-tsv body",
            "body_preview": first[:200],
        }
    # Kelkoo raw reports are tab-separated; accept header-only (no leads that day).
    if "\t" not in first and "publisherclickid" not in first.lower() and "custom1" not in first.lower():
        return {
            "status": "not_ready",
            "geo": geo,
            "http_status": 200,
            "reason": "unrecognized header",
            "body_preview": first[:200],
        }

    return {
        "status": "ready",
        "geo": geo,
        "http_status": 200,
        "reason": "ok",
        "bytes": len(text),
    }


def _geo_already_done(state_path: Path, feed: str, report_date: str, geo: str) -> bool:
    from integrations.daily_conversion_postback_state import load_state

    data = load_state(state_path)
    sources = data.get("sources") or {}
    src = sources.get(feed) if isinstance(sources, dict) else None
    if not isinstance(src, dict):
        return False
    day = src.get(report_date)
    if not isinstance(day, dict):
        return False
    geos = day.get("geos") or {}
    if not isinstance(geos, dict):
        return False
    gs = geos.get(geo) or {}
    return isinstance(gs, dict) and gs.get("status") == "done"


def _schedule_retry_job(*, report_date: str, attempt: int, dry_run: bool) -> Dict[str, Any]:
    from config import (
        KELKOO_DAILY_POSTBACK_RETRY_INTERVAL_MINUTES,
        KELKOO_DAILY_POSTBACK_SCHEDULER_TZ,
    )

    minutes = int(KELKOO_DAILY_POSTBACK_RETRY_INTERVAL_MINUTES)
    try:
        tz = ZoneInfo(KELKOO_DAILY_POSTBACK_SCHEDULER_TZ or "Asia/Jerusalem")
    except Exception:
        tz = ZoneInfo("Asia/Jerusalem")
    run_at = datetime.now(tz) + timedelta(minutes=minutes)
    run_at_utc = run_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _fire() -> None:
        try:
            run_kelkoo_daily_postbacks_scheduled(
                report_date=report_date,
                attempt=attempt,
                pending_only=True,
                dry_run=dry_run,
                triggered_by="retry",
            )
        except Exception:
            logger.exception("Kelkoo daily postbacks retry attempt %s failed", attempt)

    try:
        from scheduler.autoserver_scheduler import get_scheduler

        sch = get_scheduler()
        if sch is not None and getattr(sch, "running", False):
            job_id = f"kelkoo_daily_postbacks_retry_{report_date}"
            sch.add_job(
                _fire,
                trigger="date",
                run_date=run_at,
                id=job_id,
                replace_existing=True,
                max_instances=1,
                coalesce=True,
                misfire_grace_time=max(300, minutes * 60),
            )
            logger.info(
                "Kelkoo daily postbacks: scheduled APScheduler retry attempt=%s at %s (%s)",
                attempt,
                run_at.isoformat(),
                KELKOO_DAILY_POSTBACK_SCHEDULER_TZ,
            )
            return {"ok": True, "mode": "apscheduler", "run_at_utc": run_at_utc, "attempt": attempt}
    except Exception:
        logger.exception("Kelkoo daily postbacks: could not schedule APScheduler retry")

    delay_s = max(60.0, float(minutes) * 60.0)
    timer = threading.Timer(delay_s, _fire)
    timer.daemon = True
    timer.name = f"kelkoo-daily-pb-retry-{attempt}"
    timer.start()
    logger.info(
        "Kelkoo daily postbacks: scheduled thread retry attempt=%s in %.0fs",
        attempt,
        delay_s,
    )
    return {"ok": True, "mode": "thread", "run_at_utc": run_at_utc, "attempt": attempt}


def run_kelkoo_daily_postbacks_scheduled(
    *,
    report_date: Optional[str] = None,
    attempt: int = 1,
    pending_only: bool = False,
    dry_run: bool = False,
    triggered_by: str = "cron",
    feeds: Optional[Sequence[str]] = None,
    geos: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """
    Probe → postbacks for ready geos → persist pending + schedule hourly retry if needed.
    """
    from config import (
        DAILY_CONVERSION_POSTBACK_STATE_PATH,
        KELKOO_DAILY_POSTBACK_MAX_ATTEMPTS,
        kelkoo_api_key_for_postback_tag,
    )
    from integrations.daily_conversion_postbacks import (
        default_report_date_str,
        run_kelkoo_feed_postbacks,
    )
    from integrations.kelkoo_daily_postbacks_run_log import append_run

    started = datetime.now(timezone.utc)
    date_s = (report_date or default_report_date_str()).strip()
    max_attempts = int(KELKOO_DAILY_POSTBACK_MAX_ATTEMPTS)
    attempt = max(1, int(attempt))
    state_path = Path(DAILY_CONVERSION_POSTBACK_STATE_PATH)

    feed_list = [f.strip().lower() for f in (feeds or enabled_kelkoo_postback_feeds()) if str(f).strip()]
    if not feed_list:
        out = {
            "ok": False,
            "error": "No Kelkoo postback feeds with API keys configured",
            "report_date": date_s,
            "attempt": attempt,
            "triggered_by": triggered_by,
        }
        append_run({**out, "started_at": started.strftime("%Y-%m-%dT%H:%M:%SZ"), "finished_at": _utc_now()})
        return out

    pending_doc = _load_pending()
    if pending_only:
        if str(pending_doc.get("report_date") or "") != date_s:
            logger.info(
                "Kelkoo daily postbacks retry: no pending for %s (have %s) — nothing to do",
                date_s,
                pending_doc.get("report_date"),
            )
            return {
                "ok": True,
                "report_date": date_s,
                "attempt": attempt,
                "pending_only": True,
                "skipped": "no_pending_for_date",
                "pending_by_feed": {},
            }
        pending_by_feed: Dict[str, List[str]] = {}
        raw_pend = pending_doc.get("pending_by_feed") or {}
        if isinstance(raw_pend, dict):
            for fk, gl in raw_pend.items():
                if isinstance(gl, list):
                    pending_by_feed[str(fk)] = [str(g).strip().lower() for g in gl if str(g).strip()]
        all_geos = default_kelkoo_postback_geos()
    else:
        pending_by_feed = {}
        all_geos = [g.strip().lower() for g in (geos or default_kelkoo_postback_geos()) if str(g).strip()]

    if not _run_lock.acquire(blocking=False):
        logger.warning("Kelkoo daily postbacks: another run in progress; skipping")
        return {
            "ok": False,
            "error": "another_run_in_progress",
            "report_date": date_s,
            "attempt": attempt,
        }

    session = requests.Session()
    feed_summaries: Dict[str, Any] = {}
    next_pending: Dict[str, List[str]] = {}
    any_processed = False

    try:
        for feed in feed_list:
            api_key = (kelkoo_api_key_for_postback_tag(feed) or "").strip()
            if not api_key:
                feed_summaries[feed] = {"ok": False, "error": "missing_api_key"}
                continue

            if not dry_run and not pending_only:
                from integrations.daily_postbacks_run_history import feed_live_run_succeeded_today

                if feed_live_run_succeeded_today(feed, report_date=date_s):
                    logger.info(
                        "Kelkoo daily postbacks %s %s: already succeeded today — skipping",
                        feed,
                        date_s,
                    )
                    feed_summaries[feed] = {
                        "ok": True,
                        "skipped_already_ran_today": True,
                        "ready": [],
                        "not_ready": [],
                        "already_done": [],
                        "probes": {},
                        "postback_summary": None,
                    }
                    continue

            if pending_only:
                geo_candidates = list(pending_by_feed.get(feed) or [])
            else:
                geo_candidates = list(all_geos)

            ready: List[str] = []
            not_ready: List[str] = []
            already_done: List[str] = []
            probes: Dict[str, Any] = {}
            processed_summary: Optional[Dict[str, Any]] = None

            from integrations.daily_postbacks_run_history import feed_run_marker

            with feed_run_marker(feed, report_date=date_s, triggered_by=triggered_by or "scheduler"):
                for geo in geo_candidates:
                    if _geo_already_done(state_path, feed, date_s, geo):
                        already_done.append(geo)
                        continue
                    probe = probe_kelkoo_geo_report(geo, date_s, api_key, session)
                    probes[geo] = {
                        "status": probe.get("status"),
                        "http_status": probe.get("http_status"),
                        "reason": probe.get("reason"),
                    }
                    if probe.get("status") == "ready":
                        ready.append(geo)
                    else:
                        not_ready.append(geo)

                if ready:
                    any_processed = True
                    logger.info(
                        "Kelkoo daily postbacks %s %s attempt=%s: processing ready geos %s",
                        feed,
                        date_s,
                        attempt,
                        ",".join(ready),
                    )
                    processed_summary = run_kelkoo_feed_postbacks(
                        feed,
                        date_s,
                        state_path=state_path,
                        geos=ready,
                        only_geo=None,
                        dry_run=dry_run,
                        no_resume=False,
                        session=session,
                    )
                    if not dry_run:
                        try:
                            from integrations.daily_postbacks_run_history import record_last_run

                            record_last_run(
                                feed,
                                date_s,
                                dry_run=False,
                                ok=bool(processed_summary.get("ok")),
                                summary=processed_summary,
                                batch_exit_code=0 if processed_summary.get("ok") else 1,
                            )
                        except Exception:
                            logger.exception(
                                "Kelkoo daily postbacks: last-run history write failed for %s", feed
                            )

            if not_ready:
                next_pending[feed] = not_ready

            feed_summaries[feed] = {
                "ok": True if processed_summary is None else bool(processed_summary.get("ok")),
                "ready": ready,
                "not_ready": not_ready,
                "already_done": already_done,
                "probes": probes,
                "postback_summary": processed_summary,
            }
            if not_ready:
                logger.info(
                    "Kelkoo daily postbacks %s %s attempt=%s: still waiting on %s",
                    feed,
                    date_s,
                    attempt,
                    ",".join(not_ready),
                )

        retry_info: Optional[Dict[str, Any]] = None
        abandoned: Dict[str, List[str]] = {}
        if next_pending:
            _save_pending(
                {
                    "report_date": date_s,
                    "attempt": attempt,
                    "pending_by_feed": next_pending,
                    "updated_at_utc": _utc_now(),
                    "dry_run": bool(dry_run),
                }
            )
            if attempt < max_attempts and not dry_run:
                retry_info = _schedule_retry_job(
                    report_date=date_s,
                    attempt=attempt + 1,
                    dry_run=dry_run,
                )
            elif attempt >= max_attempts:
                abandoned = dict(next_pending)
                logger.error(
                    "Kelkoo daily postbacks: max attempts (%s) reached for %s; abandoned=%s",
                    max_attempts,
                    date_s,
                    {k: ",".join(v) for k, v in abandoned.items()},
                )
            elif dry_run:
                logger.info("Kelkoo daily postbacks dry-run: would schedule retry for pending geos")
        else:
            _clear_pending()

        finished = datetime.now(timezone.utc)
        ok = not abandoned and all(
            bool(v.get("ok", True)) for v in feed_summaries.values() if isinstance(v, dict)
        )
        out: Dict[str, Any] = {
            "ok": ok,
            "report_date": date_s,
            "attempt": attempt,
            "max_attempts": max_attempts,
            "triggered_by": triggered_by,
            "pending_only": bool(pending_only),
            "dry_run": bool(dry_run),
            "feeds": feed_summaries,
            "pending_by_feed": next_pending,
            "retry_scheduled": retry_info,
            "abandoned": abandoned,
            "any_processed": any_processed,
            "started_at": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "finished_at": finished.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        try:
            append_run(out)
        except Exception:
            logger.exception("Kelkoo daily postbacks: run log append failed")
        logger.info(
            "Kelkoo daily postbacks finished: date=%s attempt=%s pending_feeds=%s retry=%s",
            date_s,
            attempt,
            list(next_pending.keys()),
            bool(retry_info),
        )
        return out
    finally:
        _run_lock.release()


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _seconds_until_local(hour: int, minute: int, tz_name: str) -> float:
    tz = ZoneInfo(tz_name)
    now = datetime.now(tz)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return max(1.0, (target - now).total_seconds())


def _thread_loop() -> None:
    from config import (
        KELKOO_DAILY_POSTBACK_SCHEDULER_HOUR_LOCAL,
        KELKOO_DAILY_POSTBACK_SCHEDULER_MINUTE,
        KELKOO_DAILY_POSTBACK_SCHEDULER_TZ,
    )

    while True:
        try:
            delay = _seconds_until_local(
                int(KELKOO_DAILY_POSTBACK_SCHEDULER_HOUR_LOCAL),
                int(KELKOO_DAILY_POSTBACK_SCHEDULER_MINUTE),
                KELKOO_DAILY_POSTBACK_SCHEDULER_TZ,
            )
            logger.info(
                "Kelkoo daily postbacks scheduler: sleeping %.0fs until %02d:%02d %s",
                delay,
                int(KELKOO_DAILY_POSTBACK_SCHEDULER_HOUR_LOCAL),
                int(KELKOO_DAILY_POSTBACK_SCHEDULER_MINUTE),
                KELKOO_DAILY_POSTBACK_SCHEDULER_TZ,
            )
            time.sleep(delay)
            # Hourly retries are scheduled inside the run (thread Timer when APScheduler is off).
            run_kelkoo_daily_postbacks_scheduled(triggered_by="cron")
        except Exception:
            logger.exception("Kelkoo daily postbacks scheduler loop failed")
            time.sleep(60)


def start_kelkoo_daily_postbacks_scheduler() -> None:
    """Start thread fallback when AutoServer APScheduler is not owning the cron."""
    global _started
    from config import (
        AUTOSERVER_SCHEDULER_ENABLED,
        KELKOO_DAILY_POSTBACK_SCHEDULER_ENABLED,
        KELKOO_DAILY_POSTBACK_SCHEDULER_HOUR_LOCAL,
        KELKOO_DAILY_POSTBACK_SCHEDULER_MINUTE,
        KELKOO_DAILY_POSTBACK_SCHEDULER_TZ,
    )

    if not KELKOO_DAILY_POSTBACK_SCHEDULER_ENABLED:
        logger.info("Kelkoo daily postbacks scheduler disabled (KELKOO_DAILY_POSTBACK_SCHEDULER_ENABLED=0)")
        return
    if not enabled_kelkoo_postback_feeds():
        logger.info("Kelkoo daily postbacks scheduler skipped (no feed API keys)")
        return
    if AUTOSERVER_SCHEDULER_ENABLED:
        logger.info(
            "Kelkoo daily postbacks use APScheduler cron (AutoServer scheduler); thread loop skipped"
        )
        return
    if os.getenv("FLASK_DEBUG") == "1" and os.environ.get("WERKZEUG_RUN_MAIN") == "false":
        return
    if _started:
        return
    threading.Thread(target=_thread_loop, name="kelkoo-daily-postbacks-scheduler", daemon=True).start()
    _started = True
    logger.info(
        "Kelkoo daily postbacks scheduler started (daily %02d:%02d %s, hourly geo retries)",
        int(KELKOO_DAILY_POSTBACK_SCHEDULER_HOUR_LOCAL),
        int(KELKOO_DAILY_POSTBACK_SCHEDULER_MINUTE),
        KELKOO_DAILY_POSTBACK_SCHEDULER_TZ,
    )
