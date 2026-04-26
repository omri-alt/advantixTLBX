"""
KLblend Flask app: workflows for Keitaro campaign management (driven by Kelkoo data).
Triggered by external timing/scheduler.
"""
import logging
import hmac
import os
import re
import hashlib
import sqlite3
import shlex
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import unquote

from flask import Flask, flash, request, jsonify, render_template, abort, redirect, url_for

from config import (
    KEITARO_BASE_URL,
    KEITARO_API_KEY,
    EC_ADVERTISER_KEY,
    EC_AUTH_KEY,
    EC_SECRET_KEY,
    EC_SHEETS_SPREADSHEET_ID,
    ECOMNIA_GLOBA_LIST_TAB,
    FEED1_API_KEY,
    FEED2_API_KEY,
    KELKOO_LATE_SALES_SPREADSHEET_ID,
    LATE_SALES_POSTBACK_BASE,
    DAILY_CONVERSION_POSTBACK_STATE_PATH,
    OVERVIEW_SNAPSHOT_TZ,
    OVERVIEW_SNAPSHOT_HOUR,
)
from workflows.campaign_setup import run_create_campaign_workflow
from integrations.keitaro import KeitaroClientError
from integrations.kelkoo_search import kelkoo_merchant_link_check
from integrations.yadore import deeplink as yadore_deeplink, YadoreClientError
from integrations.adexa import links_merchant_check as adexa_links_check, AdexaClientError
from integrations.monetization_geo import yadore_feed_class
from assistance import (
    get_campaigns_data,
    clone_campaign_copy,
    get_campaigns_then_clone_setup,
    get_campaign_streams,
    get_campaign_streams_by_alias,
    get_offers_data,
    get_full_setup,
)
from kelkoo_late_sales import run_late_sales_flow
from integrations.overview import (
    slice_ecomnia,
    slice_revenue,
    slice_sourceknowledge,
    slice_zeropark,
)
from integrations.overview_snapshot import (
    read_snapshot_for_api,
    refresh_overview_snapshot,
    start_daily_overview_scheduler,
    start_overview_snapshot_bootstrap,
)
from integrations.daily_conversion_postbacks import (
    default_report_date_str,
    run_daily_conversion_postbacks_batch,
)
from integrations.daily_postbacks_dashboard import build_dashboard_cards, feed_detail_context
from integrations.ecomnia_console import (
    all_copy_paste_text,
    apply_wl_potential_cpcbysource_updates,
    compute_global_wl_zero_click_potential,
    derived_whitelist_copy_paste,
    exploration_action_items,
    geo_map_from_sheet_values,
    pull_derived_whitelist_from_api,
    pull_derived_whitelist_with_campaigns,
    sync_geo_blacklists,
    utc_yesterday_iso,
    whitelist_check_flat_rows,
    whitelist_focus_source_traffic_no_buy,
)
from integrations.ecomnia_run_history import load_state, record_run, update_cache
from automations.autoserver import AUTOMATION_SPECS
from automations.autoserver.run_log import last_run_by_automation, read_entries_newest_first
from scheduler.autoserver_scheduler import (
    ensure_automations_initialized,
    get_automation_listeners,
    schedule_trigger_all,
    schedule_trigger_one,
    scheduler_running,
    start_autoserver_scheduler,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = (os.environ.get("FLASK_SECRET_KEY") or "").strip() or "klblend-dev-flask-secret-change-me"

ROOT_DIR = Path(__file__).resolve().parent
RUNS_DIR = ROOT_DIR / "runtime" / "workflow_runs"
RUNS_DIR.mkdir(parents=True, exist_ok=True)


def _parse_utc_iso(iso: str) -> Optional[datetime]:
    if not iso or not isinstance(iso, str):
        return None
    s = iso.strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None


def _format_duration_human(seconds: Any) -> str:
    try:
        sec = float(seconds)
    except (TypeError, ValueError):
        return "—"
    if sec < 0:
        sec = 0.0
    whole = int(round(sec))
    if whole < 60:
        if abs(sec - whole) < 0.05:
            return f"{whole}s"
        return f"{sec:.1f}s"
    if whole < 3600:
        m, s = divmod(whole, 60)
        return f"{m}m {s}s" if s else f"{m}m"
    h, r = divmod(whole, 3600)
    m, s = divmod(r, 60)
    parts: list[str] = [f"{h}h"]
    if m:
        parts.append(f"{m}m")
    if s:
        parts.append(f"{s}s")
    return " ".join(parts) if len(parts) > 1 else f"{h}h"


def _last_run_time_ui(last_run: Dict[str, Any]) -> Tuple[str, str]:
    """Return (visible label, tooltip title) for workflow last-run finished time + duration."""
    iso = str(last_run.get("finished_at_utc") or "").strip()
    dur_h = _format_duration_human(last_run.get("duration_seconds"))
    dt = _parse_utc_iso(iso)
    if not dt:
        line = f"{iso} · {dur_h}" if iso else dur_h
        return (line or "—", line or "—")
    now = datetime.now(timezone.utc)
    d = dt.date()
    today = now.date()
    yesterday = today - timedelta(days=1)
    if d == today:
        day = "Today"
    elif d == yesterday:
        day = "Yesterday"
    else:
        day = f"{dt.strftime('%b')} {dt.day}, {dt.year}"
    clock = dt.strftime("%H:%M")
    label = f"{day} · {clock} · {dur_h}"
    title = f"Finished {iso} (UTC) · duration {dur_h}"
    return (label, title)


_WORKFLOW_THREADS_LOCK = threading.Lock()
_WORKFLOW_THREADS: dict[str, threading.Thread] = {}
_DAILY_POSTBACK_THREADS_LOCK = threading.Lock()
_DAILY_POSTBACK_THREADS: dict[str, threading.Thread] = {}
PUBLISHERS_DB_PATH = ROOT_DIR / "runtime" / "publishers.db"
SK_TOOLS_SPREADSHEET_ID = "176wSQDDz9D1APmAXiYPeECwMqCQm3mvMBwgj8MKqmgk"

# Small in-memory cache to reduce repeated DSP API calls between page loads.
CACHE_TTL_SECONDS = 180
_CACHE: dict[str, dict[str, Any]] = {}


def _cache_get(key: str) -> Any | None:
    item = _CACHE.get(key)
    if not item:
        return None
    if time.time() - float(item.get("ts", 0)) > CACHE_TTL_SECONDS:
        _CACHE.pop(key, None)
        return None
    return item.get("value")


def _cache_set(key: str, value: Any) -> None:
    _CACHE[key] = {"ts": time.time(), "value": value}


def _cache_clear(prefix: str | None = None) -> None:
    if prefix is None:
        _CACHE.clear()
        return
    for k in list(_CACHE.keys()):
        if k.startswith(prefix):
            _CACHE.pop(k, None)


@app.template_filter("workflow_run_time")
def _workflow_run_time_filter(run: Any) -> Any:
    """Pretty last-run time + duration for dashboard cards (native ``title`` tooltip)."""
    from markupsafe import Markup, escape

    if not isinstance(run, dict) or not run:
        return Markup("")
    if not str(run.get("finished_at_utc") or "").strip() and run.get("duration_seconds") is None:
        return Markup("")
    lbl, ttl = _last_run_time_ui(run)
    return Markup(
        f'<span class="run-finished muted" title="{escape(ttl)}">{escape(lbl)}</span>'
    )


def _get_google_sheets_service():
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds_path = ROOT_DIR / "credentials.json"
    if not creds_path.exists():
        raise FileNotFoundError(f"credentials.json not found at {creds_path}")
    creds = service_account.Credentials.from_service_account_file(str(creds_path))
    return build("sheets", "v4", credentials=creds).spreadsheets()


def _sheet_tabs(spreadsheet_id: str) -> list[str]:
    cache_key = f"sheets:tabs:{spreadsheet_id}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return list(cached)
    service = _get_google_sheets_service()
    meta = service.get(spreadsheetId=spreadsheet_id, fields="sheets(properties(title))").execute()
    tabs = [s.get("properties", {}).get("title", "") for s in meta.get("sheets", [])]
    tabs = [t for t in tabs if t]
    _cache_set(cache_key, list(tabs))
    return tabs


def _sheet_values(spreadsheet_id: str, tab: str, limit_rows: int = 120) -> list[list[str]]:
    cache_key = f"sheets:values:{spreadsheet_id}:{tab}:{limit_rows}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return list(cached)
    service = _get_google_sheets_service()
    quoted = tab.replace("'", "''")
    rng = f"'{quoted}'!A1:ZZ{max(limit_rows, 1)}"
    result = service.values().get(spreadsheetId=spreadsheet_id, range=rng).execute()
    values = result.get("values") or []
    _cache_set(cache_key, list(values))
    return values


def _norm_brand_key(value: str) -> str:
    s = (value or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s or "unknown"


def _publishers_db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(PUBLISHERS_DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def _publishers_db_init() -> None:
    conn = _publishers_db_conn()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS campaigns (
                platform TEXT NOT NULL,
                campaign_id TEXT NOT NULL,
                campaign_name TEXT,
                brand_key TEXT NOT NULL,
                brand_display TEXT,
                merchant_name TEXT,
                geo TEXT,
                prefix TEXT,
                status TEXT,
                is_active INTEGER NOT NULL DEFAULT 0,
                reviewstatus TEXT,
                alias TEXT,
                source_payload TEXT,
                refreshed_at_utc TEXT,
                PRIMARY KEY (platform, campaign_id)
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def _publishers_rebuild_snapshot() -> dict[str, int]:
    _publishers_db_init()
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows: list[dict[str, Any]] = []

    # SK campaigns
    sk_campaigns = _sk_list_campaigns(only_active=False)
    for camp in sk_campaigns:
        adv = camp.get("advertiser") if isinstance(camp, dict) else None
        adv_name = str(adv.get("name") if isinstance(adv, dict) else "").strip()
        parsed = _extract_brand_geo_prefix(adv_name)
        brand = parsed[0] if parsed else adv_name or "unknown"
        geo = parsed[1] if parsed else ""
        prefix = parsed[2] if parsed else ""
        is_active = bool(camp.get("active"))
        rows.append(
            {
                "platform": "sk",
                "campaign_id": str(camp.get("id") or ""),
                "campaign_name": str(camp.get("name") or ""),
                "brand_key": _norm_brand_key(brand),
                "brand_display": brand,
                "merchant_name": "",
                "geo": geo,
                "prefix": prefix,
                "status": "active" if is_active else "inactive",
                "is_active": 1 if is_active else 0,
                "reviewstatus": "",
                "alias": _extract_alias_from_tracking_url(str(camp.get("trackingUrl") or "")),
                "source_payload": "",
                "refreshed_at_utc": now_utc,
            }
        )

    # EC campaigns
    ec_campaigns = _ec_get_campaigns()
    ec_merchants = _ec_get_merchants_map()
    for camp in ec_campaigns:
        brand = _ec_extract_brand(camp, ec_merchants)
        mid = str(camp.get("mid") or "").strip()
        merchant_name = ec_merchants.get(mid, "")
        name = str(camp.get("name") or "")
        parsed = _extract_brand_geo_prefix(name)
        geo = parsed[1] if parsed else str(camp.get("geo") or "")
        prefix = parsed[2] if parsed else ""
        status = str(camp.get("status") or "")
        rows.append(
            {
                "platform": "ec",
                "campaign_id": str(camp.get("id") or ""),
                "campaign_name": name,
                "brand_key": _norm_brand_key(brand),
                "brand_display": brand,
                "merchant_name": merchant_name,
                "geo": geo,
                "prefix": prefix,
                "status": status,
                "is_active": 1 if status.lower() == "active" else 0,
                "reviewstatus": str(camp.get("reviewstatus") or ""),
                "alias": _extract_alias_from_tracking_url(str(camp.get("url") or "")),
                "source_payload": "",
                "refreshed_at_utc": now_utc,
            }
        )

    conn = _publishers_db_conn()
    try:
        conn.execute("DELETE FROM campaigns")
        conn.executemany(
            """
            INSERT INTO campaigns (
                platform, campaign_id, campaign_name, brand_key, brand_display, merchant_name,
                geo, prefix, status, is_active, reviewstatus, alias, source_payload, refreshed_at_utc
            ) VALUES (
                :platform, :campaign_id, :campaign_name, :brand_key, :brand_display, :merchant_name,
                :geo, :prefix, :status, :is_active, :reviewstatus, :alias, :source_payload, :refreshed_at_utc
            )
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()

    return {"total": len(rows), "sk": len(sk_campaigns), "ec": len(ec_campaigns)}


def _publishers_brand_rows() -> list[dict[str, Any]]:
    _publishers_db_init()
    conn = _publishers_db_conn()
    try:
        q = """
        SELECT
            brand_key,
            MAX(brand_display) AS brand_display,
            COUNT(*) AS campaigns_total,
            SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_total,
            SUM(CASE WHEN platform = 'sk' THEN 1 ELSE 0 END) AS sk_total,
            SUM(CASE WHEN platform = 'ec' THEN 1 ELSE 0 END) AS ec_total,
            SUM(CASE WHEN platform = 'sk' AND is_active = 1 THEN 1 ELSE 0 END) AS sk_active,
            SUM(CASE WHEN platform = 'ec' AND is_active = 1 THEN 1 ELSE 0 END) AS ec_active
        FROM campaigns
        GROUP BY brand_key
        ORDER BY brand_display COLLATE NOCASE
        """
        out = []
        for r in conn.execute(q).fetchall():
            out.append(dict(r))
        return out
    finally:
        conn.close()


def _publishers_campaigns_for_brand(brand_key: str) -> list[dict[str, Any]]:
    _publishers_db_init()
    conn = _publishers_db_conn()
    try:
        rows = conn.execute(
            """
            SELECT platform, campaign_id, campaign_name, brand_display, merchant_name, geo, prefix, status,
                   is_active, reviewstatus, alias, refreshed_at_utc
            FROM campaigns
            WHERE brand_key = ?
            ORDER BY is_active DESC, platform ASC, campaign_name COLLATE NOCASE ASC
            """,
            (brand_key,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _ui_auth_enabled() -> bool:
    # Default: enabled if credentials are set, otherwise disabled (so health checks still work).
    enabled_raw = (os.getenv("UI_AUTH_ENABLED") or "").strip().lower()
    if enabled_raw in ("0", "false", "no"):
        return False
    return True


def _ui_expected_user_pass() -> tuple[str, str]:
    return (os.getenv("UI_BASIC_USER") or "").strip(), (os.getenv("UI_BASIC_PASS") or "").strip()


def _ui_is_authed() -> bool:
    if not _ui_auth_enabled():
        return True

    expected_user, expected_pass = _ui_expected_user_pass()
    if not expected_user or not expected_pass:
        # If not configured, fail closed.
        return False

    auth = request.authorization
    if not auth or not auth.username or not auth.password:
        return False

    return hmac.compare_digest(auth.username, expected_user) and hmac.compare_digest(auth.password, expected_pass)


@app.before_request
def ui_require_auth():
    # Keep liveness endpoint open for monitoring/orchestrator.
    if request.path == "/health":
        return None
    if not _ui_is_authed():
        return (
            jsonify({"error": "Unauthorized"}),
            401,
            {"WWW-Authenticate": 'Basic realm="KLblend UI"'},
        )

WORKFLOWS: Dict[str, Dict[str, Any]] = {
    "daily": {
        "title": "Nipuhim-Keitaro workflow",
        "script": "run_daily_workflow.py",
        "description": "Full daily pipeline: feeds, reports, offers, and Keitaro sync.",
        "group": "daily-automations",
        "args_hint": "Optional args, e.g. --date 2026-03-08 --skip-keitaro (country/blend sync use controls below)",
        "args_templates": [
            {"label": "Default (no args)", "value": ""},
            {"label": "Skip Keitaro", "value": "--skip-keitaro"},
            {"label": "Feed1 traffic only", "value": "--feed1-traffic-only"},
            {"label": "Skip Blend sync to Keitaro", "value": "--skip-blend-sync"},
            {"label": "UK+FR offers rerun (merge geos)", "value": "--geo uk,fr"},
            {"label": "Merchant override (example)", "value": "--geo uk --merchant-override 1:uk=15248713"},
            {"label": "Platform picks next best (example)", "value": "--geo uk --merchant-auto-override 1:uk"},
            {"label": "Offers + Keitaro only (fast)", "value": "--offers-and-keitaro-only --geo uk"},
            {"label": "Date + Skip Keitaro (example)", "value": "--date 2026-03-08 --skip-keitaro"},
            {"label": "Date only (example)", "value": "--date 2026-03-08"},
        ],
    },
    "keitaro-sync": {
        "title": "Keitaro sync",
        "script": "run_keitaro_sync.py",
        "description": "Sync offers from sheets to Keitaro only.",
        "group": "daily-automations",
        "args_hint": "Optional args, e.g. --date 2026-03-08",
        "args_templates": [
            {"label": "Default (today)", "value": ""},
            {"label": "Date only (example)", "value": "--date 2026-03-08"},
        ],
    },
    "blend": {
        "title": "Blend Workflow",
        "script": "run_blend_workflow.py",
        "description": (
            "Full Blend: sync Keitaro from the Blend sheet, then refresh potential sheets (Kelkoo, Adexa, Yadore; "
            "use feed dropdown). Column ``feed`` on the Blend sheet should match the source (``kelkoo1``/``kelkoo2``/"
            "``adexa``/``yadore``). Kelkoo rows use feed API keys for prune checks; other feeds use direct offer URLs."
        ),
        "group": "daily-automations",
        "args_hint": "Optional args, e.g. --geo fr --skip-potential (feed: use dropdown)",
        "args_templates": [
            {"label": "Default (extra args empty)", "value": ""},
            {"label": "Geo only (example)", "value": "--geo fr"},
            {"label": "Geo + Skip potential (example)", "value": "--geo fr --skip-potential"},
            {"label": "Sheet → Keitaro only (after manual caps)", "value": "--skip-potential"},
            {"label": "Only potential (no sync)", "value": "--only-potential"},
            {"label": "Range (example)", "value": "--start 2026-03-01 --end 2026-03-10"},
            {"label": "Only monetized merchants (example)", "value": "--only-monetized"},
        ],
    },
    "blend-keitaro-sync": {
        "title": "Blend sheet → Keitaro",
        "script": "blend_sync_from_sheet.py",
        "description": (
            "Runs only ``blend_sync_from_sheet``: prune bad auto=v rows, rebuild Blend campaign offers/flows "
            "from the Blend tab (e.g. after editing clickCap). Does not touch potentialKelkoo sheets."
        ),
        "group": "daily-automations",
        "args_hint": "Optional: --geo fr",
        "args_templates": [
            {"label": "All geos", "value": ""},
            {"label": "Geo fr", "value": "--geo fr"},
            {"label": "Geo it", "value": "--geo it"},
            {"label": "Geo uk", "value": "--geo uk"},
        ],
    },
    "monetization-check": {
        "title": "Monetization Check",
        "script": "monetization_check.py",
        "description": "Check source URLs against Kelkoo/Yadore and write Matches sheet.",
        "group": "match-making",
        "args_hint": "Optional args, e.g. --max-rows 20",
        "args_templates": [
            {"label": "Default (no limit)", "value": ""},
            {"label": "Max rows 20", "value": "--max-rows 20"},
            {"label": "Max rows 50", "value": "--max-rows 50"},
        ],
    },
    "blend-stop-closed": {
        "title": "Blend Stop Closed",
        "script": "blend_stop_closed_merchants.py",
        "description": "During-day: set non-monetized auto=v Blend offers share=0 (Kelkoo1/2 only).",
        "group": "daily-automations",
        "args_hint": "Optional args, e.g. --geo it",
        "args_templates": [
            {"label": "All geos", "value": ""},
            {"label": "Geo it", "value": "--geo it"},
            {"label": "Geo uk", "value": "--geo uk"},
            {"label": "Geo cz", "value": "--geo cz"},
        ],
    },
}


def _run_file_path(workflow_key: str) -> Path:
    return RUNS_DIR / f"{workflow_key}.json"


def _load_last_run(workflow_key: str) -> Dict[str, Any]:
    p = _run_file_path(workflow_key)
    if not p.exists():
        return {}
    try:
        import json
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_last_run(workflow_key: str, data: Dict[str, Any]) -> None:
    import json
    p = _run_file_path(workflow_key)
    p.write_text(json.dumps(data, ensure_ascii=True, indent=2), encoding="utf-8")


def _run_workflow(workflow_key: str, extra_args: str = "") -> Dict[str, Any]:
    wf = WORKFLOWS[workflow_key]
    script = ROOT_DIR / wf["script"]
    args = shlex.split(extra_args.strip(), posix=False) if extra_args.strip() else []
    cmd = [sys.executable, str(script)] + args

    started = time.time()
    started_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    proc = subprocess.run(
        cmd,
        cwd=str(ROOT_DIR),
        capture_output=True,
        text=True,
    )
    finished = time.time()
    finished_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    output = (proc.stdout or "") + ("\n" if proc.stdout and proc.stderr else "") + (proc.stderr or "")
    output = output.strip()

    result = {
        "workflow_key": workflow_key,
        "workflow_title": wf["title"],
        "status": "success" if proc.returncode == 0 else "failed",
        "exit_code": proc.returncode,
        "started_at_utc": started_iso,
        "finished_at_utc": finished_iso,
        "duration_seconds": round(finished - started, 2),
        "command": cmd,
        "args": args,
        "log": output[-20000:],  # keep last ~20k chars
    }
    _save_last_run(workflow_key, result)
    return result


def _run_workflow_in_background(workflow_key: str, extra_args: str = "") -> Dict[str, Any]:
    """
    Start workflow process in background and return immediately.
    Prevents gateway/proxy timeouts for long workflows (daily/blend/etc.).
    """
    wf = WORKFLOWS[workflow_key]
    script = ROOT_DIR / wf["script"]
    args = shlex.split(extra_args.strip(), posix=False) if extra_args.strip() else []
    cmd = [sys.executable, str(script)] + args

    with _WORKFLOW_THREADS_LOCK:
        existing = _WORKFLOW_THREADS.get(workflow_key)
        if existing and existing.is_alive():
            last = _load_last_run(workflow_key)
            if last:
                return last

        started = time.time()
        started_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        proc = subprocess.Popen(
            cmd,
            cwd=str(ROOT_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        running_result = {
            "workflow_key": workflow_key,
            "workflow_title": wf["title"],
            "status": "running",
            "exit_code": None,
            "started_at_utc": started_iso,
            "finished_at_utc": "",
            "duration_seconds": 0,
            "command": cmd,
            "args": args,
            "pid": proc.pid,
            "log": "Workflow started in background. Refresh to see final logs.",
        }
        _save_last_run(workflow_key, running_result)

        def _wait_and_store() -> None:
            try:
                out, err = proc.communicate()
                finished = time.time()
                finished_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                output = (out or "") + ("\n" if out and err else "") + (err or "")
                output = output.strip()
                result = {
                    "workflow_key": workflow_key,
                    "workflow_title": wf["title"],
                    "status": "success" if proc.returncode == 0 else "failed",
                    "exit_code": proc.returncode,
                    "started_at_utc": started_iso,
                    "finished_at_utc": finished_iso,
                    "duration_seconds": round(finished - started, 2),
                    "command": cmd,
                    "args": args,
                    "pid": proc.pid,
                    "log": output[-20000:],
                }
                _save_last_run(workflow_key, result)
            except Exception as e:
                failed = {
                    "workflow_key": workflow_key,
                    "workflow_title": wf["title"],
                    "status": "failed",
                    "exit_code": -1,
                    "started_at_utc": started_iso,
                    "finished_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "duration_seconds": 0,
                    "command": cmd,
                    "args": args,
                    "pid": proc.pid,
                    "log": f"Background workflow runner error: {e}",
                }
                _save_last_run(workflow_key, failed)
            finally:
                with _WORKFLOW_THREADS_LOCK:
                    _WORKFLOW_THREADS.pop(workflow_key, None)

        t = threading.Thread(target=_wait_and_store, daemon=True, name=f"wf-{workflow_key}")
        _WORKFLOW_THREADS[workflow_key] = t
        t.start()
        return running_result


def _run_daily_postbacks_feed_in_background(
    *,
    feed_key: str,
    report_date: str,
    only_geo: str | None,
    dry_run: bool,
    no_resume: bool,
    reset_sources: list[str] | None,
) -> Dict[str, Any]:
    """
    Start one daily-postbacks feed run in a background thread.
    Prevents 504s when a feed (especially Kelkoo all-geos) runs for minutes.
    """
    task_key = f"{feed_key}:{report_date}:{only_geo or 'all'}:{'dry' if dry_run else 'apply'}"
    with _DAILY_POSTBACK_THREADS_LOCK:
        existing = _DAILY_POSTBACK_THREADS.get(feed_key)
        if existing and existing.is_alive():
            return {"status": "already_running", "task_key": task_key}

        def _worker() -> None:
            try:
                run_daily_conversion_postbacks_batch(
                    report_date=report_date,
                    only=feed_key,
                    only_geo=only_geo,
                    dry_run=dry_run,
                    no_resume=no_resume,
                    reset_sources=reset_sources,
                )
            except Exception:
                logger.exception(
                    "daily postbacks background run failed feed=%s date=%s geo=%s",
                    feed_key,
                    report_date,
                    only_geo or "",
                )
            finally:
                with _DAILY_POSTBACK_THREADS_LOCK:
                    _DAILY_POSTBACK_THREADS.pop(feed_key, None)

        t = threading.Thread(target=_worker, daemon=True, name=f"postbacks-{feed_key}")
        _DAILY_POSTBACK_THREADS[feed_key] = t
        t.start()
        return {"status": "started", "task_key": task_key}


def _sk_headers() -> dict[str, str]:
    from config import SOURCEKNOWLEDGE_API_KEY
    return {
        "accept": "application/json",
        "X-API-KEY": SOURCEKNOWLEDGE_API_KEY,
    }


def _sk_request(method: str, url: str, *, json_body: dict[str, Any] | None = None) -> Any:
    import requests

    while True:
        try:
            r = requests.request(method, url, headers=_sk_headers(), json=json_body, timeout=60)
        except requests.RequestException:
            time.sleep(60)
            continue
        if r.status_code == 429:
            time.sleep(60)
            continue
        return r


def _sk_list_campaigns(only_active: bool = False) -> list[dict[str, Any]]:
    cache_key = f"sk:campaigns:{'active' if only_active else 'all'}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return list(cached)

    items_all: list[dict[str, Any]] = []
    page = 1
    while True:
        r = _sk_request("GET", f"https://api.sourceknowledge.com/affiliate/v2/campaigns?page={page}")
        if r.status_code != 200:
            break
        payload = r.json()
        items = payload.get("items", [])
        if not isinstance(items, list) or not items:
            break
        for it in items:
            if not isinstance(it, dict):
                continue
            if only_active and not bool(it.get("active")):
                continue
            items_all.append(it)
        page += 1
    _cache_set(cache_key, list(items_all))
    return items_all


def _extract_brand_geo_prefix(advertiser_name: str) -> tuple[str, str, str] | None:
    parts = [p.strip() for p in (advertiser_name or "").split("-")]
    if len(parts) < 3:
        return None
    prefix = parts[-1]
    geo = parts[-2].lower()
    brand = "-".join(parts[:-2]).strip()
    if not brand:
        return None
    return brand, geo, prefix


def _extract_alias_from_tracking_url(url: str) -> str:
    m = re.search(r"https://trck\.shopli\.city/([^?&/]+)", url or "")
    return m.group(1) if m else ""


def _ec_authtoken() -> str:
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    return hashlib.md5((ts + (EC_SECRET_KEY or "")).encode("utf-8")).hexdigest().upper()


def _ec_auth_params(extra: dict[str, Any] | None = None) -> dict[str, Any]:
    base = {
        "advertiserkey": EC_ADVERTISER_KEY,
        "authkey": EC_AUTH_KEY,
        "authtoken": _ec_authtoken(),
    }
    if extra:
        base.update(extra)
    return base


def _ec_request(method: str, url: str, *, params: dict[str, Any], json_body: dict[str, Any] | None = None) -> Any:
    import requests

    while True:
        try:
            r = requests.request(
                method,
                url,
                params=params,
                json=json_body,
                headers={"content-type": "application/json"},
                timeout=60,
            )
        except requests.RequestException:
            time.sleep(30)
            continue
        if r.status_code in (429, 503):
            time.sleep(30)
            continue
        return r


def _ec_get_campaigns() -> list[dict[str, Any]]:
    cache_key = "ec:campaigns:all"
    cached = _cache_get(cache_key)
    if cached is not None:
        return list(cached)

    if not EC_ADVERTISER_KEY or not EC_AUTH_KEY or not EC_SECRET_KEY:
        return []
    url = "https://advertiser.ecomnia.com/get-advertiser-campaigns"
    r = _ec_request("GET", url, params=_ec_auth_params())
    if r.status_code != 200:
        return []
    data = r.json()
    if isinstance(data, dict) and isinstance(data.get("campaigns"), list):
        out = data["campaigns"]
        _cache_set(cache_key, list(out))
        return out
    if isinstance(data, list):
        _cache_set(cache_key, list(data))
        return data
    return []


def _ec_get_merchants_map() -> dict[str, str]:
    cache_key = "ec:merchants"
    cached = _cache_get(cache_key)
    if cached is not None:
        return dict(cached)

    if not EC_ADVERTISER_KEY or not EC_AUTH_KEY or not EC_SECRET_KEY:
        return {}
    url = "https://advertiser.ecomnia.com/get-merchants"
    r = _ec_request("GET", url, params=_ec_auth_params())
    if r.status_code != 200:
        return {}
    data = r.json()
    merchants = data.get("merchants", []) if isinstance(data, dict) else []
    out: dict[str, str] = {}
    if isinstance(merchants, list):
        for m in merchants:
            if isinstance(m, dict):
                mid = str(m.get("mid") or "").strip()
                mname = str(m.get("mname") or "").strip()
                if mid and mname:
                    out[mid] = mname
    _cache_set(cache_key, dict(out))
    return out


def _ec_extract_brand(camp: dict[str, Any], merchants_by_mid: dict[str, str]) -> str:
    brand = str(camp.get("brand") or "").strip()
    if brand:
        return brand
    name = str(camp.get("name") or "").strip()
    parsed = _extract_brand_geo_prefix(name)
    if parsed:
        return parsed[0]
    url = str(camp.get("url") or "")
    m = re.search(r"(?:[?&])brand=([^&]+)", url)
    if m:
        return unquote(m.group(1)).strip() or "unknown"
    mid = str(camp.get("mid") or "").strip()
    if mid and mid in merchants_by_mid:
        return merchants_by_mid[mid]
    return "unknown"


@app.route("/", methods=["GET"])
def ui_home():
    group_titles: Dict[str, str] = {
        "daily-automations": "Daily Automations",
        "match-making": "Match Making",
    }
    group_desc: Dict[str, str] = {
        "daily-automations": "Run and monitor core production automations.",
        "match-making": "Monetization checks for manual entries and sheets.",
    }

    groups: Dict[str, Dict[str, Any]] = {}
    for key, wf in WORKFLOWS.items():
        group_key = wf.get("group") or "other"
        groups.setdefault(group_key, {"title": group_titles.get(group_key, group_key), "description": group_desc.get(group_key, ""), "items": []})
        groups[group_key]["items"].append({
            "key": key,
            "title": wf["title"],
            "description": wf["description"],
            "last_run": _load_last_run(key),
        })

    # Keep a stable order inside groups (daily -> keitaro-sync, then others by insertion order)
    ordered_group_keys = ["daily-automations", "match-making"]
    out_groups: list[dict[str, Any]] = []
    for gk in ordered_group_keys:
        if gk in groups:
            out_groups.append(groups[gk])
    for gk, v in groups.items():
        if gk not in ordered_group_keys:
            out_groups.append(v)

    return render_template(
        "index.html",
        groups=out_groups,
        overview_snapshot_tz=OVERVIEW_SNAPSHOT_TZ,
        overview_snapshot_hour=OVERVIEW_SNAPSHOT_HOUR,
    )


@app.route("/automations", methods=["GET"])
def ui_automations():
    """AutoServer-derived automations: scheduler status, manual triggers, run log."""
    return render_template("automations.html")


@app.route("/help", methods=["GET"])
def ui_help():
    """Help Center: flags, examples, and caveats moved out of tool pages."""
    return render_template("help.html", workflows=WORKFLOWS)


@app.route("/github", methods=["GET"])
def ui_github_connect():
    repo_root = ROOT_DIR
    git_exists = (repo_root / ".git").exists()
    remotes = ""
    gh_status = ""
    try:
        if git_exists:
            out = subprocess.run(
                ["git", "remote", "-v"],
                cwd=str(repo_root),
                capture_output=True,
                text=True,
            )
            remotes = (out.stdout or out.stderr or "").strip()
    except Exception as e:
        remotes = f"Could not read git remotes: {e}"
    try:
        out = subprocess.run(
            ["gh", "auth", "status"],
            cwd=str(repo_root),
            capture_output=True,
            text=True,
        )
        gh_status = ((out.stdout or "") + ("\n" if out.stderr else "") + (out.stderr or "")).strip()
    except Exception as e:
        gh_status = f"Could not run gh auth status: {e}"
    return render_template(
        "github_connect.html",
        git_exists=git_exists,
        remotes=remotes,
        gh_status=gh_status,
    )


def _normalize_geo_list_for_check(geo_raw: str) -> list[str]:
    geo_raw = (geo_raw or "").strip().lower()
    if not geo_raw or geo_raw == "all":
        return []
    return [geo_raw[:2]]


@app.route("/matchmaking", methods=["GET"])
def ui_matchmaking_hub():
    """Entry point: manual domain check + sheet bulk monetization from one console."""
    return render_template("matchmaking_hub.html")


@app.route("/matchmaking/manual", methods=["GET", "POST"])
def ui_matchmaking_manual():
    result_rows: list[dict[str, Any]] = []
    submitted = {"domain": "", "geo": "all"}
    error = ""
    if request.method == "POST":
        domain = (request.form.get("domain") or "").strip()
        geo = (request.form.get("geo") or "all").strip().lower()
        submitted = {"domain": domain, "geo": geo}
        if not domain:
            error = "Domain/URL is required."
        else:
            if not domain.startswith("http://") and not domain.startswith("https://"):
                domain = "https://" + domain.lstrip("/")
            geos = _normalize_geo_list_for_check(geo)
            if not geos:
                geos = ["fi", "fr", "gr", "de", "hu", "id", "ie", "in", "it", "mx", "nl", "no", "nz", "pl", "pt", "ro", "se", "sk", "uk", "us", "at", "be", "ch", "cz"]
            for g in geos:
                k1 = kelkoo_merchant_link_check(domain, g, FEED1_API_KEY)
                k2 = kelkoo_merchant_link_check(domain, g, FEED2_API_KEY)
                try:
                    y_nc = yadore_deeplink(domain, g, is_couponing=False)
                    y_nc_found = bool(y_nc.get("found"))
                except YadoreClientError:
                    y_nc_found = False
                try:
                    y_c = yadore_deeplink(domain, g, is_couponing=True)
                    y_c_found = bool(y_c.get("found"))
                except YadoreClientError:
                    y_c_found = False
                try:
                    ax = adexa_links_check(domain, g)
                    adexa_found = bool(ax.get("found"))
                    adexa_note = str(ax.get("note") or "")
                except AdexaClientError as e:
                    adexa_found = False
                    adexa_note = str(e)[:120]
                result_rows.append(
                    {
                        "geo": g,
                        "kelkoo1_found": bool(k1.get("found")),
                        "kelkoo2_found": bool(k2.get("found")),
                        "yadore_non_coupon_found": y_nc_found,
                        "yadore_coupon_found": y_c_found,
                        "yadore_class": yadore_feed_class(y_nc_found, y_c_found),
                        "adexa_found": adexa_found,
                        "adexa_note": adexa_note,
                        "kelkoo1_cpc": str(k1.get("estimatedCpc", "")),
                        "kelkoo2_cpc": str(k2.get("estimatedCpc", "")),
                    }
                )
    return render_template(
        "matchmaking_manual.html",
        result_rows=result_rows,
        submitted=submitted,
        error=error,
    )


@app.route("/matchmaking/sheets", methods=["GET", "POST"])
def ui_matchmaking_sheets():
    run_result = None
    if request.method == "POST":
        max_rows = (request.form.get("max_rows") or "").strip()
        args = []
        if max_rows:
            args = ["--max-rows", max_rows]
        run_result = _run_workflow("monetization-check", " ".join(args))
    last_run = run_result or _load_last_run("monetization-check")
    return render_template("matchmaking_sheets.html", last_run=last_run)


@app.route("/sk", methods=["GET", "POST"])
def ui_sk():
    bulk_open_result = None
    if request.method == "POST":
        prefix = (request.form.get("prefix") or "").strip()
        alias = (request.form.get("alias") or "").strip()
        tab = (request.form.get("tab") or "bulkSK-KLFIX").strip()
        mode = (request.form.get("mode") or "dry-run").strip().lower()
        apply = mode == "apply"

        if prefix and alias and tab:
            script = ROOT_DIR / "sk_bulk_open_from_sheet.py"
            args = [
                sys.executable,
                str(script),
                "--prefix",
                prefix,
                "--alias",
                alias,
                "--tab",
                tab,
                "--apply" if apply else "--dry-run",
            ]
            proc = subprocess.run(args, cwd=str(ROOT_DIR), capture_output=True, text=True)
            output = (proc.stdout or "") + ("\n" if proc.stdout and proc.stderr else "") + (proc.stderr or "")
            bulk_open_result = {
                "status": "success" if proc.returncode == 0 else "failed",
                "exit_code": proc.returncode,
                "log": output[-20000:],
                "prefix": prefix,
                "alias": alias,
                "tab": tab,
                "mode": "apply" if apply else "dry-run",
            }
            _cache_clear("sk:")

    return render_template("sk.html", bulk_open_result=bulk_open_result)


@app.route("/kelkoo/late-sales", methods=["GET", "POST"])
def ui_kelkoo_late_sales():
    late_sales_result: dict[str, Any] | None = None
    if request.method == "POST":
        mode = (request.form.get("mode") or "dry-run").strip().lower()
        apply = mode == "apply"
        as_of = (request.form.get("as_of") or "").strip()
        creds = ROOT_DIR / "credentials.json"
        try:
            late_sales_result = run_late_sales_flow(
                credentials_path=creds,
                spreadsheet_id=KELKOO_LATE_SALES_SPREADSHEET_ID,
                postback_base=LATE_SALES_POSTBACK_BASE,
                as_of_str=as_of,
                apply=apply,
            )
        except Exception as e:
            logger.exception("Kelkoo late-sales flow")
            late_sales_result = {
                "ok": False,
                "error": str(e),
                "mode": "apply" if apply else "dry-run",
            }
    return render_template("late_sales.html", late_sales_result=late_sales_result)


DAILY_POSTBACK_FEED_KEYS = frozenset({"kelkoo1", "kelkoo2", "adexa", "yadore"})


@app.route("/kelkoo/daily-postbacks", methods=["GET"])
def ui_kelkoo_daily_postbacks_hub():
    """Tile dashboard: last run + resume snapshot per feed; click through for log + run form."""
    state_path = Path(DAILY_CONVERSION_POSTBACK_STATE_PATH)
    feeds = build_dashboard_cards(state_path)
    return render_template(
        "daily_postbacks_dashboard.html",
        feeds=feeds,
        default_report_date=default_report_date_str(),
        state_path_display=str(state_path),
    )


@app.route("/kelkoo/daily-postbacks/<feed_key>", methods=["GET", "POST"])
def ui_kelkoo_daily_postbacks_feed(feed_key: str):
    """
    Per-feed detail: last run JSON, geo / flat resume table, and run controls.
    Full Kelkoo all-geos runs can take many minutes — raise proxy/worker timeouts if needed.
    """
    fk = (feed_key or "").strip().lower()
    if fk not in DAILY_POSTBACK_FEED_KEYS:
        abort(404)

    state_path = Path(DAILY_CONVERSION_POSTBACK_STATE_PATH)
    run_result: dict[str, Any] | None = None
    focus_date: str | None = (request.args.get("date") or "").strip() or None

    if request.method == "POST":
        report_date = (request.form.get("report_date") or "").strip() or default_report_date_str()
        focus_date = report_date
        only_geo_raw = (request.form.get("only_geo") or "").strip().lower()
        only_geo = only_geo_raw[:2] if len(only_geo_raw) >= 2 else None
        if fk not in ("kelkoo1", "kelkoo2"):
            only_geo = None
        mode = (request.form.get("mode") or "dry-run").strip().lower()
        dry_run = mode != "apply"
        no_resume = (request.form.get("no_resume") or "").strip().lower() in ("1", "on", "yes", "true")
        reset_sources = (
            [fk]
            if (request.form.get("reset_state") or "").strip().lower() in ("1", "on", "yes", "true")
            else None
        )
        run_in_background = (not dry_run) and (fk in ("kelkoo1", "kelkoo2")) and (only_geo is None)
        if run_in_background:
            bg = _run_daily_postbacks_feed_in_background(
                feed_key=fk,
                report_date=report_date,
                only_geo=only_geo,
                dry_run=dry_run,
                no_resume=no_resume,
                reset_sources=reset_sources,
            )
            if bg.get("status") == "already_running":
                flash(f"{fk} postbacks are already running in background.", "error")
            else:
                flash(
                    f"Started {fk} apply run in background for {report_date}. "
                    f"Refresh this page to track resume snapshot and last-run log.",
                    "success",
                )
            return redirect(url_for("ui_kelkoo_daily_postbacks_feed", feed_key=fk, date=report_date))
        try:
            run_result = run_daily_conversion_postbacks_batch(
                report_date=report_date,
                only=fk,
                only_geo=only_geo,
                dry_run=dry_run,
                no_resume=no_resume,
                reset_sources=reset_sources,
            )
        except Exception as e:
            logger.exception("Kelkoo daily postbacks feed %s", fk)
            run_result = {
                "ok": False,
                "exit_code": 1,
                "error": str(e),
                "results": [],
                "report_date": report_date,
            }

    ctx = feed_detail_context(state_path, fk, focus_date)
    return render_template(
        "daily_postbacks_feed.html",
        run_result=run_result,
        default_report_date=default_report_date_str(),
        state_path_display=str(state_path),
        **ctx,
    )


@app.route("/sk/brands/<path:brand_name>", methods=["GET"])
def ui_sk_brand_details(brand_name: str):
    refresh = (request.args.get("refresh") or "").strip().lower() in ("1", "true", "yes")
    if refresh:
        _cache_clear("sk:")

    campaigns = _sk_list_campaigns(only_active=False)
    target = unquote(brand_name)
    rows: list[dict[str, Any]] = []
    for camp in campaigns:
        adv = camp.get("advertiser") if isinstance(camp, dict) else None
        adv_name = str(adv.get("name") if isinstance(adv, dict) else "").strip()
        parsed = _extract_brand_geo_prefix(adv_name)
        if parsed:
            brand, geo, prefix = parsed
        else:
            brand, geo, prefix = adv_name or "unknown", "", ""
        if brand.lower() != target.lower():
            continue
        rows.append(
            {
                "campaign_id": camp.get("id"),
                "campaign_name": camp.get("name") or "",
                "active": bool(camp.get("active")),
                "advertiser_id": adv.get("id") if isinstance(adv, dict) else "",
                "advertiser_name": adv_name,
                "geo": geo,
                "prefix": prefix,
                "alias": _extract_alias_from_tracking_url(str(camp.get("trackingUrl") or "")),
            }
        )
    rows.sort(key=lambda x: (not x["active"], str(x["campaign_name"]).lower()))
    return render_template("sk_brand.html", brand=target, campaigns=rows)


@app.route("/ec", methods=["GET", "POST"])
def ui_ec():
    bulk_open_result = None
    if request.method == "POST":
        prefix = (request.form.get("prefix") or "").strip()
        alias = (request.form.get("alias") or "").strip()
        tab = (request.form.get("tab") or "bulk").strip()
        mode = (request.form.get("mode") or "dry-run").strip().lower()
        apply = mode == "apply"

        if prefix and alias and tab:
            script = ROOT_DIR / "ec_bulk_open_from_sheet.py"
            args = [
                sys.executable,
                str(script),
                "--prefix",
                prefix,
                "--alias",
                alias,
                "--tab",
                tab,
                "--apply" if apply else "--dry-run",
            ]
            proc = subprocess.run(args, cwd=str(ROOT_DIR), capture_output=True, text=True)
            output = (proc.stdout or "") + ("\n" if proc.stdout and proc.stderr else "") + (proc.stderr or "")
            bulk_open_result = {
                "status": "success" if proc.returncode == 0 else "failed",
                "exit_code": proc.returncode,
                "log": output[-20000:],
                "prefix": prefix,
                "alias": alias,
                "tab": tab,
                "mode": "apply" if apply else "dry-run",
            }
            _cache_clear("ec:")

    return render_template("ec.html", bulk_open_result=bulk_open_result)


EC_CONSOLE_PAGES = frozenset(
    {
        "lists",
        "whitelist-derived",
        "sync-blacklists",
        "whitelist-check",
        "action-items",
    }
)

EC_CONSOLE_ACTION_TO_PAGE = {
    "refresh_lists": "lists",
    "refresh_whitelist_derived": "whitelist-derived",
    "sync_blacklists_dry": "sync-blacklists",
    "sync_blacklists_apply": "sync-blacklists",
    "whitelist_check": "whitelist-check",
    "whitelist_focus_source": "whitelist-check",
    "action_items": "action-items",
}


def _ecomnia_post_redirect_target(form) -> str:
    explicit = (form.get("page") or "").strip()
    if explicit in EC_CONSOLE_PAGES:
        return explicit
    action = (form.get("action") or "").strip().lower()
    return EC_CONSOLE_ACTION_TO_PAGE.get(action, "hub")


def _ecomnia_console_view_context() -> Dict[str, Any]:
    state = load_state()
    geo_map = dict(state.get("geo_map") or {})
    wl_rows = list(state.get("whitelist_campaign_source_rows") or [])
    wl_focus = state.get("whitelist_focus_audit") or {}
    if not isinstance(wl_focus, dict):
        wl_focus = {}
    action_block = state.get("action_items_block") or {}
    if not isinstance(action_block, dict):
        action_block = {}
    derived_wl = state.get("derived_whitelist") or {}
    if not isinstance(derived_wl, dict):
        derived_wl = {}
    runs = dict(state.get("runs") or {})
    copy_all = all_copy_paste_text(geo_map) if geo_map else ""
    copy_derived_wl = derived_whitelist_copy_paste(derived_wl) if derived_wl else ""
    ec_ok = bool(EC_ADVERTISER_KEY and EC_AUTH_KEY and EC_SECRET_KEY)
    sheet_ok = bool(EC_SHEETS_SPREADSHEET_ID)
    return {
        "ec_ok": ec_ok,
        "sheet_ok": sheet_ok,
        "spreadsheet_id": EC_SHEETS_SPREADSHEET_ID,
        "globa_tab": ECOMNIA_GLOBA_LIST_TAB,
        "geo_map": geo_map,
        "copy_all": copy_all,
        "copy_derived_wl": copy_derived_wl,
        "derived_wl": derived_wl,
        "wl_rows": wl_rows,
        "wl_focus_audit": wl_focus,
        "action_items": list(action_block.get("items") or []),
        "action_errors": list(action_block.get("errors") or []),
        "runs": runs,
        "default_yesterday": utc_yesterday_iso(),
    }


def _process_ecomnia_console_post(form) -> Tuple[Optional[str], Optional[str]]:
    """
    Run one console action from POST form. Returns (success_message, error_message); either may be None.
    """
    ec_ok = bool(EC_ADVERTISER_KEY and EC_AUTH_KEY and EC_SECRET_KEY)
    sheet_ok = bool(EC_SHEETS_SPREADSHEET_ID)
    action = (form.get("action") or "").strip().lower()
    flash_msg: Optional[str] = None
    flash_err: Optional[str] = None

    if not ec_ok:
        return None, "Set ADVERTISER_KEY, AUTH_KEY, SECRET_KEY in .env for Ecomnia API actions."

    try:
        if action == "refresh_lists":
            if not sheet_ok:
                return None, "Set EC_SHEETS_SPREADSHEET_ID to load geo lists from Google Sheets."
            vals = _sheet_values(EC_SHEETS_SPREADSHEET_ID, ECOMNIA_GLOBA_LIST_TAB, limit_rows=200)
            geo_map_new = geo_map_from_sheet_values(vals)
            update_cache(geo_map=geo_map_new)
            _cache_clear(f"sheets:values:{EC_SHEETS_SPREADSHEET_ID}:{ECOMNIA_GLOBA_LIST_TAB}")
            record_run("refresh_lists", True, {"geos": len(geo_map_new)})
            flash_msg = f"Loaded {len(geo_map_new)} geo rows from sheet tab {ECOMNIA_GLOBA_LIST_TAB!r}."

        elif action == "refresh_whitelist_derived":
            try:
                pot_days = int((form.get("wl_potential_days") or "30").strip() or "30")
            except ValueError:
                pot_days = 30
            pot_days = max(1, min(pot_days, 90))
            derived, campaigns = pull_derived_whitelist_with_campaigns(
                EC_ADVERTISER_KEY,
                EC_AUTH_KEY,
                EC_SECRET_KEY,
            )
            gm = dict(load_state().get("geo_map") or {})
            pot = compute_global_wl_zero_click_potential(
                campaigns,
                gm,
                EC_ADVERTISER_KEY,
                EC_AUTH_KEY,
                EC_SECRET_KEY,
                days=pot_days,
                skip_wl_campaigns=True,
            )
            derived["wl_potential"] = pot
            if not gm:
                derived["wl_potential_note"] = (
                    "Sheet geo lists not loaded — merged WL uses campaign API only. "
                    "Refresh Sheet lists on the lists page for sheet ∪ campaign merge."
                )
            by_src = pot.get("by_source") or {}
            for item in derived.get("global_whitelist") or []:
                if isinstance(item, dict):
                    s = str(item.get("source") or "")
                    item["potential_count"] = len(by_src.get(s) or [])
            update_cache(derived_whitelist=derived)
            record_run(
                "refresh_whitelist_derived",
                True,
                {
                    "campaigns": derived.get("campaigns_fetched"),
                    "global_wl_sources": len(derived.get("global_whitelist") or []),
                    "wl_potential_days": pot_days,
                    "wl_potential_errors": len(pot.get("errors") or []),
                },
            )
            _cache_clear("ec:")
            flash_msg = (
                f"Whitelist from API: {derived.get('campaigns_fetched', 0)} campaigns; "
                f"global WL (≥2 campaigns): {len(derived.get('global_whitelist') or [])} sources; "
                f"0-click potential computed ({pot_days}d window)."
            )
            if pot.get("errors"):
                flash_err = f"Potential pass had {len(pot['errors'])} campaign fetch errors (see run detail)."

        elif action == "sync_blacklists_dry":
            out = sync_geo_blacklists(
                EC_ADVERTISER_KEY,
                EC_AUTH_KEY,
                EC_SECRET_KEY,
                dry_run=True,
                skip_wl_campaigns=True,
            )
            record_run("sync_blacklists_dry", True, {**out, "dry_run": True})
            _cache_clear("ec:")
            flash_msg = (
                f"Dry-run: would touch {out.get('campaign_updates', 0)} campaigns; "
                f"global candidates {out.get('global_candidates_count', 0)}."
            )

        elif action == "sync_blacklists_apply":
            out = sync_geo_blacklists(
                EC_ADVERTISER_KEY,
                EC_AUTH_KEY,
                EC_SECRET_KEY,
                dry_run=False,
                skip_wl_campaigns=True,
            )
            record_run("sync_blacklists_apply", bool(out.get("ok")), out)
            _cache_clear("ec:")
            flash_msg = f"Sync blacklists: {out.get('campaign_updates', 0)} campaign updates."
            if out.get("errors"):
                flash_err = "; ".join(str(e) for e in out["errors"][:5])

        elif action == "whitelist_check":
            try:
                days = int((form.get("whitelist_days") or "30").strip() or "30")
            except ValueError:
                days = 30
            days = max(1, min(days, 90))
            try:
                lim = int((form.get("campaign_limit") or "0").strip() or "0")
            except ValueError:
                lim = 0
            gm = dict(load_state().get("geo_map") or {})
            flat, summaries = whitelist_check_flat_rows(
                EC_ADVERTISER_KEY,
                EC_AUTH_KEY,
                EC_SECRET_KEY,
                gm,
                days=days,
                skip_wl_campaigns=True,
                limit_campaigns=max(0, lim),
            )
            update_cache(whitelist_rows=flat)
            record_run(
                "whitelist_check",
                True,
                {"rows": len(flat), "summaries": len(summaries), "days": days},
            )
            flash_msg = f"Whitelist check: {len(flat)} campaign×source rows ({days}d)."

        elif action == "whitelist_focus_source":
            focus = (form.get("focus_source") or "").strip()
            if not focus:
                return None, "Enter a source id for the focus audit."
            try:
                f_days = int((form.get("focus_days") or "30").strip() or "30")
            except ValueError:
                f_days = 30
            f_days = max(1, min(f_days, 90))
            try:
                min_m = int((form.get("focus_min_campaigns") or "2").strip() or "2")
            except ValueError:
                min_m = 2
            min_m = max(1, min_m)
            try:
                lim = int((form.get("focus_campaign_limit") or "0").strip() or "0")
            except ValueError:
                lim = 0
            gm = dict(load_state().get("geo_map") or {})
            block = whitelist_focus_source_traffic_no_buy(
                EC_ADVERTISER_KEY,
                EC_AUTH_KEY,
                EC_SECRET_KEY,
                gm,
                focus,
                days=f_days,
                skip_wl_campaigns=True,
                limit_campaigns=max(0, lim),
                min_campaign_matches=min_m,
            )
            update_cache(whitelist_focus_audit=block)
            raw_n = int(block.get("raw_match_count") or 0)
            shown_n = int(block.get("shown_match_count") or 0)
            on_wl = int(block.get("campaigns_on_whitelist") or 0)
            record_run(
                "whitelist_focus_source",
                True,
                {
                    "source": focus,
                    "days": f_days,
                    "min_campaign_matches": min_m,
                    "raw_matches": raw_n,
                    "shown_matches": shown_n,
                    "on_whitelist_campaigns": on_wl,
                    "errors": len(block.get("errors") or []),
                },
            )
            if block.get("error") == "empty_source":
                return None, "Enter a source id for the focus audit."
            if shown_n:
                flash_msg = (
                    f"Source {focus!r}: {shown_n} campaign(s) with clicks & no conversions "
                    f"({f_days}d, min {min_m} campaigns). On merged WL in {on_wl} campaign(s)."
                )
            elif raw_n and raw_n < min_m:
                flash_msg = (
                    f"Source {focus!r}: only {raw_n} campaign(s) match (clicks, 0 conversions) — "
                    f"below minimum {min_m}; table left empty."
                )
            else:
                flash_msg = (
                    f"Source {focus!r}: no campaigns with merged WL + clicks + zero conversions "
                    f"({f_days}d, min {min_m}). Seen on WL in {on_wl} campaign(s)."
                )

        elif action == "action_items":
            y = (form.get("yesterday") or "").strip() or utc_yesterday_iso()
            try:
                lb = int((form.get("source_lookback_days") or "7").strip() or "7")
            except ValueError:
                lb = 7
            lb = max(1, min(lb, 30))
            block = exploration_action_items(
                EC_ADVERTISER_KEY,
                EC_AUTH_KEY,
                EC_SECRET_KEY,
                yesterday_ymd=y[:10],
                source_lookback_days=lb,
                skip_wl_campaigns=True,
            )
            update_cache(action_items_block=block)
            record_run(
                "action_items",
                True,
                {"items": len(block.get("items") or []), "yesterday": y[:10]},
            )
            flash_msg = f"Action items: {len(block.get('items') or [])} campaigns (yesterday {y[:10]})."

        else:
            return None, None

    except Exception as e:
        logger.exception("Ecomnia console %s", action)
        flash_err = str(e)
        record_run(action or "unknown", False, {"error": str(e)})

    return flash_msg, flash_err


@app.route("/ec/console", methods=["GET", "POST"])
def ui_ec_console_hub():
    """Tile hub: links to each Ecomnia console tool page. POST retained for legacy form actions."""
    if request.method == "POST":
        target = _ecomnia_post_redirect_target(request.form)
        flash_msg, flash_err = _process_ecomnia_console_post(request.form)
        if flash_msg:
            flash(flash_msg, "success")
        if flash_err:
            flash(flash_err, "error")
        if target not in EC_CONSOLE_PAGES:
            return redirect(url_for("ui_ec_console_hub"))
        return redirect(url_for("ui_ec_console_tool", page_slug=target))
    ctx = _ecomnia_console_view_context()
    return render_template("ecomnia_console_hub.html", active_page="hub", **ctx)


@app.route("/ec/console/do", methods=["POST"])
def ui_ec_console_do():
    """Single POST endpoint for all console forms; redirects back to the tool page with flash messages."""
    target = _ecomnia_post_redirect_target(request.form)
    flash_msg, flash_err = _process_ecomnia_console_post(request.form)
    if flash_msg:
        flash(flash_msg, "success")
    if flash_err:
        flash(flash_err, "error")
    if target not in EC_CONSOLE_PAGES:
        return redirect(url_for("ui_ec_console_hub"))
    return redirect(url_for("ui_ec_console_tool", page_slug=target))


@app.route("/ec/console/whitelist-potential-detail", methods=["GET", "POST"])
def ui_ec_console_whitelist_potential_detail():
    """
    Per global-WL source: campaigns on merged WL with 0 clicks in the last computed window;
    set ``cpcbysource[source]`` via Ecomnia update (dry-run or apply).
    """
    ec_ok = bool(EC_ADVERTISER_KEY and EC_AUTH_KEY and EC_SECRET_KEY)
    source = (request.args.get("source") or request.form.get("source") or "").strip()
    if not source:
        flash("Missing source.", "error")
        return redirect(url_for("ui_ec_console_tool", page_slug="whitelist-derived"))

    if request.method == "POST":
        if not ec_ok:
            flash("Set Ecomnia API keys in .env.", "error")
            return redirect(url_for("ui_ec_console_whitelist_potential_detail", source=source))
        raw_ids = request.form.getlist("campaign_id")
        try:
            new_cpc = float((request.form.get("new_cpc") or "").strip().replace(",", "."))
        except ValueError:
            flash("Invalid CPC value.", "error")
            return redirect(url_for("ui_ec_console_whitelist_potential_detail", source=source))
        if new_cpc <= 0:
            flash("CPC must be greater than zero.", "error")
            return redirect(url_for("ui_ec_console_whitelist_potential_detail", source=source))
        dry = (request.form.get("dry_run") or "").strip().lower() in ("1", "on", "true", "yes")
        st = load_state()
        pot = (st.get("derived_whitelist") or {}).get("wl_potential") or {}
        bys = pot.get("by_source") or {}
        allowed = {str(r.get("campaign_id")) for r in bys.get(source, []) if r.get("campaign_id")}
        picked = [x for x in raw_ids if str(x).strip() in allowed]
        if not picked:
            flash("No valid campaigns selected. Refresh WL from Ecomnia on the Campaign WL page, then try again.", "error")
            return redirect(url_for("ui_ec_console_whitelist_potential_detail", source=source))
        try:
            out = apply_wl_potential_cpcbysource_updates(
                EC_ADVERTISER_KEY,
                EC_AUTH_KEY,
                EC_SECRET_KEY,
                source=source,
                campaign_ids=picked,
                new_cpc=new_cpc,
                dry_run=dry,
            )
        except Exception as e:
            logger.exception("wl_potential_cpc_apply")
            flash(str(e), "error")
            return redirect(url_for("ui_ec_console_whitelist_potential_detail", source=source))
        record_run(
            "wl_potential_cpc_apply",
            bool(out.get("ok")),
            {
                "source": source,
                "selected": len(picked),
                "dry_run": dry,
                "errors": (out.get("errors") or [])[:12],
            },
        )
        _cache_clear("ec:")
        if dry:
            flash(
                f"Dry-run: would set cpcbysource[{source!r}] = {new_cpc} on {len(picked)} campaign(s). "
                f"See app log for full merged cpcbysource per campaign.",
                "success",
            )
        elif out.get("ok"):
            flash(f"Updated cpcbysource[{source!r}] = {new_cpc} on {len(picked)} campaign(s).", "success")
        else:
            flash(
                "Some updates failed: " + "; ".join(str(e) for e in (out.get("errors") or [])[:8]),
                "error",
            )
        return redirect(url_for("ui_ec_console_whitelist_potential_detail", source=source))

    st = load_state()
    derived = st.get("derived_whitelist") or {}
    pot = derived.get("wl_potential") if isinstance(derived.get("wl_potential"), dict) else {}
    rows = list((pot.get("by_source") or {}).get(source) or [])
    return render_template(
        "ecomnia_console_whitelist_potential_detail.html",
        active_page="whitelist-derived",
        ec_ok=ec_ok,
        source=source,
        rows=rows,
        wl_potential_meta=pot,
        wl_potential_note=derived.get("wl_potential_note"),
    )


@app.route("/ec/console/<path:page_slug>", methods=["GET"])
def ui_ec_console_tool(page_slug: str):
    """
    Per-tool pages: lists, whitelist-derived, sync-blacklists, whitelist-check, action-items.
    """
    if page_slug not in EC_CONSOLE_PAGES:
        abort(404)
    ctx = _ecomnia_console_view_context()
    template = "ecomnia_console_" + page_slug.replace("-", "_") + ".html"
    return render_template(template, active_page=page_slug, **ctx)


@app.route("/ec/brands/<path:brand_name>", methods=["GET"])
def ui_ec_brand_details(brand_name: str):
    refresh = (request.args.get("refresh") or "").strip().lower() in ("1", "true", "yes")
    if refresh:
        _cache_clear("ec:")

    campaigns = _ec_get_campaigns()
    merchants = _ec_get_merchants_map()
    target = unquote(brand_name)
    rows: list[dict[str, Any]] = []
    for camp in campaigns:
        brand = _ec_extract_brand(camp, merchants)
        if brand.lower() != target.lower():
            continue
        name = str(camp.get("name") or "")
        parsed = _extract_brand_geo_prefix(name)
        geo = parsed[1] if parsed else str(camp.get("geo") or "")
        prefix = parsed[2] if parsed else ""
        mid = str(camp.get("mid") or "").strip()
        rows.append(
            {
                "campaign_id": str(camp.get("id") or ""),
                "campaign_name": name,
                "status": str(camp.get("status") or ""),
                "reviewstatus": str(camp.get("reviewstatus") or ""),
                "merchant_name": merchants.get(mid, ""),
                "geo": geo,
                "prefix": prefix,
                "alias": _extract_alias_from_tracking_url(str(camp.get("url") or "")),
            }
        )
    rows.sort(key=lambda x: (x["status"] != "active", x["campaign_name"].lower()))
    return render_template("ec_brand.html", brand=target, campaigns=rows)


@app.route("/publishers/brands", methods=["GET", "POST"])
def ui_publishers_brands():
    if request.method == "POST":
        _publishers_rebuild_snapshot()
        return redirect(url_for("ui_publishers_brands"))

    refresh = (request.args.get("refresh") or "").strip().lower() in ("1", "true", "yes")
    if refresh:
        _publishers_rebuild_snapshot()

    brands = _publishers_brand_rows()
    return render_template("publishers_brands.html", brands=brands)


@app.route("/publishers/brands/<path:brand_key>", methods=["GET"])
def ui_publishers_brand_detail(brand_key: str):
    key = _norm_brand_key(unquote(brand_key))
    campaigns = _publishers_campaigns_for_brand(key)
    brand_display = campaigns[0]["brand_display"] if campaigns else key
    return render_template("publishers_brand_detail.html", brand_key=key, brand_display=brand_display, campaigns=campaigns)


@app.route("/sk-tools-sheet", methods=["GET"])
def ui_sk_tools_sheet():
    refresh = (request.args.get("refresh") or "").strip().lower() in ("1", "true", "yes")
    if refresh:
        _cache_clear("sheets:")

    tab = (request.args.get("tab") or "").strip()
    rows_limit = request.args.get("rows", 120, type=int) or 120
    rows_limit = max(20, min(rows_limit, 1000))

    tabs: list[str] = []
    rows: list[list[str]] = []
    error = ""
    try:
        tabs = _sheet_tabs(SK_TOOLS_SPREADSHEET_ID)
        if not tab and tabs:
            tab = tabs[0]
        if tab:
            rows = _sheet_values(SK_TOOLS_SPREADSHEET_ID, tab, rows_limit)
    except Exception as e:
        error = str(e)

    return render_template(
        "sk_tools_sheet.html",
        spreadsheet_id=SK_TOOLS_SPREADSHEET_ID,
        tabs=tabs,
        selected_tab=tab,
        rows=rows,
        rows_limit=rows_limit,
        error=error,
    )


@app.route("/sk/qualitywl", methods=["GET"])
def ui_sk_qualitywl():
    refresh = (request.args.get("refresh") or "").strip().lower() in ("1", "true", "yes")
    if refresh:
        _cache_clear("sheets:")

    rows_limit = request.args.get("rows", 500, type=int) or 500
    rows_limit = max(50, min(rows_limit, 2000))
    tab = "QualityWL"
    error = ""
    header: list[str] = []
    data_rows: list[list[str]] = []
    metrics = {
        "total_rows": 0,
        "active_like": 0,
        "monetized_like": 0,
        "not_monetized_like": 0,
    }

    try:
        values = _sheet_values(SK_TOOLS_SPREADSHEET_ID, tab, rows_limit)
        if values:
            header = [str(x or "").strip() for x in values[0]]
            data_rows = values[1:]

            # Lightweight derived counters from common column names/values.
            header_l = [h.lower() for h in header]
            idx_status = header_l.index("status") if "status" in header_l else -1
            idx_monet = -1
            for candidate in ("monetization", "monetized", "kelkoo monetization", "yadore_monetization"):
                if candidate in header_l:
                    idx_monet = header_l.index(candidate)
                    break

            metrics["total_rows"] = len(data_rows)
            for row in data_rows:
                status_val = (str(row[idx_status]).strip().lower() if idx_status >= 0 and idx_status < len(row) else "")
                monet_val = (str(row[idx_monet]).strip().lower() if idx_monet >= 0 and idx_monet < len(row) else "")
                if status_val in ("active", "running", "live", "approved"):
                    metrics["active_like"] += 1
                if "not_monet" in monet_val or monet_val in ("no", "false", "none"):
                    metrics["not_monetized_like"] += 1
                elif "monet" in monet_val or monet_val in (
                    "yes",
                    "true",
                    "any",
                    "both",
                    "non_coupon_only",
                    "coupon_only",
                ):
                    metrics["monetized_like"] += 1
    except Exception as e:
        error = str(e)

    return render_template(
        "sk_qualitywl.html",
        spreadsheet_id=SK_TOOLS_SPREADSHEET_ID,
        tab=tab,
        header=header,
        rows=data_rows,
        rows_limit=rows_limit,
        metrics=metrics,
        error=error,
    )


@app.route("/workflows/<workflow_key>", methods=["GET", "POST"])
def ui_workflow(workflow_key: str):
    if workflow_key not in WORKFLOWS:
        abort(404)
    wf = WORKFLOWS[workflow_key]
    current_run = None
    if request.method == "POST":
        extra_args = (request.form.get("extra_args") or "").strip()
        if workflow_key == "blend":
            bf = (request.form.get("blend_feed") or "both").strip().lower()
            if bf in ("kelkoo1", "kelkoo2", "adexa", "yadore", "both", "all"):
                # Avoid duplicate --feed if user pasted one in the text box; dropdown wins.
                extra_args = re.sub(
                    r"--feed\s+(kelkoo1|kelkoo2|adexa|yadore|both|all)\b",
                    "",
                    extra_args,
                    flags=re.IGNORECASE,
                )
                extra_args = " ".join(extra_args.split())
                extra_args = f"--feed {bf} {extra_args}".strip()
        if workflow_key == "blend-keitaro-sync":
            bg = (request.form.get("blend_sync_geo") or "").strip().lower()[:2]
            extra_args = re.sub(
                r"--geo\s+[a-zA-Z]{2}\b",
                "",
                extra_args,
                flags=re.IGNORECASE,
            )
            extra_args = " ".join(extra_args.split())
            if len(bg) == 2:
                extra_args = f"--geo {bg} {extra_args}".strip()
        if workflow_key == "daily":
            dg = (request.form.get("daily_geo") or "").strip()
            # Remove a manually-typed --geo; the dedicated control wins when set.
            extra_args = re.sub(
                r"--geo\s+\S+",
                "",
                extra_args,
                flags=re.IGNORECASE,
            )
            extra_args = " ".join(extra_args.split())
            if dg:
                extra_args = f"--geo {dg} {extra_args}".strip()

            blend_sync_mode = (request.form.get("daily_blend_sync") or "on").strip().lower()
            # Normalize user args to a single source of truth.
            extra_args = re.sub(r"--skip-blend-sync\b", "", extra_args, flags=re.IGNORECASE)
            extra_args = " ".join(extra_args.split())
            if blend_sync_mode == "off":
                extra_args = f"--skip-blend-sync {extra_args}".strip()

            # Normalize merchant override flags; dropdown mode controls become source of truth.
            extra_args = re.sub(r"--merchant-override\s+\S+", "", extra_args, flags=re.IGNORECASE)
            extra_args = re.sub(r"--merchant-auto-override\s+\S+", "", extra_args, flags=re.IGNORECASE)
            extra_args = " ".join(extra_args.split())

            mode = (request.form.get("daily_merchant_mode") or "none").strip().lower()
            if mode == "manual":
                mf = (request.form.get("daily_manual_feed") or "1").strip()
                mg = (request.form.get("daily_manual_geo") or "").strip().lower()[:2]
                mids = (request.form.get("daily_manual_ids") or "").strip().replace(" ", "")
                if mf in ("1", "2") and len(mg) == 2 and mids:
                    extra_args = f"--merchant-override {mf}:{mg}={mids} {extra_args}".strip()
            elif mode == "platform":
                pf = (request.form.get("daily_platform_feed") or "1").strip()
                pg = (request.form.get("daily_platform_geo") or "").strip().lower()[:2]
                pr_raw = (request.form.get("daily_platform_rank") or "2").strip()
                try:
                    pr = max(1, int(pr_raw))
                except ValueError:
                    pr = 2
                if pf in ("1", "2") and len(pg) == 2:
                    extra_args = f"--merchant-auto-override {pf}:{pg}:{pr} {extra_args}".strip()
        # Run workflow pages in background to avoid nginx/gateway timeout on long jobs.
        current_run = _run_workflow_in_background(workflow_key, extra_args)
    last_run = current_run or _load_last_run(workflow_key)
    return render_template(
        "workflow.html",
        workflow_key=workflow_key,
        workflow=wf,
        last_run=last_run,
    )


@app.route("/health", methods=["GET"])
def health():
    """Liveness/readiness for orchestrator."""
    return jsonify({"status": "ok"})


def _overview_missing_payload() -> Dict[str, Any]:
    """Shape returned when no snapshot file exists yet."""
    return {
        "snapshot_status": "missing",
        "snapshot_saved_utc": None,
        "revenue": {
            "yesterday": None,
            "mtd": None,
            "error": "No snapshot yet. Use \"Refresh from APIs\" or wait for the daily scheduled job.",
        },
        "costs": {
            "zeropark": {"yesterday": None, "mtd": None, "error": None},
            "sourceknowledge": {"yesterday": None, "mtd": None, "error": None},
            "ecomnia": {"yesterday": None, "mtd": None, "error": None},
            "thrillion": None,
            "yesshh": None,
        },
        "total_cost": {"yesterday": None, "mtd": None},
        "net": {"yesterday": None, "mtd": None},
        "as_of_utc": None,
        "ranges": {},
    }


@app.route("/api/overview/refresh", methods=["POST"])
def api_overview_refresh():
    """Rebuild the overview snapshot from live APIs (can take several minutes). Same UI auth as other routes."""
    try:
        data, saved = refresh_overview_snapshot()
    except Exception as e:
        logger.exception("POST /api/overview/refresh failed")
        return jsonify({"error": str(e)}), 500
    out: Dict[str, Any] = dict(data)
    out["snapshot_status"] = "ready"
    out["snapshot_saved_utc"] = saved
    return jsonify(out)


@app.route("/api/overview", methods=["GET"])
def api_overview():
    """Dashboard metrics from the last snapshot (fast). Rebuild via ``POST /api/overview/refresh`` or daily scheduler."""
    payload, saved = read_snapshot_for_api()
    if payload is None:
        return jsonify(_overview_missing_payload())
    out: Dict[str, Any] = dict(payload)
    out["snapshot_status"] = "ready"
    out["snapshot_saved_utc"] = saved
    return jsonify(out)


@app.route("/api/postback-status", methods=["GET"])
def api_postback_status():
    """Daily conversion postback completion rollup for the Control Center banner (UTC day)."""
    from integrations.daily_postbacks_run_history import postback_banner_payload_for_today

    return jsonify(postback_banner_payload_for_today())


def _api_automations_payload() -> Dict[str, Any]:
    ensure_automations_initialized()
    last_map = last_run_by_automation()
    listeners = get_automation_listeners()
    by_class = {a.__class__.__name__: a for a in listeners}
    rows: list[dict[str, Any]] = []
    for spec in AUTOMATION_SPECS:
        cn = spec["class_name"]
        lr = last_map.get(cn)
        rows.append(
            {
                "class_name": cn,
                "label": spec["label"],
                "schedule": spec["schedule"],
                "registered": cn in by_class,
                "last_run": lr,
            }
        )
    return {"scheduler_running": scheduler_running(), "automations": rows}


@app.route("/api/automations", methods=["GET"])
def api_automations():
    """List AutoServer automations + scheduler state + last run per job (from run log)."""
    return jsonify(_api_automations_payload())


@app.route("/api/automations/log", methods=["GET"])
def api_automations_log():
    """Newest-first run log entries (JSON file under ``data/autoserver_run_log.json``)."""
    limit = request.args.get("limit", default=20, type=int)
    if limit is None or limit < 1:
        limit = 20
    limit = min(limit, 500)
    entries = read_entries_newest_first(limit=limit)
    return jsonify({"limit": limit, "entries": entries})


@app.route("/api/automations/trigger/all", methods=["GET"])
def api_automations_trigger_all():
    """Queue all automations in the background (202 Accepted)."""
    ensure_automations_initialized()
    schedule_trigger_all()
    payload = _api_automations_payload()
    payload["status"] = "scheduled"
    payload["message"] = "All automations scheduled to run in the background"
    return jsonify(payload), 202


@app.route("/api/automations/trigger/<name>", methods=["GET"])
def api_automations_trigger_one(name: str):
    """Queue one automation by class name (case-insensitive)."""
    ensure_automations_initialized()
    key = (name or "").strip().lower()
    for automation in get_automation_listeners():
        if automation.__class__.__name__.lower() == key:
            schedule_trigger_one(automation)
            return jsonify(
                {
                    "status": "scheduled",
                    "message": f"Automation {automation.__class__.__name__} scheduled in the background",
                    "automation": automation.__class__.__name__,
                }
            ), 202
    return jsonify(
        {
            "status": "error",
            "message": f'Automation "{name}" not found',
            "available": [a.__class__.__name__ for a in get_automation_listeners()],
        }
    ), 404


@app.route("/api/overview/slice/revenue", methods=["GET"])
def api_overview_slice_revenue():
    """Live Keitaro revenue slice for dashboard tiles (independent of snapshot)."""
    return jsonify(slice_revenue())


@app.route("/api/overview/slice/zeropark", methods=["GET"])
def api_overview_slice_zeropark():
    return jsonify(slice_zeropark())


@app.route("/api/overview/slice/sourceknowledge", methods=["GET"])
def api_overview_slice_sourceknowledge():
    return jsonify(slice_sourceknowledge())


@app.route("/api/overview/slice/ecomnia", methods=["GET"])
def api_overview_slice_ecomnia():
    return jsonify(slice_ecomnia())


@app.route("/api/v1/workflows/create-campaign", methods=["POST"])
def workflow_create_campaign():
    """
    Workflow 1: Create a Keitaro campaign.
    Body (JSON):
      - Option A: { "alias": "...", "name": "..." } plus optional payload fields.
      - Option B: { "payload": { ... } } to send the full Keitaro campaign payload.
    Returns created campaign data or error.
    """
    if not request.is_json:
        return jsonify({"error": "Content-Type must be application/json"}), 400

    data = request.get_json() or {}

    # Full payload override (e.g. from Kelkoo-derived builder later)
    if "payload" in data:
        payload = data["payload"]
        if not isinstance(payload, dict):
            return jsonify({"error": "payload must be an object"}), 400
        alias = payload.get("alias") or "unnamed"
        name = payload.get("name") or alias
        try:
            result = run_create_campaign_workflow(
                alias=alias,
                name=name,
                payload_override=payload,
            )
            return jsonify(result), 201
        except KeitaroClientError as e:
            return jsonify({
                "error": str(e),
                "status_code": e.status_code,
                "response_body": e.response_body,
            }), (e.status_code or 502)

    # Build from alias + name + optional kwargs
    alias = data.get("alias")
    name = data.get("name")
    if not alias or not name:
        return jsonify({
            "error": "Missing required fields: alias and name (or provide full payload in 'payload')",
        }), 400

    payload_kwargs = {k: v for k, v in data.items() if k not in ("alias", "name")}
    try:
        result = run_create_campaign_workflow(
            alias=alias,
            name=name,
            payload_override=None,
            **payload_kwargs,
        )
        return jsonify(result), 201
    except KeitaroClientError as e:
        return jsonify({
            "error": str(e),
            "status_code": e.status_code,
            "response_body": e.response_body,
        }), (e.status_code or 502)


# --- Assistance: get campaigns (e.g. test from UI) and clone to verify campaign_setup ---


@app.route("/api/v1/assistance/campaigns", methods=["GET"])
def assistance_get_campaigns():
    """
    Get all campaigns (offset/limit query params). Use to inspect your test campaign from the UI.
    """
    offset = request.args.get("offset", 0, type=int)
    limit = request.args.get("limit", 100, type=int)
    try:
        campaigns = get_campaigns_data(offset=offset, limit=limit)
        return jsonify({"campaigns": campaigns, "count": len(campaigns)})
    except KeitaroClientError as e:
        return jsonify({
            "error": str(e),
            "status_code": e.status_code,
            "response_body": e.response_body,
        }), (e.status_code or 502)


@app.route("/api/v1/assistance/clone-campaign", methods=["POST"])
def assistance_clone_campaign():
    """
    Clone a campaign by id. Body: { "campaign_id": 123 }. Returns the new campaign.
    """
    data = request.get_json() or {}
    cid = data.get("campaign_id")
    if cid is None:
        return jsonify({"error": "Missing campaign_id"}), 400
    try:
        result = clone_campaign_copy(int(cid))
        return jsonify(result), 201
    except KeitaroClientError as e:
        return jsonify({
            "error": str(e),
            "status_code": e.status_code,
            "response_body": e.response_body,
        }), (e.status_code or 502)


@app.route("/api/v1/assistance/clone-campaign-setup", methods=["POST"])
def assistance_clone_campaign_setup():
    """
    Fetch campaigns, find one by alias or name (default: campaign_setup), clone it.
    Body (optional): { "alias_or_name": "campaign_setup" }. Returns the cloned campaign.
    Use this to verify campaign creation by creating a copy of your test campaign_setup.
    """
    data = request.get_json() or {}
    alias_or_name = data.get("alias_or_name", "campaign_setup")
    try:
        result = get_campaigns_then_clone_setup(alias_or_name=alias_or_name)
        return jsonify(result), 201
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except KeitaroClientError as e:
        return jsonify({
            "error": str(e),
            "status_code": e.status_code,
            "response_body": e.response_body,
        }), (e.status_code or 502)


@app.route("/api/v1/assistance/campaigns/<int:campaign_id>/streams", methods=["GET"])
def assistance_get_campaign_streams(campaign_id):
    """
    Get traffic flows (streams) for a campaign by id.
    """
    try:
        streams = get_campaign_streams(campaign_id)
        return jsonify({"streams": streams, "count": len(streams), "campaign_id": campaign_id})
    except KeitaroClientError as e:
        return jsonify({
            "error": str(e),
            "status_code": e.status_code,
            "response_body": e.response_body,
        }), (e.status_code or 502)


@app.route("/api/v1/assistance/offers", methods=["GET"])
def assistance_get_offers():
    """
    Get all offers. Use to inspect offer payloads (id, name, action_type, affiliate_network_id, etc.).
    """
    try:
        offers = get_offers_data()
        return jsonify({"offers": offers, "count": len(offers)})
    except KeitaroClientError as e:
        return jsonify({
            "error": str(e),
            "status_code": e.status_code,
            "response_body": e.response_body,
        }), (e.status_code or 502)


@app.route("/api/v1/assistance/setup", methods=["GET"])
def assistance_get_setup():
    """
    Get full setup: one campaign (by alias/name) + its flows + all offers.
    Query: alias=... or campaign=... (default: campaign_setup).
    Use the response to see real payloads for create/update.
    """
    alias_or_name = request.args.get("alias") or request.args.get("campaign", "campaign_setup")
    try:
        data = get_full_setup(alias_or_name)
        return jsonify(data)
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except KeitaroClientError as e:
        return jsonify({
            "error": str(e),
            "status_code": e.status_code,
            "response_body": e.response_body,
        }), (e.status_code or 502)


@app.route("/api/v1/assistance/streams", methods=["GET"])
def assistance_get_streams_by_campaign():
    """
    Get flows for a campaign by alias/name. Query: alias=... or campaign=...
    Example: GET /api/v1/assistance/streams?alias=campaign_setup
    """
    alias_or_name = request.args.get("alias") or request.args.get("campaign")
    if not alias_or_name:
        return jsonify({"error": "Missing query param: alias= or campaign="}), 400
    try:
        streams = get_campaign_streams_by_alias(alias_or_name=alias_or_name)
        return jsonify({"streams": streams, "count": len(streams)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 404
    except KeitaroClientError as e:
        return jsonify({
            "error": str(e),
            "status_code": e.status_code,
            "response_body": e.response_body,
        }), (e.status_code or 502)


try:
    start_autoserver_scheduler()
except Exception as e:
    logger.warning("AutoServer scheduler did not start: %s", e)

try:
    start_daily_overview_scheduler()
except Exception as e:
    logger.warning("Overview snapshot scheduler did not start: %s", e)

try:
    start_overview_snapshot_bootstrap()
except Exception as e:
    logger.warning("Overview snapshot bootstrap did not start: %s", e)


def main():
    if not KEITARO_BASE_URL or not KEITARO_API_KEY:
        logger.warning("KEITARO_BASE_URL or KEITARO_API_KEY not set; workflow will fail until configured")
    app.run(host="0.0.0.0", port=5000, debug=True)


if __name__ == "__main__":
    main()
