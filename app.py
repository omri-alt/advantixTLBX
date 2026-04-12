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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict
from urllib.parse import unquote

from flask import Flask, request, jsonify, render_template, abort, redirect, url_for

from config import (
    KEITARO_BASE_URL,
    KEITARO_API_KEY,
    EC_ADVERTISER_KEY,
    EC_AUTH_KEY,
    EC_SECRET_KEY,
    FEED1_API_KEY,
    FEED2_API_KEY,
    KELKOO_LATE_SALES_SPREADSHEET_ID,
    LATE_SALES_POSTBACK_BASE,
)
from workflows.campaign_setup import run_create_campaign_workflow
from integrations.keitaro import KeitaroClientError
from integrations.kelkoo_search import kelkoo_merchant_link_check
from integrations.yadore import deeplink as yadore_deeplink, YadoreClientError
from integrations.adexa import links_merchant_check as adexa_links_check, AdexaClientError
from integrations.monetization_geo import geo_for_adexa, yadore_feed_class
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
from integrations.overview import build_overview_json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

ROOT_DIR = Path(__file__).resolve().parent
RUNS_DIR = ROOT_DIR / "runtime" / "workflow_runs"
RUNS_DIR.mkdir(parents=True, exist_ok=True)

_WORKFLOW_THREADS_LOCK = threading.Lock()
_WORKFLOW_THREADS: dict[str, threading.Thread] = {}
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
            {"label": "UK rerun (example)", "value": "--geo uk"},
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
            "Full Blend: sync Keitaro from the Blend sheet, then refresh potentialKelkoo* from reports "
            "(use feed dropdown). Column ``feed`` on the Blend sheet must be ``kelkoo1`` or ``kelkoo2``; "
            "sync uses ``FEED1_API_KEY`` / ``FEED2_API_KEY`` for monetization checks per row."
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

    coming_soon = {
        "daily_automations": ["Daily data pull from Kelkoo feed"],
        "publisher_tools": ["Publisher tools dashboard (SK/EC under refinement)"],
    }
    return render_template("index.html", groups=out_groups, coming_soon=coming_soon)


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
                    ax = adexa_links_check(domain, geo_for_adexa(g))
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
            if bf in ("kelkoo1", "kelkoo2", "both"):
                # Avoid duplicate --feed if user pasted one in the text box; dropdown wins.
                extra_args = re.sub(
                    r"--feed\s+(kelkoo1|kelkoo2|both)\b",
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
            dg = (request.form.get("daily_geo") or "").strip().lower()[:2]
            # Remove a manually-typed --geo; the dedicated control wins when set.
            extra_args = re.sub(
                r"--geo\s+[a-zA-Z]{2}\b",
                "",
                extra_args,
                flags=re.IGNORECASE,
            )
            extra_args = " ".join(extra_args.split())
            if len(dg) == 2:
                extra_args = f"--geo {dg} {extra_args}".strip()

            blend_sync_mode = (request.form.get("daily_blend_sync") or "on").strip().lower()
            # Normalize user args to a single source of truth.
            extra_args = re.sub(r"--skip-blend-sync\b", "", extra_args, flags=re.IGNORECASE)
            extra_args = " ".join(extra_args.split())
            if blend_sync_mode == "off":
                extra_args = f"--skip-blend-sync {extra_args}".strip()
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


@app.route("/api/overview", methods=["GET"])
def api_overview():
    """Dashboard: Keitaro revenue + traffic costs (ZP, SK, EC) + net. Requires same UI auth as other routes."""
    return jsonify(build_overview_json())


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


def main():
    if not KEITARO_BASE_URL or not KEITARO_API_KEY:
        logger.warning("KEITARO_BASE_URL or KEITARO_API_KEY not set; workflow will fail until configured")
    app.run(host="0.0.0.0", port=5000, debug=True)


if __name__ == "__main__":
    main()
