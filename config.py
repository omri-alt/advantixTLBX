"""
Central configuration (env-backed) for KLblend scripts.

Loaded via python-dotenv from project `.env` when present.
"""
from __future__ import annotations

import os
import re
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _read_env_fallback(var_name: str) -> str:
    """
    Fallback reader for malformed .env lines that python-dotenv may skip.
    Example it can recover from:
      YADORE_API_KEY= = <key>
      KEYZP = <key>
    """
    try:
        env_path = Path(__file__).resolve().parent / ".env"
        if not env_path.exists():
            return ""
        raw = env_path.read_bytes()
        # Try a few decodes to survive odd Windows encodings / hidden characters.
        candidates = []
        for enc in ("utf-8", "utf-8-sig", "cp1252", "latin-1", "utf-16", "utf-16-le", "utf-16-be"):
            try:
                candidates.append(raw.decode(enc, errors="ignore"))
            except Exception:
                continue
        text = "\n".join(candidates) if candidates else raw.decode("utf-8", errors="ignore")
        # Normalize line endings + strip nulls/zero-width-ish chars that break regex.
        text = text.replace("\r\n", "\n").replace("\r", "\n").replace("\x00", "")

        # Find line in a forgiving way (don't rely on strict regex matching).
        target = var_name.strip().lower()
        val = ""
        for line in text.split("\n"):
            clean = "".join(ch for ch in line if ch.isprintable())
            if clean.strip().startswith("#"):
                continue
            if "=" not in clean:
                continue
            k, v = clean.split("=", 1)
            if k.strip().lower() == target:
                val = v.strip()
                break
        if not val:
            return ""
        # handle accidental extra '=' tokens
        val = val.lstrip("= ").strip()
        return val
    except Exception:
        return ""


# Keitaro tracker
KEITARO_BASE_URL = (os.getenv("KEITARO_BASE_URL") or "").strip().rstrip("/")
KEITARO_API_KEY = (os.getenv("KEITARO_API_KEY") or "").strip()

# Optional: default campaign (id=1, alias=HrQBXp) for helper scripts
KEITARO_CAMPAIGN_ID = (os.getenv("KEITARO_CAMPAIGN_ID") or "").strip()
KEITARO_CAMPAIGN_ALIAS = (os.getenv("KEITARO_CAMPAIGN_ALIAS") or "HrQBXp").strip() or None

# Hub campaign (Domain): bought traffic lands here, then routes to per-feed Blend/Nipuhim child campaigns.
KEITARO_HUB_CAMPAIGN_ID = int((os.getenv("KEITARO_HUB_CAMPAIGN_ID") or "94").strip() or "94")
KEITARO_HUB_BLEND_PCT = int((os.getenv("KEITARO_HUB_BLEND_PCT") or "50").strip() or "50")
KEITARO_HUB_NIPUHIM_PCT = int((os.getenv("KEITARO_HUB_NIPUHIM_PCT") or "50").strip() or "50")
# Comma list of feed keys that receive hub traffic (others stay attached at share 0).
# Default: Kelkoo trio only until adexa/yadore/shopnomix are wired in daily sync.
KEITARO_HUB_ACTIVE_FEEDS = tuple(
    x.strip().lower()
    for x in (
        os.getenv("KEITARO_HUB_ACTIVE_FEEDS") or "kelkoo1,kelkoo2,kelkoo5"
    ).split(",")
    if x.strip()
)
KEITARO_HUB_STATE_PATH = (
    os.getenv("KEITARO_HUB_STATE_PATH") or "data/keitaro_hub_state.json"
).strip()
# Outer shell for hub offer URLs (bought traffic → campaign 94 → child campaigns).
KEITARO_HUB_RAIN_SHELL = (
    os.getenv("KEITARO_HUB_RAIN_SHELL") or "https://shopli.city/raini?rain="
).strip()
# Nipuhim template for hub children: country flows + static product URLs (not KL-Main dynamic oadest).
KEITARO_NIPUHIM_HUB_TEMPLATE_CAMPAIGN_ID = int(
    (os.getenv("KEITARO_NIPUHIM_HUB_TEMPLATE_CAMPAIGN_ID") or "1").strip() or "1"
)
# When enabled, daily v2 (and legacy with flag) also sync offers into NIPUHIM-feed* hub children.
NIPUHIM_BLEND_V2_ENABLED = str(
    os.getenv("NIPUHIM_BLEND_V2_ENABLED") or "1"
).strip().lower() in ("1", "true", "yes", "on")
# When enabled, blend_sync also populates BLEND-feed* hub children (legacy Blend campaign unchanged).
BLEND_HUB_V2_ENABLED = str(
    os.getenv("BLEND_HUB_V2_ENABLED") or "1"
).strip().lower() in ("1", "true", "yes", "on")
# After Blend + Nipuhim v2 sync, rewire hub campaign 94 stream weights from click caps.
KEITARO_HUB_REWIRE_ENABLED = str(
    os.getenv("KEITARO_HUB_REWIRE_ENABLED") or "1"
).strip().lower() in ("1", "true", "yes", "on")

# Keitaro PHP admin bulk (POST /admin/?bulk): object names to try for removing offers.
# - offers.update: postData {"id": N, "state": "deleted"} (soft-delete; matches UI response).
# - others: postData {"ids": [N]} (e.g. offers.delete / clone-style).
# Override with comma-separated object names if your build differs.
KEITARO_ADMIN_BULK_DELETE_OBJECTS = tuple(
    x.strip()
    for x in (
        os.getenv("KEITARO_ADMIN_BULK_DELETE_OBJECTS")
        or "offers.update,offers.delete,offers.remove,offers.archive"
    ).split(",")
    if x.strip()
) or ("offers.update", "offers.delete", "offers.remove", "offers.archive")

# Google Sheets — Kelkoo daily notebook (fixim / offers / logs)
KELKOO_SHEETS_SPREADSHEET_ID = (
    os.getenv("KELKOO_SHEETS_SPREADSHEET_ID") or "1XUkQoWqnNRqaSEnFVRAV36-oi9ENrNWtH5Ct8M4vNuU"
).strip()

# Google Sheets — late conversion / MTD sales workbook (KLsalesreport).
# One tab per feed per month, refreshed daily: ``SalesMTD_{feed}_{YYYY-MM}``.
# Legacy ``SalesReport_*`` tabs are pruned on each late-conversion run.
KELKOO_LATE_SALES_SPREADSHEET_ID = (
    os.getenv("KELKOO_LATE_SALES_SPREADSHEET_ID") or "1hVhQ_BfrKOh8OCojTFkuTLKeU1KLbK_kWgm1BmJFteU"
).strip()

# Late-sale postbacks (GET): ``subid`` = Kelkoo ``click_id``, ``payout`` = ``sale_value_usd``, ``status`` = LateSale
LATE_SALES_POSTBACK_BASE = (
    os.getenv("LATE_SALES_POSTBACK_BASE") or "http://207.154.244.157/2ea006b/postback"
).strip()

# Daily on-time click / sale postbacks (Keitaro GET). Defaults share the same base URL as late sales; only query differs.
DAILY_CONVERSION_POSTBACK_BASE = (
    (os.getenv("DAILY_CONVERSION_POSTBACK_BASE") or "").strip() or LATE_SALES_POSTBACK_BASE
).strip()
DAILY_CONVERSION_POSTBACK_CLICK_STATUS = (os.getenv("DAILY_CONVERSION_POSTBACK_CLICK_STATUS") or "click").strip()
DAILY_CONVERSION_POSTBACK_SALE_STATUS = (os.getenv("DAILY_CONVERSION_POSTBACK_SALE_STATUS") or "SaleOur").strip()
# Effinity affiliate sale postbacks use status=salecpa (not SaleOur).
EFFINITY_SALE_POSTBACK_STATUS = (os.getenv("EFFINITY_SALE_POSTBACK_STATUS") or "salecpa").strip()
_dcp_state = (os.getenv("DAILY_CONVERSION_POSTBACK_STATE_PATH") or "").strip()
DAILY_CONVERSION_POSTBACK_STATE_PATH = _dcp_state or str(
    Path(__file__).resolve().parent / "runtime" / "daily_conversion_postbacks_state.json"
)

# Late conversion scheduler (MTD sales refresh + Keitaro check + LateSale postbacks).
# Default: 10:00 Asia/Jerusalem. Disable extras with ``KELKOO_LATE_SALES_SCHEDULER_ENABLED=0``.
KELKOO_LATE_SALES_SCHEDULER_ENABLED = (
    os.getenv("KELKOO_LATE_SALES_SCHEDULER_ENABLED", "1").strip().lower() not in ("0", "false", "no")
)
LATE_CONVERSION_SCHEDULER_TZ = (os.getenv("LATE_CONVERSION_SCHEDULER_TZ") or "Asia/Jerusalem").strip()
try:
    LATE_CONVERSION_SCHEDULER_HOUR_LOCAL = int(
        (os.getenv("LATE_CONVERSION_SCHEDULER_HOUR_LOCAL") or "10").strip()
    )
except Exception:
    LATE_CONVERSION_SCHEDULER_HOUR_LOCAL = 10
LATE_CONVERSION_SCHEDULER_HOUR_LOCAL = max(0, min(23, LATE_CONVERSION_SCHEDULER_HOUR_LOCAL))

# Yadore daily sales postbacks (conversion/detail → SaleOur, payout=0). Clicks stay manual via postbacks UI.
YADORE_SALES_SCHEDULER_ENABLED = (
    os.getenv("YADORE_SALES_SCHEDULER_ENABLED", "1").strip().lower() not in ("0", "false", "no")
)
YADORE_SALES_SCHEDULER_TZ = (os.getenv("YADORE_SALES_SCHEDULER_TZ") or "Asia/Jerusalem").strip()
try:
    YADORE_SALES_SCHEDULER_HOUR_LOCAL = int((os.getenv("YADORE_SALES_SCHEDULER_HOUR_LOCAL") or "10").strip())
except Exception:
    YADORE_SALES_SCHEDULER_HOUR_LOCAL = 10
YADORE_SALES_SCHEDULER_HOUR_LOCAL = max(0, min(23, YADORE_SALES_SCHEDULER_HOUR_LOCAL))

# Effinity MTD salecpa postbacks (publisher conversions API → Keitaro). Same daily slot as Yadore by default.
EFFINITY_SALES_SCHEDULER_ENABLED = (
    os.getenv("EFFINITY_SALES_SCHEDULER_ENABLED", "1").strip().lower() not in ("0", "false", "no")
)
EFFINITY_SALES_SCHEDULER_TZ = (
    os.getenv("EFFINITY_SALES_SCHEDULER_TZ") or os.getenv("YADORE_SALES_SCHEDULER_TZ") or "Asia/Jerusalem"
).strip()
try:
    EFFINITY_SALES_SCHEDULER_HOUR_LOCAL = int(
        (os.getenv("EFFINITY_SALES_SCHEDULER_HOUR_LOCAL") or os.getenv("YADORE_SALES_SCHEDULER_HOUR_LOCAL") or "10").strip()
    )
except Exception:
    EFFINITY_SALES_SCHEDULER_HOUR_LOCAL = 10
EFFINITY_SALES_SCHEDULER_HOUR_LOCAL = max(0, min(23, EFFINITY_SALES_SCHEDULER_HOUR_LOCAL))
try:
    EFFINITY_SALES_SCHEDULER_MINUTE = int((os.getenv("EFFINITY_SALES_SCHEDULER_MINUTE") or "15").strip())
except Exception:
    EFFINITY_SALES_SCHEDULER_MINUTE = 15
EFFINITY_SALES_SCHEDULER_MINUTE = max(0, min(59, EFFINITY_SALES_SCHEDULER_MINUTE))

# Legacy UTC hour (unused when ``LATE_CONVERSION_SCHEDULER_*`` is set); kept for env compatibility.
try:
    KELKOO_LATE_SALES_SCHEDULER_HOUR_UTC = int((os.getenv("KELKOO_LATE_SALES_SCHEDULER_HOUR_UTC") or "7").strip())
except Exception:
    KELKOO_LATE_SALES_SCHEDULER_HOUR_UTC = 7
KELKOO_LATE_SALES_SCHEDULER_HOUR_UTC = max(0, min(23, KELKOO_LATE_SALES_SCHEDULER_HOUR_UTC))
# After sales tabs are built, run late-sales diff and apply GET postbacks (not dry-run).
KELKOO_LATE_SALES_APPLY_ENABLED = (
    os.getenv("KELKOO_LATE_SALES_APPLY_ENABLED", "1").strip().lower() not in ("0", "false", "no")
)
try:
    KELKOO_LATE_SALES_KEITARO_LOOKBACK_DAYS = int(
        (os.getenv("KELKOO_LATE_SALES_KEITARO_LOOKBACK_DAYS") or "45").strip()
    )
except Exception:
    KELKOO_LATE_SALES_KEITARO_LOOKBACK_DAYS = 45
KELKOO_LATE_SALES_KEITARO_LOOKBACK_DAYS = max(7, min(120, KELKOO_LATE_SALES_KEITARO_LOOKBACK_DAYS))
# When 1 (default), skip late-sale if click_id is on a daily SalesReport tab (SaleOur already sent).
LATE_SALES_SKIP_IF_IN_DAILY_TAB = (
    os.getenv("LATE_SALES_SKIP_IF_IN_DAILY_TAB", "1").strip().lower() not in ("0", "false", "no")
)
try:
    KELKOO_SALES_TAB_RETENTION_DAYS = int((os.getenv("KELKOO_SALES_TAB_RETENTION_DAYS") or "14").strip())
except Exception:
    KELKOO_SALES_TAB_RETENTION_DAYS = 14
KELKOO_SALES_TAB_RETENTION_DAYS = max(3, min(90, KELKOO_SALES_TAB_RETENTION_DAYS))
# When 1, also include any row on the latest 7-day tab missing Keitaro SaleOur/LateSale (can duplicate daily sales; default off).
LATE_SALES_INCLUDE_MISSED_KEITARO = (
    os.getenv("LATE_SALES_INCLUDE_MISSED_KEITARO", "0").strip().lower() in ("1", "true", "yes")
)
# Comma-separated merchant substrings for extra Kelkoo raw day-by-day backfill (e.g. joueclub,passagedudesir).
LATE_SALES_RAW_BACKFILL_MERCHANTS = (
    os.getenv("LATE_SALES_RAW_BACKFILL_MERCHANTS") or "joueclub,passagedudesir"
).strip().lower()
# Geos for raw backfill only (default fr — watchlist merchants are usually FR).
LATE_SALES_RAW_BACKFILL_GEOS: tuple[str, ...] = tuple(
    g.strip().lower()[:2]
    for g in (os.getenv("LATE_SALES_RAW_BACKFILL_GEOS") or "fr").split(",")
    if g.strip()
) or ("fr",)

# Google Sheets — Blend workflow spreadsheet (Blend tab + potential sheets)
BLEND_SHEETS_SPREADSHEET_ID = (
    os.getenv("BLEND_SHEETS_SPREADSHEET_ID") or "1h9lBPTREEJO9VVvj6wctCgCOn3YcwJBGIk_MBwXw-xY"
).strip()


def _parse_blend_potential_feeds() -> tuple[str, ...]:
    """
    Comma-separated feeds used for Blend potential sheets (``potentialKelkoo*``,
    ``potentialAdexa``, ``potentialYadore``) and ``populate_blend_from_potential``.
    Default includes all four feeds; missing API keys for a feed are skipped in the daily workflow.
    """
    raw = (os.getenv("BLEND_POTENTIAL_FEEDS") or "kelkoo1,kelkoo2,kelkoo5,adexa,yadore").strip().lower()
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    allowed = {"kelkoo1", "kelkoo2", "kelkoo5", "adexa", "yadore"}
    out = tuple(p for p in parts if p in allowed)
    return out if out else ("kelkoo1", "kelkoo2", "adexa", "yadore")


BLEND_POTENTIAL_FEEDS: tuple[str, ...] = _parse_blend_potential_feeds()

try:
    # Default high so daily populate does not silently skip monetized merchants that passed
    # blend_potential CR rules; lower BLEND_POPULATE_MAX_ADD only if you need a safety ceiling.
    BLEND_POPULATE_MAX_ADD = int((os.getenv("BLEND_POPULATE_MAX_ADD") or "5000").strip())
except Exception:
    BLEND_POPULATE_MAX_ADD = 5000
BLEND_POPULATE_MAX_ADD = max(1, min(20000, BLEND_POPULATE_MAX_ADD))

try:
    BLEND_DEVICE_CPC_MIN = float((os.getenv("BLEND_DEVICE_CPC_MIN") or "0.05").strip())
except Exception:
    BLEND_DEVICE_CPC_MIN = 0.05
BLEND_DEVICE_CPC_MIN = max(0.0, BLEND_DEVICE_CPC_MIN)

_blend_cap_split = (os.getenv("BLEND_DEVICE_CAP_SPLIT_BY_CPC") or "1").strip().lower()
BLEND_DEVICE_CAP_SPLIT_BY_CPC = _blend_cap_split not in ("0", "false", "no", "off")

BLEND_CAP_PROGRESS_CACHE_PATH = (
    os.getenv("BLEND_CAP_PROGRESS_CACHE_PATH") or "runtime/blend_cap_progress.json"
).strip()
try:
    BLEND_CAP_PROGRESS_INTERVAL_HOURS = float(
        (os.getenv("BLEND_CAP_PROGRESS_INTERVAL_HOURS") or "3").strip()
    )
except Exception:
    BLEND_CAP_PROGRESS_INTERVAL_HOURS = 3.0
BLEND_CAP_PROGRESS_INTERVAL_HOURS = max(0.5, min(24.0, BLEND_CAP_PROGRESS_INTERVAL_HOURS))
# Keitaro report for Blend cap progress / Trillion+ZP cap guards (``interval: today`` + grouping).
BLEND_CAP_REPORT_TIMEZONE = (
    os.getenv("BLEND_CAP_REPORT_TIMEZONE") or "America/Danmarkshavn"
).strip()
_blend_cap_metric = (os.getenv("BLEND_CAP_CLICK_METRIC") or "clicks").strip().lower()
BLEND_CAP_CLICK_METRIC = (
    _blend_cap_metric
    if _blend_cap_metric
    in ("clicks", "campaign_unique_clicks", "stream_unique_clicks", "global_unique_clicks")
    else "clicks"
)
ZEROPARK_BLEND_CAP_SPREADSHEET_ID = (
    os.getenv("ZEROPARK_BLEND_CAP_SPREADSHEET_ID") or BLEND_SHEETS_SPREADSHEET_ID
).strip()
ZEROPARK_BLEND_CAP_SHEET_NAME = (
    os.getenv("ZEROPARK_BLEND_CAP_SHEET_NAME") or "ZP BLEND campaignsID"
).strip()
ZEROPARK_BLEND_CAP_GUARD_ENABLED = (
    os.getenv("ZEROPARK_BLEND_CAP_GUARD_ENABLED", "1").strip().lower()
    not in ("0", "false", "no", "off")
)
try:
    ZEROPARK_BLEND_CAP_GUARD_INTERVAL_MINUTES = int(
        (os.getenv("ZEROPARK_BLEND_CAP_GUARD_INTERVAL_MINUTES") or "20").strip()
    )
except Exception:
    ZEROPARK_BLEND_CAP_GUARD_INTERVAL_MINUTES = 20
ZEROPARK_BLEND_CAP_GUARD_INTERVAL_MINUTES = max(
    5, min(60, ZEROPARK_BLEND_CAP_GUARD_INTERVAL_MINUTES)
)
TRILLION_BLEND_CAP_FOLDER = (os.getenv("TRILLION_BLEND_CAP_FOLDER") or "Blend").strip()
TRILLION_BLEND_CAP_GUARD_ENABLED = (
    os.getenv("TRILLION_BLEND_CAP_GUARD_ENABLED", "1").strip().lower()
    not in ("0", "false", "no", "off")
)
try:
    TRILLION_BLEND_CAP_GUARD_INTERVAL_MINUTES = int(
        (os.getenv("TRILLION_BLEND_CAP_GUARD_INTERVAL_MINUTES") or "20").strip()
    )
except Exception:
    TRILLION_BLEND_CAP_GUARD_INTERVAL_MINUTES = 20
TRILLION_BLEND_CAP_GUARD_INTERVAL_MINUTES = max(
    5, min(60, TRILLION_BLEND_CAP_GUARD_INTERVAL_MINUTES)
)

BLEND_CPC_REFRESH_STATE_PATH = (
    os.getenv("BLEND_CPC_REFRESH_STATE_PATH") or "runtime/blend_cpc_refresh_state.json"
).strip()
BLEND_CPC_REFRESH_SCHEDULER_ENABLED = (
    os.getenv("BLEND_CPC_REFRESH_SCHEDULER_ENABLED", "1").strip().lower() not in ("0", "false", "no")
)
BLEND_CPC_REFRESH_SCHEDULER_TZ = (os.getenv("BLEND_CPC_REFRESH_SCHEDULER_TZ") or "Asia/Jerusalem").strip()
try:
    BLEND_CPC_REFRESH_SCHEDULER_HOUR_LOCAL = int(
        (os.getenv("BLEND_CPC_REFRESH_SCHEDULER_HOUR_LOCAL") or "14").strip()
    )
except Exception:
    BLEND_CPC_REFRESH_SCHEDULER_HOUR_LOCAL = 14
BLEND_CPC_REFRESH_SCHEDULER_HOUR_LOCAL = max(0, min(23, BLEND_CPC_REFRESH_SCHEDULER_HOUR_LOCAL))
try:
    BLEND_CPC_REFRESH_SCHEDULER_MINUTE_LOCAL = int(
        (os.getenv("BLEND_CPC_REFRESH_SCHEDULER_MINUTE_LOCAL") or "0").strip()
    )
except Exception:
    BLEND_CPC_REFRESH_SCHEDULER_MINUTE_LOCAL = 0
BLEND_CPC_REFRESH_SCHEDULER_MINUTE_LOCAL = max(0, min(59, BLEND_CPC_REFRESH_SCHEDULER_MINUTE_LOCAL))
try:
    BLEND_CPC_REFRESH_LOOKBACK_DAYS = int((os.getenv("BLEND_CPC_REFRESH_LOOKBACK_DAYS") or "4").strip())
except Exception:
    BLEND_CPC_REFRESH_LOOKBACK_DAYS = 4
BLEND_CPC_REFRESH_LOOKBACK_DAYS = max(1, min(14, BLEND_CPC_REFRESH_LOOKBACK_DAYS))

# Kelkoo
FEED1_API_KEY = (os.getenv("FEED1_API_KEY") or "").strip()
FEED2_API_KEY = (os.getenv("FEED2_API_KEY") or "").strip()
# Kelkoo feed 5 (Blend tag ``kelkoo5``): ``FEED5_API_KEY`` or legacy aliases in .env
FEED5_API_KEY = (
    os.getenv("FEED5_API_KEY")
    or os.getenv("FEED5_API_KEY_KL")
    or os.getenv("KLFEED3_API_KEY")
    or ""
).strip()
if not FEED5_API_KEY:
    FEED5_API_KEY = (
        _read_env_fallback("FEED5_API_KEY")
        or _read_env_fallback("FEED5_API_KEY_KL")
        or _read_env_fallback("KLFEED3_API_KEY")
        or ""
    ).strip()
FEED5_API_KEY = (FEED5_API_KEY or "").strip().lstrip("= ").strip().strip('"').strip("'")

# argparse / UI feed tags for Blend tooling
BLEND_FEED_CHOICES: tuple[str, ...] = ("kelkoo1", "kelkoo2", "kelkoo5", "adexa", "yadore")

# Daily conversion postbacks UI / CLI (Kelkoo per-geo + Adexa/Yadore flat)
KELKOO_POSTBACK_FEED_TAGS: tuple[str, ...] = ("kelkoo1", "kelkoo2", "kelkoo5")


def kelkoo_postback_tag_to_index(feed_tag: str) -> int:
    """Map postback source tag to Kelkoo feed index (1, 2, 5, …)."""
    return {"kelkoo1": 1, "kelkoo2": 2, "kelkoo5": 5}.get((feed_tag or "").strip().lower(), 0)


def kelkoo_api_key_for_postback_tag(feed_tag: str) -> str:
    idx = kelkoo_postback_tag_to_index(feed_tag)
    by_idx = {1: FEED1_API_KEY, 2: FEED2_API_KEY, 5: FEED5_API_KEY}
    return (by_idx.get(idx) or "").strip()


def kelkoo_raw_report_uses_custom1_subid(*, feed_tag: str = "", feed_index: int = 0) -> bool:
    """Kelkoo2 raw TSV uses ``custom1`` for Keitaro subid; other feeds use ``publisherClickId``."""
    if feed_index == 2 or (feed_tag or "").strip().lower() == "kelkoo2":
        return True
    return False


def discover_kelkoo_feed_api_keys() -> tuple[tuple[int, str], ...]:
    """
    All configured Kelkoo publisher keys: ``FEED1_API_KEY``, ``FEED2_API_KEY``, …
    Any index with a non-empty key is included (gaps allowed, e.g. only ``FEED3``).

    If none are set, falls back to legacy ``KEY_KL`` / ``keyKL`` as feed index ``1``
    (same role as the standalone KLsales report script).
    """
    found: list[tuple[int, str]] = []
    for n in range(1, 33):
        k = (os.getenv(f"FEED{n}_API_KEY") or "").strip()
        if not k and n == 5:
            k = (os.getenv("KLFEED3_API_KEY") or _read_env_fallback("KLFEED3_API_KEY") or "").strip()
        if k:
            found.append((n, k))
    # ``KLFEED3_API_KEY`` / resolved ``FEED5_API_KEY`` when ``FEED5_API_KEY`` env name is unset
    if (FEED5_API_KEY or "").strip() and not any(n == 5 for n, _ in found):
        found.append((5, (FEED5_API_KEY or "").strip()))
    if found:
        found.sort(key=lambda x: x[0])
    if not found:
        legacy = (
            (os.getenv("KEY_KL") or os.getenv("keyKL") or "").strip()
            or _read_env_fallback("KEY_KL")
            or _read_env_fallback("keyKL")
        ).strip()
        if legacy:
            found.append((1, legacy))
    return tuple(found)

KELKOO_ACCOUNT_ID = (os.getenv("KELKOO_ACCOUNT_ID") or "47692679-139a-4232-9170-574f76601827").strip()
KELKOO_ACCOUNT_ID_2 = (os.getenv("KELKOO_ACCOUNT_ID_2") or "").strip() or None
FEED1_KELKOO_ACCOUNT_ID = (os.getenv("FEED1_KELKOO_ACCOUNT_ID") or "").strip() or KELKOO_ACCOUNT_ID
FEED2_KELKOO_ACCOUNT_ID = (os.getenv("FEED2_KELKOO_ACCOUNT_ID") or "").strip() or (KELKOO_ACCOUNT_ID_2 or "")

# Kelkoo feed 5 publisher account (Nipuhim intentix template + Blend kelkoo5). Not feed1.
FEED5_KELKOO_ACCOUNT_ID = (os.getenv("FEED5_KELKOO_ACCOUNT_ID") or "").strip()
if not FEED5_KELKOO_ACCOUNT_ID:
    FEED5_KELKOO_ACCOUNT_ID = _read_env_fallback("FEED5_KELKOO_ACCOUNT_ID")
FEED5_KELKOO_ACCOUNT_ID = (
    (FEED5_KELKOO_ACCOUNT_ID or "").strip() or "696d41b1-490f-4ee1-90c5-113efff53cb6"
)
FEED5_KELKOO_PUBLISHER_SUB_ID = (
    os.getenv("FEED5_KELKOO_PUBLISHER_SUB_ID") or "intentix"
).strip()


def _parse_feed5_merchants_geos() -> tuple[str, ...] | None:
    """Optional geo filter for Kelkoo feed5 merchants API (same idea as ``FEED2_MERCHANTS_GEOS``)."""
    raw = (os.getenv("FEED5_MERCHANTS_GEOS") or os.getenv("KLFEED3_MERCHANTS_GEOS") or "").strip().lower()
    if not raw:
        return None
    out: list[str] = []
    for part in raw.split(","):
        p = part.strip().lower()
        if len(p) >= 2 and p[:2].isalpha():
            out.append(p[:2])
    seen: set[str] = set()
    uniq = []
    for g in out:
        if g not in seen:
            seen.add(g)
            uniq.append(g)
    return tuple(uniq) if uniq else None


FEED5_MERCHANTS_GEOS: tuple[str, ...] | None = _parse_feed5_merchants_geos()


def _parse_feed2_merchants_geos() -> tuple[str, ...] | None:
    """
    Optional comma-separated 2-letter geos for Kelkoo **feed2** merchants API only.
    When set, feed2 merchant downloads (daily fixim, Blend potential, monthly name/url lookups)
    request only these countries—avoids HTTP 403 noise for markets enabled on feed1 but not on feed2.
    Example: ``FEED2_MERCHANTS_GEOS=it,fr,uk``
    """
    raw = (os.getenv("FEED2_MERCHANTS_GEOS") or "").strip().lower()
    if not raw:
        return None
    out: list[str] = []
    for part in raw.split(","):
        p = part.strip().lower()
        if len(p) >= 2 and p[:2].isalpha():
            out.append(p[:2])
    # de-dupe preserving order
    seen: set[str] = set()
    uniq = []
    for g in out:
        if g not in seen:
            seen.add(g)
            uniq.append(g)
    return tuple(uniq) if uniq else None


FEED2_MERCHANTS_GEOS: tuple[str, ...] | None = _parse_feed2_merchants_geos()


def _parse_kelkoo_raw_report_geos() -> tuple[str, ...]:
    """
    Lowercase Kelkoo ``country=`` codes for raw report postbacks (``/publisher/reports/v1/raw``).
    Override with comma-separated ``KELKOO_RAW_REPORT_GEOS``.
    """
    raw = (os.getenv("KELKOO_RAW_REPORT_GEOS") or "").strip().lower()
    if raw:
        out: list[str] = []
        for part in raw.split(","):
            p = part.strip().lower()
            if not p:
                continue
            out.append(p[:2] if len(p) >= 2 and p[:2].isalpha() else p)
        if out:
            return tuple(out)
    return (
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


KELKOO_RAW_REPORT_GEOS: tuple[str, ...] = _parse_kelkoo_raw_report_geos()


def raw_report_geos_for_feed_index(feed_index: int) -> tuple[str, ...]:
    """
    Kelkoo raw report ``country=`` list for a feed index.
    Set ``FEEDn_RAW_REPORT_GEOS`` (comma-separated); otherwise ``KELKOO_RAW_REPORT_GEOS``.
    """
    raw = (os.getenv(f"FEED{feed_index}_RAW_REPORT_GEOS") or "").strip().lower()
    if raw:
        out: list[str] = []
        for part in raw.split(","):
            p = part.strip().lower()
            if not p:
                continue
            out.append(p[:2] if len(p) >= 2 and p[:2].isalpha() else p)
        if out:
            return tuple(out)
    return KELKOO_RAW_REPORT_GEOS


# Zeropark
KEYZP = (os.getenv("KEYZP") or "").strip()
if not KEYZP:
    KEYZP = _read_env_fallback("KEYZP")
# Publisher stats API (``panel/reports/*``) needs domainer id from Zeropark Publisher Team
ZEROPARK_PUBLISHER_DID = (os.getenv("ZEROPARK_PUBLISHER_DID") or os.getenv("ZEROPARK_DID") or "").strip()

# CloseNipuhimAuto: pause all ``generalMehila-*`` Zeropark campaigns (panel clock / TZ).
# Default 23:30 — 1.5h before a previous 01:00 panel-time close that used server hour 23.
ZEROPARK_CLOSE_TZ = (os.getenv("ZEROPARK_CLOSE_TZ") or "Europe/Warsaw").strip()
_zp_close_h = (os.getenv("ZEROPARK_CLOSE_HOUR") or "23").strip()
_zp_close_m = (os.getenv("ZEROPARK_CLOSE_MINUTE") or "30").strip()
try:
    ZEROPARK_CLOSE_HOUR = int(_zp_close_h)
except ValueError:
    ZEROPARK_CLOSE_HOUR = 23
try:
    ZEROPARK_CLOSE_MINUTE = int(_zp_close_m)
except ValueError:
    ZEROPARK_CLOSE_MINUTE = 30
ZEROPARK_CLOSE_HOUR = max(0, min(23, ZEROPARK_CLOSE_HOUR))
ZEROPARK_CLOSE_MINUTE = max(0, min(59, ZEROPARK_CLOSE_MINUTE))

# CloseBlendZpAuto: nightly pause for mapped Zeropark Blend campaigns (defaults = Nipuhim close).
ZEROPARK_BLEND_NIGHTLY_CLOSE_ENABLED = (
    os.getenv("ZEROPARK_BLEND_NIGHTLY_CLOSE_ENABLED", "1").strip().lower()
    not in ("0", "false", "no", "off")
)
ZEROPARK_BLEND_CLOSE_TZ = (os.getenv("ZEROPARK_BLEND_CLOSE_TZ") or ZEROPARK_CLOSE_TZ).strip()
_blend_close_h = (os.getenv("ZEROPARK_BLEND_CLOSE_HOUR") or str(ZEROPARK_CLOSE_HOUR)).strip()
_blend_close_m = (os.getenv("ZEROPARK_BLEND_CLOSE_MINUTE") or str(ZEROPARK_CLOSE_MINUTE)).strip()
try:
    ZEROPARK_BLEND_CLOSE_HOUR = int(_blend_close_h)
except ValueError:
    ZEROPARK_BLEND_CLOSE_HOUR = ZEROPARK_CLOSE_HOUR
try:
    ZEROPARK_BLEND_CLOSE_MINUTE = int(_blend_close_m)
except ValueError:
    ZEROPARK_BLEND_CLOSE_MINUTE = ZEROPARK_CLOSE_MINUTE
ZEROPARK_BLEND_CLOSE_HOUR = max(0, min(23, ZEROPARK_BLEND_CLOSE_HOUR))
ZEROPARK_BLEND_CLOSE_MINUTE = max(0, min(59, ZEROPARK_BLEND_CLOSE_MINUTE))

# SourceKnowledge (affiliate API — X-API-KEY)
SOURCEKNOWLEDGE_API_KEY = (os.getenv("KEYSK") or os.getenv("keySK") or "").strip()
if not SOURCEKNOWLEDGE_API_KEY:
    SOURCEKNOWLEDGE_API_KEY = _read_env_fallback("KEYSK") or _read_env_fallback("keySK")
SOURCEKNOWLEDGE_API_KEY = (SOURCEKNOWLEDGE_API_KEY or "").strip()

# Optional: GET URL template for SK account-level spend in overview (``{from}`` / ``{to}`` = YYYY-MM-DD)
SK_ACCOUNT_STATS_URL = (os.getenv("SK_ACCOUNT_STATS_URL") or "").strip()
# Overview SK fallback (by-publisher per campaign): optional cap after inactive filter. ``0`` = no cap (default).
_sk_cap_raw = (os.getenv("SK_OVERVIEW_MAX_CAMPAIGNS") or "0").strip()
try:
    SK_OVERVIEW_MAX_CAMPAIGNS = int(_sk_cap_raw)
except ValueError:
    SK_OVERVIEW_MAX_CAMPAIGNS = 0
# Optional JSON from ``cli/build_sk_overview_skip_campaign_ids.py`` — extra campaign IDs to skip (merged with ``active: false`` from the API).
SK_OVERVIEW_SKIP_CAMPAIGN_IDS_FILE = (os.getenv("SK_OVERVIEW_SKIP_CAMPAIGN_IDS_FILE") or "").strip()

# Trillion Direct (traffic source — Bearer token on POST ``https://www.trillion.com/api.html``)
KEYTR = (os.getenv("KEYTR") or os.getenv("keyTR") or "").strip()
if not KEYTR:
    KEYTR = _read_env_fallback("KEYTR") or _read_env_fallback("keyTR")
if not KEYTR:
    KEYTR = _read_env_fallback("KEY")
KEYTR = (KEYTR or "").strip()
TRILLION_API_KEY = KEYTR

# Overview snapshot (``GET /api/overview`` reads from disk; rebuild via ``POST /api/overview/refresh`` or scheduler)
OVERVIEW_SNAPSHOT_PATH = (os.getenv("OVERVIEW_SNAPSHOT_PATH") or "").strip()
OVERVIEW_SNAPSHOT_TZ = (os.getenv("OVERVIEW_SNAPSHOT_TZ") or "UTC").strip()
_ov_h = (os.getenv("OVERVIEW_SNAPSHOT_HOUR") or "8").strip()
try:
    OVERVIEW_SNAPSHOT_HOUR = int(_ov_h)
except ValueError:
    OVERVIEW_SNAPSHOT_HOUR = 8
OVERVIEW_SNAPSHOT_HOUR = max(0, min(23, OVERVIEW_SNAPSHOT_HOUR))
OVERVIEW_SCHEDULER_ENABLED = (os.getenv("OVERVIEW_SCHEDULER_ENABLED", "1").strip().lower() not in ("0", "false", "no"))
# At process start: ``missing`` = build snapshot in background only if file absent; ``always`` = always rebuild once; ``off`` = never.
OVERVIEW_SNAPSHOT_BOOTSTRAP = (os.getenv("OVERVIEW_SNAPSHOT_BOOTSTRAP") or "missing").strip().lower()

# Ecomnia (advertiser API keys)
EC_ADVERTISER_KEY = (os.getenv("ADVERTISER_KEY") or "").strip()
if not EC_ADVERTISER_KEY:
    EC_ADVERTISER_KEY = _read_env_fallback("ADVERTISER_KEY")

EC_AUTH_KEY = (os.getenv("AUTH_KEY") or "").strip()
if not EC_AUTH_KEY:
    EC_AUTH_KEY = _read_env_fallback("AUTH_KEY")

EC_SECRET_KEY = (os.getenv("SECRET_KEY") or "").strip()
if not EC_SECRET_KEY:
    EC_SECRET_KEY = _read_env_fallback("SECRET_KEY")

# Ecomnia reporting host (``adv-stats-by-date`` / similar)
ECOMNIA_REPORT_BASE = (os.getenv("ECOMNIA_REPORT_BASE") or "https://report.ecomnia.com").strip().rstrip("/")

# Ecomnia bulk sheet (default: same notebook as legacy bulkEC / ec (2).py)
EC_SHEETS_SPREADSHEET_ID = (
    os.getenv("EC_SHEETS_SPREADSHEET_ID") or "1-kclsSvR7LUtpi-Ymrd9wRYbbmkraP2tGLTrvSnih9c"
).strip()

# Ecomnia console: tab with columns geo / blacklist / whitelist (legacy ``globaList``).
ECOMNIA_GLOBA_LIST_TAB = (os.getenv("ECOMNIA_GLOBA_LIST_TAB") or "globaList").strip()

# Yadore (feed3)
YADORE_API_KEY = (os.getenv("YADORE_API_KEY") or "").strip().lstrip("= ").strip()
if not YADORE_API_KEY:
    YADORE_API_KEY = _read_env_fallback("YADORE_API_KEY")
YADORE_API_KEY = (YADORE_API_KEY or "").strip().lstrip("= ").strip().strip('"').strip("'")
YADORE_PROJECT_ID = (os.getenv("YADORE_PROJECT_ID") or "").strip()
if not YADORE_PROJECT_ID:
    YADORE_PROJECT_ID = _read_env_fallback("YADORE_PROJECT_ID")
YADORE_PROJECT_ID = (YADORE_PROJECT_ID or "").strip().lstrip("= ").strip().strip('"').strip("'")

# Blend → Keitaro (``blend_sync_from_sheet``): Yadore offer URL uses ``{sub_id_3}`` / ``{sub_id_2}`` / ``{subid}``
# (rain inner) when true; otherwise merchant + market from the Blend sheet row are embedded in ``url=`` / ``market=``.
_yad_blend_sub = (os.getenv("BLEND_YADORE_OFFER_USE_SUB_MACROS") or "").strip().lower()
BLEND_YADORE_OFFER_USE_SUB_MACROS = _yad_blend_sub in ("1", "true", "yes", "on")

# Yadore deeplink / checkmon: this publisher site is coupon-inclusive (single probe with isCouponing=true).
_yad_coupon = (os.getenv("YADORE_IS_COUPONING") or "1").strip().lower()
YADORE_IS_COUPONING = _yad_coupon not in ("0", "false", "no", "off")

# Yadore GET /v2/report/detail — comma-separated markets (e.g. de,uk,fr). Multi-market accounts must list each market.
_yad_rd_m = (os.getenv("YADORE_REPORT_DETAIL_MARKETS") or "").strip()
if not _yad_rd_m:
    _yad_rd_m = (_read_env_fallback("YADORE_REPORT_DETAIL_MARKETS") or "").strip()
YADORE_REPORT_DETAIL_MARKETS = [x.strip().lower() for x in _yad_rd_m.split(",") if x.strip()]

# When YADORE_REPORT_DETAIL_MARKETS is unset, conversion/detail sales scans use this list
# (EU core + AU/BE/AT/ES where account traffic commonly converts).
YADORE_DEFAULT_DETAIL_MARKETS = [
    "de",
    "fr",
    "uk",
    "nl",
    "be",
    "au",
    "ca",
    "us",
    "es",
    "at",
    "it",
]

# Adexa (feed4) — site ID + API key (GetMerchant, feeds). Names from .env:
#   ADEXA_SITE_ID or AdexSiteID | ADEXA_API_KEY or KeyAdex or KEY_ADEX
ADEXA_SITE_ID = (os.getenv("ADEXA_SITE_ID") or os.getenv("AdexSiteID") or "").strip()
if not ADEXA_SITE_ID:
    ADEXA_SITE_ID = _read_env_fallback("ADEXA_SITE_ID") or _read_env_fallback("AdexSiteID")
ADEXA_SITE_ID = (ADEXA_SITE_ID or "").strip().lstrip("= ").strip().strip('"').strip("'")

ADEXA_API_KEY = (os.getenv("ADEXA_API_KEY") or os.getenv("KeyAdex") or os.getenv("KEY_ADEX") or "").strip()
if not ADEXA_API_KEY:
    ADEXA_API_KEY = (
        _read_env_fallback("ADEXA_API_KEY")
        or _read_env_fallback("KeyAdex")
        or _read_env_fallback("KEY_ADEX")
    )
ADEXA_API_KEY = (ADEXA_API_KEY or "").strip().lstrip("= ").strip().strip('"').strip("'")

# Shopnomix (feed6) — demand API campaign ids (tile/native vs coupons placements)
SHOPNOMIX_BASE_URL = (os.getenv("SHOPNOMIX_BASE_URL") or "https://r.v2i8b.com").strip().rstrip("/")
SHOPNOMIX_TILE_CAMPAIGN_ID = (
    os.getenv("SHOPNOMIX_TILE_CAMPAIGN_ID")
    or os.getenv("FEED6_SHOPNOMIX_TILE_CAMPAIGN_ID")
    or ""
).strip()
SHOPNOMIX_TILE_REPORTING_ID = (
    os.getenv("SHOPNOMIX_TILE_REPORTING_ID")
    or os.getenv("FEED6_SHOPNOMIX_TILE_REPORTING_ID")
    or ""
).strip()
SHOPNOMIX_COUPONS_CAMPAIGN_ID = (
    os.getenv("SHOPNOMIX_COUPONS_CAMPAIGN_ID")
    or os.getenv("FEED6_SHOPNOMIX_COUPONS_CAMPAIGN_ID")
    or ""
).strip()
SHOPNOMIX_COUPONS_REPORTING_ID = (
    os.getenv("SHOPNOMIX_COUPONS_REPORTING_ID")
    or os.getenv("FEED6_SHOPNOMIX_COUPONS_REPORTING_ID")
    or ""
).strip()
# Bearer tokens for GET /api/v2/reporting/conversion (coupons + tile placements).
SHOPNOMIX_COUPONS_REPORTING_API_TOKEN = (
    os.getenv("SHOPNOMIX_COUPONS_REPORTING_API_TOKEN")
    or os.getenv("SHOPNOMIX_REPORTING_API_TOKEN")
    or os.getenv("FEED6_SHOPNOMIX_COUPONS_REPORTING_API_TOKEN")
    or os.getenv("FEED6_SHOPNOMIX_REPORTING_API_TOKEN")
    or SHOPNOMIX_COUPONS_REPORTING_ID  # legacy: token was stored in *_REPORTING_ID
    or ""
).strip()
SHOPNOMIX_TILE_REPORTING_API_TOKEN = (
    os.getenv("SHOPNOMIX_TILE_REPORTING_API_TOKEN")
    or os.getenv("FEED6_SHOPNOMIX_TILE_REPORTING_API_TOKEN")
    or SHOPNOMIX_TILE_REPORTING_ID  # legacy alias
    or ""
).strip()
# Back-compat name for coupons reporting token.
SHOPNOMIX_REPORTING_API_TOKEN = SHOPNOMIX_COUPONS_REPORTING_API_TOKEN
if not SHOPNOMIX_TILE_CAMPAIGN_ID:
    SHOPNOMIX_TILE_CAMPAIGN_ID = _read_env_fallback("SHOPNOMIX_TILE_CAMPAIGN_ID") or _read_env_fallback(
        "FEED6_SHOPNOMIX_TILE_CAMPAIGN_ID"
    )
if not SHOPNOMIX_COUPONS_CAMPAIGN_ID:
    SHOPNOMIX_COUPONS_CAMPAIGN_ID = _read_env_fallback("SHOPNOMIX_COUPONS_CAMPAIGN_ID") or _read_env_fallback(
        "FEED6_SHOPNOMIX_COUPONS_CAMPAIGN_ID"
    )
SHOPNOMIX_TILE_CAMPAIGN_ID = (SHOPNOMIX_TILE_CAMPAIGN_ID or "").strip()
SHOPNOMIX_TILE_REPORTING_ID = (SHOPNOMIX_TILE_REPORTING_ID or "").strip()
SHOPNOMIX_COUPONS_CAMPAIGN_ID = (SHOPNOMIX_COUPONS_CAMPAIGN_ID or "").strip()
SHOPNOMIX_COUPONS_REPORTING_ID = (SHOPNOMIX_COUPONS_REPORTING_ID or "").strip()
SHOPNOMIX_COUPONS_REPORTING_API_TOKEN = (SHOPNOMIX_COUPONS_REPORTING_API_TOKEN or "").strip()
SHOPNOMIX_TILE_REPORTING_API_TOKEN = (SHOPNOMIX_TILE_REPORTING_API_TOKEN or "").strip()
SHOPNOMIX_REPORTING_API_TOKEN = (SHOPNOMIX_REPORTING_API_TOKEN or "").strip()


def shopnomix_reporting_enabled() -> bool:
    """True when both Shopnomix placements can pull v2 conversion reporting."""
    return bool(
        SHOPNOMIX_TILE_CAMPAIGN_ID
        and SHOPNOMIX_COUPONS_CAMPAIGN_ID
        and SHOPNOMIX_TILE_REPORTING_API_TOKEN
        and SHOPNOMIX_COUPONS_REPORTING_API_TOKEN
    )


def shopnomix_monetization_enabled() -> bool:
    return bool(SHOPNOMIX_TILE_CAMPAIGN_ID and SHOPNOMIX_COUPONS_CAMPAIGN_ID)


# Effinity publisher API (``KEYEFFINITY`` — key is path segment after ``/apiv3/``)
EFFINITY_API_KEY = (os.getenv("KEYEFFINITY") or os.getenv("EFFINITY_API_KEY") or "").strip()
if not EFFINITY_API_KEY:
    EFFINITY_API_KEY = _read_env_fallback("KEYEFFINITY") or _read_env_fallback("EFFINITY_API_KEY")
EFFINITY_API_KEY = (EFFINITY_API_KEY or "").strip().lstrip("= ").strip().strip('"').strip("'")
EFFINITY_API_BASE = (os.getenv("EFFINITY_API_BASE") or "https://api.effinity.fr/apiv3").strip().rstrip("/")

# --- AutoServer (migrated APScheduler + libz automations) ---
# Hourly jobs at minute 0; manual triggers return 202 and run in background (see ``scheduler/autoserver_scheduler.py``).
# Multi-worker Gunicorn: set ``AUTOSERVER_SCHEDULER_ENABLED=0`` on all but one worker (same idea as overview scheduler).
AUTOSERVER_SCHEDULER_ENABLED = (
    os.getenv("AUTOSERVER_SCHEDULER_ENABLED", "1").strip().lower() not in ("0", "false", "no")
)
# Hour gate for even/odd automations (``on_hourly_signal``). Default UTC; set e.g. ``Europe/Warsaw`` for panel time.
AUTOSERVER_SCHEDULER_TZ = (os.getenv("AUTOSERVER_SCHEDULER_TZ") or "UTC").strip()
# Off by default: parallel catch-up on deploy can starve the Gunicorn scheduler worker.
AUTOSERVER_STARTUP_CATCHUP = (
    os.getenv("AUTOSERVER_STARTUP_CATCHUP", "0").strip().lower() in ("1", "true", "yes", "on")
)
_as_hb = (os.getenv("AUTOSERVER_SCHEDULER_HEARTBEAT_PATH") or "").strip()
if _as_hb:
    _as_hb_p = Path(_as_hb)
    AUTOSERVER_SCHEDULER_HEARTBEAT_PATH = (
        _as_hb_p if _as_hb_p.is_absolute() else Path(__file__).resolve().parent / _as_hb
    )
else:
    AUTOSERVER_SCHEDULER_HEARTBEAT_PATH = (
        Path(__file__).resolve().parent / "runtime" / "autoserver_scheduler_heartbeat.json"
    )
AUTOSERVER_RUN_LOG_MAX = int((os.getenv("AUTOSERVER_RUN_LOG_MAX") or "500").strip() or "500")
_as_log_raw = (os.getenv("AUTOSERVER_RUN_LOG_PATH") or "").strip()
if _as_log_raw:
    _as_p = Path(_as_log_raw)
    AUTOSERVER_RUN_LOG_PATH = _as_p if _as_p.is_absolute() else Path(__file__).resolve().parent / _as_p
else:
    AUTOSERVER_RUN_LOG_PATH = Path(__file__).resolve().parent / "data" / "autoserver_run_log.json"
# QualityWL / SK tools workbook (gspread). Defaults to the same id used in ``app.py`` when unset.
SK_TOOLS_SPREADSHEET_ID = (
    os.getenv("SK_TOOLS_SPREADSHEET_ID") or "176wSQDDz9D1APmAXiYPeECwMqCQm3mvMBwgj8MKqmgk"
).strip()

# SourceKnowledge exploration optimizer (SKtrackExploration / SKtrackWL tabs).
# Defaults to the same workbook as ``SK_TOOLS_SPREADSHEET_ID`` when unset.
SK_OPTIMIZER_SHEET_ID = (os.getenv("SK_OPTIMIZER_SHEET_ID") or SK_TOOLS_SPREADSHEET_ID).strip()

# Garbage-source bombardment (hourly delta clicks between optimizer runs).
SK_GARBAGE_CLICK_THRESHOLD = max(
    1, int((os.getenv("SK_GARBAGE_CLICK_THRESHOLD") or "100").strip() or "100")
)
SK_GLOBAL_BLACKLIST_CONTROL_LIST_ID = int(
    (os.getenv("SK_GLOBAL_BLACKLIST_CONTROL_LIST_ID") or "48365").strip() or "48365"
)
# Optional CSV fallback when building efficiency-audit garbage sub list (control list is preferred).
SK_GARBAGE_SUBIDS_CSV = (os.getenv("SK_GARBAGE_SUBIDS_CSV") or "").strip()


def _parse_sk_unmon_skip_campaign_ids() -> tuple[int, ...]:
    """
    Optional comma-separated SK campaign ids to bypass unmon pause checks in
    SK exploration / WL optimizer.
    Example: ``SK_UNMON_SKIP_CAMPAIGN_IDS=380809,381111``
    """
    raw = (os.getenv("SK_UNMON_SKIP_CAMPAIGN_IDS") or "").strip()
    if not raw:
        return ()
    out: list[int] = []
    seen: set[int] = set()
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        try:
            cid = int(p)
        except ValueError:
            continue
        if cid not in seen:
            seen.add(cid)
            out.append(cid)
    return tuple(out)


SK_UNMON_SKIP_CAMPAIGN_IDS: tuple[int, ...] = _parse_sk_unmon_skip_campaign_ids()

# SK exploration WL sync — Keitaro SaleOur/LateSale → SKtrackExploration.wl (daily).
SK_EXPLORATION_WL_SYNC_ENABLED = (
    os.getenv("SK_EXPLORATION_WL_SYNC_ENABLED", "1").strip().lower() not in ("0", "false", "no")
)
SK_EXPLORATION_WL_SYNC_TZ = (
    os.getenv("SK_EXPLORATION_WL_SYNC_TZ") or os.getenv("AUTOSERVER_SCHEDULER_TZ") or "Asia/Jerusalem"
).strip()
try:
    SK_EXPLORATION_WL_SYNC_HOUR_LOCAL = int(
        (os.getenv("SK_EXPLORATION_WL_SYNC_HOUR_LOCAL") or "12").strip()
    )
except Exception:
    SK_EXPLORATION_WL_SYNC_HOUR_LOCAL = 12
SK_EXPLORATION_WL_SYNC_HOUR_LOCAL = max(0, min(23, SK_EXPLORATION_WL_SYNC_HOUR_LOCAL))
try:
    SK_EXPLORATION_WL_SYNC_MINUTE = int((os.getenv("SK_EXPLORATION_WL_SYNC_MINUTE") or "0").strip())
except Exception:
    SK_EXPLORATION_WL_SYNC_MINUTE = 0
SK_EXPLORATION_WL_SYNC_MINUTE = max(0, min(59, SK_EXPLORATION_WL_SYNC_MINUTE))
try:
    SK_EXPLORATION_WL_LOOKBACK_DAYS = int((os.getenv("SK_EXPLORATION_WL_LOOKBACK_DAYS") or "30").strip())
except Exception:
    SK_EXPLORATION_WL_LOOKBACK_DAYS = 30
SK_EXPLORATION_WL_LOOKBACK_DAYS = max(1, min(90, SK_EXPLORATION_WL_LOOKBACK_DAYS))
try:
    SK_EXPLORATION_WL_REACTIVATE_TARGET_BID_USD = float(
        (
            os.getenv("SK_EXPLORATION_WL_REACTIVATE_TARGET_BID_USD")
            or os.getenv("SK_EXPLORATION_WL_REACTIVATE_TARGET_BID")
            or "0.10"
        ).strip()
    )
except Exception:
    SK_EXPLORATION_WL_REACTIVATE_TARGET_BID_USD = 0.10

