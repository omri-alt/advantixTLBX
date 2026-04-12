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

# Google Sheets — Kelkoo late-sales / click sales report ("KLtools").
# One workbook for both feeds (feed1 + feed2). Tabs look like
# ``SalesReport_7days-generated-YYYY-MM-DD`` and ``SalesReport_YYYY-MM-DD_generated-YYYY-MM-DD``.
# Automation (planned): drop stale dated tabs and keep a monthly log, same pattern as Nipuhim
# ``{month}_log_1`` / ``_log_2`` beside the daily notebook.
KELKOO_LATE_SALES_SPREADSHEET_ID = (
    os.getenv("KELKOO_LATE_SALES_SPREADSHEET_ID") or "1hVhQ_BfrKOh8OCojTFkuTLKeU1KLbK_kWgm1BmJFteU"
).strip()

# Late-sale postbacks (GET): ``subid`` = Kelkoo ``click_id``, ``payout`` = ``sale_value_usd``, ``status`` = LateSale
LATE_SALES_POSTBACK_BASE = (
    os.getenv("LATE_SALES_POSTBACK_BASE") or "http://207.154.244.157/2ea006b/postback"
).strip()

# Google Sheets — Blend workflow spreadsheet (Blend tab + potential sheets)
BLEND_SHEETS_SPREADSHEET_ID = (
    os.getenv("BLEND_SHEETS_SPREADSHEET_ID") or "1h9lBPTREEJO9VVvj6wctCgCOn3YcwJBGIk_MBwXw-xY"
).strip()


def _parse_blend_potential_feeds() -> tuple[str, ...]:
    """
    Comma-separated Kelkoo feeds used for Blend ``potentialKelkoo*`` refresh and
    ``populate_blend_from_potential`` in the daily workflow. Each run still requires
    the matching ``FEED*_API_KEY`` or that feed is skipped with a log line.
    """
    raw = (os.getenv("BLEND_POTENTIAL_FEEDS") or "kelkoo1,kelkoo2").strip().lower()
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    allowed = {"kelkoo1", "kelkoo2"}
    out = tuple(p for p in parts if p in allowed)
    return out if out else ("kelkoo1", "kelkoo2")


BLEND_POTENTIAL_FEEDS: tuple[str, ...] = _parse_blend_potential_feeds()

# Kelkoo
FEED1_API_KEY = (os.getenv("FEED1_API_KEY") or "").strip()
FEED2_API_KEY = (os.getenv("FEED2_API_KEY") or "").strip()

KELKOO_ACCOUNT_ID = (os.getenv("KELKOO_ACCOUNT_ID") or "47692679-139a-4232-9170-574f76601827").strip()
KELKOO_ACCOUNT_ID_2 = (os.getenv("KELKOO_ACCOUNT_ID_2") or "").strip() or None
FEED1_KELKOO_ACCOUNT_ID = (os.getenv("FEED1_KELKOO_ACCOUNT_ID") or "").strip() or KELKOO_ACCOUNT_ID
FEED2_KELKOO_ACCOUNT_ID = (os.getenv("FEED2_KELKOO_ACCOUNT_ID") or "").strip() or (KELKOO_ACCOUNT_ID_2 or "")


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

# Zeropark
KEYZP = (os.getenv("KEYZP") or "").strip()
if not KEYZP:
    KEYZP = _read_env_fallback("KEYZP")
# Publisher stats API (``panel/reports/*``) needs domainer id from Zeropark Publisher Team
ZEROPARK_PUBLISHER_DID = (os.getenv("ZEROPARK_PUBLISHER_DID") or os.getenv("ZEROPARK_DID") or "").strip()

# SourceKnowledge (affiliate API — X-API-KEY)
SOURCEKNOWLEDGE_API_KEY = (os.getenv("KEYSK") or os.getenv("keySK") or "").strip()
if not SOURCEKNOWLEDGE_API_KEY:
    SOURCEKNOWLEDGE_API_KEY = _read_env_fallback("KEYSK") or _read_env_fallback("keySK")
SOURCEKNOWLEDGE_API_KEY = (SOURCEKNOWLEDGE_API_KEY or "").strip()

# Optional: GET URL template for SK account-level spend in overview (``{from}`` / ``{to}`` = YYYY-MM-DD)
SK_ACCOUNT_STATS_URL = (os.getenv("SK_ACCOUNT_STATS_URL") or "").strip()

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

# Yadore (feed3)
YADORE_API_KEY = (os.getenv("YADORE_API_KEY") or "").strip().lstrip("= ").strip()
if not YADORE_API_KEY:
    YADORE_API_KEY = _read_env_fallback("YADORE_API_KEY")
YADORE_API_KEY = (YADORE_API_KEY or "").strip().lstrip("= ").strip().strip('"').strip("'")
YADORE_PROJECT_ID = (os.getenv("YADORE_PROJECT_ID") or "").strip()
if not YADORE_PROJECT_ID:
    YADORE_PROJECT_ID = _read_env_fallback("YADORE_PROJECT_ID")
YADORE_PROJECT_ID = (YADORE_PROJECT_ID or "").strip().lstrip("= ").strip().strip('"').strip("'")

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

