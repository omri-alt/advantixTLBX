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

# Google Sheets — Blend workflow spreadsheet (Blend tab + potential sheets)
BLEND_SHEETS_SPREADSHEET_ID = (
    os.getenv("BLEND_SHEETS_SPREADSHEET_ID") or "1h9lBPTREEJO9VVvj6wctCgCOn3YcwJBGIk_MBwXw-xY"
).strip()

# Kelkoo
FEED1_API_KEY = (os.getenv("FEED1_API_KEY") or "").strip()
FEED2_API_KEY = (os.getenv("FEED2_API_KEY") or "").strip()

KELKOO_ACCOUNT_ID = (os.getenv("KELKOO_ACCOUNT_ID") or "47692679-139a-4232-9170-574f76601827").strip()
KELKOO_ACCOUNT_ID_2 = (os.getenv("KELKOO_ACCOUNT_ID_2") or "").strip() or None
FEED1_KELKOO_ACCOUNT_ID = (os.getenv("FEED1_KELKOO_ACCOUNT_ID") or "").strip() or KELKOO_ACCOUNT_ID
FEED2_KELKOO_ACCOUNT_ID = (os.getenv("FEED2_KELKOO_ACCOUNT_ID") or "").strip() or (KELKOO_ACCOUNT_ID_2 or "")

# Zeropark
KEYZP = (os.getenv("KEYZP") or "").strip()
if not KEYZP:
    KEYZP = _read_env_fallback("KEYZP")

# SourceKnowledge (affiliate API — X-API-KEY)
SOURCEKNOWLEDGE_API_KEY = (os.getenv("KEYSK") or os.getenv("keySK") or "").strip()
if not SOURCEKNOWLEDGE_API_KEY:
    SOURCEKNOWLEDGE_API_KEY = _read_env_fallback("KEYSK") or _read_env_fallback("keySK")
SOURCEKNOWLEDGE_API_KEY = (SOURCEKNOWLEDGE_API_KEY or "").strip()

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

# Yadore (feed3)
YADORE_API_KEY = (os.getenv("YADORE_API_KEY") or "").strip().lstrip("= ").strip()
if not YADORE_API_KEY:
    YADORE_API_KEY = _read_env_fallback("YADORE_API_KEY")
YADORE_API_KEY = (YADORE_API_KEY or "").strip().lstrip("= ").strip().strip('"').strip("'")
YADORE_PROJECT_ID = (os.getenv("YADORE_PROJECT_ID") or "").strip()
if not YADORE_PROJECT_ID:
    YADORE_PROJECT_ID = _read_env_fallback("YADORE_PROJECT_ID")
YADORE_PROJECT_ID = (YADORE_PROJECT_ID or "").strip().lstrip("= ").strip().strip('"').strip("'")

