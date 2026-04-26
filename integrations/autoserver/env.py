"""
Sync AutoServer-style ``os.getenv`` / ``os.environ`` names used by migrated ``libz``
clients with KLblend ``config`` values so credentials stay in ``.env`` only.
Call ``ensure_autoserver_env()`` before importing ``integrations.autoserver.zp`` / ``ec`` / ``sk`` / ``kl_as``.
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


def ensure_autoserver_env() -> None:
    try:
        import config
    except Exception as e:
        logger.warning("ensure_autoserver_env: config not loaded (%s)", e)
        return

    if getattr(config, "KEYZP", None):
        os.environ.setdefault("keyZP", str(config.KEYZP).strip())
    if getattr(config, "SOURCEKNOWLEDGE_API_KEY", None):
        os.environ.setdefault("keySK", str(config.SOURCEKNOWLEDGE_API_KEY).strip())
    if getattr(config, "EC_ADVERTISER_KEY", None):
        os.environ.setdefault("ECadvKey", str(config.EC_ADVERTISER_KEY).strip())
    if getattr(config, "EC_AUTH_KEY", None):
        os.environ.setdefault("ECauthKey", str(config.EC_AUTH_KEY).strip())
    if getattr(config, "EC_SECRET_KEY", None):
        os.environ.setdefault("ECsecretKey", str(config.EC_SECRET_KEY).strip())
    kl = (getattr(config, "FEED1_API_KEY", None) or "").strip()
    if not kl:
        from config import discover_kelkoo_feed_api_keys

        keys = discover_kelkoo_feed_api_keys()
        if keys:
            kl = keys[0][1]
    if kl:
        os.environ.setdefault("keyKL", kl)
