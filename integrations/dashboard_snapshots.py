"""
Refresh Control Center dashboard caches after deploy or on a cron.

Covers:
- homepage overview snapshot (revenue / costs / affiliation MTD)
- domain-demand fill (hub 94)
- Blend click-cap progress
"""
from __future__ import annotations

import logging
import threading
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def overview_snapshot_is_stale_schema() -> bool:
    """True when the on-disk overview snapshot is missing or lacks affiliation revenue."""
    from integrations.overview_snapshot import read_snapshot_for_api, snapshot_path

    if not snapshot_path().exists():
        return True
    payload, _ = read_snapshot_for_api()
    if not isinstance(payload, dict):
        return True
    aff = payload.get("affiliation_revenue")
    return not isinstance(aff, dict)


def refresh_dashboard_snapshots(*, reason: str = "cli") -> Dict[str, Any]:
    """
    Rebuild dashboard caches. Overview refresh is queued (subprocess); domain-demand
    and Blend-cap run in-process (they are shorter).
    """
    out: Dict[str, Any] = {"reason": reason, "jobs": []}

    try:
        from integrations.overview_snapshot import queue_overview_refresh, snapshot_path

        st = queue_overview_refresh(reason=reason)
        out["jobs"].append(
            {
                "id": "overview",
                "status": st.get("status"),
                "queued": st.get("queued"),
                "path": str(snapshot_path()),
                "error": st.get("error"),
            }
        )
    except Exception as e:
        logger.exception("Dashboard snapshot: overview queue failed")
        out["jobs"].append({"id": "overview", "status": "error", "error": str(e)})

    try:
        from integrations.domain_demand_progress import refresh_domain_demand_progress

        dd = refresh_domain_demand_progress(reason=reason, resume_trillion=False)
        out["jobs"].append(
            {
                "id": "domain_demand",
                "status": dd.get("status") or "ok",
                "error": dd.get("error"),
            }
        )
    except Exception as e:
        logger.exception("Dashboard snapshot: domain-demand refresh failed")
        out["jobs"].append({"id": "domain_demand", "status": "error", "error": str(e)})

    try:
        from integrations.blend_cap_progress import refresh_blend_cap_progress

        bc = refresh_blend_cap_progress(reason=reason)
        out["jobs"].append(
            {
                "id": "blend_cap",
                "status": bc.get("status") or "ok",
                "error": bc.get("error"),
            }
        )
    except Exception as e:
        logger.exception("Dashboard snapshot: blend-cap refresh failed")
        out["jobs"].append({"id": "blend_cap", "status": "error", "error": str(e)})

    return out


def start_dashboard_snapshot_bootstrap() -> None:
    """
    After process start (deploy), refresh dashboard caches that are missing or schema-stale.

    ``OVERVIEW_SNAPSHOT_BOOTSTRAP``: ``missing`` (default, also treats old overview
    snapshots without affiliation revenue as missing), ``always``, ``off``.
    """
    import os
    import time

    from config import OVERVIEW_SNAPSHOT_BOOTSTRAP

    mode = (OVERVIEW_SNAPSHOT_BOOTSTRAP or "missing").strip().lower()
    if mode in ("0", "off", "false", "no"):
        return
    if os.getenv("FLASK_DEBUG") == "1" and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        return

    def run() -> None:
        time.sleep(3.0)
        always = mode in ("always", "force", "yes", "1", "true")
        need_overview = always or overview_snapshot_is_stale_schema()
        need_domain = always
        need_blend = always
        try:
            from integrations.domain_demand_progress import _CACHE_PATH as dd_path

            if not dd_path.is_file():
                need_domain = True
        except Exception:
            need_domain = True
        try:
            from integrations.blend_cap_progress import cache_path

            if not cache_path().is_file():
                need_blend = True
        except Exception:
            need_blend = True

        if not (need_overview or need_domain or need_blend):
            logger.info("Dashboard snapshot bootstrap skipped (caches present, schema current)")
            return

        logger.info(
            "Dashboard snapshot bootstrap starting (overview=%s domain=%s blend_cap=%s)",
            need_overview,
            need_domain,
            need_blend,
        )
        if need_overview:
            try:
                from integrations.overview_snapshot import queue_overview_refresh

                queue_overview_refresh(reason="bootstrap")
            except Exception:
                logger.exception("Dashboard bootstrap: overview queue failed")
        if need_domain:
            try:
                from integrations.domain_demand_progress import refresh_domain_demand_progress

                refresh_domain_demand_progress(reason="bootstrap", resume_trillion=False)
            except Exception:
                logger.exception("Dashboard bootstrap: domain-demand failed")
        if need_blend:
            try:
                from integrations.blend_cap_progress import refresh_blend_cap_progress

                refresh_blend_cap_progress(reason="bootstrap")
            except Exception:
                logger.exception("Dashboard bootstrap: blend-cap failed")

    threading.Thread(target=run, name="dashboard-snapshot-bootstrap", daemon=True).start()
    logger.info("Dashboard snapshot bootstrap thread scheduled (mode=%s)", mode)
