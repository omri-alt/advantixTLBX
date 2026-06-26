"""
Resolve per-feed Nipuhim / Blend child campaign IDs from hub bootstrap state.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from config import KEITARO_HUB_STATE_PATH

ACCOUNT_TO_NIPUHIM_FEED_KEY: Dict[int, str] = {
    1: "kelkoo1",
    2: "kelkoo2",
    5: "kelkoo5",
}

ACCOUNT_TO_FEED_PREFIX: Dict[int, str] = {
    1: "feed1",
    2: "feed2",
    5: "feed5",
}


def load_hub_state(path: Optional[str] = None) -> Dict[str, Any]:
    p = Path(path or KEITARO_HUB_STATE_PATH)
    if not p.is_file():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def nipuhim_child_campaign_id(
    feed_key: str,
    *,
    state: Optional[Dict[str, Any]] = None,
    state_path: Optional[str] = None,
) -> int:
    """Return Keitaro campaign id for ``nipuhim_{feed_key}`` child."""
    state = state if state is not None else load_hub_state(state_path)
    key = f"nipuhim_{feed_key}"
    child = (state.get("child_campaigns") or {}).get(key) or {}
    cid = child.get("id")
    if cid is None:
        raise ValueError(
            f"No Nipuhim child campaign for feed {feed_key!r} in hub state "
            f"({KEITARO_HUB_STATE_PATH}). Run keitaro_hub_campaign_bootstrap.py --apply first."
        )
    return int(cid)


def nipuhim_child_campaign_id_for_account(
    account: int,
    *,
    state_path: Optional[str] = None,
) -> tuple[int, str, str]:
    """Return (campaign_id, feed_key, feed_prefix) for Kelkoo account 1/2/5."""
    feed_key = ACCOUNT_TO_NIPUHIM_FEED_KEY.get(int(account))
    if not feed_key:
        raise ValueError(f"Nipuhim v2 sync supports account 1, 2, or 5 only (got {account})")
    feed_prefix = ACCOUNT_TO_FEED_PREFIX[int(account)]
    cid = nipuhim_child_campaign_id(feed_key, state_path=state_path)
    return cid, feed_key, feed_prefix
