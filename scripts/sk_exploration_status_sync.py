#!/usr/bin/env python3
"""CLI: sync SKtrackExploration / SKtrackWL status from SK API (fast, no optimizer side effects)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env")
except ImportError:
    pass

from integrations.autoserver.env import ensure_autoserver_env

ensure_autoserver_env()

from integrations.autoserver.sk_optimizer import sync_sk_track_status_from_api


def main() -> None:
    print("Syncing SK track sheet status from SourceKnowledge API...")
    result = sync_sk_track_status_from_api()
    print(json.dumps(result, indent=2, ensure_ascii=False))
    for tab_key, tab in (result.get("tabs") or {}).items():
        print(
            f"{tab_key}: processed={tab.get('processed')} "
            f"changed={tab.get('changed')} errors={tab.get('errors')}"
        )


if __name__ == "__main__":
    main()
