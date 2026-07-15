"""One-shot: create missing hub 94 geo streams, fix UK→GB filters, wire click-cap weights."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from integrations.domain_demand_guard import restore_hub_stream_weights_from_click_caps
from integrations.keitaro_hub import (
    ensure_hub_child_device_streams,
    ensure_hub_routing_geos,
    load_hub_state,
)


def main() -> int:
    dry_run = "--dry-run" in sys.argv
    print("=== ensure hub routing geos ===")
    ensure = ensure_hub_routing_geos(dry_run=dry_run)
    print(f"created={ensure.get('created')}")
    print(f"refreshed={len(ensure.get('refreshed') or [])}")
    for line in (ensure.get("logs") or [])[-20:]:
        print(" ", line)

    print("\n=== restore hub weights (click-caps) ===")
    restore = restore_hub_stream_weights_from_click_caps(dry_run=dry_run)
    print(f"status={restore.get('status')} updated={restore.get('hub_streams_updated')}")
    for line in (restore.get("logs") or [])[-15:]:
        print(" ", line)

    print("\n=== ensure blend child device streams ===")
    state = load_hub_state()
    child_logs, geos = ensure_hub_child_device_streams(
        dry_run=dry_run, state=state
    )
    print(f"hub_geos_channels={len(geos)}")
    for line in child_logs[-20:]:
        print(" ", line)

    print("\n=== done ===")
    print(json.dumps({"dry_run": dry_run, "created": ensure.get("created")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
