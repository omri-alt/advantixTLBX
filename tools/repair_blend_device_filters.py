#!/usr/bin/env python3
"""Re-apply Blend desktop/mobile device_type filters (Keitaro flow 142 shape)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv

load_dotenv()

from assistance import (
    get_campaign_streams_by_alias,
    parse_blend_stream_geo_channel,
    refresh_all_blend_device_stream_filters,
)


def main() -> int:
    n, errs = refresh_all_blend_device_stream_filters(2)
    print(f"Refreshed {n} flow(s). Errors: {len(errs)}")
    for e in errs:
        print(f"  {e}")
    streams = get_campaign_streams_by_alias("9Xq9dSMh")
    for ref_id in (140, 142):
        s = next(x for x in streams if int(x.get("id") or 0) == ref_id)
        dt = next((f for f in s.get("filters") or [] if f.get("name") == "device_type"), {})
        print(
            f"  {s.get('name')} id={ref_id}: payload={dt.get('payload')!r} filter_id={dt.get('id')}"
        )
    return 1 if errs else 0


if __name__ == "__main__":
    raise SystemExit(main())
