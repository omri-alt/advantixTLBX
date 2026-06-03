#!/usr/bin/env python3
"""Re-apply Blend device stream filters (mobile: country + IS NOT desktop)."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dotenv import load_dotenv

load_dotenv()

from assistance import (
    _geo_for_api,
    assert_blend_stream_filters_sane,
    get_campaign_streams_by_alias,
    parse_blend_stream_geo_channel,
    refresh_all_blend_device_stream_filters,
)
from integrations.keitaro import KeitaroClient


def _print_stream(stream_id: int, label: str) -> None:
    data = KeitaroClient()._session.get(
        KeitaroClient()._api_path(f"streams/{stream_id}"), timeout=30
    ).json()
    print(
        f"=== {label} id={stream_id} filter_or={data.get('filter_or')} "
        f"n={len(data.get('filters') or [])} ==="
    )
    print(json.dumps(data.get("filters"), indent=2))


def main() -> int:
    n, errs = refresh_all_blend_device_stream_filters(2)
    print(f"Refreshed {n} flow(s). Errors: {len(errs)}")
    for e in errs:
        print(f"  {e}")

    streams = get_campaign_streams_by_alias("9Xq9dSMh")
    for ref_id in (140, 142):
        s = next(x for x in streams if int(x.get("id") or 0) == ref_id)
        geo, ch = parse_blend_stream_geo_channel(s.get("name") or "")
        try:
            assert_blend_stream_filters_sane(
                s.get("filters") or [], ch or "", geo_code=_geo_for_api(geo or "")
            )
            print(f"  OK {s.get('name')}: 2 filters (country + IS NOT desktop)")
        except ValueError as ex:
            print(f"  FAIL {s.get('name')}: {ex}")

    _print_stream(142, "ch_mobile")
    return 1 if errs else 0


if __name__ == "__main__":
    raise SystemExit(main())
