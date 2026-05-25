#!/usr/bin/env python3
"""Review legacy Blend rows using Keitaro offer/device EPC and update the Blend sheet."""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path
from typing import Dict, Optional, Tuple

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency in some environments
    load_dotenv = None  # type: ignore[assignment]

from integrations.blend_device import (  # noqa: E402
    DEVICE_MODE_DESKTOP_ONLY,
    DEVICE_MODE_LEGACY,
    DEVICE_MODE_MOBILE_ONLY,
    DEVICE_MODE_SPLIT,
    normalize_device_mode,
    split_click_cap_weights,
)
from integrations.blend_legacy_review import (  # noqa: E402
    DEFAULT_DATE_FROM,
    DEFAULT_DATE_TO,
    BlendReviewRow,
    OfferDeviceStat,
    RowDecision,
    ensure_review_headers,
    fetch_blend_offer_device_epc,
    get_review_service,
    load_blend_review_sheet,
    suggest_row_decision,
    update_blend_review_row,
)

VALID_MODES = (
    DEVICE_MODE_LEGACY,
    DEVICE_MODE_SPLIT,
    DEVICE_MODE_DESKTOP_ONLY,
    DEVICE_MODE_MOBILE_ONLY,
)


def _parse_date(value: str) -> date:
    return date.fromisoformat((value or "").strip())


def _fmt_float(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return f"{float(value):.6f}".rstrip("0").rstrip(".")


def _prompt(prompt: str, default: Optional[str] = None) -> str:
    suffix = f" [{default}]" if default not in (None, "") else ""
    return input(f"{prompt}{suffix}: ").strip()


def _prompt_mode(default: str) -> str:
    while True:
        raw = _prompt(
            "device_mode (legacy/split/desktop_only/mobile_only)",
            default,
        )
        mode = normalize_device_mode(raw or default)
        if mode in VALID_MODES:
            return mode
        print("Invalid mode; choose one of:", ", ".join(VALID_MODES))


def _prompt_float(prompt: str, default: float) -> float:
    while True:
        raw = _prompt(prompt, _fmt_float(default))
        if not raw:
            return float(default)
        try:
            return float(raw)
        except ValueError:
            print("Enter a number or press Enter to accept the default.")


def _stat_text(stat: Optional[OfferDeviceStat]) -> str:
    if not stat:
        return "no stats"
    clicks = stat.denominator if stat.denominator > 0 else 0
    click_label = stat.denominator_key or "click metric missing"
    return (
        f"revenue={stat.revenue:.4f}, "
        f"{click_label}={clicks}, "
        f"epc={stat.epc:.6f} ({stat.epc_source})"
    )


def _current_decision(row: BlendReviewRow, suggested: RowDecision) -> RowDecision:
    return RowDecision(
        device_mode=row.current_device_mode,
        weight_desktop=row.current_weight_desktop,
        weight_mobile=row.current_weight_mobile,
        cpc_desktop=suggested.cpc_desktop,
        cpc_mobile=suggested.cpc_mobile,
    )


def _edit_decision(row: BlendReviewRow, suggested: RowDecision) -> RowDecision:
    mode = _prompt_mode(suggested.device_mode)
    if mode == DEVICE_MODE_LEGACY:
        weight_desktop, weight_mobile = split_click_cap_weights(row.click_cap, mode)
        print("Legacy mode ignores custom split in sync; using 50/50 of clickCap.")
    elif mode == DEVICE_MODE_DESKTOP_ONLY:
        weight_desktop, weight_mobile = row.click_cap, 0.0
    elif mode == DEVICE_MODE_MOBILE_ONLY:
        weight_desktop, weight_mobile = 0.0, row.click_cap
    else:
        while True:
            weight_desktop = _prompt_float("desktop click cap (weight_desktop)", suggested.weight_desktop)
            weight_mobile = _prompt_float(
                "mobile click cap (weight_mobile)",
                row.click_cap - weight_desktop if suggested.weight_mobile <= 0 else suggested.weight_mobile,
            )
            if abs((weight_desktop + weight_mobile) - row.click_cap) <= 0.0001:
                break
            print(f"Desktop + mobile must equal clickCap {_fmt_float(row.click_cap)}.")
    cpc_desktop = _prompt("cpc_desktop", suggested.cpc_desktop or row.cpc_desktop_raw).strip()
    cpc_mobile = _prompt("cpc_mobile", suggested.cpc_mobile or row.cpc_mobile_raw).strip()
    return RowDecision(
        device_mode=mode,
        weight_desktop=weight_desktop,
        weight_mobile=weight_mobile,
        cpc_desktop=cpc_desktop,
        cpc_mobile=cpc_mobile,
    )


def _print_row_context(
    row: BlendReviewRow,
    stat_d: Optional[OfferDeviceStat],
    stat_m: Optional[OfferDeviceStat],
    suggested: RowDecision,
) -> None:
    print("")
    print("=" * 88)
    print(
        f"Row {row.sheet_row} | {row.brand_name} | geo={row.geo} | feed={row.feed_tag} "
        f"| auto={row.auto_flag} | clickCap={_fmt_float(row.click_cap)}"
    )
    print(f"Offer: {row.offer_name}")
    print(
        "Current: "
        f"mode={row.current_device_mode}, "
        f"weight_desktop={_fmt_float(row.current_weight_desktop)}, "
        f"weight_mobile={_fmt_float(row.current_weight_mobile)}, "
        f"cpc_desktop={row.cpc_desktop_raw or '-'}, "
        f"cpc_mobile={row.cpc_mobile_raw or '-'}"
    )
    print(f"Desktop stats: {_stat_text(stat_d)}")
    print(f"Mobile stats:  {_stat_text(stat_m)}")
    print(
        "Suggested: "
        f"mode={suggested.device_mode}, "
        f"weight_desktop={_fmt_float(suggested.weight_desktop)}, "
        f"weight_mobile={_fmt_float(suggested.weight_mobile)}, "
        f"cpc_desktop={suggested.cpc_desktop or '-'}, "
        f"cpc_mobile={suggested.cpc_mobile or '-'}"
    )


def _choose_decision(
    row: BlendReviewRow,
    suggested: RowDecision,
) -> Optional[RowDecision]:
    while True:
        action = _prompt(
            "Action: Enter=apply suggestion, e=edit, k=keep current mode, s=skip, q=quit",
            "",
        ).lower()
        if not action:
            return suggested
        if action == "e":
            return _edit_decision(row, suggested)
        if action == "k":
            return _current_decision(row, suggested)
        if action == "s":
            return None
        if action == "q":
            raise KeyboardInterrupt
        print("Choose Enter, e, k, s, or q.")


def _preview_row(
    row: BlendReviewRow,
    stat_d: Optional[OfferDeviceStat],
    stat_m: Optional[OfferDeviceStat],
    suggested: RowDecision,
) -> None:
    _print_row_context(row, stat_d, stat_m, suggested)
    print("Preview only; no sheet updates in dry-run mode.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Review Blend legacy rows one by one and write device fields back to the Blend sheet."
    )
    parser.add_argument("--apply", action="store_true", help="Write confirmed values to the Google Sheet.")
    parser.add_argument(
        "--all-rows",
        action="store_true",
        help="Review all Blend rows instead of only rows currently treated as legacy.",
    )
    parser.add_argument("--include-no-stats", action="store_true", help="Do not skip rows with no Keitaro offer stats.")
    parser.add_argument("--geo", default="", help="Only review one geo, e.g. uk or fr.")
    parser.add_argument("--start-row", type=int, default=2, help="First worksheet row number to review.")
    parser.add_argument("--limit", type=int, default=0, help="Max rows to review.")
    parser.add_argument("--date-from", default=DEFAULT_DATE_FROM.isoformat(), help="Report start date (YYYY-MM-DD).")
    parser.add_argument("--date-to", default=DEFAULT_DATE_TO.isoformat(), help="Report end date (YYYY-MM-DD).")
    args = parser.parse_args()

    if load_dotenv:
        load_dotenv(ROOT / ".env")

    d_from = _parse_date(args.date_from)
    d_to = _parse_date(args.date_to)
    if d_from > d_to:
        raise SystemExit("--date-from must be <= --date-to")

    service = get_review_service()
    if args.apply:
        ensure_review_headers(service)
    sheet = load_blend_review_sheet(
        service,
        only_geo=(args.geo or None),
        legacy_only=not args.all_rows,
        start_row=args.start_row,
        limit=(args.limit or None),
    )
    stats = fetch_blend_offer_device_epc(d_from=d_from, d_to=d_to)

    if not sheet.rows:
        print("No Blend rows matched the current filters.")
        return

    reviewed = 0
    updated = 0
    skipped = 0
    no_stats = 0
    print(
        f"Loaded {len(sheet.rows)} row(s); mode={'apply' if args.apply else 'dry-run'}; "
        f"date range={d_from.isoformat()}..{d_to.isoformat()}"
    )

    try:
        for row in sheet.rows:
            stat_d = stats.get((row.offer_name, "desktop"))
            stat_m = stats.get((row.offer_name, "mobile"))
            has_stats = bool(
                (stat_d and stat_d.denominator > 0) or (stat_m and stat_m.denominator > 0)
            )
            if not has_stats and not args.include_no_stats:
                no_stats += 1
                print(f"Skipping row {row.sheet_row} ({row.offer_name}) - no offer/device stats.")
                continue

            suggested = suggest_row_decision(row, stats)
            reviewed += 1

            if not args.apply:
                _preview_row(row, stat_d, stat_m, suggested)
                continue

            _print_row_context(row, stat_d, stat_m, suggested)
            decision = _choose_decision(row, suggested)
            if decision is None:
                skipped += 1
                continue
            updates = update_blend_review_row(service, sheet, row, decision, dry_run=False)
            updated += 1
            print(
                f"Updated row {row.sheet_row}: "
                + ", ".join(f"{k}={v or '-'}" for k, v in updates.items())
            )
    except KeyboardInterrupt:
        print("\nStopped by operator.")

    print("")
    print(
        f"Summary: reviewed={reviewed}, updated={updated}, skipped={skipped}, "
        f"no_stats_skipped={no_stats}, total_candidates={len(sheet.rows)}"
    )


if __name__ == "__main__":
    main()
