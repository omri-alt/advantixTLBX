from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, List, Optional

from workflows.daily_v2.context import RunContext


@dataclass(frozen=True)
class StageDef:
    id: str
    title: str
    depends_on: tuple[str, ...]
    skip_if: Optional[Callable[[RunContext], bool]] = None
    fatal: bool = True  # stop pipeline on non-zero exit


def _skip_offers_only(ctx: RunContext) -> bool:
    return bool(ctx.pa.get("offers_and_keitaro_only"))


def _skip_blend(ctx: RunContext) -> bool:
    return bool(ctx.pa.get("skip_blend"))


def _skip_keitaro(ctx: RunContext) -> bool:
    return bool(ctx.pa.get("skip_keitaro"))


def _skip_nipuhim_v2(ctx: RunContext) -> bool:
    if _skip_keitaro(ctx):
        return True
    if ctx.pa.get("skip_nipuhim_v2"):
        return True
    if ctx.pa.get("nipuhim_v2"):
        return False
    from config import NIPUHIM_BLEND_V2_ENABLED

    return not bool(NIPUHIM_BLEND_V2_ENABLED)


def _skip_late_sales(ctx: RunContext) -> bool:
    return bool(ctx.pa.get("skip_late_sales"))


def _skip_postbacks(ctx: RunContext) -> bool:
    return not bool(ctx.pa.get("run_daily_conversion_postbacks"))


def _skip_full_download(ctx: RunContext) -> bool:
    return _skip_offers_only(ctx)


def _skip_hub_rewire(ctx: RunContext) -> bool:
    if _skip_keitaro(ctx):
        return True
    if ctx.pa.get("skip_hub_rewire"):
        return True
    from config import KEITARO_HUB_REWIRE_ENABLED

    return not bool(KEITARO_HUB_REWIRE_ENABLED)


def _skip_blend_v2(ctx: RunContext) -> bool:
    if _skip_blend(ctx):
        return True
    rdw = __import__("run_daily_workflow", fromlist=["blend_hub_v2_enabled"])
    if not rdw.blend_hub_v2_enabled(pa=ctx.pa):
        return True
    if ctx.pa.get("skip_blend_v2"):
        return True
    return False


def _skip_domain_demand(ctx: RunContext) -> bool:
    if _skip_keitaro(ctx):
        return True
    from config import DOMAIN_DEMAND_ENABLED

    return not DOMAIN_DEMAND_ENABLED


def _skip_hub_blend_child_flows(ctx: RunContext) -> bool:
    if _skip_keitaro(ctx):
        return True
    from config import KEITARO_HUB_BLEND_DOMAIN_ENABLED

    return not KEITARO_HUB_BLEND_DOMAIN_ENABLED


def _skip_trillion_activate(ctx: RunContext) -> bool:
    if _skip_keitaro(ctx):
        return True
    from config import DOMAIN_DEMAND_ENABLED, DOMAIN_TRILLION_GUARD_ENABLED, KEYTR

    if not DOMAIN_DEMAND_ENABLED or not DOMAIN_TRILLION_GUARD_ENABLED:
        return True
    return not bool(KEYTR)


STAGES: tuple[StageDef, ...] = (
    StageDef("monthly_log", "0a - Monthly log (yesterday)", ()),
    StageDef("blend_potential", "0b - Blend potential sheets", (), skip_if=_skip_offers_only),
    StageDef("delete_prev_tabs", "0 - Delete previous day tabs", (), skip_if=_skip_offers_only),
    StageDef(
        "download_fixim",
        "1 - Download merchants -> fixim",
        ("delete_prev_tabs",),
        skip_if=_skip_offers_only,
    ),
    StageDef(
        "merchants_pla_alt",
        "1 - Merchants download (PLA alternates only)",
        (),
        skip_if=lambda ctx: not _skip_offers_only(ctx),
    ),
    StageDef("reports_color", "2 - Kelkoo reports & fixim colors", ()),
    StageDef("merchant_pick", "3 - Merchant selection", ("reports_color",)),
    StageDef("pla_offers", "4 - PLA offers -> sheets", ("merchant_pick",)),
    StageDef("combined_offers", "5 - Combined offers tab", ("pla_offers",)),
    StageDef(
        "keitaro_sync",
        "6 - Keitaro sync (legacy HrQBXp)",
        ("combined_offers",),
        skip_if=_skip_keitaro,
        fatal=True,
    ),
    StageDef(
        "keitaro_sync_nipuhim_v2",
        "6b - Nipuhim v2 sync (NIPUHIM-feed*)",
        ("keitaro_sync",),
        skip_if=_skip_nipuhim_v2,
        fatal=True,
    ),
    StageDef(
        "hub_rewire",
        "7d - Hub campaign 94 (nipuhim kelkoo feeds)",
        ("keitaro_sync_nipuhim_v2",),
        skip_if=_skip_hub_rewire,
        fatal=True,
    ),
    StageDef(
        "blend",
        "7 - Blend populate + sync",
        ("combined_offers",),
        skip_if=_skip_blend,
        fatal=True,
    ),
    StageDef(
        "blend_v2",
        "7c - Blend v2 sync (BLEND-feed*)",
        ("blend",),
        skip_if=_skip_blend_v2,
        fatal=True,
    ),
    StageDef(
        "domain_demand",
        "7e - Domain demand bill (hub 94)",
        ("hub_rewire", "blend_v2"),
        skip_if=_skip_domain_demand,
        fatal=False,
    ),
    StageDef(
        "hub_blend_child_flows",
        "7f - Hub blend routing (sub_id_15 on child flows)",
        ("blend_v2", "domain_demand"),
        skip_if=_skip_hub_blend_child_flows,
        fatal=False,
    ),
    StageDef(
        "trillion_activate",
        "7g - Trillion activate/pause (domain demand segments)",
        ("domain_demand", "hub_blend_child_flows"),
        skip_if=_skip_trillion_activate,
        fatal=False,
    ),
    StageDef(
        "late_sales",
        "8 - Late conversion sales",
        ("combined_offers",),
        skip_if=_skip_late_sales,
        fatal=False,
    ),
    StageDef(
        "conversion_postbacks",
        "Postbacks - Daily conversion",
        ("combined_offers",),
        skip_if=_skip_postbacks,
        fatal=False,
    ),
)

STAGE_IDS: tuple[str, ...] = tuple(s.id for s in STAGES)


def stage_by_id(stage_id: str) -> StageDef:
    for s in STAGES:
        if s.id == stage_id:
            return s
    raise KeyError(f"Unknown stage: {stage_id}")


def resolve_stage_order(
    ctx: RunContext,
    *,
    from_stage: Optional[str] = None,
    only_stage: Optional[str] = None,
) -> List[StageDef]:
    """Return stages to run respecting skip rules and optional from/only filters."""
    if only_stage:
        return [stage_by_id(only_stage)]

    out: List[StageDef] = []
    seen_skip = False
    for s in STAGES:
        if s.skip_if and s.skip_if(ctx):
            continue
        out.append(s)

    if from_stage:
        try:
            idx = next(i for i, st in enumerate(out) if st.id == from_stage)
        except StopIteration:
            raise SystemExit(f"Unknown or skipped --from-stage: {from_stage}")
        out = out[idx:]
    return out


def dependencies_met(ctx: RunContext, stage: StageDef) -> bool:
    """True when every required dependency succeeded or was skipped."""
    for dep in stage.depends_on:
        dep_def = stage_by_id(dep)
        if dep_def.skip_if and dep_def.skip_if(ctx):
            continue
        rec = ctx.stages.get(dep) or {}
        st = str(rec.get("status") or "")
        if st in ("success", "skipped"):
            continue
        return False
    return True
