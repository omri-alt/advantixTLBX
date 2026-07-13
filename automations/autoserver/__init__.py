from __future__ import annotations

from typing import Any, Callable, Dict, List

# Human-facing metadata for dashboard + API (class_name must match automation class).
AUTOMATION_SPECS: List[Dict[str, Any]] = [
    {
        "class_name": "EcomniaTrackAuto",
        "label": "Ecomnia track sheets",
        "schedule": "Every hour",
    },
    {
        "class_name": "KLFIXoptimize",
        "label": "SK KLFIX new-source optimize",
        "schedule": "Every hour",
    },
    {
        "class_name": "PauseUnmonSK",
        "label": "Pause unmonetized SK (Kelkoo check)",
        "schedule": "Every hour",
    },
    {
        "class_name": "SKExplorationOptimizer",
        "label": "SK exploration + WL optimizer (sheets)",
        "schedule": "Every hour",
    },
    {
        "class_name": "SKExplorationWlSyncAuto",
        "label": "SK exploration WL from Keitaro sales",
        "schedule": "Daily 12:00 Asia/Jerusalem (SK_EXPLORATION_WL_SYNC_* env)",
    },
    {
        "class_name": "KLWL",
        "label": "SK KLWL sources",
        "schedule": "Every even hour",
    },
    {
        "class_name": "QualityWL",
        "label": "QualityWL winrates",
        "schedule": "Every even hour",
    },
    {
        "class_name": "CloseNipuhimAuto",
        "label": "Close Nipuhim (Zeropark)",
        "schedule": "Daily 23:30 Europe/Warsaw (ZEROPARK_CLOSE_* env)",
    },
    {
        "class_name": "CloseBlendZpAuto",
        "label": "Close Blend Zeropark (mapped campaigns)",
        "schedule": "Daily (ZEROPARK_BLEND_CLOSE_* env, defaults to Nipuhim close)",
    },
    {
        "class_name": "CloseNipuhimTrAuto",
        "label": "Close Nipuhim Trillion (hub campaign 94)",
        "schedule": "Daily 01:00 Asia/Jerusalem (TRILLION_HUB_CLOSE_* env)",
    },
    {
        "class_name": "BlendSync2h",
        "label": "Blend sheet → Keitaro sync (monetization + weights)",
        "schedule": "Every 2 hours (even hours)",
    },
    {
        "class_name": "BlendTrCapGuard",
        "label": "Pause Blend Trillion campaigns on cap",
        "schedule": "Every 20 minutes",
        "actions": [
            {"id": "default", "label": "Pause over cap"},
            {"id": "resume_under_cap", "label": "Activate under-cap"},
        ],
    },
    {
        "class_name": "NipuhimUnmonRepair",
        "label": "Nipuhim monetization repair (PLA + Keitaro)",
        "schedule": "Every 2 hours (odd hours)",
    },
    {
        "class_name": "KeitaroFeedBalanceCheckmon",
        "label": "Keitaro feed balance checkmon (notes only)",
        "schedule": "Every 2 hours (even hours)",
    },
    {
        "class_name": "DomainDemandRefresh",
        "label": "Domain-demand guard (weights + Trillion pause)",
        "schedule": "Every 30 minutes (DOMAIN_DEMAND_REFRESH_INTERVAL_MINUTES)",
    },
]


def setup_automations(register_func: Callable[[Any], None]) -> None:
    from automations.autoserver.blend_sync_2h import BlendSync2h
    from automations.autoserver.blend_tr_cap_guard import BlendTrCapGuard
    from automations.autoserver.close_blend_zp import CloseBlendZpAuto
    from automations.autoserver.close_nipuhim import CloseNipuhimAuto
    from automations.autoserver.close_nipuhim_tr import CloseNipuhimTrAuto
    from automations.autoserver.domain_demand_refresh import DomainDemandRefresh
    from automations.autoserver.keitaro_feed_balance_checkmon import KeitaroFeedBalanceCheckmon
    from automations.autoserver.klfix_optimize import KLFIXoptimize
    from automations.autoserver.klwl import KLWL
    from automations.autoserver.ecomnia_track_auto import EcomniaTrackAuto
    from automations.autoserver.nipuhim_unmon_repair import NipuhimUnmonRepair
    from automations.autoserver.pause_unmon_sk import PauseUnmonSK
    from automations.autoserver.quality_wl import QualityWL
    from automations.autoserver.sk_exploration_optimizer import SKExplorationOptimizer
    from automations.autoserver.sk_exploration_wl_sync_auto import SKExplorationWlSyncAuto

    register_func(EcomniaTrackAuto())
    register_func(KLFIXoptimize())
    register_func(PauseUnmonSK())
    register_func(SKExplorationOptimizer())
    register_func(SKExplorationWlSyncAuto())
    register_func(KLWL())
    register_func(QualityWL())
    register_func(CloseNipuhimAuto())
    register_func(CloseBlendZpAuto())
    register_func(CloseNipuhimTrAuto())
    register_func(BlendSync2h())
    register_func(BlendTrCapGuard())
    register_func(NipuhimUnmonRepair())
    register_func(KeitaroFeedBalanceCheckmon())
    register_func(DomainDemandRefresh())
