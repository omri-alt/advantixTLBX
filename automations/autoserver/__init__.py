from __future__ import annotations

from typing import Any, Callable, Dict, List

# Human-facing metadata for dashboard + API (class_name must match automation class).
AUTOMATION_SPECS: List[Dict[str, str]] = [
    {
        "class_name": "MehilotAuto",
        "label": "Mehilot + Ecomnia tracks",
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
        "schedule": "Hour 23 only",
    },
]


def setup_automations(register_func: Callable[[Any], None]) -> None:
    from automations.autoserver.close_nipuhim import CloseNipuhimAuto
    from automations.autoserver.klfix_optimize import KLFIXoptimize
    from automations.autoserver.klwl import KLWL
    from automations.autoserver.mehilot_auto import MehilotAuto
    from automations.autoserver.pause_unmon_sk import PauseUnmonSK
    from automations.autoserver.quality_wl import QualityWL
    from automations.autoserver.sk_exploration_optimizer import SKExplorationOptimizer

    register_func(MehilotAuto())
    register_func(KLFIXoptimize())
    register_func(PauseUnmonSK())
    register_func(SKExplorationOptimizer())
    register_func(KLWL())
    register_func(QualityWL())
    register_func(CloseNipuhimAuto())
