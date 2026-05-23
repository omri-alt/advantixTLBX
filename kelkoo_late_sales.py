"""
Late conversion sales (all feeds). Implementation lives in ``late_conversion_sales``.

Re-exports keep imports stable for ``app.py``, schedulers, and CLI tools.
"""
from __future__ import annotations

from late_conversion_sales import (  # noqa: F401
    LateSaleDiffRow,
    apply_yadore_saleour_backlog,
    build_postback_url,
    late_sale_date_window,
    mtd_tab_title,
    prune_legacy_sales_workbook_tabs,
    refresh_mtd_sales_sheets,
    run_late_sales_flow,
    send_postback_gets,
    sheet_title_a1_range,
)

# Legacy name used by scheduler / CLI
prune_old_sales_workbook_tabs = prune_legacy_sales_workbook_tabs

__all__ = [
    "LateSaleDiffRow",
    "build_postback_url",
    "late_sale_date_window",
    "mtd_tab_title",
    "prune_legacy_sales_workbook_tabs",
    "prune_old_sales_workbook_tabs",
    "refresh_mtd_sales_sheets",
    "run_late_sales_flow",
    "send_postback_gets",
    "sheet_title_a1_range",
]
