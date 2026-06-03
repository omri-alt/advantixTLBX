"""
Blend device routing: CPC floors, device_mode classification, clickCap split.

Mobile flows accept Keitaro device types ``mobile phone`` and ``tablet`` (both required).
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from config import BLEND_DEVICE_CAP_SPLIT_BY_CPC, BLEND_DEVICE_CPC_MIN
from workflows.kelkoo_daily import _has_cpc_cell_value, _parse_cpc_value

DEVICE_MODE_LEGACY = "legacy"
DEVICE_MODE_SPLIT = "split"
DEVICE_MODE_DESKTOP_ONLY = "desktop_only"
DEVICE_MODE_MOBILE_ONLY = "mobile_only"

VALID_DEVICE_MODES = frozenset(
    {
        DEVICE_MODE_LEGACY,
        DEVICE_MODE_SPLIT,
        DEVICE_MODE_DESKTOP_ONLY,
        DEVICE_MODE_MOBILE_ONLY,
    }
)

# Keitaro stream filter payloads (device type names from tracker docs).
KEITARO_DEVICE_DESKTOP = ["desktop"]
KEITARO_DEVICE_MOBILE = ["mobile phone", "tablet"]


def cpcs_from_merchant_dict(m: Dict[str, Any]) -> Tuple[float, float, bool, bool]:
    """Desktop/mobile CPC and whether each channel has a meaningful feed value."""
    d_raw = m.get("merchantEstimatedCpc")
    if d_raw is None:
        d_raw = m.get("cpc_desktop")
    m_raw = m.get("merchantMobileEstimatedCpc")
    if m_raw is None:
        m_raw = m.get("cpc_mobile")
    has_d = _has_cpc_cell_value(d_raw)
    has_m = _has_cpc_cell_value(m_raw)
    return _parse_cpc_value(d_raw), _parse_cpc_value(m_raw), has_d, has_m


def cpcs_from_strings(
    desktop_raw: Any,
    mobile_raw: Any,
) -> Tuple[float, float, bool, bool]:
    has_d = _has_cpc_cell_value(desktop_raw)
    has_m = _has_cpc_cell_value(mobile_raw)
    return _parse_cpc_value(desktop_raw), _parse_cpc_value(mobile_raw), has_d, has_m


def classify_device_mode(
    desktop_cpc: float,
    mobile_cpc: float,
    *,
    has_desktop: bool,
    has_mobile: bool,
    min_cpc: Optional[float] = None,
) -> str:
    """
    ``legacy`` when neither channel meets the Blend device floor (still blend if on sheet).
    Otherwise ``split``, ``desktop_only``, or ``mobile_only``.
    """
    floor = BLEND_DEVICE_CPC_MIN if min_cpc is None else float(min_cpc)
    elig_d = has_desktop and desktop_cpc >= floor
    elig_m = has_mobile and mobile_cpc >= floor
    if elig_d and elig_m:
        return DEVICE_MODE_SPLIT
    if elig_d:
        return DEVICE_MODE_DESKTOP_ONLY
    if elig_m:
        return DEVICE_MODE_MOBILE_ONLY
    return DEVICE_MODE_LEGACY


def split_click_cap_weights(
    total_cap: float,
    device_mode: str,
    *,
    desktop_cpc: float = 0.0,
    mobile_cpc: float = 0.0,
) -> Tuple[float, float]:
    """
    Return (weight_desktop, weight_mobile) for Keitaro stream weighting.
    ``legacy``: split cap 50/50 (unknown device CPC). ``desktop_only`` / ``mobile_only``: one side only.
    ``split``: majority to higher CPC when ``BLEND_DEVICE_CAP_SPLIT_BY_CPC`` is on.
    """
    cap = max(0.0, float(total_cap))
    mode = normalize_device_mode(device_mode)
    if mode == DEVICE_MODE_LEGACY:
        half = cap / 2.0
        return half, cap - half
    if mode == DEVICE_MODE_DESKTOP_ONLY:
        return cap, 0.0
    if mode == DEVICE_MODE_MOBILE_ONLY:
        return 0.0, cap
    # split
    if not BLEND_DEVICE_CAP_SPLIT_BY_CPC:
        half = cap / 2.0
        return half, cap - half
    d = max(0.0, desktop_cpc)
    m = max(0.0, mobile_cpc)
    total = d + m
    if total <= 0:
        half = cap / 2.0
        return half, cap - half
    w_d = round(cap * d / total)
    w_m = cap - w_d
    return float(w_d), float(w_m)


def normalize_device_mode(mode: str) -> str:
    m = (mode or "").strip().lower()
    if m in VALID_DEVICE_MODES:
        return m
    return DEVICE_MODE_LEGACY


def device_mode_from_sheet_row(
    mode_raw: str,
    click_cap: float,
    desktop_raw: Any = None,
    mobile_raw: Any = None,
) -> Tuple[str, float, float]:
    """
    Resolve device_mode and weights for a Blend row.
    Empty mode + CPC columns → classify; empty mode without CPC → legacy.
    """
    mode = (mode_raw or "").strip().lower()
    if mode in VALID_DEVICE_MODES and mode != DEVICE_MODE_LEGACY:
        w_d, w_m = split_click_cap_weights(
            click_cap,
            mode,
            desktop_cpc=_parse_cpc_value(desktop_raw),
            mobile_cpc=_parse_cpc_value(mobile_raw),
        )
        return mode, w_d, w_m
    if mode == DEVICE_MODE_LEGACY:
        w_d, w_m = split_click_cap_weights(click_cap, DEVICE_MODE_LEGACY)
        return DEVICE_MODE_LEGACY, w_d, w_m
    d, m, has_d, has_m = cpcs_from_strings(desktop_raw, mobile_raw)
    classified = classify_device_mode(d, m, has_desktop=has_d, has_mobile=has_m)
    w_d, w_m = split_click_cap_weights(click_cap, classified, desktop_cpc=d, mobile_cpc=m)
    return classified, w_d, w_m


def blend_stream_weight_for_channel(
    device_mode: str,
    channel: str,
    *,
    click_cap: float,
    weight_desktop: float,
    weight_mobile: float,
) -> Optional[float]:
    """
    Weight for attaching an offer to a Keitaro stream (``desktop`` / ``mobile``).
    Returns None if the offer must not be on that stream.
    """
    mode = normalize_device_mode(device_mode)
    ch = (channel or "").strip().lower()
    if ch not in ("desktop", "mobile"):
        return None
    if mode == DEVICE_MODE_LEGACY:
        if ch == "desktop":
            return weight_desktop if weight_desktop > 0 else click_cap / 2.0
        return weight_mobile if weight_mobile > 0 else click_cap / 2.0
    if mode == DEVICE_MODE_DESKTOP_ONLY:
        return weight_desktop if ch == "desktop" and weight_desktop > 0 else None
    if mode == DEVICE_MODE_MOBILE_ONLY:
        return weight_mobile if ch == "mobile" and weight_mobile > 0 else None
    # split
    if ch == "desktop":
        return weight_desktop if weight_desktop > 0 else None
    return weight_mobile if weight_mobile > 0 else None


def potential_device_columns(
    desktop_raw: Any,
    mobile_raw: Any,
    *,
    default_click_cap: float = 50.0,
) -> Tuple[str, str, str, str, str]:
    """
    Build sheet cells: cpc_desktop, cpc_mobile, device_mode, weight_desktop, weight_mobile.
    """
    d, m, has_d, has_m = cpcs_from_strings(desktop_raw, mobile_raw)
    mode = classify_device_mode(d, m, has_desktop=has_d, has_mobile=has_m)
    w_d, w_m = split_click_cap_weights(default_click_cap, mode, desktop_cpc=d, mobile_cpc=m)
    d_s = str(desktop_raw).strip() if has_d else ""
    m_s = str(mobile_raw).strip() if has_m else ""
    return d_s, m_s, mode, str(int(w_d) if w_d == int(w_d) else w_d), str(int(w_m) if w_m == int(w_m) else w_m)
