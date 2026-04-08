"""
Workflow 1: Create a Keitaro campaign.
Can be triggered by the timing/scheduler; payload can be overridden with Kelkoo-derived data.
"""
import logging
from typing import Any, Dict, Optional

from integrations.keitaro import KeitaroClient, KeitaroClientError

logger = logging.getLogger(__name__)


def default_campaign_payload(
    alias: str,
    name: str,
    *,
    type: str = "position",
    cookies_ttl: int = 24,
    state: str = "active",
    cost_type: str = "CPC",
    cost_value: float = 0,
    cost_currency: str = "USD",
    cost_auto: bool = False,
    group_id: Optional[str] = None,
    traffic_source_id: Optional[int] = None,
    domain_id: Optional[int] = None,
    notes: Optional[str] = None,
    token: Optional[str] = None,
    bind_visitors: Optional[str] = None,
    parameters: Optional[Dict[str, Any]] = None,
    postbacks: Optional[list] = None,
    **extra: Any,
) -> Dict[str, Any]:
    """
    Build the campaign payload per Keitaro API schema.
    Only includes fields that are set (no None sent unless required).
    """
    body: Dict[str, Any] = {
        "alias": alias,
        "type": type,
        "name": name,
        "cookies_ttl": cookies_ttl,
        "state": state,
        "cost_type": cost_type,
        "cost_value": cost_value,
        "cost_currency": cost_currency,
        "cost_auto": cost_auto,
    }
    if group_id is not None:
        body["group_id"] = group_id
    if bind_visitors is not None:
        body["bind_visitors"] = bind_visitors
    if traffic_source_id is not None:
        body["traffic_source_id"] = traffic_source_id
    if parameters is not None:
        body["parameters"] = parameters
    if token is not None:
        body["token"] = token
    if domain_id is not None:
        body["domain_id"] = domain_id
    if postbacks is not None:
        body["postbacks"] = postbacks
    if notes is not None:
        body["notes"] = notes
    body.update(extra)
    return body


def run_create_campaign_workflow(
    alias: str,
    name: str,
    payload_override: Optional[Dict[str, Any]] = None,
    *,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
    **payload_kwargs: Any,
) -> Dict[str, Any]:
    """
    Execute workflow 1: create a Keitaro campaign.
    - If payload_override is provided, it is sent as-is (must match Keitaro schema).
    - Otherwise builds payload from alias, name, and payload_kwargs (e.g. cost_currency, parameters).
    Returns the API response (created campaign data).
    """
    if payload_override is not None:
        payload = payload_override
    else:
        payload = default_campaign_payload(alias, name, **payload_kwargs)

    client = KeitaroClient(base_url=base_url, api_key=api_key)
    result = client.create_campaign(payload)
    logger.info("Campaign created: alias=%s name=%s id=%s", alias, name, result.get("id"))
    return result
