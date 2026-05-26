"""
Zeropark API client helpers.

Campaign state actions:
  POST https://panel.zeropark.com/api/campaign/{campaignId}/resume
  POST https://panel.zeropark.com/api/campaign/{campaignId}/pause
  Header: api-token
"""
from typing import Any, Dict, List, Optional

import requests

ZEROPARK_BASE_URL = "https://panel.zeropark.com"


class ZeroparkClientError(Exception):
    def __init__(self, message: str, status_code: Optional[int] = None, response_body: Optional[str] = None):
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(message)


def _decode_json_response(r: requests.Response) -> Dict[str, Any]:
    try:
        return r.json() if r.text else {}
    except Exception:
        return {}


def _campaign_state_action(
    campaign_id: str,
    action: str,
    api_token: str,
    base_url: str = ZEROPARK_BASE_URL,
) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}/api/campaign/{campaign_id}/{action}"
    headers = {"accept": "*/*", "api-token": api_token}
    r = requests.post(url, headers=headers, timeout=30)
    body = _decode_json_response(r)
    if r.status_code != 200:
        raise ZeroparkClientError(
            f"Zeropark API error: {r.status_code}",
            status_code=r.status_code,
            response_body=r.text[:500] if r.text else None,
        )
    return body


def list_campaign_rows_today(
    api_token: str,
    *,
    base_url: str = ZEROPARK_BASE_URL,
    page_size: int = 100,
) -> List[Dict[str, Any]]:
    headers = {"accept": "application/json", "api-token": api_token}
    page = 0
    rows: List[Dict[str, Any]] = []
    while True:
        url = (
            f"{base_url.rstrip('/')}/api/stats/campaign/all"
            f"?interval=TODAY&page={page}&limit={page_size}"
        )
        r = requests.get(url, headers=headers, timeout=45)
        body = _decode_json_response(r)
        if r.status_code != 200:
            raise ZeroparkClientError(
                f"Zeropark API error: {r.status_code}",
                status_code=r.status_code,
                response_body=r.text[:500] if r.text else None,
            )
        elems = body.get("elements") or []
        if not isinstance(elems, list) or not elems:
            break
        rows.extend(x for x in elems if isinstance(x, dict))
        if len(elems) < page_size:
            break
        page += 1
    return rows


def resume_campaign(campaign_id: str, api_token: str, base_url: str = ZEROPARK_BASE_URL) -> Dict[str, Any]:
    return _campaign_state_action(campaign_id, "resume", api_token, base_url=base_url)


def pause_campaign(campaign_id: str, api_token: str, base_url: str = ZEROPARK_BASE_URL) -> Dict[str, Any]:
    return _campaign_state_action(campaign_id, "pause", api_token, base_url=base_url)

