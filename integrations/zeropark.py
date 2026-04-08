"""
Zeropark API client helpers.

Resume campaign:
  POST https://panel.zeropark.com/api/campaign/{campaignId}/resume
  Header: api-token
"""
from typing import Any, Dict, Optional

import requests

ZEROPARK_BASE_URL = "https://panel.zeropark.com"


class ZeroparkClientError(Exception):
    def __init__(self, message: str, status_code: Optional[int] = None, response_body: Optional[str] = None):
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(message)


def resume_campaign(campaign_id: str, api_token: str, base_url: str = ZEROPARK_BASE_URL) -> Dict[str, Any]:
    url = f"{base_url.rstrip('/')}/api/campaign/{campaign_id}/resume"
    headers = {"accept": "*/*", "api-token": api_token}
    r = requests.post(url, headers=headers, timeout=30)
    try:
        body = r.json() if r.text else {}
    except Exception:
        body = {}
    if r.status_code != 200:
        raise ZeroparkClientError(
            f"Zeropark API error: {r.status_code}",
            status_code=r.status_code,
            response_body=r.text[:500] if r.text else None,
        )
    return body

