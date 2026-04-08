"""
Keitaro Admin API client.

Uses Api-Key header. Base URL can be tracker root or full admin_api/v1 root.
"""
import json
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

from config import (
    KEITARO_ADMIN_BULK_DELETE_OBJECTS,
    KEITARO_API_KEY,
    KEITARO_BASE_URL,
)

logger = logging.getLogger(__name__)

CAMPAIGNS_PATH = "admin_api/v1/campaigns"


class KeitaroClientError(Exception):
    """Raised when a Keitaro API request fails."""

    def __init__(self, message: str, status_code: Optional[int] = None, response_body: Optional[str] = None):
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(message)


class KeitaroClient:
    """Client for Keitaro Admin API (campaigns, streams, offers)."""

    # PHP /admin/?bulk often requires browser session; Api-Key gets 401. Skip after first 401 per process.
    _admin_bulk_api_key_rejected: bool = False

    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None):
        raw = (base_url or KEITARO_BASE_URL).strip().rstrip("/")
        self.base_url = raw
        self.api_key = (api_key or KEITARO_API_KEY or "").strip()
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json", "Api-Key": self.api_key})

    def _api_path(self, path: str) -> str:
        path = path.lstrip("/")
        if self.base_url.endswith("admin_api/v1"):
            return f"{self.base_url}/{path}"
        prefix = self.base_url.rstrip("/") + "/" + "admin_api/v1"
        return f"{prefix}/{path}"

    def _campaigns_url(self) -> str:
        if self.base_url.endswith("admin_api/v1"):
            return f"{self.base_url}/campaigns"
        return f"{self.base_url}/{CAMPAIGNS_PATH}"

    def _tracker_origin(self) -> str:
        """Site root without admin_api/v1 (for /admin/?bulk and similar UI routes)."""
        b = self.base_url.rstrip("/")
        suffix = "/admin_api/v1"
        if b.endswith(suffix):
            return b[: -len(suffix)]
        return b

    def _admin_bulk_url(self) -> str:
        return f"{self._tracker_origin()}/admin/?bulk"

    def _admin_batch_url(self) -> str:
        """Some builds use ?batch instead of ?bulk for the same command array."""
        return f"{self._tracker_origin()}/admin/?batch"

    @staticmethod
    def _bulk_http_ok(data: Any) -> bool:
        """Heuristic: bulk JSON response should not report per-command failure."""
        if data is None:
            return True
        if isinstance(data, list):
            if not data:
                return True
            # UI returns [{"body": {...}, "statusCode": 200, "headers": [...]}}, ...]
            if all(isinstance(x, dict) and "statusCode" in x for x in data):
                for item in data:
                    sc = item.get("statusCode", 200)
                    if isinstance(sc, int) and sc >= 400:
                        return False
                    if item.get("success") is False:
                        return False
                    if item.get("error") or item.get("errors"):
                        return False
                    body = item.get("body")
                    if isinstance(body, dict):
                        if body.get("error") or body.get("errors"):
                            return False
                return True
            for item in data:
                if not isinstance(item, dict):
                    continue
                if item.get("success") is False:
                    return False
                if item.get("error") or item.get("errors"):
                    return False
            return True
        if isinstance(data, dict):
            if data.get("success") is False:
                return False
            if data.get("error") or data.get("errors"):
                return False
            return True
        return True

    @staticmethod
    def _bulk_post_data_variants(object_name: str, offer_id: int) -> List[Dict[str, Any]]:
        """postData dict(s) to try for this bulk object (Kelkoo/Keitaro UI differs per command)."""
        oid = int(offer_id)
        on = (object_name or "").strip().lower()
        if on == "offers.update":
            return [
                {"id": oid, "state": "deleted"},
                {"ids": [oid], "state": "deleted"},
            ]
        return [{"ids": [oid]}]

    def _remove_offer_via_admin_bulk(self, offer_id: int) -> bool:
        """
        Keitaro UI uses POST /admin/?bulk (or ?batch) with a JSON array of commands.

        Soft-delete in panel sets ``state: "deleted"`` via ``offers.update`` (see API response).
        Clone-style commands use ``{"ids": [id]}``.

        Payload shapes:
        - Flat: ``object`` + ``postData`` as a **string** (JSON).
        - Nested: ``params.object`` + ``postData`` as an **object**.
        """
        oid = int(offer_id)
        if KeitaroClient._admin_bulk_api_key_rejected:
            return False
        endpoints = [self._admin_bulk_url(), self._admin_batch_url()]

        for object_name in KEITARO_ADMIN_BULK_DELETE_OBJECTS:
            for post_dict in self._bulk_post_data_variants(object_name, oid):
                post_data_str = json.dumps(post_dict)
                payloads: List[List[Dict[str, Any]]] = [
                    [
                        {
                            "method": "POST",
                            "postData": post_data_str,
                            "object": object_name,
                        }
                    ],
                    [
                        {
                            "method": "POST",
                            "path": "",
                            "params": {"object": object_name},
                            "postData": post_dict,
                        }
                    ],
                ]
                for url in endpoints:
                    for payload in payloads:
                        label = f"{url.split('?')[-1]} {object_name}"
                        try:
                            resp = self._session.post(url, json=payload, timeout=45)
                        except requests.RequestException as e:
                            logger.warning("Keitaro admin %s offer %s: %s", label, oid, e)
                            continue
                        if not resp.ok:
                            if resp.status_code == 401:
                                if not KeitaroClient._admin_bulk_api_key_rejected:
                                    KeitaroClient._admin_bulk_api_key_rejected = True
                                    logger.warning(
                                        "Keitaro /admin/?bulk|batch returned 401 — Api-Key is not accepted there "
                                        "(browser session only). Skipping admin bulk for the rest of this run."
                                    )
                                return False
                            logger.warning(
                                "Keitaro admin %s offer %s: HTTP %s",
                                label,
                                oid,
                                resp.status_code,
                            )
                            continue
                        try:
                            data = resp.json()
                        except Exception:
                            logger.info(
                                "Keitaro removed offer %s via admin %s (non-JSON 200)",
                                oid,
                                label,
                            )
                            return True
                        if self._bulk_http_ok(data):
                            logger.info("Keitaro removed offer %s via admin %s", oid, label)
                            return True
                        logger.warning(
                            "Keitaro admin %s offer %s: response indicates failure: %s",
                            label,
                            oid,
                            str(data)[:400],
                        )
        return False

    def get_campaigns(self, offset: int = 0, limit: int = 100) -> list:
        url = self._campaigns_url()
        params = {"offset": offset, "limit": limit}
        try:
            resp = self._session.get(url, params=params, timeout=30)
        except requests.RequestException as e:
            raise KeitaroClientError(str(e)) from e
        if not resp.ok:
            raise KeitaroClientError(f"Keitaro API error: {resp.status_code}", resp.status_code, resp.text)
        data = resp.json()
        return data if isinstance(data, list) else data.get("campaigns", data) or []

    def get_streams(self, campaign_id: int) -> list:
        cid = int(campaign_id)
        base = self._campaigns_url()
        url = f"{base.rstrip('/')}/{cid}/streams"
        try:
            resp = self._session.get(url, timeout=30)
        except requests.RequestException as e:
            raise KeitaroClientError(str(e)) from e
        if not resp.ok:
            raise KeitaroClientError(f"Keitaro API error: {resp.status_code}", resp.status_code, resp.text)
        data = resp.json()
        return data if isinstance(data, list) else data.get("streams", data) or []

    def create_stream(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = self._api_path("streams")
        try:
            resp = self._session.post(url, json=payload, timeout=30)
        except requests.RequestException as e:
            raise KeitaroClientError(str(e)) from e
        if not resp.ok:
            raise KeitaroClientError(f"Keitaro API error: {resp.status_code}", resp.status_code, resp.text)
        data = resp.json()
        return data if isinstance(data, dict) else {"raw": data}

    def update_stream(self, stream_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        sid = int(stream_id)
        url = self._api_path(f"streams/{sid}")
        try:
            resp = self._session.put(url, json=payload, timeout=30)
        except requests.RequestException as e:
            raise KeitaroClientError(str(e)) from e
        if not resp.ok:
            raise KeitaroClientError(f"Keitaro API error: {resp.status_code}", resp.status_code, resp.text)
        data = resp.json()
        return data if isinstance(data, dict) else {"raw": data}

    def get_offers(self) -> list:
        url = self._api_path("offers")
        try:
            resp = self._session.get(url, timeout=30)
        except requests.RequestException as e:
            raise KeitaroClientError(str(e)) from e
        if not resp.ok:
            raise KeitaroClientError(f"Keitaro API error: {resp.status_code}", resp.status_code, resp.text)
        data = resp.json()
        return data if isinstance(data, list) else data.get("offers", data) or []

    def create_offer(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = self._api_path("offers")
        try:
            resp = self._session.post(url, json=payload, timeout=30)
        except requests.RequestException as e:
            raise KeitaroClientError(str(e)) from e
        if not resp.ok:
            raise KeitaroClientError(f"Keitaro API error: {resp.status_code}", resp.status_code, resp.text)
        data = resp.json()
        return data if isinstance(data, dict) else {"raw": data}

    def update_offer(self, offer_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        oid = int(offer_id)
        url = self._api_path(f"offers/{oid}")
        try:
            resp = self._session.put(url, json=payload, timeout=30)
        except requests.RequestException as e:
            raise KeitaroClientError(str(e)) from e
        if not resp.ok:
            raise KeitaroClientError(f"Keitaro API error: {resp.status_code}", resp.status_code, resp.text)
        data = resp.json()
        return data if isinstance(data, dict) else {"raw": data}

    def archive_offer(self, offer_id: int) -> Dict[str, Any]:
        oid = int(offer_id)
        url = self._api_path(f"offers/{oid}/archive")
        try:
            resp = self._session.post(url, timeout=30)
        except requests.RequestException as e:
            raise KeitaroClientError(str(e)) from e
        if not resp.ok:
            raise KeitaroClientError(f"Keitaro API error: {resp.status_code}", resp.status_code, resp.text)
        data = resp.json()
        return data if isinstance(data, dict) else {"raw": data}

    def delete_offer(self, offer_id: int) -> None:
        """Permanently delete an offer. Detach from streams first or API may error."""
        oid = int(offer_id)
        url = self._api_path(f"offers/{oid}")
        try:
            resp = self._session.delete(url, timeout=30)
        except requests.RequestException as e:
            raise KeitaroClientError(str(e)) from e
        if not resp.ok:
            raise KeitaroClientError(f"Keitaro API error: {resp.status_code}", resp.status_code, resp.text)

    def remove_offer_best_effort(self, offer_id: int) -> bool:
        """
        After detaching from flows, try to remove the offer from the tracker.
        Different Keitaro / nginx setups expose different endpoints; some return 404
        for DELETE .../offers/{id} even when PUT streams works.

        Tries, in order: PHP admin /admin/?bulk (offers.delete, …), DELETE offer,
        POST archive, DELETE archive.
        Returns True if any call succeeds (2xx).
        """
        oid = int(offer_id)
        if self._remove_offer_via_admin_bulk(oid):
            return True
        attempts: List[Tuple[str, Callable[[], Any]]] = [
            ("DELETE offer", lambda: self._session.delete(self._api_path(f"offers/{oid}"), timeout=30)),
            (
                "POST archive",
                lambda: self._session.post(
                    self._api_path(f"offers/{oid}/archive"), json={}, timeout=30
                ),
            ),
            (
                "DELETE archive",
                lambda: self._session.delete(self._api_path(f"offers/{oid}/archive"), timeout=30),
            ),
        ]
        last: Tuple[Optional[int], str] = (None, "")
        for label, do_request in attempts:
            try:
                resp = do_request()
            except requests.RequestException as e:
                last = (None, str(e))
                logger.warning("Keitaro %s offer %s: %s", label, oid, e)
                continue
            if resp.ok:
                logger.info("Keitaro removed offer %s via %s", oid, label)
                return True
            last = (resp.status_code, (resp.text or "")[:300])
            logger.warning("Keitaro %s offer %s: HTTP %s", label, oid, resp.status_code)
        logger.warning("Keitaro could not remove offer %s after all methods; last=%s", oid, last)
        return False

