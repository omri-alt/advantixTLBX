"""Backwards-compatible import shim. Use `integrations.keitaro` instead."""

from integrations.keitaro import KeitaroClient, KeitaroClientError  # noqa: F401

    def get_offers(self) -> list:
        """
        Get all offers. GET .../admin_api/v1/offers.
        Returns list of offer objects (id, name, group_id, action_type, action_payload,
        affiliate_network_id, payout_*, state, country, etc.).
        """
        url = self._api_path("offers")
        try:
            resp = self._session.get(url, timeout=30)
        except requests.RequestException as e:
            logger.exception("Keitaro get offers request failed")
            raise KeitaroClientError(str(e)) from e
        if not resp.ok:
            raise KeitaroClientError(
                f"Keitaro API error: {resp.status_code}",
                status_code=resp.status_code,
                response_body=resp.text,
            )
        data = resp.json()
        return data if isinstance(data, list) else data.get("offers", data) or []

    def get_offer(self, offer_id: int) -> Dict[str, Any]:
        """Get single offer. GET .../offers/{id}."""
        oid = int(offer_id)
        url = self._api_path(f"offers/{oid}")
        try:
            resp = self._session.get(url, timeout=30)
        except requests.RequestException as e:
            logger.exception("Keitaro get offer request failed")
            raise KeitaroClientError(str(e)) from e
        if not resp.ok:
            raise KeitaroClientError(
                f"Keitaro API error: {resp.status_code}",
                status_code=resp.status_code,
                response_body=resp.text,
            )
        data = resp.json()
        return data if isinstance(data, dict) else {"raw": data}

    def archive_offer(self, offer_id: int) -> Dict[str, Any]:
        """
        Archive an offer. DELETE .../offers/{id}/archive.
        Moves offer to archive (does not permanently delete). Returns archived offer data.
        """
        oid = int(offer_id)
        url = self._api_path(f"offers/{oid}/archive")
        try:
            resp = self._session.delete(url, timeout=30)
        except requests.RequestException as e:
            logger.exception("Keitaro archive offer request failed")
            raise KeitaroClientError(str(e)) from e
        if not resp.ok:
            raise KeitaroClientError(
                f"Keitaro API error: {resp.status_code}",
                status_code=resp.status_code,
                response_body=resp.text,
            )
        data = resp.json()
        return data if isinstance(data, dict) else {"raw": data}

    def clone_campaign(self, campaign_id: int) -> Dict[str, Any]:
        """
        Clone a campaign. Tries POST .../campaigns/{id}/clone, then if 404/405
        tries POST .../campaigns/clone with body {"campaign_id": id}.
        Returns the new campaign (copy, no statistics).
        """
        cid = int(campaign_id)
        base = self._campaigns_url()

        # Most docs: POST .../admin_api/v1/campaigns/{id}/clone
        url_with_id = f"{base.rstrip('/')}/{cid}/clone"
        try:
            resp = self._session.post(url_with_id, json={}, timeout=30)
        except requests.RequestException as e:
            logger.exception("Keitaro clone campaign request failed")
            raise KeitaroClientError(str(e)) from e

        if resp.ok:
            try:
                data = resp.json()
            except ValueError:
                data = {"raw": resp.text}
            return data if isinstance(data, dict) else {"raw": data}

        # Fallback: some versions use POST .../campaigns/clone with body
        if resp.status_code in (404, 405, 422):
            url_clone = f"{base.rstrip('/')}/clone"
            try:
                resp2 = self._session.post(url_clone, json={"campaign_id": cid}, timeout=30)
            except requests.RequestException:
                pass
            else:
                if resp2.ok:
                    try:
                        data = resp2.json()
                    except ValueError:
                        data = {"raw": resp2.text}
                    return data if isinstance(data, dict) else {"raw": data}

        logger.warning("Clone failed: %s %s -> %s", resp.status_code, url_with_id, resp.text[:300])
        raise KeitaroClientError(
            f"Keitaro API error: {resp.status_code}",
            status_code=resp.status_code,
            response_body=resp.text,
        )

    def create_campaign(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a campaign via POST to admin_api/v1/campaigns.
        Requires ApiKeyAuth. Request body: application/json.
        """
        url = self._campaigns_url()
        logger.debug("POST %s", url)
        try:
            resp = self._session.post(url, json=payload, timeout=30)
        except requests.RequestException as e:
            logger.exception("Keitaro create campaign request failed")
            raise KeitaroClientError(str(e)) from e

        try:
            data = resp.json()
        except ValueError:
            data = None

        if not resp.ok:
            raise KeitaroClientError(
                f"Keitaro API error: {resp.status_code}",
                status_code=resp.status_code,
                response_body=resp.text,
            )

        return data if isinstance(data, dict) else {"raw": data}
