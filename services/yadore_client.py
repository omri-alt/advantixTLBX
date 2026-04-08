"""Backwards-compatible import shim. Use `integrations.yadore` instead."""

from integrations.yadore import (  # noqa: F401
    YadoreClientError,
    deeplink,
    direct_redirect_probe,
)

