"""
Gunicorn config for KLblend production.

Scheduler leader election is in ``scheduler/background.py`` (file lock on import).
This file exists so ``Dockerfile`` can pass ``-c gunicorn.conf.py`` consistently.
"""
from __future__ import annotations

# Keep worker boot fast; heavy jobs run on the single scheduler worker only.
raw_env: list[tuple[str, str]] = []
