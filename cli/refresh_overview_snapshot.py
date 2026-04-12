#!/usr/bin/env python3
"""Rebuild ``runtime/overview_snapshot.json`` (same work as ``POST /api/overview/refresh``)."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        load_dotenv = None  # type: ignore[assignment]
    if load_dotenv:
        load_dotenv(ROOT / ".env")
    from integrations.overview_snapshot import refresh_overview_snapshot

    _, saved = refresh_overview_snapshot()
    print("OK overview snapshot saved_utc=", saved)


if __name__ == "__main__":
    main()
