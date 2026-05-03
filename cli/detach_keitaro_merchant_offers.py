#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from detach_keitaro_merchant_offers import main  # noqa: E402


if __name__ == "__main__":
    main()
