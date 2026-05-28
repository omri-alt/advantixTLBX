#!/usr/bin/env python3
"""Run a single daily workflow v2 stage (subprocess entry)."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from workflows.daily_v2.context import RunContext
from workflows.daily_v2.stage_impl import run_stage


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one daily workflow v2 stage.")
    parser.add_argument("--run-dir", required=True, help="Run directory with context.json")
    parser.add_argument("--stage", required=True, help="Stage id from manifest")
    ns = parser.parse_args()
    run_dir = Path(ns.run_dir)
    ctx = RunContext.load(run_dir)
    return run_stage(ctx, ns.stage.strip())


if __name__ == "__main__":
    raise SystemExit(main())
