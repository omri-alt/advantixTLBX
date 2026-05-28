#!/usr/bin/env python3
"""
Kelkoo daily workflow v2 — staged subprocess pipeline.

Each stage runs in a fresh Python process so memory is released between heavy steps
(reports/color → merchant pick → PLA → Keitaro). Run state and per-stage logs live under
``runtime/daily_v2/runs/<run_id>/``. Progress is mirrored to ``runtime/workflow_runs/daily.json``
for the Control Center UI.

  python run_daily_workflow_v2.py
  python run_daily_workflow_v2.py --skip-keitaro --geo uk
  python run_daily_workflow_v2.py --from-stage merchant_pick
  python run_daily_workflow_v2.py --only-stage reports_color
  python run_daily_workflow_v2.py --resume-run-dir runtime/daily_v2/runs/2026-05-28_...
  python run_daily_workflow_v2.py --list-stages

Legacy single-process runner: ``python run_daily_workflow.py --legacy`` (or without v2).
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dotenv import load_dotenv

load_dotenv()

from workflows.daily_v2.orchestrator import main

if __name__ == "__main__":
    raise SystemExit(main())
