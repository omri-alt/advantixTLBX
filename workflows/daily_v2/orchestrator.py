"""Daily workflow v2 orchestrator: subprocess per stage, resumable state, UI progress."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from workflows.daily_v2.context import RunContext, init_context_from_argv, new_run_id
from workflows.daily_v2.manifest import STAGES, dependencies_met, resolve_stage_order, stage_by_id
from workflows.daily_v2.stage_impl import STAGE_HANDLERS

RUNS_ROOT = _ROOT / "runtime" / "daily_v2" / "runs"
WORKFLOW_RUNS_DIR = _ROOT / "runtime" / "workflow_runs"
WORKFLOW_KEY = "daily"
STAGE_RUNNER = _ROOT / "cli" / "run_daily_stage.py"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _aggregate_log(ctx: RunContext, max_chars: int = 20000) -> str:
    parts: List[str] = []
    for s in STAGES:
        rec = ctx.stages.get(s.id) or {}
        if rec.get("status") not in ("success", "failed", "skipped", "running"):
            continue
        lp = rec.get("log_path") or ""
        if not lp:
            continue
        p = Path(lp)
        if p.exists():
            parts.append(f"--- {s.title} ({rec.get('status')}) ---\n")
            parts.append(p.read_text(encoding="utf-8", errors="replace"))
            parts.append("\n")
    text = "\n".join(parts).strip()
    if len(text) > max_chars:
        text = "…\n" + text[-max_chars:]
    return text


def _stages_ui_list(ctx: RunContext) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for s in STAGES:
        rec = dict(ctx.stages.get(s.id) or {})
        skipped = bool(s.skip_if and s.skip_if(ctx))
        if not rec and skipped:
            rec = {"status": "skipped", "exit_code": 0}
        out.append(
            {
                "id": s.id,
                "title": s.title,
                "status": rec.get("status") or ("skipped" if skipped else "pending"),
                "exit_code": rec.get("exit_code"),
                "duration_seconds": rec.get("duration_seconds"),
                "started_at_utc": rec.get("started_at_utc"),
                "finished_at_utc": rec.get("finished_at_utc"),
            }
        )
    return out


def publish_workflow_run(
    ctx: RunContext,
    *,
    status: str,
    exit_code: Optional[int],
    started_at_utc: str,
    command: List[str],
    current_stage: str = "",
) -> None:
    WORKFLOW_RUNS_DIR.mkdir(parents=True, exist_ok=True)
    path = WORKFLOW_RUNS_DIR / f"{WORKFLOW_KEY}.json"
    finished = status != "running"
    payload: Dict[str, Any] = {
        "workflow_key": WORKFLOW_KEY,
        "workflow_title": "Nipuhim-Keitaro workflow (v2 staged)",
        "engine": "daily_v2",
        "run_id": ctx.run_id,
        "run_dir": str(ctx.run_dir),
        "status": status,
        "exit_code": exit_code,
        "started_at_utc": started_at_utc,
        "finished_at_utc": _utc_now_iso() if finished else "",
        "duration_seconds": 0,
        "current_stage": current_stage,
        "stages": _stages_ui_list(ctx),
        "command": command,
        "args": ctx.argv,
        "pid": ctx.orchestrator_pid,
        "log": _aggregate_log(ctx),
    }
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    tmp.replace(path)


def _run_stage_subprocess(ctx: RunContext, stage_id: str) -> tuple[int, str]:
    log_path = ctx.stage_logs_dir / f"{stage_id}.log"
    cmd = [sys.executable, str(STAGE_RUNNER), "--run-dir", str(ctx.run_dir), "--stage", stage_id]
    popen_kwargs: Dict[str, Any] = {
        "cwd": str(_ROOT),
        "stdout": subprocess.PIPE,
        "stderr": subprocess.STDOUT,
        "text": True,
    }
    if os.name == "nt":
        creationflags = 0
        creationflags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        if creationflags:
            popen_kwargs["creationflags"] = creationflags
    else:
        popen_kwargs["start_new_session"] = True

    proc = subprocess.Popen(cmd, **popen_kwargs)
    out, _ = proc.communicate()
    log_path.write_text(out or "", encoding="utf-8")
    if out:
        print(out, end="" if out.endswith("\n") else "\n")
    return int(proc.returncode or 0), str(log_path)


def run_orchestrator(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Kelkoo daily workflow v2 (staged subprocesses, resumable)."
    )
    parser.add_argument("--list-stages", action="store_true", help="Print stage ids and exit")
    parser.add_argument("--from-stage", default="", help="Start at this stage id")
    parser.add_argument("--only-stage", default="", help="Run only this stage id")
    parser.add_argument("--resume-run-dir", default="", help="Resume existing run directory")
    parser.add_argument("--continue-on-error", action="store_true", help="Keep going after a failed stage")
    ns, passthrough = parser.parse_known_args(argv)

    if ns.list_stages:
        for s in STAGES:
            print(s.id, "-", s.title)
        return 0

    started_iso = _utc_now_iso()
    command = [sys.executable, str(_ROOT / "run_daily_workflow_v2.py")] + list(argv)

    if ns.resume_run_dir:
        run_dir = Path(ns.resume_run_dir)
        if not run_dir.is_absolute():
            candidate = RUNS_ROOT / run_dir.name
            run_dir = candidate if candidate.exists() else run_dir
        ctx = RunContext.load(run_dir.resolve())
        if passthrough:
            from run_daily_workflow import _parse_daily_workflow_argv

            ctx.argv = list(passthrough)
            ctx.pa = _parse_daily_workflow_argv(ctx.argv)
            ctx.save()
    else:
        RUNS_ROOT.mkdir(parents=True, exist_ok=True)
        ctx = init_context_from_argv(passthrough, runs_root=RUNS_ROOT)

    ctx.orchestrator_pid = os.getpid()
    ctx.save()

    stages_to_run = resolve_stage_order(
        ctx,
        from_stage=ns.from_stage.strip() or None,
        only_stage=ns.only_stage.strip() or None,
    )

    publish_workflow_run(
        ctx,
        status="running",
        exit_code=None,
        started_at_utc=started_iso,
        command=command,
    )

    print(f"Daily workflow v2 — run_id={ctx.run_id}")
    print(f"Run directory: {ctx.run_dir}")
    print(f"Stages to execute: {', '.join(s.id for s in stages_to_run)}")
    print()

    overall_rc = 0
    run_started = time.time()

    resume_mode = bool(ns.resume_run_dir.strip())

    for stage in stages_to_run:
        if resume_mode and not ns.from_stage.strip() and not ns.only_stage.strip():
            prev = ctx.stages.get(stage.id) or {}
            if prev.get("status") == "success":
                print(f"[resume] skip completed stage: {stage.title}")
                continue

        if stage.skip_if and stage.skip_if(ctx):
            ctx.mark_stage(
                stage.id,
                status="skipped",
                exit_code=0,
                started_at_utc=_utc_now_iso(),
                finished_at_utc=_utc_now_iso(),
                duration_seconds=0,
                log_path="",
            )
            publish_workflow_run(
                ctx,
                status="running",
                exit_code=None,
                started_at_utc=started_iso,
                command=command,
                current_stage=stage.id,
            )
            print(f"[skip] {stage.title}")
            continue

        if not dependencies_met(ctx, stage):
            print(f"[blocked] {stage.title} — dependencies not satisfied")
            ctx.mark_stage(
                stage.id,
                status="failed",
                exit_code=2,
                started_at_utc=_utc_now_iso(),
                log_path="",
            )
            overall_rc = 2
            if stage.fatal and not ns.continue_on_error:
                break
            continue

        if stage.id not in STAGE_HANDLERS:
            print(f"Unknown stage handler: {stage.id}")
            return 2

        stage_started = time.time()
        started_stage_iso = _utc_now_iso()
        ctx.mark_stage(
            stage.id,
            status="running",
            exit_code=None,
            started_at_utc=started_stage_iso,
            log_path="",
        )
        publish_workflow_run(
            ctx,
            status="running",
            exit_code=None,
            started_at_utc=started_iso,
            command=command,
            current_stage=stage.id,
        )

        rc, log_path = _run_stage_subprocess(ctx, stage.id)
        duration = time.time() - stage_started
        status = "success" if rc == 0 else "failed"
        ctx.mark_stage(
            stage.id,
            status=status,
            exit_code=rc,
            started_at_utc=started_stage_iso,
            finished_at_utc=_utc_now_iso(),
            duration_seconds=duration,
            log_path=log_path,
        )
        publish_workflow_run(
            ctx,
            status="running",
            exit_code=None,
            started_at_utc=started_iso,
            command=command,
            current_stage=stage.id,
        )

        if rc != 0:
            overall_rc = rc
            if stage.fatal and not ns.continue_on_error:
                print(f"Stopping pipeline: stage {stage.id} failed (exit {rc}).")
                break

    final_status = "success" if overall_rc == 0 else "failed"
    latest_ptr = RUNS_ROOT.parent / "latest_run.json"
    latest_ptr.write_text(
        json.dumps({"run_id": ctx.run_id, "run_dir": str(ctx.run_dir), "status": final_status}, indent=2),
        encoding="utf-8",
    )
    publish_workflow_run(
        ctx,
        status=final_status,
        exit_code=overall_rc,
        started_at_utc=started_iso,
        command=command,
        current_stage="",
    )
    elapsed = time.time() - run_started
    print()
    print(f"Daily workflow v2 finished: status={final_status} exit={overall_rc} duration={elapsed:.1f}s")
    print(f"State: {ctx.run_dir}")
    return overall_rc


def main() -> int:
    return run_orchestrator(sys.argv[1:])


if __name__ == "__main__":
    raise SystemExit(main())
