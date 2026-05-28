from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, FrozenSet, List, Optional, Set


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def compute_report_window(date_str: str) -> tuple[str, str, str]:
    """Return (start_str, end_str, yesterday_str) for a run date (UTC calendar)."""
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    if today.day == 1:
        start_of_period = yesterday.replace(day=1)
        end_of_period = yesterday
    else:
        start_of_period = today.replace(day=1)
        end_of_period = yesterday
    start_str = start_of_period.strftime("%Y-%m-%d")
    end_str = end_of_period.strftime("%Y-%m-%d")
    run_day = datetime.strptime(date_str, "%Y-%m-%d").date()
    yesterday_str = (run_day - timedelta(days=1)).strftime("%Y-%m-%d")
    return start_str, end_str, yesterday_str


def new_run_id(date_str: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    short = uuid.uuid4().hex[:8]
    return f"{date_str}_{stamp}_{short}"


@dataclass
class RunContext:
    run_id: str
    run_dir: Path
    pa: Dict[str, Any]
    date_str: str
    start_str: str
    end_str: str
    yesterday_str: str
    fixim_1: str = ""
    fixim_2: str = ""
    stages: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    argv: List[str] = field(default_factory=list)
    orchestrator_pid: Optional[int] = None

    @property
    def artifacts_dir(self) -> Path:
        p = self.run_dir / "artifacts"
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def stage_logs_dir(self) -> Path:
        p = self.run_dir / "stage_logs"
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def context_path(self) -> Path:
        return self.run_dir / "context.json"

    def partial_geos(self) -> FrozenSet[str]:
        raw = self.pa.get("partial_geos") or []
        if isinstance(raw, (list, tuple, set, frozenset)):
            return frozenset(str(g).strip().lower()[:2] for g in raw if str(g).strip())
        return frozenset()

    def save(self) -> None:
        payload = {
            "run_id": self.run_id,
            "run_dir": str(self.run_dir),
            "pa": self._pa_for_json(),
            "date_str": self.date_str,
            "start_str": self.start_str,
            "end_str": self.end_str,
            "yesterday_str": self.yesterday_str,
            "fixim_1": self.fixim_1,
            "fixim_2": self.fixim_2,
            "stages": self.stages,
            "argv": self.argv,
            "orchestrator_pid": self.orchestrator_pid,
            "updated_at_utc": _utc_now_iso(),
        }
        tmp = self.context_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
        tmp.replace(self.context_path)

    @classmethod
    def load(cls, run_dir: Path) -> RunContext:
        path = run_dir / "context.json"
        data = json.loads(path.read_text(encoding="utf-8"))
        pa = cls._pa_from_json(data.get("pa") or {})
        ctx = cls(
            run_id=str(data["run_id"]),
            run_dir=Path(data.get("run_dir") or run_dir),
            pa=pa,
            date_str=str(data["date_str"]),
            start_str=str(data["start_str"]),
            end_str=str(data["end_str"]),
            yesterday_str=str(data["yesterday_str"]),
            fixim_1=str(data.get("fixim_1") or ""),
            fixim_2=str(data.get("fixim_2") or ""),
            stages=dict(data.get("stages") or {}),
            argv=list(data.get("argv") or []),
            orchestrator_pid=data.get("orchestrator_pid"),
        )
        return ctx

    def _pa_for_json(self) -> Dict[str, Any]:
        pa = dict(self.pa)
        pg = pa.get("partial_geos")
        if isinstance(pg, frozenset):
            pa["partial_geos"] = sorted(pg)
        mo = pa.get("merchant_overrides") or {}
        pa["merchant_overrides"] = {str(k): v for k, v in mo.items()}
        ma = pa.get("merchant_auto_overrides") or {}
        pa["merchant_auto_overrides"] = {str(k): v for k, v in ma.items()}
        ms = pa.get("merchant_skip_replaces") or {}
        pa["merchant_skip_replaces"] = {str(k): v for k, v in ms.items()}
        return pa

    @staticmethod
    def _pa_from_json(pa: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(pa)
        pg = pa.get("partial_geos")
        if isinstance(pg, list):
            out["partial_geos"] = frozenset(str(g).strip().lower()[:2] for g in pg if str(g).strip())
        mo = pa.get("merchant_overrides") or {}
        out["merchant_overrides"] = {int(k): v for k, v in mo.items()}
        ma = pa.get("merchant_auto_overrides") or {}
        out["merchant_auto_overrides"] = {int(k): v for k, v in ma.items()}
        ms = pa.get("merchant_skip_replaces") or {}
        out["merchant_skip_replaces"] = {int(k): v for k, v in ms.items()}
        return out

    def mark_stage(
        self,
        stage_id: str,
        *,
        status: str,
        exit_code: int,
        started_at_utc: str = "",
        finished_at_utc: str = "",
        duration_seconds: float = 0,
        log_path: str = "",
    ) -> None:
        self.stages[stage_id] = {
            "status": status,
            "exit_code": exit_code,
            "started_at_utc": started_at_utc,
            "finished_at_utc": finished_at_utc or _utc_now_iso(),
            "duration_seconds": round(duration_seconds, 2),
            "log_path": log_path,
        }
        self.save()

    def write_json_artifact(self, name: str, data: Any) -> Path:
        path = self.artifacts_dir / name
        path.write_text(json.dumps(data, ensure_ascii=True), encoding="utf-8")
        return path

    def read_json_artifact(self, name: str) -> Any:
        path = self.artifacts_dir / name
        if not path.exists():
            raise FileNotFoundError(f"Missing artifact {name} in {self.run_dir}")
        return json.loads(path.read_text(encoding="utf-8"))

    def artifact_exists(self, name: str) -> bool:
        return (self.artifacts_dir / name).exists()


def init_context_from_argv(
    argv: List[str],
    *,
    runs_root: Path,
    run_id: Optional[str] = None,
    run_dir: Optional[Path] = None,
) -> RunContext:
    from run_daily_workflow import _parse_daily_workflow_argv

    pa = _parse_daily_workflow_argv(argv)
    date_str = str(pa["date_str"])
    start_str, end_str, yesterday_str = compute_report_window(date_str)
    rid = run_id or new_run_id(date_str)
    rdir = run_dir or (runs_root / rid)
    rdir.mkdir(parents=True, exist_ok=True)
    fixim_1 = f"{date_str}_fixim_1"
    fixim_2 = f"{date_str}_fixim_2"
    ctx = RunContext(
        run_id=rid,
        run_dir=rdir,
        pa=pa,
        date_str=date_str,
        start_str=start_str,
        end_str=end_str,
        yesterday_str=yesterday_str,
        fixim_1=fixim_1,
        fixim_2=fixim_2,
        argv=list(argv),
    )
    ctx.save()
    return ctx
