from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    project_root: Path
    db_path: Path
    timezone: str
    default_workflow_base: str
    run_script_path: Path
    api_host: str
    api_port: int

    @classmethod
    def load(cls) -> "Settings":
        project_root = Path(__file__).resolve().parents[2]
        db_path = Path(os.getenv("RANKING_API_DB_PATH", project_root / "data" / "app" / "ranking_service.db"))
        return cls(
            project_root=project_root,
            db_path=db_path,
            timezone=os.getenv("RANKING_API_TIMEZONE", "America/Edmonton"),
            default_workflow_base=os.getenv(
                "RANKING_API_DEFAULT_WORKFLOW",
                "examples/workflow_us_lgb_2020_port.yaml",
            ),
            run_script_path=Path(os.getenv("RANKING_API_RUN_SCRIPT", project_root / "run_daily_ranking.sh")),
            api_host=os.getenv("RANKING_API_HOST", "0.0.0.0"),
            api_port=int(os.getenv("RANKING_API_PORT", "8000")),
        )
