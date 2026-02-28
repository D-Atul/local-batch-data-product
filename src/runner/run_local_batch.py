from __future__ import annotations

import argparse
import json
import os
import shutil
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from src.contracts.metrics_contracts import (
    validate_season_summaries,
    validate_team_outcomes,
    validate_venue_neutral_counts,
)
from src.contracts.raw_events_contract import (
    validate_raw_guardrails,  # Stage 1: RAW guardrails (pre-transform)
    validate_raw_events,      # Stage 2: CURATED contract (post-transform)
)
from src.pipelines.build_metrics import (
    build_season_summaries,
    build_team_outcomes,
    build_venue_neutral_counts,
)
from src.pipelines.transform_events import transform_events


# =========================
# Run logging
# =========================


@dataclass
class RunLog:
    run_id: str
    started_at_utc: str
    finished_at_utc: str | None
    inputs: dict
    row_counts: dict
    validations: dict
    outputs: dict
    status: str
    error: str | None


def new_run_id() -> str:
    return uuid.uuid4().hex


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_run_log(log: RunLog, logs_dir: str = "logs") -> str:
    Path(logs_dir).mkdir(parents=True, exist_ok=True)
    path = Path(logs_dir) / f"run_{log.run_id}.json"
    path.write_text(json.dumps(asdict(log), indent=2), encoding="utf-8")
    return str(path)


def run_check(
    *,
    name: str,
    fn: Callable[[], Any],
    log: RunLog,
    severity: str = "HARD",
) -> None:
    assert severity in {"HARD", "SOFT"}
    try:
        fn()
        log.validations[name] = {"result": "PASS", "severity": severity}
    except Exception as e:
        log.validations[name] = {"result": "FAIL", "severity": severity, "error": repr(e)}
        if severity == "HARD":
            raise


# =========================
# Publish helper (staged)
# =========================


def atomic_publish_dir(tmp_dir: Path, final_dir: Path) -> None:
    final_dir.parent.mkdir(parents=True, exist_ok=True)

    staging_final = final_dir.parent / (final_dir.name + "._staging")
    backup_old = final_dir.parent / (final_dir.name + "._old")

    if staging_final.exists():
        shutil.rmtree(staging_final)
    if backup_old.exists():
        shutil.rmtree(backup_old)

    shutil.move(str(tmp_dir), str(staging_final))

    if final_dir.exists():
        os.replace(str(final_dir), str(backup_old))
    os.replace(str(staging_final), str(final_dir))

    if backup_old.exists():
        shutil.rmtree(backup_old)


# =========================
# CLI
# =========================


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m src.runner.run_local_batch",
        description="Run NFL Local Batch pipeline (raw guardrails -> transform -> curated contract -> metrics).",
    )
    p.add_argument(
        "--input_csv",
        default="raw_data/spreadspoke_scores.csv",
        help="Path to raw input CSV (default: raw_data/spreadspoke_scores.csv)",
    )
    p.add_argument(
        "--outputs_dir",
        default="outputs",
        help="Base directory for outputs (default: outputs)",
    )
    p.add_argument(
        "--logs_dir",
        default="logs",
        help="Directory for run logs (default: logs)",
    )
    return p


# =========================
# Runner
# =========================


def main() -> int:
    args = build_arg_parser().parse_args()

    run_id = new_run_id()
    input_path = args.input_csv
    outputs_root = Path(args.outputs_dir)
    logs_dir = args.logs_dir

    log = RunLog(
        run_id=run_id,
        started_at_utc=utc_now_iso(),
        finished_at_utc=None,
        inputs={"events": input_path},
        row_counts={},
        validations={},
        outputs={},
        status="STARTED",
        error=None,
    )

    final_metrics_dir = outputs_root / "metrics"
    tmp_run_root = outputs_root / f".tmp_run_{run_id}"
    tmp_metrics_dir = tmp_run_root / "metrics"

    try:
        # -------------------------
        # 1) Ingest
        # -------------------------
        raw_events = pd.read_csv(input_path)
        log.row_counts["raw_events"] = int(len(raw_events))

        # -------------------------
        # 2) Raw validation (Stage 1: guardrails, pre-transform)
        # -------------------------
        run_check(
            name="raw_guardrails",
            fn=lambda: validate_raw_guardrails(raw_events),
            log=log,
            severity="HARD",
        )

        # -------------------------
        # 3) Transform / Curate
        # -------------------------
        events = transform_events(raw_events)
        log.row_counts["events_curated"] = int(len(events))

        # Optional invariant: row-count preserved by transform
        run_check(
            name="reconciliation_event_count_preserved",
            fn=lambda: (
                (_ for _ in ()).throw(
                    ValueError(
                        f"events row count changed: raw={len(raw_events)} curated={len(events)}"
                    )
                )
                if len(events) != len(raw_events)
                else None
            ),
            log=log,
            severity="HARD",
        )

        # -------------------------
        # 4) Curated validation (Stage 2: strict curated contract, post-transform)
        # -------------------------
        run_check(
            name="curated_events_contract",
            fn=lambda: validate_raw_events(events),
            log=log,
            severity="HARD",
        )

        # -------------------------
        # 5) Build metrics
        # -------------------------
        team_outcomes = build_team_outcomes(events)
        season_summaries = build_season_summaries(events)
        venue_neutral_counts = build_venue_neutral_counts(events)

        log.row_counts["team_outcomes"] = int(len(team_outcomes))
        log.row_counts["season_summaries"] = int(len(season_summaries))
        log.row_counts["venue_neutral_counts"] = int(len(venue_neutral_counts))

        # -------------------------
        # 6) Final validation (output contracts)
        # -------------------------
        run_check(
            name="team_outcomes_contract",
            fn=lambda: validate_team_outcomes(team_outcomes),
            log=log,
            severity="HARD",
        )
        run_check(
            name="season_summaries_contract",
            fn=lambda: validate_season_summaries(season_summaries),
            log=log,
            severity="HARD",
        )
        run_check(
            name="venue_neutral_counts_contract",
            fn=lambda: validate_venue_neutral_counts(venue_neutral_counts),
            log=log,
            severity="HARD",
        )

        # -------------------------
        # 7) Publish + write logs (staged, no partial publish)
        # -------------------------
        if tmp_run_root.exists():
            shutil.rmtree(tmp_run_root)
        tmp_metrics_dir.mkdir(parents=True, exist_ok=True)

        team_outcomes.to_parquet(tmp_metrics_dir / "team_outcomes.parquet", index=False)
        season_summaries.to_parquet(tmp_metrics_dir / "season_summaries.parquet", index=False)
        venue_neutral_counts.to_parquet(
            tmp_metrics_dir / "venue_neutral_counts.parquet", index=False
        )

        atomic_publish_dir(tmp_metrics_dir, final_metrics_dir)

        log.outputs = {
            "team_outcomes": str(final_metrics_dir / "team_outcomes.parquet"),
            "season_summaries": str(final_metrics_dir / "season_summaries.parquet"),
            "venue_neutral_counts": str(final_metrics_dir / "venue_neutral_counts.parquet"),
        }

        log.status = "SUCCESS"
        log.finished_at_utc = utc_now_iso()
        write_run_log(log, logs_dir=logs_dir)

        if tmp_run_root.exists():
            shutil.rmtree(tmp_run_root)

        return 0

    except Exception as e:
        log.status = "FAILED"
        log.error = repr(e)
        log.finished_at_utc = utc_now_iso()
        write_run_log(log, logs_dir=logs_dir)

        if tmp_run_root.exists():
            shutil.rmtree(tmp_run_root)

        raise


if __name__ == "__main__":
    raise SystemExit(main())