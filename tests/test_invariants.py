import json
from pathlib import Path

import pandas as pd
import pytest

import src.runner.run_local_batch as runner


def _write_raw_csv(tmp_path: Path, n: int = 200) -> Path:
    df = pd.read_csv("raw_data/spreadspoke_scores.csv", nrows=n)
    p = tmp_path / "input.csv"
    df.to_csv(p, index=False)
    return p


def test_reconciliation_invariant_fail(monkeypatch, tmp_path):
    input_csv = _write_raw_csv(tmp_path)
    outputs_dir = tmp_path / "outputs"
    logs_dir = tmp_path / "logs"

    # Monkeypatch the function as used by the runner module
    def _bad_transform(df):
        return df.iloc[:-1].copy()  # drop one row -> should trigger reconciliation failure

    monkeypatch.setattr(runner, "transform_events", _bad_transform)

    # Run runner.main() with CLI args
    monkeypatch.setattr(
        "sys.argv",
        [
            "python -m src.runner.run_local_batch",
            "--input_csv",
            str(input_csv),
            "--outputs_dir",
            str(outputs_dir),
            "--logs_dir",
            str(logs_dir),
        ],
    )

    with pytest.raises(Exception):
        runner.main()

    # Confirm a FAILED run log exists
    run_logs = list(Path(logs_dir).glob("run_*.json"))
    assert run_logs, "Expected a run log to be written on failure"

    payload = json.loads(run_logs[-1].read_text())
    assert payload["status"] == "FAILED"