import pandas as pd
import pytest

from src.contracts.raw_events_contract import (
    validate_raw_guardrails,
    validate_raw_events,  # curated strict contract (post-transform)
)
from src.pipelines.transform_events import transform_events


def _load_raw_sample(n: int = 200) -> pd.DataFrame:
    return pd.read_csv("raw_data/spreadspoke_scores.csv", nrows=n)


def test_raw_guardrails_pass():
    raw = _load_raw_sample()
    validate_raw_guardrails(raw)  # should not raise


def test_raw_guardrails_fail_missing_column():
    raw = _load_raw_sample()
    raw = raw.drop(columns=["team_home"])
    with pytest.raises(ValueError):
        validate_raw_guardrails(raw)


def test_curated_contract_pass_after_transform():
    raw = _load_raw_sample()
    validate_raw_guardrails(raw)

    curated = transform_events(raw)
    validate_raw_events(curated)  # should not raise