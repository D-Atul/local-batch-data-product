import pandas as pd

from src.contracts.raw_events_contract import validate_raw_guardrails, validate_raw_events
from src.pipelines.transform_events import transform_events
from src.pipelines.build_metrics import (
    build_team_outcomes,
    build_season_summaries,
    build_venue_neutral_counts,
)


def _load_curated(n: int = 500) -> pd.DataFrame:
    raw = pd.read_csv("raw_data/spreadspoke_scores.csv", nrows=n)
    validate_raw_guardrails(raw)
    curated = transform_events(raw)
    validate_raw_events(curated)
    return curated


def test_team_outcomes_grain_unique():
    events = _load_curated()
    df = build_team_outcomes(events)
    assert not df.duplicated(subset=["team", "season"]).any()


def test_season_summaries_grain_unique():
    events = _load_curated()
    df = build_season_summaries(events)
    assert not df.duplicated(subset=["season"]).any()


def test_venue_neutral_counts_grain_unique():
    events = _load_curated()
    df = build_venue_neutral_counts(events)
    assert not df.duplicated(subset=["season"]).any()