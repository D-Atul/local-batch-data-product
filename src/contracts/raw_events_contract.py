from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera import Column, DataFrameSchema, Check


# =========================
# Stage 1: Raw Guardrail
# Light validation on raw ingested data BEFORE transform.
# Only checks:
#   - required columns exist
#   - key identifiers are not null
# No type enforcement — types are not yet normalised.
# =========================

RAW_REQUIRED_COLUMNS = [
    "schedule_season",
    "schedule_week",
    "schedule_playoff",
    "team_home",
    "team_away",
    "score_home",
    "score_away",
    "spread_favorite",
    "over_under_line",
    "stadium_neutral",
    "stadium",
    "schedule_date",
    "team_favorite_id",
    "weather_temperature",
    "weather_wind_mph",
    "weather_humidity",
    "weather_detail",
]

RAW_NOT_NULL_COLUMNS = [
    "schedule_season",
    "team_home",
    "team_away",
]


def validate_raw_guardrails(df: pd.DataFrame) -> None:
    """
    Raw guardrail validation (pre-transform).
    Raises ValueError if required columns are missing or key identifiers contain nulls.
    """
    # 1) Required columns exist
    missing = [c for c in RAW_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required raw columns: {missing}")

    # 2) Key identifiers not null
    for col in RAW_NOT_NULL_COLUMNS:
        null_count = df[col].isna().sum()
        if null_count > 0:
            raise ValueError(f"Key identifier '{col}' has {null_count} null value(s) in raw data")


# =========================
# Stage 2: Curated Contract
# Strict validation on transformed/curated data AFTER transform.
# Checks:
#   - correct types on all columns
#   - nullability rules per column
#   - no exact duplicate rows
#   - no extra columns (strict=True)
# =========================

def curated_events_schema() -> DataFrameSchema:
    return DataFrameSchema(
        {
            "schedule_date":      Column(pa.String,  nullable=False),
            "schedule_season":    Column(pa.Int64,   nullable=False),
            "schedule_week":      Column(pa.String,  nullable=False),
            "schedule_playoff":   Column(pa.Bool,    nullable=False),
            "team_home":          Column(pa.String,  nullable=False),
            "team_away":          Column(pa.String,  nullable=False),
            "score_home":         Column(pa.Float64, nullable=True),
            "score_away":         Column(pa.Float64, nullable=True),
            "stadium":            Column(pa.String,  nullable=False),
            "stadium_neutral":    Column(pa.Bool,    nullable=False),
            "team_favorite_id":   Column(pa.String,  nullable=True),
            "spread_favorite":    Column(pa.Float64, nullable=True),
            "over_under_line":    Column(pa.String,  nullable=True),
            "weather_temperature":Column(pa.Float64, nullable=True),
            "weather_wind_mph":   Column(pa.Float64, nullable=True),
            "weather_humidity":   Column(pa.Float64, nullable=True),
            "weather_detail":     Column(pa.String,  nullable=True),
        },
        strict=True,
        coerce=False,
        checks=[
            Check(
                lambda df: df.duplicated().sum() == 0,
                error="Exact duplicate rows detected in curated data",
            ),
        ],
    )


def validate_raw_events(df: pd.DataFrame):
    """
    Curated contract validation (post-transform).
    Enforces strict types, nullability, and no duplicates.
    """
    return curated_events_schema().validate(df, lazy=False)