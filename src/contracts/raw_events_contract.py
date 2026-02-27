from __future__ import annotations

import pandera.pandas as pa
from pandera import Column, DataFrameSchema, Check


def raw_events_schema() -> DataFrameSchema:

    return DataFrameSchema(
        {
            "schedule_date": Column(pa.String, nullable=False),
            "schedule_season": Column(pa.Int64, nullable=False),
            "schedule_week": Column(pa.String, nullable=False),
            "schedule_playoff": Column(pa.Bool, nullable=False),
            "team_home": Column(pa.String, nullable=False),
            "team_away": Column(pa.String, nullable=False),
            "score_home": Column(pa.Float64, nullable=True),
            "score_away": Column(pa.Float64, nullable=True),
            "stadium": Column(pa.String, nullable=False),
            "stadium_neutral": Column(pa.Bool, nullable=False),
            "team_favorite_id": Column(pa.String, nullable=True),
            "spread_favorite": Column(pa.Float64, nullable=True),
            "over_under_line": Column(pa.String, nullable=True),
            "weather_temperature": Column(pa.Float64, nullable=True),
            "weather_wind_mph": Column(pa.Float64, nullable=True),
            "weather_humidity": Column(pa.Float64, nullable=True),
            "weather_detail": Column(pa.String, nullable=True),
        },
        strict=True,
        coerce=False,
        checks=[
            Check(
                lambda df: df.duplicated().sum() == 0,
                error="Exact duplicate rows detected",
            ),
        ],
    )


def validate_raw_events(df):
    return raw_events_schema().validate(df, lazy=False)
