from __future__ import annotations
import pandera as pa
import pandera as pa

from pandera import Column, DataFrameSchema, Check


def team_outcomes_schema() -> DataFrameSchema:
    return DataFrameSchema(
        {
            "team": Column(pa.String, nullable=False),
            "season": Column(pa.Int64, nullable=False),
            "games_played": Column(pa.Int64, nullable=False, checks=Check.ge(0)),
            "wins": Column(pa.Int64, nullable=False, checks=Check.ge(0)),
            "losses": Column(pa.Int64, nullable=False, checks=Check.ge(0)),
            "ties": Column(pa.Int64, nullable=False, checks=Check.ge(0)),
            "points_for": Column(pa.Int64, nullable=False, checks=Check.ge(0)),
            "points_against": Column(pa.Int64, nullable=False, checks=Check.ge(0)),
        },
        strict=True,
        coerce=False,
        checks=[
            Check(
                lambda df: (
                    df["wins"] + df["losses"] + df["ties"] == df["games_played"]
                ).all(),
                error="wins+losses+ties must equal games_played",
            ),
        ],
    )


def season_summaries_schema() -> DataFrameSchema:
    return DataFrameSchema(
        {
            "season": Column(pa.Int64, nullable=False),
            "games_total": Column(pa.Int64, nullable=False, checks=Check.ge(0)),
            "playoff_games": Column(pa.Int64, nullable=False, checks=Check.ge(0)),
            "regular_games": Column(pa.Int64, nullable=False, checks=Check.ge(0)),
        },
        strict=True,
        coerce=False,
        checks=[
            Check(
                lambda df: (
                    df["playoff_games"] + df["regular_games"] == df["games_total"]
                ).all(),
                error="playoff_games+regular_games must equal games_total",
            ),
        ],
    )


def venue_neutral_counts_schema() -> DataFrameSchema:
    return DataFrameSchema(
        {
            "season": Column(pa.Int64, nullable=False),
            "neutral_games": Column(pa.Int64, nullable=False, checks=Check.ge(0)),
        },
        strict=True,
        coerce=False,
    )


def validate_team_outcomes(df):
    return team_outcomes_schema().validate(df, lazy=False)


def validate_season_summaries(df):
    return season_summaries_schema().validate(df, lazy=False)


def validate_venue_neutral_counts(df):
    return venue_neutral_counts_schema().validate(df, lazy=False)
