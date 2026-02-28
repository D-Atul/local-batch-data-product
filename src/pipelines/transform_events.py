from __future__ import annotations

import pandas as pd


# String columns that should be trimmed and have empty strings normalised to null
_STRING_COLS = [
    "schedule_date",
    "schedule_week",
    "team_home",
    "team_away",
    "stadium",
    "team_favorite_id",
    "over_under_line",
    "weather_detail",
]


def transform_events(raw_events: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise types and clean raw ingested data into the curated contract shape.

    Curated contract expectations:
        schedule_date        -> str       (ISO date string, e.g. "1978-09-03")
        schedule_season      -> Int64
        schedule_week        -> str
        schedule_playoff     -> bool
        team_home            -> str
        team_away            -> str
        score_home           -> Float64   (nullable)
        score_away           -> Float64   (nullable)
        stadium              -> str
        stadium_neutral      -> bool
        team_favorite_id     -> str       (nullable)
        spread_favorite      -> Float64   (nullable)
        over_under_line      -> str       (nullable)
        weather_temperature  -> Float64   (nullable)
        weather_wind_mph     -> Float64   (nullable)
        weather_humidity     -> Float64   (nullable)
        weather_detail       -> str       (nullable)
    """
    df = raw_events.copy()

    # -------------------------
    # 1) Trim strings + normalise empty strings to None
    # -------------------------
    for col in _STRING_COLS:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace({"": None, "nan": None, "None": None, "NaN": None})

    # -------------------------
    # 2) schedule_date -> ISO date string (YYYY-MM-DD)
    # -------------------------
    df["schedule_date"] = (
        pd.to_datetime(df["schedule_date"], errors="coerce")
        .dt.strftime("%Y-%m-%d")
    )

    # -------------------------
    # 3) schedule_season -> Int64
    # -------------------------
    df["schedule_season"] = pd.to_numeric(df["schedule_season"], errors="coerce").astype("Int64")

    # -------------------------
    # 4) schedule_playoff -> bool
    # -------------------------
    df["schedule_playoff"] = df["schedule_playoff"].astype(str).str.strip().str.lower().map(
        {"true": True, "false": False, "1": True, "0": False}
    ).fillna(False).astype(bool)

    df["stadium_neutral"] = df["stadium_neutral"].astype(str).str.strip().str.lower().map(
        {"true": True, "false": False, "1": True, "0": False}
    ).fillna(False).astype(bool)

    # -------------------------
    # 6) Numeric float columns
    # -------------------------
    for col in ["score_home", "score_away", "spread_favorite",
                "weather_temperature", "weather_wind_mph", "weather_humidity"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")

    return df