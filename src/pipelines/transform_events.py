from __future__ import annotations

import pandas as pd


def transform_events(raw_events: pd.DataFrame) -> pd.DataFrame:

    df = raw_events.copy()

    df["schedule_date"] = pd.to_datetime(df["schedule_date"], errors="coerce")
    df["score_home"] = pd.to_numeric(df["score_home"], errors="coerce")
    df["score_away"] = pd.to_numeric(df["score_away"], errors="coerce")

    return df
