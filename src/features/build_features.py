# src/features/build_features.py

import pandas as pd
from pathlib import Path
from datetime import timedelta


PROCESSED_DATA_PATH = Path("data/processed/transactions_clean.parquet")
FEATURE_DATA_PATH = Path("data/features/transactions_features.parquet")


def rolling_nunique(df, group_col, value_col, window):
    """
    Compute rolling unique counts using time-based filtering.
    Deterministic, pandas-safe.
    """
    result = []

    for idx, row in df.iterrows():
        user = row[group_col]
        current_time = row["timestamp"]
        start_time = current_time - window

        mask = (
            (df[group_col] == user)
            & (df["timestamp"] >= start_time)
            & (df["timestamp"] <= current_time)
        )

        result.append(df.loc[mask, value_col].nunique())

    return result


def build_features():
    df = pd.read_parquet(PROCESSED_DATA_PATH)

    # Sort for time correctness
    df = df.sort_values(by=["user_id", "timestamp"]).reset_index(drop=True)

    # Basic time features
    df["hour_of_day"] = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.dayofweek

    # Set timestamp index for rolling
    df.set_index("timestamp", inplace=True)

    # Velocity features
    df["txn_count_1h"] = (
        df.groupby("user_id")["transaction_id"]
        .rolling("1h")
        .count()
        .reset_index(level=0, drop=True)
    )

    df["txn_count_24h"] = (
        df.groupby("user_id")["transaction_id"]
        .rolling("24h")
        .count()
        .reset_index(level=0, drop=True)
    )

    df["amount_sum_24h"] = (
        df.groupby("user_id")["amount"]
        .rolling("24h")
        .sum()
        .reset_index(level=0, drop=True)
    )

    df.reset_index(inplace=True)

    # Rolling unique counts (SAFE METHOD)
    df["device_count_7d"] = rolling_nunique(
        df, "user_id", "device_type", timedelta(days=7)
    )

    df["country_count_7d"] = rolling_nunique(
        df, "user_id", "geo_country", timedelta(days=7)
    )

    # Fill NaNs
    df.fillna(0, inplace=True)

    FEATURE_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(FEATURE_DATA_PATH, index=False)

    return str(FEATURE_DATA_PATH)


if __name__ == "__main__":
    build_features()
