# src/preprocessing/preprocess_transactions.py

import pandas as pd
from pathlib import Path


RAW_DATA_PATH = Path("data/raw/transactions.parquet")
PROCESSED_DATA_PATH = Path("data/processed/transactions_clean.parquet")


def preprocess_transactions():
    """
    Deterministic preprocessing for fraud detection data.
    No randomness. No feature engineering. No leakage.
    """

    # 1. Load raw data
    df = pd.read_parquet(RAW_DATA_PATH)

    # 2. Type normalization
    df["amount"] = df["amount"].astype(float)
    df["is_fraud"] = df["is_fraud"].astype(int)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    # 3. Drop rows with critical nulls
    df = df.dropna(subset=["transaction_id", "user_id", "timestamp", "is_fraud"])

    # 4. Deduplication
    df = df.drop_duplicates(subset=["transaction_id"], keep="first")

    # 5. Sanity checks
    df = df[df["amount"] >= 0]
    df = df[df["timestamp"] <= pd.Timestamp.utcnow()]

    # 6. Sort for determinism
    df = df.sort_values(by=["timestamp", "transaction_id"]).reset_index(drop=True)

    # 7. Save processed data
    PROCESSED_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(PROCESSED_DATA_PATH, index=False)

    return str(PROCESSED_DATA_PATH)


if __name__ == "__main__":
    preprocess_transactions()
