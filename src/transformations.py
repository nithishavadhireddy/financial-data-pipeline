"""
transformations.py

Utility functions for cleaning and validating raw financial records
before they land in the staging layer.
"""

import logging
from decimal import Decimal, InvalidOperation
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Columns we absolutely cannot have nulls on
REQUIRED_COLS = ["transaction_id", "account_id", "amount", "transaction_ts"]

# Expected dtypes after casting
DTYPE_MAP = {
    "transaction_id": str,
    "account_id": str,
    "amount": float,
    "currency": str,
    "transaction_type": str,
}


def cast_amount(value) -> Optional[float]:
    """Try to cast amount to float. Return None if unparseable."""
    try:
        return float(Decimal(str(value)))
    except (InvalidOperation, TypeError, ValueError):
        logger.warning("Could not cast amount value: %s", value)
        return None


def clean_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply cleaning rules to a raw transactions DataFrame.
    - Drop rows missing required columns
    - Cast amount
    - Normalise string columns
    - Deduplicate on transaction_id
    """
    original_len = len(df)

    # Drop rows missing required columns
    df = df.dropna(subset=REQUIRED_COLS)

    # Cast amount
    df["amount"] = df["amount"].apply(cast_amount)
    df = df.dropna(subset=["amount"])

    # Normalise strings
    for col in ["currency", "transaction_type"]:
        if col in df.columns:
            df[col] = df[col].str.strip().str.upper()

    # Deduplicate
    df = df.drop_duplicates(subset=["transaction_id"], keep="last")

    dropped = original_len - len(df)
    if dropped > 0:
        logger.info("Dropped %d rows during cleaning (nulls / duplicates / bad amounts)", dropped)

    return df.reset_index(drop=True)


def validate_schema(df: pd.DataFrame) -> bool:
    """Check that required columns are all present."""
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        logger.error("Schema validation failed. Missing columns: %s", missing)
        return False
    return True
