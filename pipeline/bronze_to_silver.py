"""
pipeline/bronze_to_silver.py
Reads unprocessed Bronze CSV files for all 3 domains, applies domain-specific
cleaning/validation, and writes cleaned Parquet files to the Silver layer.

Called as a Airflow PythonOperator task.
"""

import logging
from datetime import datetime, timezone

import pandas as pd

from config import DOMAINS
from storage.local_storage import (
    get_unprocessed_bronze_files,
    mark_bronze_files_processed,
    save_to_silver,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Allowed enumeration values per domain
# ---------------------------------------------------------------------------

VALID_EVENT_TYPES   = {"login", "browse", "add_to_cart", "checkout", "logout"}
VALID_MOVEMENT_TYPES = {"inbound", "outbound", "adjustment"}


# ---------------------------------------------------------------------------
# Domain-specific transformations
# ---------------------------------------------------------------------------

def _process_sales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate fact_sales data.

    Rules:
      1. Parse timestamp → UTC-aware datetime
      2. Deduplicate on sale_id
      3. Flag rows with NULL in any required column
      4. For valid rows re-compute total_amount = quantity × unit_price
         if the original value differs by > £0.01
      5. Add is_valid, validation_errors, processed_at
    """
    required_cols = ["sale_id", "timestamp", "customer_id", "product_id",
                     "quantity", "unit_price", "total_amount"]

    # 1. Parse timestamp
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

    # 2. Deduplicate
    before = len(df)
    df = df.drop_duplicates(subset=["sale_id"])
    logger.info("  [SALES]  deduplication removed %d rows", before - len(df))

    # 3. Validate: collect per-row error messages
    errors = pd.Series([""] * len(df), index=df.index)

    nulls = df[required_cols].isnull()
    for col in required_cols:
        mask = nulls[col]
        errors[mask] += f"NULL:{col}; "

    # 4. For rows with quantity & unit_price present, fix/check total_amount
    computable = df["quantity"].notna() & df["unit_price"].notna()
    expected = (df.loc[computable, "quantity"] * df.loc[computable, "unit_price"]).round(2)
    mismatch = (df.loc[computable, "total_amount"] - expected).abs() > 0.01
    df.loc[computable & mismatch, "total_amount"] = expected[mismatch]
    n_fixed = int(mismatch.sum())
    if n_fixed:
        logger.info("  [SALES]  fixed total_amount on %d rows", n_fixed)

    # 5. Finalise
    df["is_valid"]         = errors.str.strip() == ""
    df["validation_errors"] = errors.str.strip()
    df["processed_at"]     = datetime.now(timezone.utc).isoformat()

    valid_count = int(df["is_valid"].sum())
    logger.info(
        "  [SALES]  %d rows total | %d valid | %d invalid",
        len(df), valid_count, len(df) - valid_count,
    )
    return df


def _process_customer_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate fact_customer_events data.

    Rules:
      1. Parse timestamp → UTC-aware datetime
      2. Deduplicate on event_id
      3. Flag rows with NULL in required columns
      4. Flag rows with unrecognised event_type
      5. Add is_valid, validation_errors, processed_at
    """
    required_cols = ["event_id", "timestamp", "customer_id", "session_id", "event_type"]

    # 1. Parse timestamp
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

    # 2. Deduplicate
    before = len(df)
    df = df.drop_duplicates(subset=["event_id"])
    logger.info("  [EVENTS] deduplication removed %d rows", before - len(df))

    # 3 & 4. Validate
    errors = pd.Series([""] * len(df), index=df.index)

    nulls = df[required_cols].isnull()
    for col in required_cols:
        errors[nulls[col]] += f"NULL:{col}; "

    invalid_enum = ~df["event_type"].isin(VALID_EVENT_TYPES) & df["event_type"].notna()
    errors[invalid_enum] += "INVALID_EVENT_TYPE; "

    df["is_valid"]         = errors.str.strip() == ""
    df["validation_errors"] = errors.str.strip()
    df["processed_at"]     = datetime.now(timezone.utc).isoformat()

    valid_count = int(df["is_valid"].sum())
    logger.info(
        "  [EVENTS] %d rows total | %d valid | %d invalid",
        len(df), valid_count, len(df) - valid_count,
    )
    return df


def _process_inventory(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate fact_inventory_movements data.

    Rules:
      1. Parse timestamp → UTC-aware datetime
      2. Deduplicate on movement_id
      3. Flag rows with NULL in required columns
      4. Flag invalid movement_type
      5. Flag zero / negative quantity
      6. Add is_valid, validation_errors, processed_at
    """
    required_cols = ["movement_id", "timestamp", "product_id",
                     "warehouse_id", "movement_type", "quantity"]

    # 1. Parse timestamp
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

    # 2. Deduplicate
    before = len(df)
    df = df.drop_duplicates(subset=["movement_id"])
    logger.info("  [INV]    deduplication removed %d rows", before - len(df))

    # 3, 4, 5. Validate
    errors = pd.Series([""] * len(df), index=df.index)

    nulls = df[required_cols].isnull()
    for col in required_cols:
        errors[nulls[col]] += f"NULL:{col}; "

    invalid_type = ~df["movement_type"].isin(VALID_MOVEMENT_TYPES) & df["movement_type"].notna()
    errors[invalid_type] += "INVALID_MOVEMENT_TYPE; "

    bad_qty = df["quantity"].notna() & (pd.to_numeric(df["quantity"], errors="coerce") <= 0)
    errors[bad_qty] += "NON_POSITIVE_QUANTITY; "

    df["is_valid"]         = errors.str.strip() == ""
    df["validation_errors"] = errors.str.strip()
    df["processed_at"]     = datetime.now(timezone.utc).isoformat()

    valid_count = int(df["is_valid"].sum())
    logger.info(
        "  [INV]    %d rows total | %d valid | %d invalid",
        len(df), valid_count, len(df) - valid_count,
    )
    return df


_PROCESSORS = {
    "sales":           _process_sales,
    "customer_events": _process_customer_events,
    "inventory":       _process_inventory,
}


# ---------------------------------------------------------------------------
# Public entry point (Airflow PythonOperator)
# ---------------------------------------------------------------------------

def run() -> None:
    """
    For each domain, read unprocessed Bronze CSVs → clean → write Silver Parquet.
    Idempotent: bronze files already processed are skipped.
    """
    logger.info("=== Bronze → Silver pipeline started ===")

    for domain in DOMAINS:
        files = get_unprocessed_bronze_files(domain)
        if not files:
            logger.info("[%s] No new bronze files to process.", domain)
            continue

        logger.info("[%s] Processing %d new bronze file(s)...", domain, len(files))
        dfs = []
        for f in files:
            try:
                dfs.append(pd.read_csv(f))
            except Exception as e:
                logger.error("[%s] Failed to read %s: %s", domain, f.name, e)

        if not dfs:
            continue

        combined = pd.concat(dfs, ignore_index=True)
        cleaned  = _PROCESSORS[domain](combined)
        save_to_silver(domain, cleaned)
        mark_bronze_files_processed(domain, files)

    logger.info("=== Bronze → Silver pipeline complete ===")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
    run()
