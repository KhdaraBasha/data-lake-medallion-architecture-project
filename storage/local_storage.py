"""
storage/local_storage.py
Handles all read/write operations across the Bronze, Silver, and Gold layers
of the local filesystem-based data lake.

Directory layout (Hive-style partitioning):
  datalake/
    bronze/<domain>/year=YYYY/month=MM/day=DD/<file>.csv
    silver/<domain>/year=YYYY/month=MM/day=DD/<file>.parquet
    gold/<table_name>/<file>.parquet

State tracking (incremental Silver processing):
  .state/<domain>_processed.json  — list of bronze file paths already processed
"""

import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from config import DATALAKE_DIR, STATE_DIR

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _hive_path(layer_dir: Path, domain: str, dt: datetime) -> Path:
    """Return a Hive-partitioned directory path for the given domain + date."""
    return (
        layer_dir
        / domain
        / f"year={dt.year}"
        / f"month={dt.month:02d}"
        / f"day={dt.day:02d}"
    )


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Bronze Layer
# ---------------------------------------------------------------------------

def save_to_bronze(domain: str, source_file: Path) -> Path:
    """
    Copy a raw CSV from local_output/<domain>/ into the Bronze layer
    with Hive-style partitioning. The raw file is preserved as-is.

    Returns the destination path.
    """
    dt = _utcnow()
    dest_dir = _hive_path(DATALAKE_DIR / "bronze", domain, dt)
    dest_dir.mkdir(parents=True, exist_ok=True)

    dest_file = dest_dir / source_file.name
    shutil.copy2(source_file, dest_file)

    logger.info("[BRONZE] %-20s | %s → %s", domain, source_file.name, dest_file.relative_to(DATALAKE_DIR))
    return dest_file


# ---------------------------------------------------------------------------
# State tracking (incremental bronze → silver)
# ---------------------------------------------------------------------------

def _load_processed_state(domain: str) -> set:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    state_file = STATE_DIR / f"{domain}_processed.json"
    if state_file.exists():
        with open(state_file) as f:
            return set(json.load(f))
    return set()


def _save_processed_state(domain: str, processed: set) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    state_file = STATE_DIR / f"{domain}_processed.json"
    with open(state_file, "w") as f:
        json.dump(sorted(processed), f, indent=2)


def get_unprocessed_bronze_files(domain: str) -> list[Path]:
    """Return bronze CSV files not yet processed into Silver."""
    bronze_dir = DATALAKE_DIR / "bronze" / domain
    if not bronze_dir.exists():
        return []
    processed = _load_processed_state(domain)
    all_files = sorted(bronze_dir.rglob("*.csv"))
    return [f for f in all_files if str(f) not in processed]


def mark_bronze_files_processed(domain: str, files: list[Path]) -> None:
    """Mark a list of bronze files as processed."""
    processed = _load_processed_state(domain)
    processed.update(str(f) for f in files)
    _save_processed_state(domain, processed)


# ---------------------------------------------------------------------------
# Silver Layer
# ---------------------------------------------------------------------------

def save_to_silver(domain: str, df: pd.DataFrame) -> Path:
    """Save a cleaned DataFrame as Parquet into the Silver layer."""
    dt = _utcnow()
    dest_dir = _hive_path(DATALAKE_DIR / "silver", domain, dt)
    dest_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{domain}_{dt.strftime('%Y%m%d_%H%M%S')}.parquet"
    dest_file = dest_dir / filename
    df.to_parquet(dest_file, index=False)

    valid_count = int(df["is_valid"].sum()) if "is_valid" in df.columns else len(df)
    logger.info(
        "[SILVER] %-20s | %d rows (%d valid) → %s",
        domain, len(df), valid_count, dest_file.relative_to(DATALAKE_DIR),
    )
    return dest_file


def read_from_silver(domain: str) -> pd.DataFrame:
    """Read all Silver Parquet files for a domain into a single DataFrame."""
    silver_dir = DATALAKE_DIR / "silver" / domain
    if not silver_dir.exists():
        return pd.DataFrame()
    parquet_files = sorted(silver_dir.rglob("*.parquet"))
    if not parquet_files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)


# ---------------------------------------------------------------------------
# Gold Layer
# ---------------------------------------------------------------------------

def save_to_gold(table_name: str, df: pd.DataFrame) -> Path:
    """Save an aggregated DataFrame as Parquet into the Gold layer."""
    dt = _utcnow()
    dest_dir = DATALAKE_DIR / "gold" / table_name
    dest_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{table_name}_{dt.strftime('%Y%m%d_%H%M%S')}.parquet"
    dest_file = dest_dir / filename
    df.to_parquet(dest_file, index=False)

    logger.info("[GOLD]   %-20s | %d rows → %s", table_name, len(df), dest_file.relative_to(DATALAKE_DIR))
    return dest_file
