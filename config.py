"""
config.py — Central configuration for the Data Lake Medallion Architecture project.
All paths, domain names, and tuneable parameters live here.
"""

from pathlib import Path

# ---------------------------------------------------------------------------
# Directory layout
# ---------------------------------------------------------------------------

# Project root (same directory as this file)
BASE_DIR = Path(__file__).resolve().parent

# Staging area: raw CSVs written by generators before they hit the datalake
LOCAL_OUTPUT_DIR = BASE_DIR / "local_output"

# Main data lake store (Bronze / Silver / Gold layers)
DATALAKE_DIR = BASE_DIR / "datalake"

# State tracking for incremental processing
STATE_DIR = BASE_DIR / ".state"

# ---------------------------------------------------------------------------
# Domains (one per fact table)
# ---------------------------------------------------------------------------

DOMAINS = ["sales", "customer_events", "inventory"]

# ---------------------------------------------------------------------------
# Generator batch sizes
# ---------------------------------------------------------------------------

SALES_ROWS_PER_BATCH = 10          # rows per generator run
CUSTOMER_EVENTS_ROWS_PER_BATCH = 15
INVENTORY_ROWS_PER_BATCH = 8

# ---------------------------------------------------------------------------
# Airflow schedule (informational — referenced in the DAG file)
# ---------------------------------------------------------------------------

# Generators + Bronze upload: every 5 minutes  →  "*/5 * * * *"
# Silver + Gold pipeline:     every 30 minutes →  "*/30 * * * *"
GENERATOR_CRON = "*/5 * * * *"
PIPELINE_CRON = "*/30 * * * *"
