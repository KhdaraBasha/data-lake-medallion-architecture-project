"""
pipeline/silver_to_gold.py
Reads all Silver Parquet data for each domain and produces 3 Gold aggregate tables:

  1. gold/daily_sales_summary      — daily revenue KPIs + category breakdown
  2. gold/customer_activity_summary — daily event counts by type + device
  3. gold/inventory_summary        — daily net stock movement per product/warehouse

Called as an Airflow PythonOperator task (runs after bronze_to_silver).
"""

import logging
from datetime import datetime, timezone

import pandas as pd

from storage.local_storage import read_from_silver, save_to_gold

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Gold Table 1 — daily_sales_summary
# ---------------------------------------------------------------------------

def _build_daily_sales_summary(df: pd.DataFrame) -> None:
    """
    Input: Silver sales DataFrame (all valid + invalid rows present).

    Outputs two Gold tables:
      • daily_sales_summary   — per-day KPIs
      • category_sales_summary — per-day, per-category breakdown
    """
    if df.empty:
        logger.warning("[GOLD] sales silver is empty — skipping.")
        return

    valid = df[df["is_valid"] == True].copy()
    if valid.empty:
        logger.warning("[GOLD] No valid sales rows — skipping.")
        return

    valid["timestamp"] = pd.to_datetime(valid["timestamp"], utc=True, errors="coerce")
    valid["date"] = valid["timestamp"].dt.date

    # ---- Daily KPIs ----
    daily = (
        valid.groupby("date")
        .agg(
            total_revenue=("total_amount", "sum"),
            order_count=("sale_id", "nunique"),
            avg_order_value=("total_amount", "mean"),
            unique_customers=("customer_id", "nunique"),
        )
        .round(2)
        .reset_index()
    )
    daily["generated_at"] = datetime.now(timezone.utc).isoformat()
    save_to_gold("daily_sales_summary", daily)

    # ---- Category breakdown ----
    cat = (
        valid.groupby(["date", "category"])
        .agg(
            category_revenue=("total_amount", "sum"),
            category_orders=("sale_id", "nunique"),
            avg_unit_price=("unit_price", "mean"),
        )
        .round(2)
        .reset_index()
    )
    cat["generated_at"] = datetime.now(timezone.utc).isoformat()
    save_to_gold("category_sales_summary", cat)

    # ---- Payment method breakdown ----
    pay = (
        valid.groupby(["date", "payment_method"])
        .agg(
            payment_revenue=("total_amount", "sum"),
            payment_count=("sale_id", "nunique"),
        )
        .round(2)
        .reset_index()
    )
    pay["generated_at"] = datetime.now(timezone.utc).isoformat()
    save_to_gold("payment_method_summary", pay)

    logger.info(
        "[GOLD] daily_sales_summary: %d day(s) | category_sales_summary: %d rows | payment_method_summary: %d rows",
        len(daily), len(cat), len(pay),
    )


# ---------------------------------------------------------------------------
# Gold Table 2 — customer_activity_summary
# ---------------------------------------------------------------------------

def _build_customer_activity_summary(df: pd.DataFrame) -> None:
    """
    Input: Silver customer_events DataFrame.

    Outputs two Gold tables:
      • customer_activity_summary — event counts by type per day
      • device_usage_summary      — session counts by device per day
    """
    if df.empty:
        logger.warning("[GOLD] customer_events silver is empty — skipping.")
        return

    valid = df[df["is_valid"] == True].copy()
    if valid.empty:
        logger.warning("[GOLD] No valid customer event rows — skipping.")
        return

    valid["timestamp"] = pd.to_datetime(valid["timestamp"], utc=True, errors="coerce")
    valid["date"] = valid["timestamp"].dt.date

    # ---- Event type counts per day ----
    events = (
        valid.groupby(["date", "event_type"])
        .agg(
            event_count=("event_id", "count"),
            unique_customers=("customer_id", "nunique"),
            unique_sessions=("session_id", "nunique"),
        )
        .reset_index()
    )
    events["generated_at"] = datetime.now(timezone.utc).isoformat()
    save_to_gold("customer_activity_summary", events)

    # ---- Device breakdown per day ----
    devices = (
        valid.groupby(["date", "device_type"])
        .agg(
            session_count=("session_id", "nunique"),
            event_count=("event_id", "count"),
        )
        .reset_index()
    )
    devices["generated_at"] = datetime.now(timezone.utc).isoformat()
    save_to_gold("device_usage_summary", devices)

    logger.info(
        "[GOLD] customer_activity_summary: %d rows | device_usage_summary: %d rows",
        len(events), len(devices),
    )


# ---------------------------------------------------------------------------
# Gold Table 3 — inventory_summary
# ---------------------------------------------------------------------------

def _build_inventory_summary(df: pd.DataFrame) -> None:
    """
    Input: Silver inventory_movements DataFrame.

    Outputs:
      • inventory_movement_summary — daily inbound / outbound / adjustment qty per product + warehouse
      • inventory_net_position     — net stock position (inbound − outbound) per product + warehouse
    """
    if df.empty:
        logger.warning("[GOLD] inventory silver is empty — skipping.")
        return

    valid = df[df["is_valid"] == True].copy()
    if valid.empty:
        logger.warning("[GOLD] No valid inventory rows — skipping.")
        return

    valid["timestamp"] = pd.to_datetime(valid["timestamp"], utc=True, errors="coerce")
    valid["date"] = valid["timestamp"].dt.date
    valid["quantity"] = pd.to_numeric(valid["quantity"], errors="coerce")

    # ---- Movement breakdown per product/warehouse/type ----
    movement = (
        valid.groupby(["date", "product_id", "product_name", "warehouse_id", "movement_type"])
        .agg(
            total_quantity=("quantity", "sum"),
            total_cost=("unit_cost", "sum"),
            movement_count=("movement_id", "count"),
        )
        .round(2)
        .reset_index()
    )
    movement["generated_at"] = datetime.now(timezone.utc).isoformat()
    save_to_gold("inventory_movement_summary", movement)

    # ---- Net position (inbound − outbound) per product/warehouse/day ----
    pivot = (
        valid.pivot_table(
            index=["date", "product_id", "product_name", "warehouse_id"],
            columns="movement_type",
            values="quantity",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
    )
    # Ensure all columns exist even if a movement_type never appeared
    for col in ("inbound", "outbound", "adjustment"):
        if col not in pivot.columns:
            pivot[col] = 0

    pivot["net_position"] = pivot["inbound"] - pivot["outbound"]
    pivot["generated_at"] = datetime.now(timezone.utc).isoformat()
    pivot.columns.name = None          # remove pandas MultiIndex name artefact
    save_to_gold("inventory_net_position", pivot)

    logger.info(
        "[GOLD] inventory_movement_summary: %d rows | inventory_net_position: %d rows",
        len(movement), len(pivot),
    )


# ---------------------------------------------------------------------------
# Public entry point (Airflow PythonOperator)
# ---------------------------------------------------------------------------

def run() -> None:
    """
    Read all Silver data for each domain and produce Gold aggregate tables.
    Produces a full snapshot on every run (idempotent — overwrites nothing,
    simply appends a new timestamped Parquet file).
    """
    logger.info("=== Silver → Gold pipeline started ===")

    sales_df   = read_from_silver("sales")
    events_df  = read_from_silver("customer_events")
    inv_df     = read_from_silver("inventory")

    _build_daily_sales_summary(sales_df)
    _build_customer_activity_summary(events_df)
    _build_inventory_summary(inv_df)

    logger.info("=== Silver → Gold pipeline complete ===")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
    run()
