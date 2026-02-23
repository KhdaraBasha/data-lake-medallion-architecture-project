"""
generator/inventory_generator.py
Generates synthetic warehouse inventory movements (fact_inventory_movements).

Schema:
  movement_id, timestamp, product_id, product_name, warehouse_id,
  movement_type, quantity, unit_cost, supplier_id

Intentional data-quality issues:
  - ~4%  rows: movement_type set to 'TRANSFER' (invalid enum)
  - ~3%  rows: quantity is NULL or zero
  - ~5%  run:  one duplicate row appended
"""

import logging
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from faker import Faker

from config import LOCAL_OUTPUT_DIR, INVENTORY_ROWS_PER_BATCH
from storage.local_storage import save_to_bronze

logger = logging.getLogger(__name__)
fake = Faker()

# --------------------------------------------------------------------------
# Reference data
# --------------------------------------------------------------------------

VALID_MOVEMENT_TYPES = ["inbound", "outbound", "adjustment"]
WAREHOUSES = ["WH-NORTH-01", "WH-SOUTH-02", "WH-EAST-03", "WH-WEST-04"]
SUPPLIERS = [f"SUP-{str(i).zfill(3)}" for i in range(1, 11)]

PRODUCT_CATALOGUE = [
    ("PROD-101", "Laptop"),        ("PROD-102", "Smartphone"),
    ("PROD-103", "Tablet"),        ("PROD-201", "T-Shirt"),
    ("PROD-202", "Jeans"),         ("PROD-301", "Coffee"),
    ("PROD-401", "Blender"),       ("PROD-501", "Yoga Mat"),
    ("PROD-502", "Dumbbell"),      ("PROD-601", "Python Programming"),
]


# --------------------------------------------------------------------------
# Row builder
# --------------------------------------------------------------------------

def _make_row() -> dict:
    product_id, product_name = random.choice(PRODUCT_CATALOGUE)
    movement_type = random.choice(VALID_MOVEMENT_TYPES)
    quantity = random.randint(1, 200)
    unit_cost = round(random.uniform(1.0, 300.0), 2)

    # ~4% chance: invalid movement type  (Silver will flag this)
    if random.random() < 0.04:
        movement_type = "TRANSFER"

    # ~3% chance: NULL or zero quantity  (Silver will flag as invalid)
    if random.random() < 0.03:
        quantity = None if random.random() < 0.5 else 0

    return {
        "movement_id":   str(uuid.uuid4()),
        "timestamp":     (datetime.now(timezone.utc) - timedelta(seconds=random.randint(0, 120))).isoformat(),
        "product_id":    product_id,
        "product_name":  product_name,
        "warehouse_id":  random.choice(WAREHOUSES),
        "movement_type": movement_type,
        "quantity":      quantity,
        "unit_cost":     unit_cost,
        "supplier_id":   random.choice(SUPPLIERS) if movement_type == "inbound" else None,
    }


# --------------------------------------------------------------------------
# Public entry point (called by Airflow PythonOperator)
# --------------------------------------------------------------------------

def run() -> Path:
    """
    Generate one batch of inventory movements, write a CSV to local_output/inventory/,
    then copy it to the Bronze layer.

    Returns the destination Bronze path.
    """
    output_dir = LOCAL_OUTPUT_DIR / "inventory"
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = [_make_row() for _ in range(INVENTORY_ROWS_PER_BATCH)]

    # ~5% chance: append a duplicate of the first row
    if random.random() < 0.05 and rows:
        rows.append(rows[0].copy())

    df = pd.DataFrame(rows)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    local_file = output_dir / f"inventory_{ts}.csv"
    df.to_csv(local_file, index=False)
    logger.info("[GENERATOR] inventory: %d rows → %s", len(df), local_file)

    bronze_path = save_to_bronze("inventory", local_file)
    return bronze_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
    run()
