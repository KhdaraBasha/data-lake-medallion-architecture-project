"""
generator/sales_generator.py
Generates synthetic e-commerce sales transactions (fact_sales).

Schema:
  sale_id, timestamp, customer_id, product_id, product_name, category,
  quantity, unit_price, total_amount, payment_method, status

Intentional data-quality issues (for Silver cleaning):
  - ~5%  rows: total_amount ≠ quantity × unit_price
  - ~3%  rows: quantity is NULL
  - ~5%  run:  one duplicate row appended
"""

import logging
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from faker import Faker

from config import LOCAL_OUTPUT_DIR, SALES_ROWS_PER_BATCH
from storage.local_storage import save_to_bronze

logger = logging.getLogger(__name__)
fake = Faker()

# --------------------------------------------------------------------------
# Reference data
# --------------------------------------------------------------------------

CATEGORIES = [
    "Electronics", "Clothing", "Food & Beverage",
    "Home & Kitchen", "Sports", "Books",
]

PRODUCTS: dict[str, list[str]] = {
    "Electronics":      ["Laptop", "Smartphone", "Tablet", "Headphones", "Smartwatch"],
    "Clothing":         ["T-Shirt", "Jeans", "Jacket", "Shoes", "Dress"],
    "Food & Beverage":  ["Coffee", "Tea", "Juice", "Snack Pack", "Energy Drink"],
    "Home & Kitchen":   ["Blender", "Coffee Maker", "Toaster", "Knife Set", "Cookware"],
    "Sports":           ["Running Shoes", "Yoga Mat", "Dumbbell", "Resistance Band", "Water Bottle"],
    "Books":            ["Python Programming", "Data Engineering", "Machine Learning", "SQL Guide", "Cloud Architecture"],
}

PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "bank_transfer", "crypto"]
# Weighted toward 'completed'
STATUSES = ["completed", "completed", "completed", "pending", "refunded"]

PRODUCT_ID_MAP: dict[str, str] = {}  # product_name → stable PROD-xxx id


def _get_product_id(product_name: str) -> str:
    if product_name not in PRODUCT_ID_MAP:
        PRODUCT_ID_MAP[product_name] = f"PROD-{random.randint(100, 999)}"
    return PRODUCT_ID_MAP[product_name]


# --------------------------------------------------------------------------
# Row builder
# --------------------------------------------------------------------------

def _make_row() -> dict:
    category = random.choice(CATEGORIES)
    product = random.choice(PRODUCTS[category])
    quantity = random.randint(1, 10)
    unit_price = round(random.uniform(5.0, 500.0), 2)
    total_amount = round(quantity * unit_price, 2)

    # ~5% chance: corrupt total_amount  (Silver will fix/flag this)
    if random.random() < 0.05:
        total_amount = round(total_amount * random.uniform(0.7, 1.3), 2)

    # ~3% chance: NULL quantity  (Silver will flag as invalid)
    if random.random() < 0.03:
        quantity = None

    return {
        "sale_id":        str(uuid.uuid4()),
        "timestamp":      (datetime.now(timezone.utc) - timedelta(seconds=random.randint(0, 30))).isoformat(),
        "customer_id":    f"CUST-{random.randint(1000, 9999)}",
        "product_id":     _get_product_id(product),
        "product_name":   product,
        "category":       category,
        "quantity":       quantity,
        "unit_price":     unit_price,
        "total_amount":   total_amount,
        "payment_method": random.choice(PAYMENT_METHODS),
        "status":         random.choice(STATUSES),
    }


# --------------------------------------------------------------------------
# Public entry point (called by Airflow PythonOperator)
# --------------------------------------------------------------------------

def run() -> Path:
    """
    Generate one batch of sales records, write a CSV to local_output/sales/,
    then copy it to the Bronze layer.

    Returns the destination Bronze path.
    """
    output_dir = LOCAL_OUTPUT_DIR / "sales"
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = [_make_row() for _ in range(SALES_ROWS_PER_BATCH)]

    # ~5% chance: append a duplicate of the first row
    if random.random() < 0.05 and rows:
        rows.append(rows[0].copy())

    df = pd.DataFrame(rows)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    local_file = output_dir / f"sales_{ts}.csv"
    df.to_csv(local_file, index=False)
    logger.info("[GENERATOR] sales: %d rows → %s", len(df), local_file)

    bronze_path = save_to_bronze("sales", local_file)
    return bronze_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
    run()
