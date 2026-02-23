"""
generator/customer_events_generator.py
Generates synthetic customer clickstream events (fact_customer_events).

Schema:
  event_id, timestamp, customer_id, session_id, event_type,
  product_id, page_url, device_type

Intentional data-quality issues:
  - ~4%  rows: event_type set to 'UNKNOWN' (invalid enum)
  - ~3%  rows: customer_id is NULL
  - ~5%  run:  one duplicate row appended
"""

import logging
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from faker import Faker

from config import LOCAL_OUTPUT_DIR, CUSTOMER_EVENTS_ROWS_PER_BATCH
from storage.local_storage import save_to_bronze

logger = logging.getLogger(__name__)
fake = Faker()

# --------------------------------------------------------------------------
# Reference data
# --------------------------------------------------------------------------

VALID_EVENT_TYPES = ["login", "browse", "add_to_cart", "checkout", "logout"]
DEVICE_TYPES = ["desktop", "mobile", "tablet"]
PAGES = [
    "/home", "/products", "/products/electronics", "/products/clothing",
    "/cart", "/checkout", "/profile", "/search", "/promotions",
]
PRODUCT_IDS = [f"PROD-{i}" for i in range(100, 200)]


# --------------------------------------------------------------------------
# Row builder
# --------------------------------------------------------------------------

def _make_row(session_id: str) -> dict:
    customer_id = f"CUST-{random.randint(1000, 9999)}"
    event_type = random.choice(VALID_EVENT_TYPES)

    # ~4% chance: invalid event type  (Silver will flag this)
    if random.random() < 0.04:
        event_type = "UNKNOWN"

    # ~3% chance: NULL customer_id  (Silver will flag as invalid)
    if random.random() < 0.03:
        customer_id = None

    return {
        "event_id":    str(uuid.uuid4()),
        "timestamp":   (datetime.now(timezone.utc) - timedelta(seconds=random.randint(0, 60))).isoformat(),
        "customer_id": customer_id,
        "session_id":  session_id,
        "event_type":  event_type,
        "product_id":  random.choice(PRODUCT_IDS) if event_type in ("browse", "add_to_cart", "checkout") else None,
        "page_url":    random.choice(PAGES),
        "device_type": random.choice(DEVICE_TYPES),
    }


# --------------------------------------------------------------------------
# Public entry point (called by Airflow PythonOperator)
# --------------------------------------------------------------------------

def run() -> Path:
    """
    Generate one batch of customer events, write a CSV to local_output/customer_events/,
    then copy it to the Bronze layer.

    Returns the destination Bronze path.
    """
    output_dir = LOCAL_OUTPUT_DIR / "customer_events"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate events across a handful of concurrent sessions
    session_ids = [str(uuid.uuid4()) for _ in range(3)]
    rows = [_make_row(random.choice(session_ids)) for _ in range(CUSTOMER_EVENTS_ROWS_PER_BATCH)]

    # ~5% chance: append a duplicate of the first row
    if random.random() < 0.05 and rows:
        rows.append(rows[0].copy())

    df = pd.DataFrame(rows)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    local_file = output_dir / f"events_{ts}.csv"
    df.to_csv(local_file, index=False)
    logger.info("[GENERATOR] customer_events: %d rows → %s", len(df), local_file)

    bronze_path = save_to_bronze("customer_events", local_file)
    return bronze_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
    run()
