# Data Lake — Medallion Architecture Project

A fully local Data Lake pipeline built on the **Medallion Architecture** (Bronze → Silver → Gold) orchestrated by **Apache Airflow**. Three independent fact tables are generated, cleaned, and aggregated through each layer.

---

## Architecture Overview

```
                          ┌─────────────────────────────────┐
                          │      Apache Airflow              │
                          │                                  │
                          │  DAG 1: generator_dag (*/5 min)  │
                          │    ├── generate_sales            │
                          │    ├── generate_customer_events  │
                          │    └── generate_inventory        │
                          │                                  │
                          │  DAG 2: pipeline_dag (*/30 min)  │
                          │    └── bronze_to_silver          │
                          │         └── silver_to_gold       │
                          └─────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────┐
│  local_output/           (staging: raw CSVs before bronze)             │
│    ├── sales/            │  customer_events/  │  inventory/            │
└────────────────────────────────────────────────────────────────────────┘
                    │ copy (save_to_bronze)
                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  datalake/bronze/<domain>/year=YYYY/month=MM/day=DD/*.csv              │
│  Raw, unmodified files. Nothing is ever deleted here.                  │
└────────────────────────────────────────────────────────────────────────┘
                    │ clean + validate (bronze_to_silver)
                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  datalake/silver/<domain>/year=YYYY/month=MM/day=DD/*.parquet          │
│  Deduplicated, type-cast, validated. is_valid + validation_errors cols │
└────────────────────────────────────────────────────────────────────────┘
                    │ aggregate (silver_to_gold)
                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│  datalake/gold/                                                        │
│    ├── daily_sales_summary/          ├── category_sales_summary/       │
│    ├── payment_method_summary/       ├── customer_activity_summary/    │
│    ├── device_usage_summary/         ├── inventory_movement_summary/   │
│    └── inventory_net_position/                                         │
└────────────────────────────────────────────────────────────────────────┘
```

---

## Fact Tables

| Domain | Generator | Rows/batch | Key Fields |
|--------|-----------|-----------|------------|
| 🛒 Sales | `sales_generator.py` | 10 | sale_id, customer_id, product, category, quantity, unit_price, total_amount, status |
| 👤 Customer Events | `customer_events_generator.py` | 15 | event_id, session_id, event_type, customer_id, device_type, page_url |
| 📦 Inventory | `inventory_generator.py` | 8 | movement_id, warehouse_id, product_id, movement_type, quantity, unit_cost |

---

## Project Structure

```
data-lake-medallion-architecture-project/
│
├── config.py                        # Paths, domain names, intervals
├── requirements.txt
│
├── generator/
│   ├── sales_generator.py
│   ├── customer_events_generator.py
│   └── inventory_generator.py
│
├── storage/
│   └── local_storage.py             # Bronze/Silver/Gold read-write helpers
│
├── pipeline/
│   ├── bronze_to_silver.py          # Clean + validate
│   └── silver_to_gold.py           # Aggregate KPIs
│
├── dags/
│   └── data_lake_pipeline.py       # Airflow DAG definitions (2 DAGs)
│
├── local_output/                    # Auto-created; gitignored
├── datalake/                        # Auto-created; gitignored
└── .state/                          # Auto-created; gitignored
```

---

## Setup

### 1. Choose your environment

This project uses two separate virtual environments to handle different use cases:

*   **`.venv` (Windows Native)**: For running generators and pipelines manually in PowerShell. (Already created)
*   **`.venv_linux` (WSL/Ubuntu)**: For running Airflow 3.0+ orchestration. (Already created)

To activate them:
- **Windows**: `.venv\Scripts\Activate.ps1`
- **WSL**: `source .venv_linux/bin/activate`

### 2. Install dependencies (if adding new ones)
```bash
pip install -r requirements.txt
```

### 3. Set the Airflow home and PYTHONPATH

```powershell
$env:AIRFLOW_HOME = "$PWD\airflow_home"
$env:PYTHONPATH   = "$PWD"
```

### 3. Initialise Airflow (v3.0+)

In Airflow 3.0+, user management is handled via the configuration. The `admin` user is already defined in your `airflow.cfg`.

```bash
# Initialize the DB
airflow db migrate
```

> [!NOTE]
> The `airflow users create` command was removed in v3.0. For the default Simple Auth Manager, users are defined in `airflow.cfg` under `[core] simple_auth_manager_users`. On the first run, Airflow will generate a password file in your `AIRFLOW_HOME`.


### 4. Copy DAGs folder into Airflow home

```powershell
# Tell Airflow where to find our DAGs
$env:AIRFLOW__CORE__DAGS_FOLDER = "$PWD\dags"
```

Or add to `airflow_home/airflow.cfg`:
```ini
[core]
dags_folder = <absolute path to project>\dags
```

---

## Running

### Option A — Airflow (full orchestration)

> [!IMPORTANT]
> **Windows Compatibility**: Airflow requires a POSIX environment and cannot be run natively on Windows. To use the Airflow webserver and scheduler, please use **WSL2** (Windows Subsystem for Linux), or use **Option B** below for native Windows execution.

Open **two PowerShell windows** (within WSL2):

```powershell
# Window 1 — API / Web server
airflow api-server --port 8080

# Window 2 — Scheduler
airflow scheduler
```

Open `http://localhost:8080`, log in with `admin / admin`, and enable:
- `data_lake_generator_dag`  → runs every 5 minutes
- `data_lake_pipeline_dag`   → runs every 30 minutes

### Option B — Run scripts manually (no Airflow)

```powershell
# Set PYTHONPATH so imports work
$env:PYTHONPATH = "$PWD"

# Step 1 — Generate data + push to Bronze (all 3 generators)
python -m generator.sales_generator
python -m generator.customer_events_generator
python -m generator.inventory_generator

# Step 2 — Bronze → Silver
python -m pipeline.bronze_to_silver

# Step 3 — Silver → Gold
python -m pipeline.silver_to_gold
```

---

## Verifying Output

```powershell
# Check staging CSVs
Get-ChildItem local_output -Recurse -Filter *.csv

# Check Bronze layer
Get-ChildItem datalake\bronze -Recurse -Filter *.csv

# Check Silver layer (Parquet)
Get-ChildItem datalake\silver -Recurse -Filter *.parquet

# Check Gold layer (Parquet)
Get-ChildItem datalake\gold -Recurse -Filter *.parquet
```

**Inspect a Gold table in Python:**

```python
import pandas as pd
import glob

files = sorted(glob.glob("datalake/gold/daily_sales_summary/*.parquet"))
df = pd.read_parquet(files[-1])   # latest snapshot
print(df.to_string())
```

---

## Silver Validation Rules

| Domain | Check |
|--------|-------|
| sales | Null check on sale_id, customer_id, product_id, quantity, unit_price, total_amount |
| sales | `total_amount == quantity × unit_price` (auto-corrected if both operands present) |
| customer_events | Null check on event_id, customer_id, session_id, event_type |
| customer_events | `event_type` must be one of: login, browse, add_to_cart, checkout, logout |
| inventory | Null check on movement_id, product_id, warehouse_id, movement_type, quantity |
| inventory | `movement_type` must be one of: inbound, outbound, adjustment |
| inventory | `quantity` must be a positive number |

Invalid rows are **kept** in Silver with `is_valid = False` and a `validation_errors` description — they flow to Silver but are excluded from Gold aggregations.

---

## Gold Tables

| Table | Source | Description |
|-------|--------|-------------|
| `daily_sales_summary` | silver/sales | Revenue, order count, avg order value per day |
| `category_sales_summary` | silver/sales | Revenue breakdown by product category per day |
| `payment_method_summary` | silver/sales | Revenue by payment method per day |
| `customer_activity_summary` | silver/customer_events | Event counts by type per day |
| `device_usage_summary` | silver/customer_events | Session counts by device type per day |
| `inventory_movement_summary` | silver/inventory | Qty moved per product/warehouse/type per day |
| `inventory_net_position` | silver/inventory | Net stock (inbound − outbound) per product/warehouse |