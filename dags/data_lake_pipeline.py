"""
dags/data_lake_pipeline.py
Apache Airflow DAG — Data Lake Medallion Architecture Pipeline

DAG 1: data_lake_generator_dag  (schedule: every 5 min)
  Runs all 3 generators in parallel, then uploads each result to Bronze.
  Tasks:
    generate_sales
    generate_customer_events
    generate_inventory

DAG 2: data_lake_pipeline_dag   (schedule: every 30 min)
  After generators have had time to accumulate data, transforms Bronze→Silver→Gold.
  Tasks:
    bronze_to_silver  →  silver_to_gold

Both DAGs are defined in this single file for easy discovery by Airflow.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Default task arguments  (applied to every operator unless overridden)
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner":            "data_engineering",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=1),
    "email_on_failure": False,
    "email_on_retry":   False,
}

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DAG 1 — Generator DAG  (every 5 minutes)
# ---------------------------------------------------------------------------

with DAG(
    dag_id="data_lake_generator_dag",
    description="Generate synthetic data for 3 fact tables and push to Bronze layer",
    schedule="*/5 * * * *",            # every 5 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,                 # avoid overlapping runs
    default_args=DEFAULT_ARGS,
    tags=["datalake", "bronze", "generator"],
) as generator_dag:

    # ---- Sales ----
    from generator.sales_generator import run as run_sales

    t_sales = PythonOperator(
        task_id="generate_sales",
        python_callable=run_sales,
        do_xcom_push=False,
        doc_md="""
        Generates 10 synthetic e-commerce sales rows (fact_sales),
        writes a CSV to local_output/sales/, then copies it to
        datalake/bronze/sales/year=.../month=.../day=.../
        """,
    )

    # ---- Customer Events ----
    from generator.customer_events_generator import run as run_events

    t_events = PythonOperator(
        task_id="generate_customer_events",
        python_callable=run_events,
        do_xcom_push=False,
        doc_md="""
        Generates 15 synthetic clickstream event rows (fact_customer_events),
        writes a CSV to local_output/customer_events/, then copies it to
        datalake/bronze/customer_events/year=.../month=.../day=.../
        """,
    )

    # ---- Inventory ----
    from generator.inventory_generator import run as run_inventory

    t_inventory = PythonOperator(
        task_id="generate_inventory",
        python_callable=run_inventory,
        do_xcom_push=False,
        doc_md="""
        Generates 8 synthetic warehouse movement rows (fact_inventory_movements),
        writes a CSV to local_output/inventory/, then copies it to
        datalake/bronze/inventory/year=.../month=.../day=.../
        """,
    )

    # All 3 generators run in parallel (no inter-dependency)
    [t_sales, t_events, t_inventory]


# ---------------------------------------------------------------------------
# DAG 2 — Pipeline DAG  (every 30 minutes)
# ---------------------------------------------------------------------------

with DAG(
    dag_id="data_lake_pipeline_dag",
    description="Transform Bronze→Silver (clean) then Silver→Gold (aggregate) for all 3 domains",
    schedule="*/30 * * * *",           # every 30 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["datalake", "silver", "gold", "pipeline"],
) as pipeline_dag:

    from pipeline.bronze_to_silver import run as run_bronze_to_silver
    from pipeline.silver_to_gold   import run as run_silver_to_gold

    t_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=run_bronze_to_silver,
        do_xcom_push=False,
        doc_md="""
        Reads all unprocessed Bronze CSVs for every domain.
        Applies domain-specific cleaning:
          - sales:           dedup, null checks, fix total_amount
          - customer_events: dedup, null checks, validate event_type enum
          - inventory:       dedup, null checks, validate movement_type, non-zero qty
        Writes cleaned Parquet to datalake/silver/<domain>/year=.../month=.../day=.../
        Marks processed files so they are not re-processed on the next run.
        """,
    )

    t_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=run_silver_to_gold,
        do_xcom_push=False,
        doc_md="""
        Reads all Silver Parquet files and writes Gold aggregate snapshots:
          - daily_sales_summary + category_sales_summary + payment_method_summary
          - customer_activity_summary + device_usage_summary
          - inventory_movement_summary + inventory_net_position
        """,
    )

    # Bronze→Silver must complete before Silver→Gold
    t_silver >> t_gold
