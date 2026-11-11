"""E-commerce ELT: PostgreSQL -> dlt -> DuckDB -> (dbt in Stage 4)."""
import logging
import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)

DUCKDB_PATH = os.getenv(
    "DUCKDB_PATH", "/opt/airflow/data/warehouse/ecommerce_warehouse.duckdb"
)
RAW_TABLES = {"customers": "customer_id", "products": "product_id", "sales": "order_id"}


def notify_failure(context):
    """Runs when a task finally fails (after all retries). Hook for Slack/email later."""
    ti = context["task_instance"]
    log.error(
        "PIPELINE FAILURE | dag=%s task=%s run=%s try=%s | log=%s",
        ti.dag_id, ti.task_id, context["run_id"], ti.try_number, ti.log_url,
    )


default_args = {
    "owner": "tesak",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,       # 2 min, then 4 min, ...
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": notify_failure,
}


@dag(
    dag_id="ecommerce_elt_pipeline",
    description="Incremental ELT: PostgreSQL -> dlt -> DuckDB -> dbt",
    default_args=default_args,
    schedule="0 2 * * *",                                     # 02:00 every day
    start_date=pendulum.datetime(2026, 1, 1, tz="Africa/Lagos"),
    catchup=False,           # dlt tracks its own state, so interval backfills add nothing
    max_active_runs=1,       # DuckDB is single-writer: never overlap runs
    dagrun_timeout=timedelta(hours=1),
    tags=["ecommerce", "dlt", "duckdb", "dbt"],
    doc_md=__doc__,
)
def ecommerce_elt_pipeline():
    start = EmptyOperator(task_id="start_pipeline")

    ingest = BashOperator(
        task_id="ingest_postgres_to_duckdb",
        bash_command="cd /opt/airflow && python dlt_pipeline/extract_load.py",
    )

    @task(retries=0)  # a data problem will not fix itself, so retrying is pointless
    def validate_raw_data() -> dict:
        import duckdb

        con = duckdb.connect(DUCKDB_PATH, read_only=True)
        counts = {}
        try:
            for table, pk in RAW_TABLES.items():
                total, distinct, nulls = con.execute(
                    f"select count(*), count(distinct {pk}), "
                    f"count(*) filter (where {pk} is null) from raw_data.{table}"
                ).fetchone()
                if total == 0:
                    raise AirflowFailException(f"raw_data.{table} is empty")
                if nulls:
                    raise AirflowFailException(f"raw_data.{table}: {nulls} null {pk}")
                if total != distinct:
                    raise AirflowFailException(
                        f"raw_data.{table}: {total - distinct} duplicate {pk} values"
                    )
                counts[table] = total
        finally:
            con.close()
        log.info("Validation passed: %s", counts)
        return counts

    # Replaced by a real dbt task in Stage 4
    transform = EmptyOperator(task_id="dbt_transformations_placeholder")

    end = EmptyOperator(task_id="end_pipeline")

    start >> ingest >> validate_raw_data() >> transform >> end


ecommerce_elt_pipeline()