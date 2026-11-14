"""E-commerce ELT: PostgreSQL -> dlt -> DuckDB -> dbt build -> checks.

Params (Trigger DAG w/ config):
  dlt_full_refresh : drop raw tables and reload everything from Postgres
  full_refresh     : rebuild dbt incremental models from scratch
"""
import json
import logging
import os
import urllib.request
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

DUCKDB_PATH = os.getenv(
    "DUCKDB_PATH", "/opt/airflow/data/warehouse/ecommerce_warehouse.db"
)

# key = the column dlt merges on; not_null = extra columns that must never be blank
RAW_TABLES = {
    "customers_raw":  {"key": "customer_id",  "not_null": []},
    "products_raw":   {"key": "product_id",   "not_null": []},
    "sales_data_raw": {"key": "line_item_id", "not_null": ["order_id"]},
}

DBT_DIR = "/opt/airflow/dbt_project"
DBT_TARGET = "/tmp/dbt_target"
DBT_ENV = {
    "DBT_PROFILES_DIR": DBT_DIR,
    "DBT_TARGET_PATH": DBT_TARGET,      # artifacts out of the mounted repo
    "DBT_LOG_PATH": "/tmp/dbt_logs",
}


def notify_failure(context):
    """Runs once a task has failed for good (after all retries)."""
    ti = context["task_instance"]
    msg = (f"PIPELINE FAILURE | dag={ti.dag_id} task={ti.task_id} "
           f"run={context['run_id']} try={ti.try_number} | {ti.log_url}")
    log.error(msg)
    webhook = os.getenv("SLACK_WEBHOOK_URL")      # optional: set in .env to enable
    if webhook:
        try:
            req = urllib.request.Request(
                webhook, data=json.dumps({"text": msg}).encode(),
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=10)
        except Exception:
            log.exception("Could not send Slack alert")


default_args = {
    "owner": "tesak",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": notify_failure,
}


@dag(
    dag_id="ecommerce_elt_pipeline",
    description="PostgreSQL -> dlt -> DuckDB -> dbt build -> checks",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Africa/Lagos"),
    catchup=False,           # dlt and dbt both keep their own high-water marks
    max_active_runs=1,       # DuckDB is single-writer: runs must never overlap
    dagrun_timeout=timedelta(hours=1),
    params={
        "dlt_full_refresh": Param(False, type="boolean",
                                  description="Drop raw tables and reload from Postgres"),
        "full_refresh": Param(False, type="boolean",
                              description="Rebuild dbt incremental models from scratch"),
    },
    tags=["ecommerce", "dlt", "duckdb", "dbt", "production"],
    doc_md=__doc__,
)
def ecommerce_elt_pipeline():
    start = EmptyOperator(task_id="start_pipeline")

    ingest = BashOperator(
        task_id="ingest_postgres_to_duckdb",
        bash_command="cd /opt/airflow && python dlt_pipeline/extract_load.py",
        env={"DLT_FULL_REFRESH": "{{ '1' if params.dlt_full_refresh else '0' }}"},
        append_env=True,
    )

    @task(retries=0)   # bad data will not fix itself, so retrying is pointless
    def validate_raw_data() -> dict:
        import duckdb

        con = duckdb.connect(DUCKDB_PATH, read_only=True)
        counts = {}
        try:
            for table, cfg in RAW_TABLES.items():
                key = cfg["key"]
                total, distinct, blank = con.execute(
                    f"select count(*), count(distinct {key}), "
                    f"count(*) filter (where nullif(trim({key}), '') is null) "
                    f"from raw_data.{table}"
                ).fetchone()
                if total == 0:
                    raise AirflowFailException(f"raw_data.{table} is empty")
                if blank:
                    raise AirflowFailException(f"raw_data.{table}: {blank} blank/null {key}")
                if total != distinct:
                    raise AirflowFailException(
                        f"raw_data.{table}: {total - distinct} duplicate {key} values")
                for col in cfg["not_null"]:
                    bad = con.execute(
                        f"select count(*) from raw_data.{table} "
                        f"where nullif(trim({col}), '') is null").fetchone()[0]
                    if bad:
                        raise AirflowFailException(f"raw_data.{table}: {bad} blank/null {col}")
                counts[table] = total
                log.info("Validated raw_data.%s: %d rows", table, total)

            latest = con.execute(
                "select max(updated_at) from raw_data.sales_data_raw").fetchone()[0]
            log.info("Newest raw sales change: %s", latest)   # freshness: informational only
        finally:
            con.close()
        return counts

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps",
        env=DBT_ENV, append_env=True,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(f"cd {DBT_DIR} && dbt build "
                      "{{ '--full-refresh' if params.full_refresh else '' }}"),
        env=DBT_ENV, append_env=True,
        retries=1,                                   # covers a transient file lock
        execution_timeout=timedelta(minutes=30),
    )

    @task(trigger_rule=TriggerRule.ALL_DONE, retries=0)
    def log_dbt_results() -> dict:
        """Summarise dbt's run_results.json. Runs even when dbt_build failed."""
        path = Path(DBT_TARGET) / "run_results.json"
        if not path.exists():
            log.warning("No run_results.json at %s", path)
            return {}
        data = json.loads(path.read_text())
        counts = {}
        for r in data["results"]:
            counts[r["status"]] = counts.get(r["status"], 0) + 1
            if r["status"] in ("warn", "fail", "error"):
                log.warning("dbt %s | %s | failing_rows=%s | %s", r["status"],
                            r["unique_id"], r.get("failures"),
                            (r.get("message") or "")[:200])
        log.info("dbt summary: %s | elapsed=%.1fs", counts, data.get("elapsed_time", 0))
        return counts

    @task(retries=0)
    def validate_marts() -> dict:
        """Independent post-build checks, separate from dbt's own tests."""
        import duckdb

        con = duckdb.connect(DUCKDB_PATH, read_only=True)
        try:
            stg = con.execute("select count(*) from staging.stg_sales").fetchone()[0]
            fct, fct_distinct = con.execute(
                "select count(*), count(distinct line_item_id) "
                "from marts.fct_sales_profitability").fetchone()
            rev_int = con.execute(
                "select coalesce(sum(recognized_revenue),0) "
                "from intermediate.int_sales_enriched").fetchone()[0]
            rev_fct = con.execute(
                "select coalesce(sum(recognized_revenue),0) "
                "from marts.fct_sales_profitability").fetchone()[0]
        finally:
            con.close()

        if fct != fct_distinct:
            raise AirflowFailException(f"fct_sales_profitability: {fct - fct_distinct} duplicate lines")
        if fct != stg:
            raise AirflowFailException(f"Row mismatch: stg_sales={stg}, fct_sales_profitability={fct}")
        if abs(float(rev_int) - float(rev_fct)) > 0.01:
            raise AirflowFailException(f"Revenue mismatch: intermediate={rev_int}, mart={rev_fct}")
        log.info("Mart checks passed: rows=%s revenue=%s", fct, rev_fct)
        return {"rows": fct, "revenue": float(rev_fct)}

    end = EmptyOperator(task_id="end_pipeline")   # default all_success, on purpose

    raw_check = validate_raw_data()
    summary = log_dbt_results()
    mart_check = validate_marts()

    start >> ingest >> raw_check >> dbt_deps >> dbt_build
    dbt_build >> [summary, mart_check]
    [summary, mart_check] >> end


ecommerce_elt_pipeline()
