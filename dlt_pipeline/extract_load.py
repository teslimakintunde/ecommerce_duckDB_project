"""Incremental ingestion: PostgreSQL (raw schema) -> DuckDB using dlt."""
import logging
import os

import dlt
from dlt.sources.sql_database import sql_database
from sqlalchemy.engine import URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingestion")

PG_SCHEMA = "raw"

# cursor=None -> no change-tracking column in the source -> full read + merge
TABLES = {
    "customers_raw":  {"primary_key": "customer_id",  "cursor": None},
    "products_raw":   {"primary_key": "product_id",   "cursor": None},
    "sales_data_raw": {"primary_key": "line_item_id", "cursor": "updated_at"},
}

# Re-read the last hour on every run. Catches rows from transactions that
# committed late with an earlier updated_at. Merge makes the overlap harmless.
CURSOR_LAG_SECONDS = 3600


def build_pg_url() -> str:
    password = os.getenv("PG_PASSWORD")
    if not password:
        raise ValueError("PG_PASSWORD environment variable is not set")
    return URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("PG_USER", "postgres"),
        password=password,
        host=os.getenv("PG_HOST", "host.docker.internal"),
        port=int(os.getenv("PG_PORT", "5432")),
        database=os.getenv("PG_DATABASE", "duckdb_ecommerce_project"),
    ).render_as_string(hide_password=False)


def ingest_pg_to_duckdb() -> None:
    duckdb_path = os.getenv(
        "DUCKDB_PATH", "/opt/airflow/data/warehouse/ecommerce_warehouse.duckdb"
    )

    pipeline = dlt.pipeline(
        pipeline_name="pg_ecommerce_ingestion",
        destination=dlt.destinations.duckdb(duckdb_path),
        dataset_name="raw_data",
    )

    source = sql_database(
        credentials=build_pg_url(),
        schema=PG_SCHEMA,
        table_names=list(TABLES),
    )

    for table, cfg in TABLES.items():
        hints = {
            "write_disposition": "merge",        # upsert, never append
            "primary_key": cfg["primary_key"],   # dedup key
        }
        if cfg["cursor"]:
            hints["incremental"] = dlt.sources.incremental(
                cfg["cursor"],
                lag=CURSOR_LAG_SECONDS,
                on_cursor_value_missing="include",   # don't silently skip NULL cursors
            )
            # if a key appears twice in one batch, keep the latest version
            hints["columns"] = {cfg["cursor"]: {"dedup_sort": "desc"}}
        source.resources[table].apply_hints(**hints)

    # DLT_FULL_REFRESH=1 drops the tables and resets cursor state (full reload)
    refresh = "drop_sources" if os.getenv("DLT_FULL_REFRESH") == "1" else None

    log.info("Starting ingestion for: %s (refresh=%s)", list(TABLES), refresh)
    load_info = pipeline.run(source, refresh=refresh)
    load_info.raise_on_failed_jobs()   # fail loudly so Airflow retries and alerts
    log.info("Ingestion complete:\n%s", load_info)


if __name__ == "__main__":
    ingest_pg_to_duckdb()
