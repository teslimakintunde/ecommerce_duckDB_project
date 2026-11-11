"""Incremental ingestion: PostgreSQL -> DuckDB using dlt."""
import os
import logging
# from datetime import timedelta
from datetime import date, timedelta
import dlt
from dlt.sources.sql_database import sql_database
from sqlalchemy.engine import URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingestion")

# ---- Structural Configuration Aligned with your exact DDL -----------------------
TABLES = {
    "customers": {"primary_key": "customer_id", "cursor": None},
    "products":  {"primary_key": "product_id",  "cursor": None},
    "sales":     {"primary_key": "order_id",     "cursor": "order_date"}, # Aligned to DDL
}

# Look back window to securely catch late-arriving operational entries
LATE_ARRIVAL_WINDOW = timedelta(days=3)

def build_pg_url() -> str:
    password = os.getenv("PG_PASSWORD")
    if not password:
        raise ValueError("PG_PASSWORD environment variable is not set")
    # URL.create correctly escapes special characters (like '@') inside your password
    return URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("PG_USER", "postgres"),
        password=password,
        host=os.getenv("PG_HOST", "host.docker.internal"),
        port=int(os.getenv("PG_PORT", "5432")),
        database=os.getenv("PG_DATABASE", "duckdb_ecommerce_project"), # Updated to match your .env
    ).render_as_string(hide_password=False)

def ingest_pg_to_duckdb() -> None:
    # Match the explicit schema path from your active .env
    duckdb_path = os.getenv(
        "DUCKDB_PATH", "/opt/airflow/data/warehouse/ecommerce_warehouse.db"
    )

    pipeline = dlt.pipeline(
        pipeline_name="pg_ecommerce_ingestion",
        destination=dlt.destinations.duckdb(duckdb_path),
        dataset_name="raw_data",
    )

    source = sql_database(
        credentials=build_pg_url(),
        schema="public",
        table_names=list(TABLES),
    )

    for table, cfg in TABLES.items():
        hints = {
            "write_disposition": "merge",       # Upsert configuration
            "primary_key": cfg["primary_key"],  # Prevents record duplicates
        }
        if cfg["cursor"]:
            hints["incremental"] = dlt.sources.incremental(
                cfg["cursor"],
                initial_value=date(2020, 1, 1),
                lag=LATE_ARRIVAL_WINDOW.total_seconds(), # Standardized fallback compatibility
            )
        source.resources[table].apply_hints(**hints)

    log.info("Starting production ingestion for tables: %s", list(TABLES))
    load_info = pipeline.run(source)
    load_info.raise_on_failed_jobs()  
    log.info("Ingestion complete:\n%s", load_info)

if __name__ == "__main__":
    ingest_pg_to_duckdb()
