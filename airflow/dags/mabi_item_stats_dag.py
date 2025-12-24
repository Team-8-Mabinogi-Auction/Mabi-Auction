from __future__ import annotations

from pathlib import Path

import pendulum
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


POSTGRES_CONN_ID = "mabi_postgres"

_BASE_DIR = Path(__file__).resolve().parent
_SQL_DIR = _BASE_DIR / "sql"


def _read_sql(name: str) -> str:
    return (_SQL_DIR / name).read_text(encoding="utf-8").strip()


with DAG(
    dag_id="mabi_item_display_hourly_stats",
    description="Hourly aggregates (per item_display_name) from mabi_auction.auction_logs into mabi_analytics schema",
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["mabinogi", "analytics", "postgres"],
) as dag:
    init_analytics_schema = PostgresOperator(
        task_id="init_analytics_schema",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=_read_sql("init_analytics_schema.sql"),
    )

    refresh_hourly_stats = PostgresOperator(
        task_id="refresh_item_display_hourly_stats",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=_read_sql("refresh_item_display_hourly_stats.sql"),
    )

    init_analytics_schema >> refresh_hourly_stats


