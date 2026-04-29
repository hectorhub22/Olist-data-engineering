from __future__ import annotations

import psycopg2

from _olist_etl_utils import get_postgres_config


def ensure_analytics_schema() -> None:
    pg = get_postgres_config()
    with psycopg2.connect(
        host=pg.host,
        port=pg.port,
        dbname=pg.dbname,
        user=pg.user,
        password=pg.password,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS analytics;")

