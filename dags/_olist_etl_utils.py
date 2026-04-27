from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


def get_postgres_config() -> PostgresConfig:
    return PostgresConfig(
        host=os.getenv("OLIST_PG_HOST", "postgres"),
        port=int(os.getenv("OLIST_PG_PORT", "5432")),
        dbname=os.getenv("OLIST_PG_DB", "olist"),
        user=os.getenv("OLIST_PG_USER", "airflow"),
        password=os.getenv("OLIST_PG_PASSWORD", "airflow"),
    )


def get_data_dir() -> Path:
    return Path(os.getenv("OLIST_DATA_DIR", "/opt/airflow/data"))


def _iter_csv_rows(csv_path: Path) -> tuple[list[str], Iterable[Sequence[str]]]:
    f = csv_path.open("r", encoding="utf-8", newline="")
    reader = csv.reader(f)
    header = next(reader)
    header = [h.strip().lstrip("\ufeff") for h in header]

    def row_iter() -> Iterable[Sequence[str]]:
        try:
            for row in reader:
                yield row
        finally:
            f.close()

    return header, row_iter()


def load_csv_to_postgres_raw(
    *,
    csv_path: str | Path,
    table_name: str,
    schema: str = "raw",
    truncate_first: bool = True,
    batch_size: int = 10_000,
) -> None:
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(
            f"CSV not found: {csv_path}. Expected files under {get_data_dir()} (mounted from ./data)."
        )

    pg = get_postgres_config()
    columns, rows_iter = _iter_csv_rows(csv_path)

    if not columns:
        raise ValueError(f"CSV has no header columns: {csv_path}")

    with psycopg2.connect(
        host=pg.host,
        port=pg.port,
        dbname=pg.dbname,
        user=pg.user,
        password=pg.password,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))

            col_defs = sql.SQL(", ").join(
                sql.SQL("{} TEXT").format(sql.Identifier(col)) for col in columns
            )
            cur.execute(
                sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
                    sql.Identifier(schema), sql.Identifier(table_name), col_defs
                )
            )

            if truncate_first:
                cur.execute(
                    sql.SQL("TRUNCATE TABLE {}.{}").format(
                        sql.Identifier(schema), sql.Identifier(table_name)
                    )
                )

            insert_sql = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
                sql.Identifier(schema),
                sql.Identifier(table_name),
                sql.SQL(", ").join(sql.Identifier(c) for c in columns),
            )

            batch: list[Sequence[str]] = []
            for row in rows_iter:
                batch.append(row)
                if len(batch) >= batch_size:
                    execute_values(cur, insert_sql, batch, page_size=batch_size)
                    batch.clear()

            if batch:
                execute_values(cur, insert_sql, batch, page_size=len(batch))

