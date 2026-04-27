from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from transform._transform_utils import refresh_curated_tables


@dag(
    dag_id="10_olist_transform_curated",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["olist", "transform", "curated"],
)
def olist_transform_curated():
    @task
    def transform_raw_to_curated():
        refresh_curated_tables()

    transform_raw_to_curated()


olist_transform_curated()

