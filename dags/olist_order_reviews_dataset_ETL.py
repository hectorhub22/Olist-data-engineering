from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from _olist_etl_utils import get_data_dir, load_csv_to_postgres_raw


@dag(
    dag_id="olist_order_reviews_dataset_etl",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["olist", "raw", "etl"],
)
def olist_order_reviews_dataset_etl():
    @task
    def load():
        csv_path = get_data_dir() / "olist_order_reviews_dataset.csv"
        load_csv_to_postgres_raw(
            csv_path=csv_path, table_name="olist_order_reviews_dataset"
        )

    load()


olist_order_reviews_dataset_etl()

