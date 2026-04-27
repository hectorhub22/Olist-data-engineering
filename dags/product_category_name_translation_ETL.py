from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from _olist_etl_utils import get_data_dir, load_csv_to_postgres_raw


@dag(
    dag_id="product_category_name_translation_etl",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["olist", "raw", "etl"],
)
def product_category_name_translation_etl():
    @task
    def load():
        csv_path = get_data_dir() / "product_category_name_translation.csv"
        load_csv_to_postgres_raw(
            csv_path=csv_path, table_name="product_category_name_translation"
        )

    load()


product_category_name_translation_etl()

