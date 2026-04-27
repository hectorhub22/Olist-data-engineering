from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="00_olist_master_etl",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["olist", "orchestration"],
) as dag:
    start = EmptyOperator(task_id="start")

    customers = TriggerDagRunOperator(
        task_id="trigger_customers",
        trigger_dag_id="olist_customers_dataset_etl",
        wait_for_completion=True,
    )
    geolocation = TriggerDagRunOperator(
        task_id="trigger_geolocation",
        trigger_dag_id="olist_geolocation_dataset_etl",
        wait_for_completion=True,
    )
    sellers = TriggerDagRunOperator(
        task_id="trigger_sellers",
        trigger_dag_id="olist_sellers_dataset_etl",
        wait_for_completion=True,
    )
    products = TriggerDagRunOperator(
        task_id="trigger_products",
        trigger_dag_id="olist_products_dataset_etl",
        wait_for_completion=True,
    )
    translation = TriggerDagRunOperator(
        task_id="trigger_product_category_translation",
        trigger_dag_id="product_category_name_translation_etl",
        wait_for_completion=True,
    )
    orders = TriggerDagRunOperator(
        task_id="trigger_orders",
        trigger_dag_id="olist_orders_dataset_etl",
        wait_for_completion=True,
    )
    order_items = TriggerDagRunOperator(
        task_id="trigger_order_items",
        trigger_dag_id="olist_order_items_dataset_etl",
        wait_for_completion=True,
    )
    order_payments = TriggerDagRunOperator(
        task_id="trigger_order_payments",
        trigger_dag_id="olist_order_payments_dataset_etl",
        wait_for_completion=True,
    )
    order_reviews = TriggerDagRunOperator(
        task_id="trigger_order_reviews",
        trigger_dag_id="olist_order_reviews_dataset_etl",
        wait_for_completion=True,
    )
    transform_curated = TriggerDagRunOperator(
        task_id="trigger_transform_curated",
        trigger_dag_id="10_olist_transform_curated",
        wait_for_completion=True,
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> [customers, geolocation, sellers, products]
        >> translation
        >> orders
        >> [order_items, order_payments, order_reviews]
        >> transform_curated
        >> end
    )

