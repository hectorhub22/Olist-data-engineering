# Olist CSVs (input)

Coloca aquí los archivos `.csv` del dataset de Olist. En Docker, esta carpeta se monta en Airflow como `/opt/airflow/data`.

## Archivos esperados por los DAGs

Estos nombres deben coincidir exactamente:

- `olist_customers_dataset.csv` → Postgres: `raw.olist_customers_dataset`
- `olist_geolocation_dataset.csv` → Postgres: `raw.olist_geolocation_dataset`
- `olist_order_items_dataset.csv` → Postgres: `raw.olist_order_items_dataset`
- `olist_order_payments_dataset.csv` → Postgres: `raw.olist_order_payments_dataset`
- `olist_order_reviews_dataset.csv` → Postgres: `raw.olist_order_reviews_dataset`
- `olist_orders_dataset.csv` → Postgres: `raw.olist_orders_dataset`
- `olist_products_dataset.csv` → Postgres: `raw.olist_products_dataset`
- `olist_sellers_dataset.csv` → Postgres: `raw.olist_sellers_dataset`
- `product_category_name_translation.csv` → Postgres: `raw.product_category_name_translation`

## Variables de entorno (opcionales)

Si cambias el host/puerto/credenciales de Postgres o la ruta de datos, puedes setear:

- `OLIST_PG_HOST` (default: `postgres`)
- `OLIST_PG_PORT` (default: `5432`)
- `OLIST_PG_DB` (default: `olist`)
- `OLIST_PG_USER` (default: `airflow`)
- `OLIST_PG_PASSWORD` (default: `airflow`)
- `OLIST_DATA_DIR` (default: `/opt/airflow/data`)

