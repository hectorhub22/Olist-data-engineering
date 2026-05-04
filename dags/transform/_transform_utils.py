from __future__ import annotations

from _olist_etl_utils import get_postgres_config

from psycopg2 import sql
import psycopg2


def _find_column(cur, *, schema: str, table: str, logical_name: str) -> str:
    """
    Returns the real column name in Postgres, tolerating BOM/hidden chars.
    """
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    cols = [r[0] for r in cur.fetchall()]

    def norm(s: str) -> str:
        return s.replace("\ufeff", "").strip().lower()

    target = norm(logical_name)
    for c in cols:
        if norm(c) == target:
            return c
    for c in cols:
        if target in norm(c):
            return c
    raise KeyError(f"Column {logical_name!r} not found in {schema}.{table}. Columns: {cols}")


def refresh_curated_tables() -> None:
    pg = get_postgres_config()
    CURATED_SCHEMA = "curated"
    RAW_SCHEMA = "raw"

    statements = [
        "CREATE SCHEMA IF NOT EXISTS curated;",
        """
        DROP TABLE IF EXISTS {schema}.customers;
        CREATE TABLE {schema}.customers AS
        SELECT DISTINCT
            NULLIF(customer_id, '') AS customer_id,
            NULLIF(customer_unique_id, '') AS customer_unique_id,
            NULLIF(customer_zip_code_prefix, '') AS customer_zip_code_prefix,
            LOWER(TRIM(NULLIF(customer_city, ''))) AS customer_city,
            LOWER(TRIM(NULLIF(customer_state, ''))) AS customer_state
        FROM {raw}.olist_customers_dataset
        WHERE NULLIF(customer_id, '') IS NOT NULL;
        """.format(schema=CURATED_SCHEMA, raw=RAW_SCHEMA),
        """
        DROP TABLE IF EXISTS {schema}.orders;
        CREATE TABLE {schema}.orders AS
        SELECT DISTINCT
            NULLIF(order_id, '') AS order_id,
            NULLIF(customer_id, '') AS customer_id,
            LOWER(TRIM(NULLIF(order_status, ''))) AS order_status,
            NULLIF(order_purchase_timestamp, '')::timestamp AS order_purchase_timestamp,
            NULLIF(order_approved_at, '')::timestamp AS order_approved_at,
            NULLIF(order_delivered_carrier_date, '')::timestamp AS order_delivered_carrier_date,
            NULLIF(order_delivered_customer_date, '')::timestamp AS order_delivered_customer_date,
            NULLIF(order_estimated_delivery_date, '')::timestamp AS order_estimated_delivery_date
        FROM {raw}.olist_orders_dataset
        WHERE NULLIF(order_id, '') IS NOT NULL;
        """.format(schema=CURATED_SCHEMA, raw=RAW_SCHEMA),
        """
        DROP TABLE IF EXISTS {schema}.order_items;
        CREATE TABLE {schema}.order_items AS
        SELECT DISTINCT
            NULLIF(order_id, '') AS order_id,
            NULLIF(order_item_id, '')::integer AS order_item_id,
            NULLIF(product_id, '') AS product_id,
            NULLIF(seller_id, '') AS seller_id,
            NULLIF(shipping_limit_date, '')::timestamp AS shipping_limit_date,
            NULLIF(price, '')::numeric(12, 2) AS price,
            NULLIF(freight_value, '')::numeric(12, 2) AS freight_value
        FROM {raw}.olist_order_items_dataset
        WHERE NULLIF(order_id, '') IS NOT NULL;
        """.format(schema=CURATED_SCHEMA, raw=RAW_SCHEMA),
        """
        DROP TABLE IF EXISTS {schema}.order_payments;
        CREATE TABLE {schema}.order_payments AS
        SELECT DISTINCT
            NULLIF(order_id, '') AS order_id,
            NULLIF(payment_sequential, '')::integer AS payment_sequential,
            LOWER(TRIM(NULLIF(payment_type, ''))) AS payment_type,
            NULLIF(payment_installments, '')::integer AS payment_installments,
            NULLIF(payment_value, '')::numeric(12, 2) AS payment_value
        FROM {raw}.olist_order_payments_dataset
        WHERE NULLIF(order_id, '') IS NOT NULL;
        """.format(schema=CURATED_SCHEMA, raw=RAW_SCHEMA),
        """
        DROP TABLE IF EXISTS {schema}.order_reviews;
        CREATE TABLE {schema}.order_reviews AS
        SELECT DISTINCT
            NULLIF(review_id, '') AS review_id,
            NULLIF(order_id, '') AS order_id,
            NULLIF(review_score, '')::integer AS review_score,
            NULLIF(review_comment_title, '') AS review_comment_title,
            NULLIF(review_comment_message, '') AS review_comment_message,
            NULLIF(review_creation_date, '')::timestamp AS review_creation_date,
            NULLIF(review_answer_timestamp, '')::timestamp AS review_answer_timestamp
        FROM {raw}.olist_order_reviews_dataset
        WHERE NULLIF(order_id, '') IS NOT NULL;
        """.format(schema=CURATED_SCHEMA, raw=RAW_SCHEMA),
        """
        DROP TABLE IF EXISTS {schema}.products;
        CREATE TABLE {schema}.products AS
        SELECT DISTINCT
            NULLIF(product_id, '') AS product_id,
            LOWER(TRIM(NULLIF(product_category_name, ''))) AS product_category_name,
            NULLIF(product_name_lenght, '')::integer AS product_name_lenght,
            NULLIF(product_description_lenght, '')::integer AS product_description_lenght,
            NULLIF(product_photos_qty, '')::integer AS product_photos_qty,
            NULLIF(product_weight_g, '')::integer AS product_weight_g,
            NULLIF(product_length_cm, '')::integer AS product_length_cm,
            NULLIF(product_height_cm, '')::integer AS product_height_cm,
            NULLIF(product_width_cm, '')::integer AS product_width_cm
        FROM {raw}.olist_products_dataset
        WHERE NULLIF(product_id, '') IS NOT NULL;
        """.format(schema=CURATED_SCHEMA, raw=RAW_SCHEMA),
        """
        DROP TABLE IF EXISTS {schema}.sellers;
        CREATE TABLE {schema}.sellers AS
        SELECT DISTINCT
            NULLIF(seller_id, '') AS seller_id,
            NULLIF(seller_zip_code_prefix, '') AS seller_zip_code_prefix,
            LOWER(TRIM(NULLIF(seller_city, ''))) AS seller_city,
            LOWER(TRIM(NULLIF(seller_state, ''))) AS seller_state
        FROM {raw}.olist_sellers_dataset
        WHERE NULLIF(seller_id, '') IS NOT NULL;
        """.format(schema=CURATED_SCHEMA, raw=RAW_SCHEMA),
    ]

    with psycopg2.connect(
        host=pg.host,
        port=pg.port,
        dbname=pg.dbname,
        user=pg.user,
        password=pg.password,
    ) as conn:
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)

            # Handle BOM/hidden-char headers in this CSV (common on Windows).
            src_schema = "raw"
            src_table = "product_category_name_translation"
            col_pt = _find_column(
                cur,
                schema=src_schema,
                table=src_table,
                logical_name="product_category_name",
            )
            col_en = _find_column(
                cur,
                schema=src_schema,
                table=src_table,
                logical_name="product_category_name_english",
            )

            cur.execute("DROP TABLE IF EXISTS curated.product_category_translation;")
            curated_sql = sql.SQL(
                """
                CREATE TABLE curated.product_category_translation AS
                SELECT DISTINCT
                    LOWER(TRIM(NULLIF({col_pt}, ''))) AS product_category_name,
                    LOWER(TRIM(NULLIF({col_en}, ''))) AS product_category_name_english
                FROM {src_schema}.{src_table}
                WHERE NULLIF({col_pt}, '') IS NOT NULL;
                """
            ).format(
                col_pt=sql.Identifier(col_pt),
                col_en=sql.Identifier(col_en),
                src_schema=sql.Identifier(src_schema),
                src_table=sql.Identifier(src_table),
            )
            cur.execute(curated_sql)
