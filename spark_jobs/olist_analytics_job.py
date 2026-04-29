from __future__ import annotations

import os

from pyspark.sql import SparkSession, functions as F


def _env(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def main() -> None:
    pg_host = _env("OLIST_PG_HOST", "postgres")
    pg_port = _env("OLIST_PG_PORT", "5432")
    pg_db = _env("OLIST_PG_DB", "olist")
    pg_user = _env("OLIST_PG_USER", "airflow")
    pg_password = _env("OLIST_PG_PASSWORD", "airflow")

    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"

    spark = (
        SparkSession.builder.appName("olist_analytics")
        # Fetch Postgres JDBC driver automatically.
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )

    jdbc_props = {
        "user": pg_user,
        "password": pg_password,
        "driver": "org.postgresql.Driver",
    }

    orders = spark.read.jdbc(jdbc_url, "curated.orders", properties=jdbc_props)
    order_items = spark.read.jdbc(jdbc_url, "curated.order_items", properties=jdbc_props)
    products = spark.read.jdbc(jdbc_url, "curated.products", properties=jdbc_props)
    cat_tr = spark.read.jdbc(
        jdbc_url, "curated.product_category_translation", properties=jdbc_props
    )

    # Ensure timestamps are parsed as timestamp type.
    orders = orders.withColumn(
        "order_purchase_timestamp", F.col("order_purchase_timestamp").cast("timestamp")
    )

    # gmv_by_month: GMV + order count + avg ticket (price sum) per month.
    gmv_by_month = (
        orders.select("order_id", "order_purchase_timestamp")
        .join(order_items.select("order_id", "price"), on="order_id", how="inner")
        .withColumn("month", F.date_trunc("month", F.col("order_purchase_timestamp")))
        .groupBy("month")
        .agg(
            F.round(F.sum("price"), 2).alias("gmv"),
            F.countDistinct("order_id").alias("orders"),
            F.round((F.sum("price") / F.countDistinct("order_id")), 2).alias("avg_ticket"),
        )
        .orderBy("month")
    )

    # revenue_by_category: revenue per (pt/en) category.
    products_enriched = (
        products.select("product_id", "product_category_name")
        .join(cat_tr, on="product_category_name", how="left")
        .withColumnRenamed("product_category_name_english", "category_en")
        .withColumnRenamed("product_category_name", "category_pt")
    )
    revenue_by_category = (
        order_items.select("product_id", "price")
        .join(products_enriched, on="product_id", how="left")
        .groupBy("category_pt", "category_en")
        .agg(
            F.round(F.sum("price"), 2).alias("revenue"),
            F.count("*").alias("items_sold"),
        )
        .orderBy(F.desc("revenue"))
    )

    # Write to Postgres analytics schema.
    # NOTE: The `analytics` schema is expected to exist (created by Airflow before submitting).
    (
        gmv_by_month.write.mode("overwrite")
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "analytics.gmv_by_month")
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .save()
    )

    (
        revenue_by_category.write.mode("overwrite")
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "analytics.revenue_by_category")
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .save()
    )

    spark.stop()


if __name__ == "__main__":
    main()

