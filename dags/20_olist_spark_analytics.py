from __future__ import annotations

import os
import subprocess
from datetime import datetime

from airflow.decorators import dag, task

from analytics._analytics_utils import ensure_analytics_schema


@dag(
    dag_id="20_olist_spark_analytics",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["olist", "spark", "analytics"],
)
def olist_spark_analytics():
    @task
    def create_analytics_schema():
        ensure_analytics_schema()

    @task
    def run_spark_job():
        spark_master = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
        app_path = "/opt/airflow/spark_jobs/olist_analytics_job.py"

        cmd = [
            "spark-submit",
            # Driver JDBC must be on classpath; SparkSession spark.jars.packages alone is unreliable here.
            "--packages",
            "org.postgresql:postgresql:42.7.3",
            "--master",
            spark_master,
            "--deploy-mode",
            "client",
            app_path,
        ]

        subprocess.run(cmd, check=True)

    create_analytics_schema() >> run_spark_job()


olist_spark_analytics()

