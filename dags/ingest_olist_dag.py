from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os

def ingest_csv():
    data_path = '/opt/airflow/data'
    # Usamos el Hook de Airflow para obtener la conexión
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = pg_hook.get_sqlalchemy_engine()
    
    files = [f for f in os.listdir(data_path) if f.endswith('.csv')]
    
    for file in files:
        table_name = file.replace('.csv', '').replace('olist_', '').replace('_dataset', '')
        file_path = os.path.join(data_path, file)
        
        # Leemos con chunksize para mayor eficiencia
        df = pd.read_csv(file_path, encoding='utf-8')
        df.to_sql(table_name, engine, if_exists='replace', index=False, chunksize=1000)
        print(f"Tabla {table_name} cargada correctamente.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    dag_id='ingestion_olist_raw',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    task_ingest = PythonOperator(
        task_id='ingest_all_csvs',
        python_callable=ingest_csv
    )