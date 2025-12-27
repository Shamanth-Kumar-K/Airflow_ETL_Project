from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from sprint_1.Code.etl_code import extract_data, transform_data, load_data


with DAG(
    dag_id="sensor_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_data
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_data
    )

    extract >> transform >> load
