from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pathlib import Path

# Import ETL entry point
from sprint_6.etl.pipeline import run_pipeline


DEFAULT_ARGS = {
    "owner": "team_5",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="sprint_6_etl_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # Manual trigger
    catchup=False,
    tags=["sprint_6", "etl"],
) as dag:

    def trigger_etl():
        input_path = Path("/opt/airflow/sprint_6/data/raw")
        output_path = Path("/opt/airflow/sprint_6/data/curated")

        run_pipeline(input_path, output_path)

    run_sprint_6_pipeline = PythonOperator(
        task_id="run_sprint_6_pipeline",
        python_callable=trigger_etl,
    )

    run_sprint_6_pipeline
