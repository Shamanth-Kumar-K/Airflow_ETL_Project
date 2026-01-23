from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pathlib import Path

# ETL pipeline
from sprint_6.etl.pipeline import run_pipeline

# Security modules
from security.auth import validate_api_key
from security.roles import check_permission
from security.audit_logger import log_event


DEFAULT_ARGS = {
    "owner": "team_5",
    "depends_on_past": False,
    "retries": 0,
}

USER_NAME = "engineer_1"
USER_ROLE = "engineer"


def trigger_secure_etl():
    """
    Sprint 7 secured ETL trigger with authentication, authorization, and audit logging
    """

    dag_id = "sprint_7_secure_etl_pipeline"
    task_id = "run_sprint_7_secure_pipeline"

    # 1️⃣ Audit start
    log_event("ETL_START", dag_id, task_id, "STARTED")

    try:
        # 2️⃣ Authentication
        validate_api_key()

        # 3️⃣ Authorization
        check_permission(USER_ROLE, "run_etl")

        input_path = Path("/opt/airflow/sprint_6/data/raw")
        output_path = Path("/opt/airflow/data")

        # 4️⃣ Run existing ETL
        run_pipeline(input_path, output_path)

        # 5️⃣ Audit success
        log_event("ETL_END", dag_id, task_id, "SUCCESS")

    except Exception as e:
        # 6️⃣ Audit failure
        log_event("ETL_END", dag_id, task_id, "FAILED", str(e))
        raise



with DAG(
    dag_id="sprint_7_secure_etl_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["sprint_7", "security", "etl"],
) as dag:

    run_sprint_7_pipeline = PythonOperator(
        task_id="run_sprint_7_secure_pipeline",
        python_callable=trigger_secure_etl,
    )

    run_sprint_7_pipeline
