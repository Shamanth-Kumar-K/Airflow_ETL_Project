from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------
PROJECT_ROOT = "/opt/airflow/sprint_4"
SCRIPTS_DIR = f"{PROJECT_ROOT}/scripts"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0
}

# -------------------------------------------------------------------
# DAG
# -------------------------------------------------------------------
with DAG(
    dag_id="sprint_4_etl_pipeline",
    default_args=default_args,
    description="End-to-end etl with QA for Sprint 4",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command=f"python {SCRIPTS_DIR}/extract.py"
    )

    staging = BashOperator(
        task_id="staging",
        bash_command=f"python {SCRIPTS_DIR}/staging.py"
    )

    curated = BashOperator(
        task_id="curated",
        bash_command=f"python {SCRIPTS_DIR}/transform_curated.py"
    )

    qa = BashOperator(
        task_id="qa",
        bash_command=f"python {SCRIPTS_DIR}/qa_dashboard.py"
    )

    extract >> staging >> curated >> qa
