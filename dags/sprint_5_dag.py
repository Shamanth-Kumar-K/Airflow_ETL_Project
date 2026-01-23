from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# =========================
# DEFAULT ARGUMENTS
# =========================
default_args = {
    "owner": "airflow",
    "email": ["kshamanthkumar5@gmail.com"],
    "email_on_failure": True,   # Failure alert
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# =========================
# DAG DEFINITION
# =========================
with DAG(
    dag_id="sprint_5_etl_bash",
    default_args=default_args,
    description="Sprint 5 etl using BashOperator with logging and email alerts",
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["sprint5", "etl", "bash"],
) as dag:

    # =========================
    # etl TASKS
    # =========================
    extract = BashOperator(
        task_id="extract",
        bash_command="python /opt/airflow/sprint_5/scripts/extract.py",
    )

    validate = BashOperator(
        task_id="validate",
        bash_command="python /opt/airflow/sprint_5/scripts/validate.py",
    )

    transform = BashOperator(
        task_id="transform",
        bash_command="python /opt/airflow/sprint_5/scripts/transform.py",
    )

    load = BashOperator(
        task_id="load",
        bash_command="python /opt/airflow/sprint_5/scripts/load.py",
    )

    metrics = BashOperator(
        task_id="metrics",
        bash_command="python /opt/airflow/sprint_5/scripts/metrics.py",
    )

    report = BashOperator(
        task_id="report",
        bash_command="python /opt/airflow/sprint_5/scripts/report.py",
    )

    # =========================
    # SUCCESS EMAIL TASK
    # =========================
    success_email = EmailOperator(
        task_id="success_email",
        to="kshamanthkumar5@gmail.com",
        subject="✅ Sprint 5 etl DAG – SUCCESS",
        html_content="""
        <h3>etl Pipeline Completed Successfully</h3>
        <p><b>DAG:</b> sprint_5_etl_bash</p>
        <p><b>Status:</b> SUCCESS</p>
        <p>All etl tasks (Extract → Validate → Transform → Load → Metrics → Report) completed without errors.</p>
        <p>Check Airflow UI for detailed logs.</p>
        """,
    )

    # =========================
    # TASK DEPENDENCIES
    # =========================
    extract >> validate >> transform >> load >> metrics >> report >> success_email
