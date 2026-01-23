from datetime import datetime
from pathlib import Path
import csv

# Central audit log location for Sprint 7
AUDIT_LOG_PATH = Path("/opt/airflow/data/audit_logger.csv")


def log_event(event_type, dag_id, task_id, status, message=""):
    """
    Writes an audit log entry for ETL execution events.
    Parameters:
    - event_type: ETL_START / ETL_END
    - dag_id: Airflow DAG ID
    - task_id: Airflow task ID
    - status: STARTED / SUCCESS / FAILED
    - message: Optional error or info message
    """

    # Ensure audit directory exists
    AUDIT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    file_exists = AUDIT_LOG_PATH.exists()

    with open(AUDIT_LOG_PATH, mode="a", newline="") as file:
        writer = csv.writer(file)

        # Write header only once
        if not file_exists:
            writer.writerow([
                "timestamp",
                "dag_id",
                "task_id",
                "event_type",
                "status",
                "message"
            ])

        # Write audit record
        writer.writerow([
            datetime.utcnow().isoformat(),
            dag_id,
            task_id,
            event_type,
            status,
            message
        ])
