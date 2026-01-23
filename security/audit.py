
import csv
from datetime import datetime

AUDIT_FILE = "data/audit_logs.csv"

def write_audit(user, action, status):
    with open(AUDIT_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([user, action, status, datetime.now()])
from pathlib import Path
import csv
from datetime import datetime

AUDIT_DIR = Path("/opt/airflow/data")
AUDIT_DIR.mkdir(parents=True, exist_ok=True)

AUDIT_FILE = AUDIT_DIR / "audit_logs.csv"

def write_audit(user, action, status):
    with open(AUDIT_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([user, action, status, datetime.now()])
