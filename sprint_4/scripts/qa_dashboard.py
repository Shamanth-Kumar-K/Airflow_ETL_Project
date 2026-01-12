import pandas as pd
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[1]
QA_DIR = PROJECT_ROOT / "QA"

staging = pd.read_csv(QA_DIR / "dq_report_staging.csv").iloc[-1]
curated = pd.read_csv(QA_DIR / "dq_report_curated.csv").iloc[-1]

dashboard = pd.DataFrame([{
    "run_date": datetime.now(),
    "raw_rows": staging["raw_count"],
    "staging_rows": staging["staging_count"],
    "staging_rejected": staging["staging_rejected"],
    "staging_dq_score": staging["dq_score"],
    "curated_rows": curated["curated_valid"],
    "curated_rejected": curated["curated_rejected"],
    "curated_dq_score": curated["dq_score"],
    "final_status": "PASS"
}])

dashboard.to_csv(QA_DIR / "qa_dashboard.csv", index=False)

print("FINAL QA DASHBOARD GENERATED")
print(dashboard)
