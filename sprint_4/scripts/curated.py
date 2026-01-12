import pandas as pd
from pathlib import Path
from datetime import datetime

# =========================
# PATHS
# =========================
PROJECT_ROOT = Path(__file__).resolve().parents[1]

STAGING_FILE = PROJECT_ROOT / "data" / "staging" / "staging_valid.csv"
CURATED_DIR = PROJECT_ROOT / "data" / "curated"
QA_DIR = PROJECT_ROOT / "QA"

CURATED_DIR.mkdir(exist_ok=True)
QA_DIR.mkdir(exist_ok=True)

# =========================
# LOAD DATA
# =========================
df = pd.read_csv(STAGING_FILE)
input_rows = len(df)

# =========================
# BUSINESS VALIDATION
# =========================
hard_fail = (
    df["OrderID"].isna() |
    (df["TotalAmount"] <= 0)
)

curated_valid = df[~hard_fail].copy()
curated_rejected = df[hard_fail].copy()

# =========================
# WRITE OUTPUTS
# =========================
curated_valid.to_csv(
    CURATED_DIR / "curated_valid.csv", index=False
)

# =========================
# CURATED QA REPORT
# =========================
dq_score = round((len(curated_valid) / input_rows) * 100, 2)

qa_report = pd.DataFrame([{
    "run_date": datetime.now(),
    "curated_valid": len(curated_valid),
    "curated_rejected": len(curated_rejected),
    "dq_score": dq_score
}])

qa_report.to_csv(QA_DIR / "dq_report_curated.csv", index=False)

# =========================
# LOG
# =========================
print(f"CURATED INPUT: {input_rows}")
print(f"CURATED VALID: {len(curated_valid)}")
print(f"CURATED REJECTED: {len(curated_rejected)}")
print("CURATED QA GENERATED")
