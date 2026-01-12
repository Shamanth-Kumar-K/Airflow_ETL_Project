import pandas as pd
from pathlib import Path
from datetime import datetime

# =========================
# PROJECT PATHS
# =========================
PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_FILE = PROJECT_ROOT / "amazon.csv"
STAGING_DIR = PROJECT_ROOT / "data" / "staging"
QA_DIR = PROJECT_ROOT / "QA"

STAGING_DIR.mkdir(parents=True, exist_ok=True)
QA_DIR.mkdir(exist_ok=True)

# =========================
# LOAD RAW DATA
# =========================
df = pd.read_csv(RAW_FILE)
raw_count = len(df)

# =========================
# AGE STANDARDIZATION
# =========================
# Convert values like "26" → 26
df["age_cleaned"] = (
    df["Age"]
    .astype(str)
    .str.replace('"', '', regex=False)
    .str.strip()
)

df["age_numeric"] = pd.to_numeric(df["age_cleaned"], errors="coerce")

# =========================
# STAGING VALIDATION
# =========================
# Reject only if Age exists but cannot be converted
invalid_age_format = (
    df["Age"].notna() &
    df["age_numeric"].isna()
)

staging_valid = df[~invalid_age_format].copy()
staging_rejected = df[invalid_age_format].copy()

staging_rejected["rejection_reason"] = "Invalid age format"

# =========================
# WRITE OUTPUTS
# =========================
staging_valid.to_csv(
    STAGING_DIR / "staging_valid.csv", index=False
)

staging_rejected.to_csv(
    STAGING_DIR / "staging_rejected.csv", index=False
)

# =========================
# STAGING QA REPORT
# =========================
dq_score = round((len(staging_valid) / raw_count) * 100, 2)

qa_report = pd.DataFrame([{
    "run_date": datetime.now(),
    "raw_count": raw_count,
    "staging_count": len(staging_valid),
    "staging_rejected": len(staging_rejected),
    "dq_score": dq_score
}])

qa_report.to_csv(QA_DIR / "dq_report_staging.csv", index=False)

# =========================
# LOG
# =========================
print(f"RAW COUNT: {raw_count}")
print(f"STAGING COUNT: {len(staging_valid)}")
print(f"STAGING REJECTED: {len(staging_rejected)}")
print("STAGING QA GENERATED")
