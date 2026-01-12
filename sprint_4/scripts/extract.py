import pandas as pd
from datetime import datetime
from pathlib import Path

# --------------------------------------------------
# Resolve project root (sprint_4)
# --------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Paths
SOURCE_FILE = PROJECT_ROOT / "amazon.csv"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
QA_DIR = PROJECT_ROOT / "QA"
ROW_COUNT_FILE = QA_DIR / "row_counts.csv"

# --------------------------------------------------
# Ensure directories exist
# --------------------------------------------------
RAW_DIR.mkdir(parents=True, exist_ok=True)
QA_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# Read source data AS-IS (RAW means no logic)
# --------------------------------------------------
df = pd.read_csv(SOURCE_FILE)

# --------------------------------------------------
# Write RAW copy
# --------------------------------------------------
raw_file = RAW_DIR / "amazon_raw.csv"
df.to_csv(raw_file, index=False)

# --------------------------------------------------
# Capture row count for QA
# --------------------------------------------------
row_count = len(df)
run_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

row_count_record = pd.DataFrame([{
    "run_date": run_date,
    "layer": "raw",
    "row_count": row_count
}])

if ROW_COUNT_FILE.exists():
    row_count_record.to_csv(ROW_COUNT_FILE, mode="a", header=False, index=False)
else:
    row_count_record.to_csv(ROW_COUNT_FILE, index=False)

print(f"RAW layer created successfully. Rows: {row_count}")
