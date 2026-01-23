import logging
import pandas as pd
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | VALIDATE | %(message)s"
)
logger = logging.getLogger(__name__)

INPUT_FILE = "/opt/airflow/sprint_5/data/processed/extracted.csv"

def main():
    logger.info("Validation started")

    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError("Extracted file missing")

    df = pd.read_csv(INPUT_FILE)

    if df.empty:
        raise ValueError("Dataset is empty")

    logger.info(f"Validation passed. Columns: {list(df.columns)}")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.error("Validation failed", exc_info=True)
        raise
