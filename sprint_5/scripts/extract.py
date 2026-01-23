import logging
import os
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | EXTRACT | %(message)s"
)
logger = logging.getLogger(__name__)

RAW_FILE = "/opt/airflow/sprint_5/data/raw/amazon.csv"
EXTRACTED_FILE = "/opt/airflow/sprint_5/data/processed/extracted.csv"

def main():
    logger.info("Starting extraction")

    if not os.path.exists(RAW_FILE):
        raise FileNotFoundError(f"Raw file not found: {RAW_FILE}")

    df = pd.read_csv(RAW_FILE)
    os.makedirs(os.path.dirname(EXTRACTED_FILE), exist_ok=True)
    df.to_csv(EXTRACTED_FILE, index=False)

    logger.info(f"Extraction completed. Rows: {len(df)}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error("Extraction failed", exc_info=True)
        raise
