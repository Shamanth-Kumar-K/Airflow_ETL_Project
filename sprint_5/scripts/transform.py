import logging
import pandas as pd
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | TRANSFORM | %(message)s"
)
logger = logging.getLogger(__name__)

INPUT_FILE = "/opt/airflow/sprint_5/data/processed/extracted.csv"
OUTPUT_FILE = "/opt/airflow/sprint_5/data/processed/transformed.csv"

def main():
    logger.info("Transformation started")

    df = pd.read_csv(INPUT_FILE)

    # Example transform
    df.columns = [c.lower() for c in df.columns]

    df.to_csv(OUTPUT_FILE, index=False)
    logger.info("Transformation completed")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.error("Transformation failed", exc_info=True)
        raise
