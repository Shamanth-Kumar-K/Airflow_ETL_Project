import logging
import pandas as pd
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | LOAD | %(message)s"
)
logger = logging.getLogger(__name__)

INPUT_FILE = "/opt/airflow/sprint_5/data/processed/transformed.csv"

def main():
    logger.info("Load started")

    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError("Transformed file missing")

    df = pd.read_csv(INPUT_FILE)

    # Simulated load
    logger.info(f"Loaded {len(df)} records into target system")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.error("Load failed", exc_info=True)
        raise
