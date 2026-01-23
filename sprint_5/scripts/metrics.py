import logging
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | METRICS | %(message)s"
)
logger = logging.getLogger(__name__)

FILE = "/opt/airflow/sprint_5/data/processed/transformed.csv"

def main():
    logger.info("Metrics collection started")

    df = pd.read_csv(FILE)
    logger.info(f"Row count: {len(df)}")
    logger.info(f"Column count: {len(df.columns)}")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.error("Metrics failed", exc_info=True)
        raise
