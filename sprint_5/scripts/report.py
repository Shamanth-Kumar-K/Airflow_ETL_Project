import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | REPORT | %(message)s"
)
logger = logging.getLogger(__name__)

def main():
    logger.info("Daily report started")
    logger.info(f"Report date: {datetime.now().date()}")
    logger.info("Daily report completed (email handled by Airflow)")

if __name__ == "__main__":
    main()
