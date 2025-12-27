import pandas as pd
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ---------------- EXTRACT ----------------
def extract_data():
    RAW_FILE = "/opt/airflow/sprint_1/data/raw/sensor_data.csv"
    EXTRACTED_DIR = "/opt/airflow/sprint_1/data/extracted"
    EXTRACTED_FILE = "extracted_sensor_data.csv"

    COLUMNS = [
        "date", "time", "epoch", "moteid",
        "temperature", "humidity", "light", "voltage"
    ]

    os.makedirs(EXTRACTED_DIR, exist_ok=True)

    df = pd.read_csv(RAW_FILE, header=None, names=COLUMNS)

    output_path = os.path.join(EXTRACTED_DIR, EXTRACTED_FILE)
    df.to_csv(output_path, index=False)

    logging.info("EXTRACT stage completed")


# ---------------- TRANSFORM ----------------
def transform_data():
    EXTRACTED_FILE = "/opt/airflow/sprint_1/data/extracted/extracted_sensor_data.csv"
    TRANSFORMED_DIR = "/opt/airflow/sprint_1/data/transformed"
    TRANSFORMED_FILE = "transformed_sensor_data.csv"

    os.makedirs(TRANSFORMED_DIR, exist_ok=True)

    df = pd.read_csv(EXTRACTED_FILE)

    df["datetime"] = pd.to_datetime(
        df["date"] + " " + df["time"], errors="coerce"
    )

    df = df.dropna(subset=["datetime"])
    df.drop(columns=["date", "time"], inplace=True)

    df = df.dropna()
    df = df.drop_duplicates()

    df = df[
        (df["temperature"].between(-10, 60)) &
        (df["humidity"].between(0, 100)) &
        (df["voltage"] > 0) &
        (df["light"] >= 0)
    ]

    output_path = os.path.join(TRANSFORMED_DIR, TRANSFORMED_FILE)
    df.to_csv(output_path, index=False)

    logging.info("TRANSFORM stage completed")


# ---------------- LOAD ----------------
def load_data():
    TRANSFORMED_FILE = "/opt/airflow/sprint_1/data/transformed/transformed_sensor_data.csv"
    FINAL_DIR = "/opt/airflow/sprint_1/data/final"
    FINAL_FILE = "final_sensor_data.csv"

    os.makedirs(FINAL_DIR, exist_ok=True)

    df = pd.read_csv(TRANSFORMED_FILE)

    output_path = os.path.join(FINAL_DIR, FINAL_FILE)
    df.to_csv(output_path, index=False)

    logging.info("LOAD stage completed")
