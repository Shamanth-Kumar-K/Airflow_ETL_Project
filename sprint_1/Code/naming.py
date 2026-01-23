import pandas as pd
import os

# -------- CONFIG --------
RAW_FILE = "../data/raw/sensor_data.csv"
EXTRACTED_DIR = "../data/extracted"
EXTRACTED_FILE = "extracted_sensor_data.csv"

COLUMNS = [
    "date",
    "time",
    "epoch",
    "moteid",
    "temperature",
    "humidity",
    "light",
    "voltage"
]

# -------- CREATE DIR IF NOT EXISTS --------
os.makedirs(EXTRACTED_DIR, exist_ok=True)

# -------- READ RAW DATA (NO HEADER) --------
df = pd.read_csv(
    RAW_FILE,
    header=None,
    names=COLUMNS
)

# -------- VERIFY IN MEMORY --------
print("Shape:", df.shape)
print("Columns:", df.columns.tolist())
print(df.head())

# -------- SAVE EXTRACTED DATA --------
output_path = os.path.join(EXTRACTED_DIR, EXTRACTED_FILE)
df.to_csv(output_path, index=False)

print(f"✅ Extracted file saved at: {output_path}")
