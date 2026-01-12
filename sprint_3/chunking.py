import pandas as pd
import os

# ----------------------------------
# File paths
# ----------------------------------
INPUT_PATH = "/opt/airflow/sprint_3/Data/raw/ecommerce_amazon.csv"
OUTPUT_PATH = "/opt/airflow/sprint_3/Data/final/processed_chunked_data.csv"

CHUNK_SIZE = 20000


def process_chunk(chunk):
    """
    Apply transformations on each chunk
    """
    chunk["FinalPrice"] = chunk["TotalAmount"] * (1 - chunk["Discount"])
    return chunk


def run_chunking():
    print("Starting chunk-wise processing...")

    # Ensure output directory exists
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    first_chunk = True

    for chunk in pd.read_csv(INPUT_PATH, chunksize=CHUNK_SIZE):
        print(f"Processing chunk with {len(chunk)} rows")

        chunk = process_chunk(chunk)

        # Append safely without deleting file
        chunk.to_csv(
            OUTPUT_PATH,
            mode="w" if first_chunk else "a",
            header=first_chunk,
            index=False
        )

        first_chunk = False

    print("Chunk processing completed successfully!")
    print(f"Output saved at: {OUTPUT_PATH}")
