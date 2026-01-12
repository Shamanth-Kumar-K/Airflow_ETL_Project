import pandas as pd
import numpy as np

DATA_PATH = "/opt/airflow/sprint_3/Data/raw/ecommerce_amazon.csv"


def process_chunk(chunk):
    chunk['FinalPrice'] = chunk['TotalAmount'] * (1 - chunk['Discount'])
    return chunk


def run_parallel_transformation():
    print("Starting parallel-style transformation...")

    df = pd.read_csv(DATA_PATH)
    print(f"Loaded data with {len(df)} rows")

    # Simulate parallel processing via chunking
    chunks = np.array_split(df, 4)

    processed_chunks = []
    for i, chunk in enumerate(chunks):
        print(f"Processing chunk {i+1}")
        processed_chunks.append(process_chunk(chunk))

    final_df = pd.concat(processed_chunks)

    print("Transformation completed successfully")
    print(final_df.head())
