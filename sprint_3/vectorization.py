import pandas as pd
import time

DATA_PATH = "/opt/airflow/sprint_3/Data/raw/ecommerce_amazon.csv"

def calculate_final_price_loop(df):
    """Non-vectorized (slow) approach using loop"""
    final_prices = []
    for i in range(len(df)):
        final_prices.append(df.loc[i, "TotalAmount"] * (1 - df.loc[i, "Discount"]))
    df["FinalPrice_Loop"] = final_prices
    return df


def calculate_final_price_vectorized(df):
    """Vectorized (fast) approach using pandas"""
    df["FinalPrice_Vectorized"] = df["TotalAmount"] * (1 - df["Discount"])
    return df


def run_vectorization():
    print("Starting vectorization task...")

    df = pd.read_csv(DATA_PATH)

    # Loop-based calculation
    start_loop = time.time()
    df = calculate_final_price_loop(df)
    loop_time = time.time() - start_loop

    # Vectorized calculation
    start_vec = time.time()
    df = calculate_final_price_vectorized(df)
    vec_time = time.time() - start_vec

    print("Loop time:", round(loop_time, 4), "seconds")
    print("Vectorized time:", round(vec_time, 4), "seconds")

    print(df[["FinalPrice_Loop", "FinalPrice_Vectorized"]].head())

    print("Vectorization task completed.")
