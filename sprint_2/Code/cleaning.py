import pandas as pd
import os

def remove_duplicates(df):
    return df.drop_duplicates()

def handle_missing_values(df):
    return df.fillna({"age": 0, "salary": 0})

def convert_types(df):
    if "age" in df.columns:
        df["age"] = pd.to_numeric(df["age"], errors="coerce").fillna(0).astype(int)

    if "salary" in df.columns:
        df["salary"] = pd.to_numeric(df["salary"], errors="coerce").fillna(0).astype(int)

    return df

def filter_valid_rows(df):
    return df[df["salary"] > 0]

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(__file__))
    file_path = os.path.join(BASE_DIR, "data", "sample.csv")

    df = pd.read_csv(file_path)
    print("RAW DATA:")
    print(df)

    df = remove_duplicates(df)
    df = handle_missing_values(df)
    df = convert_types(df)
    df = filter_valid_rows(df)

    print("\nCleaned Data:")
    print(df)
